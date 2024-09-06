/*
Copyright 2017 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package app

import (
	"context"
	"fmt"
	"os"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/informers"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/helpers"
	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned"
	"volcano.sh/volcano/cmd/controller-manager/app/options"
	"volcano.sh/volcano/pkg/controllers/framework"
	"volcano.sh/volcano/pkg/kube"
	"volcano.sh/volcano/pkg/signals"
)

const (
	leaseDuration = 15 * time.Second
	renewDeadline = 10 * time.Second
	retryPeriod   = 5 * time.Second
)

// Run the controller.
func Run(opt *options.ServerOption) error {
	// client-go客户端工具，用于和APIServer交互
	config, err := kube.BuildConfig(opt.KubeClientOptions)
	if err != nil {
		return err
	}

	if opt.EnableHealthz {
		if err := helpers.StartHealthz(opt.HealthzBindAddress, "volcano-controller", opt.CaCertData, opt.CertData, opt.KeyData); err != nil {
			return err
		}
	}

	// TODO 启动控制器 内部应该就是在处理Queue, Job资源，尤其是Job资源的处理
	// 1、启动GC Controller，用于删除哪些已经完成的Job，并且Job设置了TTLSecondsAfterFinished参数，并且已经过期的Job
	run := startControllers(config, opt)

	// 注册SIGINT, SIGTERM信号，当接收到这两个信号时，退出协程，释放资源
	ctx := signals.SetupSignalContext()

	if !opt.EnableLeaderElection {
		run(ctx)
		return fmt.Errorf("finished without leader elect")
	}

	// leader选举，volcano可以启动多个实例，但是多个实例只有一个在运行，其它实例处于等待状态
	leaderElectionClient, err := kubeclientset.NewForConfig(rest.AddUserAgent(config, "leader-election"))
	if err != nil {
		return err
	}

	// Prepare event clients.
	// TODO 事件相关
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: leaderElectionClient.CoreV1().Events(opt.LockObjectNamespace)})
	eventRecorder := broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "vc-controller-manager"})

	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("unable to get hostname: %v", err)
	}
	// add a uniquifier so that two processes on the same host don't accidentally both become active
	id := hostname + "_" + string(uuid.NewUUID())

	// TODO 这玩意干嘛的？  根据后面的使用情况可以看出来是跟leader选举相关的东西，暂时忽略
	rl, err := resourcelock.New(resourcelock.LeasesResourceLock,
		opt.LockObjectNamespace,
		"vc-controller-manager",
		leaderElectionClient.CoreV1(),
		leaderElectionClient.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: eventRecorder,
		})
	if err != nil {
		return fmt.Errorf("couldn't create resource lock: %v", err)
	}

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: leaseDuration,
		RenewDeadline: renewDeadline,
		RetryPeriod:   retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: run, // 只有leader才有资格执行run方法
			OnStoppedLeading: func() {
				klog.Fatalf("leaderelection lost")
			},
		},
	})
	return fmt.Errorf("lost lease")
}

func startControllers(config *rest.Config, opt *options.ServerOption) func(ctx context.Context) {
	controllerOpt := &framework.ControllerOption{}

	controllerOpt.SchedulerNames = opt.SchedulerNames
	// TODO 可以简单理解为Job资源的worker数量
	controllerOpt.WorkerNum = opt.WorkerThreads
	controllerOpt.MaxRequeueNum = opt.MaxRequeueNum

	// TODO: add user agent for different controllers
	controllerOpt.KubeClient = kubeclientset.NewForConfigOrDie(config)
	// TODO 这玩意是用来干嘛的？
	controllerOpt.VolcanoClient = vcclientset.NewForConfigOrDie(config)
	controllerOpt.SharedInformerFactory = informers.NewSharedInformerFactory(controllerOpt.KubeClient, 0)
	// 用于设置pod是否继承PodGroup资源的注解，默认是继承注解的
	controllerOpt.InheritOwnerAnnotations = opt.InheritOwnerAnnotations
	// 处理PodGroup资源的worker数量
	controllerOpt.WorkerThreadsForPG = opt.WorkerThreadsForPG

	return func(ctx context.Context) {
		// TODO 会启动哪些controller?
		framework.ForeachController(func(c framework.Controller) {
			// 初始化controller
			if err := c.Initialize(controllerOpt); err != nil {
				klog.Errorf("Failed to initialize controller <%s>: %v", c.Name(), err)
				return
			}

			// 运行controller
			go c.Run(ctx.Done())
		})

		<-ctx.Done()
	}
}
