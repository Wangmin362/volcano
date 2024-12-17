/*
Copyright 2017 The Kubernetes Authors.

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
	"net/http"
	"os"
	"time"

	"volcano.sh/apis/pkg/apis/helpers"

	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/kube"
	"volcano.sh/volcano/pkg/scheduler"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/signals"
	commonutil "volcano.sh/volcano/pkg/util"
	"volcano.sh/volcano/pkg/version"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"

	// Register gcp auth
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"

	// Register rest client metrics
	_ "k8s.io/component-base/metrics/prometheus/restclient"
)

const (
	leaseDuration = 15 * time.Second
	renewDeadline = 10 * time.Second
	retryPeriod   = 5 * time.Second
)

// Run the volcano scheduler.
func Run(opt *options.ServerOption) error {
	if opt.PrintVersion {
		version.PrintVersionAndExit()
	}

	// 根据kubeconfig配置文件，构建APIServer客户端需要的rest.Config配置
	config, err := kube.BuildConfig(opt.KubeClientOptions)
	if err != nil {
		return err
	}

	// TODO 如何配置了插件目录，那么Volcano Scheduler在启动的时候需要加载用户配置的插件
	if opt.PluginsDir != "" {
		// TODO 用户自定义插件加载原理
		// TODO 自定义插件原理
		// TODO 这个特性是什么时候加入的？
		err := framework.LoadCustomPlugins(opt.PluginsDir)
		if err != nil {
			klog.Errorf("Fail to load custom plugins: %v", err)
			return err
		}
	}

	// 1. 如果给Scheduler配置的配置文件，那么需要监听这个配置文件，以实现热加载
	// 2. 实例化缓存，这个缓存中缓存的内容比较丰富，缓存了Job, Queue, Nodes, PV, PVC, CSINode, PriorityClass等等所有可以影响调度相关的信息
	// 3. 实例化一个调度器，这个调度在在初始化的过程中没有做特别的初始化
	sched, err := scheduler.NewScheduler(config, opt)
	if err != nil {
		panic(err)
	}

	if opt.EnableMetrics {
		go func() { // 给普罗米修斯添加指标
			http.Handle("/metrics", promHandler())
			klog.Fatalf("Prometheus Http Server failed %s", http.ListenAndServe(opt.ListenAddress, nil))
		}()
	}

	if opt.EnableHealthz {
		if err := helpers.StartHealthz(opt.HealthzBindAddress, "volcano-scheduler", opt.CaCertData, opt.CertData, opt.KeyData); err != nil {
			return err
		}
	}

	// 优雅退出，注册SIGINT, SIGTERM信号
	ctx := signals.SetupSignalContext()
	run := func(ctx context.Context) {
		// Scheduler开始运行
		sched.Run(ctx.Done())
		<-ctx.Done()
	}

	if !opt.EnableLeaderElection {
		run(ctx)
		return fmt.Errorf("finished without leader elect")
	}

	leaderElectionClient, err := clientset.NewForConfig(restclient.AddUserAgent(config, "leader-election"))
	if err != nil {
		return err
	}

	// Prepare event clients.
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: leaderElectionClient.CoreV1().Events(opt.LockObjectNamespace)})
	eventRecorder := broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: commonutil.GenerateComponentName(opt.SchedulerNames)})

	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("unable to get hostname: %v", err)
	}
	// add a uniquifier so that two processes on the same host don't accidentally both become active
	id := hostname + "_" + string(uuid.NewUUID())

	rl, err := resourcelock.New(resourcelock.LeasesResourceLock,
		opt.LockObjectNamespace,
		commonutil.GenerateComponentName(opt.SchedulerNames),
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
			OnStartedLeading: run,
			OnStoppedLeading: func() {
				klog.Fatalf("leaderelection lost")
			},
		},
	})
	return fmt.Errorf("lost lease")
}

func promHandler() http.Handler {
	// Unregister go and process related collector because it's duplicated and `legacyregistry.DefaultGatherer` also has registered them.
	prometheus.DefaultRegisterer.Unregister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	prometheus.DefaultRegisterer.Unregister(collectors.NewGoCollector())
	return promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer, promhttp.HandlerFor(prometheus.Gatherers{prometheus.DefaultGatherer, legacyregistry.DefaultGatherer}, promhttp.HandlerOpts{}))
}
