/*
Copyright 2019 The Volcano Authors.

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

package podgroup

import (
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	appinformers "k8s.io/client-go/informers/apps/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned"
	informerfactory "volcano.sh/apis/pkg/client/informers/externalversions"
	vcinformer "volcano.sh/apis/pkg/client/informers/externalversions"
	schedulinginformer "volcano.sh/apis/pkg/client/informers/externalversions/scheduling/v1beta1"
	schedulinglister "volcano.sh/apis/pkg/client/listers/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/controllers/framework"
	"volcano.sh/volcano/pkg/features"
	commonutil "volcano.sh/volcano/pkg/util"
)

func init() {
	framework.RegisterController(&pgcontroller{})
}

// pgcontroller the Podgroup pgcontroller type.
// 1、监听Pod的Add事件，一旦发现有新增的Add事件，并且这个Pod调度器就是Volcano，那么判断这个Pod是否创建了对应的
// PodGroup资源，如果没有创建PodGroup资源，那就创建PodGroup资源，同时更新Pod的注解，增加scheduling.k8s.io/group-name=<name>注解
// 2、为什么每个Pod都需要创建对应的PodGroup呢？ 主要是因为Volcano中的调度单元就是PodGroup，所以需要为孤立的Pod创建PodGroup。
// 实际上一般都是使用Job提交任务，此时最终创建的Pod已经添加了scheduling.k8s.io/group-name=<group>的注解，此时PodGroupController
// 就不需要做额外的事情
type pgcontroller struct {
	kubeClient kubernetes.Interface
	vcClient   vcclientset.Interface

	podInformer coreinformers.PodInformer
	pgInformer  schedulinginformer.PodGroupInformer
	rsInformer  appinformers.ReplicaSetInformer

	informerFactory   informers.SharedInformerFactory
	vcInformerFactory vcinformer.SharedInformerFactory

	// A store of pods
	podLister corelisters.PodLister
	podSynced func() bool

	// A store of podgroups
	pgLister schedulinglister.PodGroupLister
	pgSynced func() bool

	// A store of replicaset
	rsSynced func() bool

	// 队列中存放的是Pod的变化
	queue workqueue.RateLimitingInterface

	schedulerNames []string
	workers        uint32

	// To determine whether inherit owner's annotations for pods when create podgroup
	inheritOwnerAnnotations bool
}

func (pg *pgcontroller) Name() string {
	return "pg-controller"
}

// Initialize create new Podgroup Controller.
func (pg *pgcontroller) Initialize(opt *framework.ControllerOption) error {
	pg.kubeClient = opt.KubeClient
	pg.vcClient = opt.VolcanoClient
	pg.workers = opt.WorkerThreadsForPG

	pg.queue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	pg.schedulerNames = make([]string, len(opt.SchedulerNames))
	copy(pg.schedulerNames, opt.SchedulerNames)
	pg.inheritOwnerAnnotations = opt.InheritOwnerAnnotations

	pg.informerFactory = opt.SharedInformerFactory
	pg.podInformer = opt.SharedInformerFactory.Core().V1().Pods()
	pg.podLister = pg.podInformer.Lister()
	pg.podSynced = pg.podInformer.Informer().HasSynced
	// 只有Pod的变化会更新到queue
	pg.podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: pg.addPod, // 只关心Pod的事件
	})

	factory := informerfactory.NewSharedInformerFactory(pg.vcClient, 0)
	pg.vcInformerFactory = factory
	pg.pgInformer = factory.Scheduling().V1beta1().PodGroups()
	pg.pgLister = pg.pgInformer.Lister()
	pg.pgSynced = pg.pgInformer.Informer().HasSynced

	// TODO 从这里似乎可以看出来PodGroup资源和ReplicaSet资源有某种关联
	if utilfeature.DefaultFeatureGate.Enabled(features.WorkLoadSupport) {
		pg.rsInformer = pg.informerFactory.Apps().V1().ReplicaSets()
		pg.rsSynced = pg.rsInformer.Informer().HasSynced
		pg.rsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    pg.addReplicaSet,
			UpdateFunc: pg.updateReplicaSet,
		})
	}
	return nil
}

// Run start NewPodgroupController.
func (pg *pgcontroller) Run(stopCh <-chan struct{}) {
	// 等待资源同步完成
	pg.informerFactory.Start(stopCh)
	pg.vcInformerFactory.Start(stopCh)

	// 等待K8S资源同步完成
	for informerType, ok := range pg.informerFactory.WaitForCacheSync(stopCh) {
		if !ok {
			klog.Errorf("caches failed to sync: %v", informerType)
		}
	}
	// 等待Volcano资源同步完成
	for informerType, ok := range pg.vcInformerFactory.WaitForCacheSync(stopCh) {
		if !ok {
			klog.Errorf("caches failed to sync: %v", informerType)
			return
		}
	}

	// 启动指定数量的协程，消费队列中的PogGroup资源
	// TODO 为什么这里的队列设计和Job中的队列设计不一样
	for i := 0; i < int(pg.workers); i++ {
		// 1. 从队列中取出变化的Pod, 如果队列中没有变化的Pod，那么这里将会阻塞
		// 2. 若当前Pod指定的调度器不是volcano，说明这个Pod的相关的活动不归自己管，直接跳过
		// 3. 从Pod的注解中获取scheduling.k8s.io/group-name注解，判断是否为空，如果非空，说明当前Pod对应的PodGroup已经创建，直接退出
		// 4. 如果上一步判断这个注解为空，那么创建PodGroup
		go wait.Until(pg.worker, 0, stopCh)
	}

	klog.Infof("PodgroupController is running ...... ")
}

func (pg *pgcontroller) worker() {
	for pg.processNextReq() {
	}
}

// 1. 从队列中取出变化的Pod, 如果队列中没有变化的Pod，那么这里将会阻塞
// 2. 若当前Pod指定的调度器不是volcano，说明这个Pod的相关的活动不归自己管，直接跳过
// 3. 从Pod的注解中获取scheduling.k8s.io/group-name注解，判断是否为空，如果非空，说明当前Pod对应的PodGroup已经创建，直接退出
// 4. 如果上一步判断这个注解为空，那么创建PodGroup
func (pg *pgcontroller) processNextReq() bool {
	obj, shutdown := pg.queue.Get()
	if shutdown {
		klog.Errorf("Fail to pop item from queue")
		return false
	}

	req := obj.(podRequest)
	defer pg.queue.Done(req) // 如果处理过程中没有报错，则认为当前Pod处理完成，直接从队列中移除

	// 从ShardInformer缓存当中查询Pod资源
	pod, err := pg.podLister.Pods(req.podNamespace).Get(req.podName)
	if err != nil {
		klog.Errorf("Failed to get pod by <%v> from cache: %v", req, err)
		return true
	}

	// 判断当前Pod指定的调度器是否是Volcano，如果不是直接跳过这个Pod
	if !commonutil.Contains(pg.schedulerNames, pod.Spec.SchedulerName) {
		klog.V(5).Infof("pod %v/%v field SchedulerName is not matched", pod.Namespace, pod.Name)
		return true
	}

	// 判断当前Pod是否已经再某个组下面，如果Pod已经划分到了某个PodGroup下面，直接跳过
	if pod.Annotations != nil && pod.Annotations[scheduling.KubeGroupNameAnnotationKey] != "" {
		klog.V(5).Infof("pod %v/%v has created podgroup", pod.Namespace, pod.Name)
		return true
	}

	// normal pod use volcano
	if err := pg.createNormalPodPGIfNotExist(pod); err != nil {
		klog.Errorf("Failed to handle Pod <%s/%s>: %v", pod.Namespace, pod.Name, err)
		// 当前Pod处理出错了，重新放入到队列当中
		pg.queue.AddRateLimited(req)
		return true
	}

	// If no error, forget it.
	pg.queue.Forget(req)

	return true
}
