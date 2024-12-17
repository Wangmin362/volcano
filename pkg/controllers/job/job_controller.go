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

package job

import (
	"fmt"
	"hash"
	"hash/fnv"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	kubeschedulinginformers "k8s.io/client-go/informers/scheduling/v1"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	kubeschedulinglisters "k8s.io/client-go/listers/scheduling/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	busv1alpha1 "volcano.sh/apis/pkg/apis/bus/v1alpha1"
	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned"
	vcscheme "volcano.sh/apis/pkg/client/clientset/versioned/scheme"
	informerfactory "volcano.sh/apis/pkg/client/informers/externalversions"
	vcinformer "volcano.sh/apis/pkg/client/informers/externalversions"
	batchinformer "volcano.sh/apis/pkg/client/informers/externalversions/batch/v1alpha1"
	businformer "volcano.sh/apis/pkg/client/informers/externalversions/bus/v1alpha1"
	schedulinginformers "volcano.sh/apis/pkg/client/informers/externalversions/scheduling/v1beta1"
	batchlister "volcano.sh/apis/pkg/client/listers/batch/v1alpha1"
	buslister "volcano.sh/apis/pkg/client/listers/bus/v1alpha1"
	schedulinglisters "volcano.sh/apis/pkg/client/listers/scheduling/v1beta1"

	"volcano.sh/volcano/pkg/controllers/apis"
	jobcache "volcano.sh/volcano/pkg/controllers/cache"
	"volcano.sh/volcano/pkg/controllers/framework"
	"volcano.sh/volcano/pkg/controllers/job/state"
	"volcano.sh/volcano/pkg/features"
)

func init() {
	// 注册JobController
	framework.RegisterController(&jobcontroller{})
}

// jobcontroller the Job jobcontroller type.
type jobcontroller struct {
	kubeClient kubernetes.Interface  // 用于查询K8S定义的标准资源
	vcClient   vcclientset.Interface // 用于查询volcano定义的CRD资源

	jobInformer   batchinformer.JobInformer
	podInformer   coreinformers.PodInformer
	pvcInformer   coreinformers.PersistentVolumeClaimInformer
	pgInformer    schedulinginformers.PodGroupInformer
	svcInformer   coreinformers.ServiceInformer
	cmdInformer   businformer.CommandInformer
	pcInformer    kubeschedulinginformers.PriorityClassInformer
	queueInformer schedulinginformers.QueueInformer

	// 缓存k8s定义的资源
	informerFactory informers.SharedInformerFactory
	// 缓存volcano定义的资源
	vcInformerFactory vcinformer.SharedInformerFactory

	// A store of jobs
	jobLister batchlister.JobLister
	jobSynced func() bool

	// A store of pods
	podLister corelisters.PodLister
	podSynced func() bool

	pvcLister corelisters.PersistentVolumeClaimLister
	pvcSynced func() bool

	// A store of podgroups
	pgLister schedulinglisters.PodGroupLister
	pgSynced func() bool

	// A store of service
	svcLister corelisters.ServiceLister
	svcSynced func() bool

	cmdLister buslister.CommandLister
	cmdSynced func() bool

	pcLister kubeschedulinglisters.PriorityClassLister
	pcSynced func() bool

	queueLister schedulinglisters.QueueLister
	queueSynced func() bool

	// queue that need to sync up
	// 限速队列 + 延时队列
	// TODO 队列的数量是由谁决定的?
	queueList []workqueue.RateLimitingInterface
	// TODO 命令队列是用来干嘛的？ 根据之前的影响,似乎用户可以通过创建Command这种资源影响Volcano的行为
	commandQueue workqueue.RateLimitingInterface
	// job缓存本质上就是缓存了VolcanoJob相关的信息,并且缓存了Job相关的Pod信息
	cache jobcache.Cache
	// Job Event recorder
	recorder record.EventRecorder

	errTasks      workqueue.RateLimitingInterface
	workers       uint32 // 消费acjob协程的数量
	maxRequeueNum int
}

func (cc *jobcontroller) Name() string {
	return "job-controller"
}

// Initialize creates the new Job job controller.
func (cc *jobcontroller) Initialize(opt *framework.ControllerOption) error {
	cc.kubeClient = opt.KubeClient
	cc.vcClient = opt.VolcanoClient

	sharedInformers := opt.SharedInformerFactory
	workers := opt.WorkerNum // 用于消费acjob的worker的数量
	// Initialize event client
	// TODO 事件广播器这一块还需要重点看看原理
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: cc.kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(vcscheme.Scheme, v1.EventSource{Component: "vc-controller-manager"})

	cc.informerFactory = sharedInformers
	// TODO 每个worker一个队列, 当Job到来时到底应该分配给那个队列?
	cc.queueList = make([]workqueue.RateLimitingInterface, workers)
	cc.commandQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	cc.cache = jobcache.New()
	cc.errTasks = newRateLimitingQueue()
	cc.recorder = recorder
	cc.workers = workers
	cc.maxRequeueNum = opt.MaxRequeueNum
	if cc.maxRequeueNum < 0 {
		cc.maxRequeueNum = -1
	}

	for i := uint32(0); i < workers; i++ {
		cc.queueList[i] = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	}

	// 缓存volcano相关的资源信息
	factory := informerfactory.NewSharedInformerFactory(cc.vcClient, 0)
	cc.vcInformerFactory = factory
	// 1. 用于控制Volcano是否可以控制Deployment/Replica/StatefulSet资源
	// 2. Q: 这里难道可以不开启么? 如果不开启的话JobController是怎么感知Job的变化的? A: 从初始化的逻辑来看,目前只看到只有一个地方和这里
	// 是相关联的，因此目前的理解，这个特性是必须要开启的，这样才能监听Job的变化.  另外，ctrl+G搜索了一下，确实默认就是开启的
	if utilfeature.DefaultFeatureGate.Enabled(features.WorkLoadSupport) {
		cc.jobInformer = factory.Batch().V1alpha1().Jobs()
		cc.jobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			// 1. 把obj对象强制转换为VolcanoJob资源对象，如果转换失败直接退出
			// 2. 把job资源对象直接放入到JobCache当中缓存起来
			// 3. 计算当前job的jobKey，规则为: namespace/name
			// 4. 根据jobKey计算出哈希模上协程数量算出当前Job应该放入到那个队列当中，简单来说job应该放入到那个队列是通过jobKey的哈希以及协程的数量计算出来的
			AddFunc: cc.addJob,
			// 1. 把obj对象强制转换为Job资源对象，如果转换失败直接退出
			// 2. 判断新老job资源对象的版本号是否一致，如果一致，说明Job资源没有发生更改，直接退出
			// 3. 使用新的Job资源对象更新JobCache缓存
			// 4. 若新老Job资源对象的Spec完全相等，并且新老Job资源对象的Status.State.Phase相等，就认为当前Job不需要更新，忽略本次更新，直接退出
			// 5. 获取当前Job资源对象的Key, 规则为：namespace/name
			// 6. 根据资源对象的JobKey的哈希，以及消费Job资源协程的数量计算出来当前Job应该放入到那个队列当中
			UpdateFunc: cc.updateJob,
			// 1. 把obj对象强制转换为Job资源对象，如果转换失败直接退出
			// 2. 直接从JobCache缓存中删除当前Job资源
			DeleteFunc: cc.deleteJob,
		})
		cc.jobLister = cc.jobInformer.Lister()
		cc.jobSynced = cc.jobInformer.Informer().HasSynced
	}

	// TODO QueueCommand到底是干嘛的?
	if utilfeature.DefaultFeatureGate.Enabled(features.QueueCommandSync) {
		cc.cmdInformer = factory.Bus().V1alpha1().Commands()
		cc.cmdInformer.Informer().AddEventHandler(
			cache.FilteringResourceEventHandler{
				FilterFunc: func(obj interface{}) bool {
					switch v := obj.(type) {
					case *busv1alpha1.Command:
						if v.TargetObject != nil &&
							v.TargetObject.APIVersion == batchv1alpha1.SchemeGroupVersion.String() &&
							v.TargetObject.Kind == "Job" {
							return true
						}

						return false
					default:
						return false
					}
				},
				Handler: cache.ResourceEventHandlerFuncs{
					AddFunc: cc.addCommand,
				},
			},
		)
		cc.cmdLister = cc.cmdInformer.Lister()
		cc.cmdSynced = cc.cmdInformer.Informer().HasSynced
	}

	cc.podInformer = sharedInformers.Core().V1().Pods()
	cc.podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    cc.addPod, // Pod资源的变化也需要更新到Job资源
		UpdateFunc: cc.updatePod,
		DeleteFunc: cc.deletePod,
	})

	cc.podLister = cc.podInformer.Lister()
	cc.podSynced = cc.podInformer.Informer().HasSynced

	cc.pvcInformer = sharedInformers.Core().V1().PersistentVolumeClaims()
	cc.pvcLister = cc.pvcInformer.Lister()
	cc.pvcSynced = cc.pvcInformer.Informer().HasSynced

	cc.svcInformer = sharedInformers.Core().V1().Services()
	cc.svcLister = cc.svcInformer.Lister()
	cc.svcSynced = cc.svcInformer.Informer().HasSynced

	cc.pgInformer = factory.Scheduling().V1beta1().PodGroups()
	cc.pgInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: cc.updatePodGroup, // PodGroup资源的变化可有可能需要更新到Job资源的状态
	})
	cc.pgLister = cc.pgInformer.Lister()
	cc.pgSynced = cc.pgInformer.Informer().HasSynced

	if utilfeature.DefaultFeatureGate.Enabled(features.PriorityClass) {
		cc.pcInformer = sharedInformers.Scheduling().V1().PriorityClasses()
		cc.pcLister = cc.pcInformer.Lister()
		cc.pcSynced = cc.pcInformer.Informer().HasSynced
	}

	cc.queueInformer = factory.Scheduling().V1beta1().Queues()
	cc.queueLister = cc.queueInformer.Lister()
	cc.queueSynced = cc.queueInformer.Informer().HasSynced

	// Register actions
	// 同步Job的状态
	state.SyncJob = cc.syncJob
	// 杀死Job
	state.KillJob = cc.killJob

	return nil
}

// Run start JobController.
func (cc *jobcontroller) Run(stopCh <-chan struct{}) {
	// 启动K8S自己的SharedInformer
	cc.informerFactory.Start(stopCh)
	// 启动Volcano的SharedInformer
	cc.vcInformerFactory.Start(stopCh)

	// 等待K8S的资源同步完成
	for informerType, ok := range cc.informerFactory.WaitForCacheSync(stopCh) {
		if !ok {
			klog.Errorf("caches failed to sync: %v", informerType)
			return
		}
	}

	// 等待volcano资源同步完成
	for informerType, ok := range cc.vcInformerFactory.WaitForCacheSync(stopCh) {
		if !ok {
			klog.Errorf("caches failed to sync: %v", informerType)
			return
		}
	}

	// 处理命令, 如果QueueCommandSync特性没有开启,会影响这里么
	go wait.Until(cc.handleCommands, 0, stopCh)

	for i := uint32(0); i < cc.workers; i++ {
		go func(num uint32) {
			wait.Until( // 每秒钟执行一次，直到停止信号
				func() {
					// 处理Job资源的变化
					cc.worker(num)
				},
				time.Second,
				stopCh)
		}(i)
	}

	// 开始运行,job缓存接下来将会收集job, pod相关信息
	// TODO 干了啥？
	go cc.cache.Run(stopCh)

	// Re-sync error tasks.
	go wait.Until(cc.processResyncTask, 0, stopCh)

	klog.Infof("JobController is running ...... ")
}

func (cc *jobcontroller) worker(i uint32) {
	klog.Infof("worker %d start ...... ", i)

	for cc.processNextReq(i) {
	}
}

func (cc *jobcontroller) belongsToThisRoutine(key string, count uint32) bool {
	var hashVal hash.Hash32
	var val uint32

	hashVal = fnv.New32()
	hashVal.Write([]byte(key))

	val = hashVal.Sum32()

	return val%cc.workers == count
}

// 根据资源对象的JobKey的哈希，以及消费Job资源协程的数量计算出来当前Job应该放入到那个队列当中
func (cc *jobcontroller) getWorkerQueue(key string) workqueue.RateLimitingInterface {
	var hashVal hash.Hash32
	var val uint32

	// 计算哈希值，每个job通过哈希运算，进入固定的queue当中
	// TODO 显然，这里的哈希函数因该尽可能的Job均摊到不同的队列当中
	hashVal = fnv.New32()
	hashVal.Write([]byte(key))

	val = hashVal.Sum32()

	queue := cc.queueList[val%cc.workers]

	return queue
}

// 1. count可以理解为自己的编号，从0开始，每个协程的编号都不一样，每个协程都有自己的队列，每个协程只负责消费自己队列中的VolcanoJob即可
// 2. 从自己的队列当中获取Job Req资源对象，这个资源对象基本没有带什么信息，还需要从JobCache中获取全量信息的Job资源对象
// 3. 根据JobKey从JobCache中获取全量信息的Job资源
// 4. 根据获取到的Job资源对象获取去状态
func (cc *jobcontroller) processNextReq(count uint32) bool {
	// 每个协程都有自己单独的队列
	queue := cc.queueList[count]
	// 1. 这里是一个阻塞式的API, 如果队列中没有Job,将会一直被阻塞在这里,直到有真正的Job到来
	// 2. Q: 这个队列中的数据是什么时候放入的? A: 猜测应该是VCJob的Informer监听到有Job的变更事件后,会放入到队列当中
	obj, shutdown := queue.Get()
	if shutdown {
		klog.Errorf("Fail to pop item from queue")
		return false
	}

	req := obj.(apis.Request)
	// TODO 有空好好研究下Client的延迟队列
	defer queue.Done(req)

	// 这里的key一般就是namespace/name
	key := jobcache.JobKeyByReq(&req)
	// 1. 判断当前worker是否应该处理这个Job，其实之前的哈希函数只要确保了计算正确，这里一般都不会改变
	// 2. 从这里可以看出jobInformer收到Job变更事件之后是如何放入到队列当中的,虽然有多个worker,但是这里通过jobKey计算出hash,然后模上
	// worker的数量,就可以确定当前job应该放入到那个队列. 这样多个协程就不会并发的处理相同的Job
	if !cc.belongsToThisRoutine(key, count) {
		klog.Errorf("should not occur The job does not belongs to this routine key:%s, worker:%d...... ", key, count)
		queueLocal := cc.getWorkerQueue(key)
		queueLocal.Add(req)
		return true
	}

	klog.V(3).Infof("Try to handle request <%v>", req)

	// 1. 从JobCache获取当前job的jobInfo
	// 2. 由于JobController收到Job的删除事件之后直接从JobCache中删除了这个Job资源对象，没有在队列中放入删除事件。所以正常情况下，
	// 这里都可以根据JobKey找到对应资源对象
	jobInfo, err := cc.cache.Get(key)
	if err != nil {
		// TODO(k82cn): ignore not-ready error.
		klog.Errorf("Failed to get job by <%v> from cache: %v", req, err)
		return true
	}

	// 获取Job的状态
	st := state.NewState(jobInfo)
	if st == nil {
		klog.Errorf("Invalid state <%s> of Job <%v/%v>",
			jobInfo.Job.Status.State, jobInfo.Job.Namespace, jobInfo.Job.Name)
		return true
	}

	// TODO 1. 获取Job的Action 目前从代码来看，Action似乎和Volcano的Command资源有关
	// 2. 从代码来看，Action可以改变Job的状态
	// 3. 一般情况下，这里获取到的Action为SyncJobAction
	action := applyPolicies(jobInfo.Job, &req)
	klog.V(3).Infof("Execute <%v> on Job <%s/%s> in <%s> by <%T>.",
		action, req.Namespace, req.JobName, jobInfo.Job.Status.State.Phase, st)

	if action != busv1alpha1.SyncJobAction {
		cc.recordJobEvent(jobInfo.Job.Namespace, jobInfo.Job.Name, batchv1alpha1.ExecuteAction, fmt.Sprintf(
			"Start to execute action %s ", action))
	}

	// TODO 执行Action
	if err := st.Execute(action); err != nil {
		if cc.maxRequeueNum == -1 || queue.NumRequeues(req) < cc.maxRequeueNum {
			klog.V(2).Infof("Failed to handle Job <%s/%s>: %v",
				jobInfo.Job.Namespace, jobInfo.Job.Name, err)
			// If any error, requeue it.
			queue.AddRateLimited(req)
			return true
		}
		cc.recordJobEvent(jobInfo.Job.Namespace, jobInfo.Job.Name, batchv1alpha1.ExecuteAction, fmt.Sprintf(
			"Job failed on action %s for retry limit reached", action))
		klog.Warningf("Terminating Job <%s/%s> and releasing resources", jobInfo.Job.Namespace, jobInfo.Job.Name)
		if err = st.Execute(busv1alpha1.TerminateJobAction); err != nil {
			klog.Errorf("Failed to terminate Job<%s/%s>: %v", jobInfo.Job.Namespace, jobInfo.Job.Name, err)
		}
		klog.Warningf("Dropping job<%s/%s> out of the queue: %v because max retries has reached", jobInfo.Job.Namespace, jobInfo.Job.Name, err)
	}

	// If no error, forget it.
	// 如果没有错误，说明当前Job已经处理完成
	queue.Forget(req)

	return true
}
