/*
Copyright 2020 The Volcano Authors.

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

package framework

import (
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"

	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned"
)

// ControllerOption is the main context object for the controllers.
// 1、这里抽象出了所有Controller都可能需要的初始化参数，实际上一般来说Controller就是用于监听资源的变化，然后做出相应的处理。
// 所以一般只需要持有kubeClient, VolcanoClient, SharedInformerFactory这三个参数即可。其它参数都是一些辅助函数
// 2、TODO 思考一下，有没有更好的抽象方式？
type ControllerOption struct {
	// 用于获取K8S定义的资源
	KubeClient kubernetes.Interface
	// 用于获取Volcano定义的资源
	VolcanoClient         vcclientset.Interface
	SharedInformerFactory informers.SharedInformerFactory
	// volcano支持给调度器指定多个调度器名称，因该是为了处理不同的场景，可以给调度器赋予一定的语义，但是都指向同样的调度器
	SchedulerNames []string
	// 用于消费acjob的worker的数量
	WorkerNum uint32
	// 最大重新入队的次数
	MaxRequeueNum int

	// TODO 这玩意是干嘛的？
	// 猜测这玩意是用于控制当Controller监听到vcjob的时候，创建PG资源时是否需要同步注解到PG以及Pod上面
	InheritOwnerAnnotations bool
	// TODO 这玩意又是干嘛的？
	WorkerThreadsForPG uint32
}

// Controller is the interface of all controllers.
// TODO 如何理解这里的Controller抽象？ 似乎是用于抽象一种控制逻辑，这种控制逻辑需要初始化，然后才能运行
// 1. 在volcano中，每一个Controller其实就是对于一种资源的Controller，比如JobController就是对Job资源的Controller
// 2. 目前注册的Controller有GarbageCollectorController, JobController, JobFlowController, JobTemplateController,
// PodGroupController,QueueController
type Controller interface {
	// Name 获取当前Controller的名字，显然，每个Controller的名字必须不一样，否则会有冲突，注册的时候就会报错
	Name() string
	// Initialize 用于初始化Controller，一般来说Controller都需要kubeClient, VolcanoClient等参数，这些共性的参数会被封装到
	// ControllerOption当中，。
	Initialize(opt *ControllerOption) error
	// Run run the controller
	// 1. 真正运行Controller， 一般来说就是监听某种资源，然后对这种资源的变更做处理
	Run(stopCh <-chan struct{})
}
