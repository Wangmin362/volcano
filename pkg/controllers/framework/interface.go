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
	WorkerNum      uint32
	MaxRequeueNum  int

	// TODO 这玩意是干嘛的？
	InheritOwnerAnnotations bool
	WorkerThreadsForPG      uint32
}

// Controller is the interface of all controllers.
// TODO 如何理解这里的Controller抽象？ 似乎是用于抽象一种控制逻辑，这种控制逻辑需要初始化，然后才能运行
// 在volcano中，我觉得一个Controller其实就是对于一种资源的Controller，比如JobController就是对Job资源的Controller
type Controller interface {
	Name() string
	Initialize(opt *ControllerOption) error
	// Run run the controller
	Run(stopCh <-chan struct{})
}
