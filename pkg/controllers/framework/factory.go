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
	"fmt"

	"k8s.io/klog/v2"
)

// 1. GarbageCollectorController注册
// 2. JobController注册
// 3. JobFlowController注册
// 4. JobTemplateController注册
// 5. PodGroupController注册
// 6. QueueController注册
// 7. 注册原理就是每个Controller在自己的init函数当中实现注册原理
var controllers = map[string]Controller{}

// ForeachController is helper function to operator all controllers.
func ForeachController(fn func(controller Controller)) {
	for _, ctrl := range controllers {
		fn(ctrl)
	}
}

// RegisterController register controller to the controller manager.
// 1. 注册Controller，这些Controller将会处理自己关心的资源
// 2. GarbageCollectorController注册
// 3. JobController注册
// 4. JobFlowController注册
// 5. JobTemplateController注册
// 6. PodGroupController注册
// 7. QueueController注册
func RegisterController(ctrl Controller) error {
	if ctrl == nil {
		return fmt.Errorf("controller is nil")
	}

	// 不能重复注册相同名称的Controller
	if _, found := controllers[ctrl.Name()]; found {
		return fmt.Errorf("duplicated controller")
	}

	klog.V(3).Infof("Controller <%s> is registered.", ctrl.Name())
	controllers[ctrl.Name()] = ctrl
	return nil
}
