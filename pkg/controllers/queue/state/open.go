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

package state

import (
	"volcano.sh/apis/pkg/apis/bus/v1alpha1"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

type openState struct {
	queue *v1beta1.Queue
}

func (os *openState) Execute(action v1alpha1.Action) error {
	switch action {
	case v1alpha1.OpenQueueAction:
		return SyncQueue(os.queue, func(status *v1beta1.QueueStatus, podGroupList []string) {
			// 直接设置Queue的状态为Open
			status.State = v1beta1.QueueStateOpen
		})
	case v1alpha1.CloseQueueAction:
		return CloseQueue(os.queue, func(status *v1beta1.QueueStatus, podGroupList []string) {
			// 如果Queue中还存在PodGroup资源，那么设置为Closing状态，否则设置为Closed状态
			if len(podGroupList) == 0 {
				status.State = v1beta1.QueueStateClosed
				return
			}
			status.State = v1beta1.QueueStateClosing
		})
	default:
		return SyncQueue(os.queue, func(status *v1beta1.QueueStatus, podGroupList []string) {
			// 获取当前Queue中的状态
			specState := os.queue.Status.State
			// 如果Queue的状态没有设置，Queue的状态为Open,那么设置Queue的状态为Open
			if len(specState) == 0 || specState == v1beta1.QueueStateOpen {
				status.State = v1beta1.QueueStateOpen
				return
			}

			// 如果Queue的状态已经设置为Closed，那么判断这个Queue关联的PodGroup是否为空，若为空那么Queue的状态就是Closed状态，否则
			// 说明这个Queue中其实还有PodGroup，此时设置Queue的状态为Closeing
			if specState == v1beta1.QueueStateClosed {
				if len(podGroupList) == 0 {
					status.State = v1beta1.QueueStateClosed
					return
				}
				// TODO Q:如果一个Queue资源被关闭，那么Queue中剩余的PodGroup资源会如何处理？
				// A: 我猜测Queue中剩余的PodGroup资源后续肯定需要被删除，因为PodGroup不可能转移到其它的Queue资源当中，这样也就失去了
				// Queue划分集群资源的作用
				status.State = v1beta1.QueueStateClosing

				return
			}

			status.State = v1beta1.QueueStateUnknown
		})
	}
}
