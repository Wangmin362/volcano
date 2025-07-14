/*
 Copyright 2023 The Volcano Authors.

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

package api

import (
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"volcano.sh/volcano/pkg/scheduler/api/devices/nvidia/gpushare"
	"volcano.sh/volcano/pkg/scheduler/api/devices/nvidia/vgpu"
)

const (
	GPUSharingDevice = "GpuShare"
)

type Devices interface {
	// AddResource following two functions used in node_info
	//AddResource is to add the corresponding device resource of this 'pod' into current scheduler cache
	// 1. 用于更新当前Pod使用的设备到调度缓存中，记录每个GPU卡的使用情况
	// 2. 此函数被调用的时间点，是当一个Task被分配到一个Node的时候，此时就需要更新Node上设备使用情况，后续调度其它Pod的时候才能知道当前
	// Node最新的设备使用情况，方便决策。
	AddResource(pod *v1.Pod)
	//SubResource is to subtract the corresponding device resource of this 'pod' from current scheduler cache
	// 1. 当一个Task被干掉的时候，就需要调用这个函数释放Task分配的资源。后续调度才知道节点上资源的使用情况
	SubResource(pod *v1.Pod)

	//following four functions used in predicate
	//HasDeviceRequest checks if the 'pod' request this device
	// 判断当前Pod是否申请了当前设备，可以通过Pod spec.resource来确定
	HasDeviceRequest(pod *v1.Pod) bool
	// FilterNode checks if the 'pod' fit in current node
	// The first return value represents the filtering result, and the value range is "0, 1, 2, 3"
	// 0: Success
	// Success means that plugin ran correctly and found pod schedulable.

	// 1: Error
	// Error is used for internal plugin errors, unexpected input, etc.

	// 2: Unschedulable
	// Unschedulable is used when a plugin finds a pod unschedulable. The scheduler might attempt to
	// preempt other pods to get this pod scheduled. Use UnschedulableAndUnresolvable to make the
	// scheduler skip preemption.
	// The accompanying status message should explain why the pod is unschedulable.

	// 3: UnschedulableAndUnresolvable
	// UnschedulableAndUnresolvable is used when a plugin finds a pod unschedulable and
	// preemption would not change anything. Plugins should return Unschedulable if it is possible
	// that the pod can get scheduled with preemption.
	// The accompanying status message should explain why the pod is unschedulable.
	// 1. 判断当前节点是否能够满足Pod所有申请的资源，在预选阶段会被调用，核心算法就是判断Node上有用的设备资源是否满足Pod需要的
	FilterNode(pod *v1.Pod, policy string) (int, string, error)
	// ScoreNode will be invoked when using devicescore plugin, devices api can use it to implement multiple
	// scheduling policies.
	// 优选阶段会被调用到，用于计算一个节点的分数
	ScoreNode(pod *v1.Pod, policy string) float64

	// Allocate action in predicate
	// 分配设备给Pod
	Allocate(kubeClient kubernetes.Interface, pod *v1.Pod) error
	// Release action in predicate
	// 释放设备
	Release(kubeClient kubernetes.Interface, pod *v1.Pod) error

	// GetIgnoredDevices notify vc-scheduler to ignore devices in return list
	// TODO 这里的核心作用是什么？有点没看懂
	GetIgnoredDevices() []string

	// GetStatus used for debug and monitor
	GetStatus() string
}

// make sure GPUDevices implements Devices interface
var _ Devices = new(gpushare.GPUDevices)

var RegisteredDevices = []string{
	GPUSharingDevice, vgpu.DeviceName,
}

var IgnoredDevicesList = ignoredDevicesList{}

type ignoredDevicesList struct {
	sync.RWMutex
	ignoredDevices []string
}

func (l *ignoredDevicesList) Set(deviceLists ...[]string) {
	l.Lock()
	defer l.Unlock()
	l.ignoredDevices = l.ignoredDevices[:0]
	for _, devices := range deviceLists {
		l.ignoredDevices = append(l.ignoredDevices, devices...)
	}
}

func (l *ignoredDevicesList) Range(f func(i int, device string) bool) {
	l.RLock()
	defer l.RUnlock()
	for i, device := range l.ignoredDevices {
		if !f(i, device) {
			break
		}
	}
}
