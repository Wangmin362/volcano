/*
Copyright 2018 The Kubernetes Authors.

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
	"time"

	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/metrics"
)

// OpenSession start the session
func OpenSession(cache cache.Cache, tiers []conf.Tier, configurations []conf.Configuration) *Session {
	// 1. 实例化一个Session，初始化Session的各个字段
	// 2. 从缓存中拷贝Job, Node, Queue等资源副本，volcano将会使用这些资源的副本信息完成资源的调度
	// 3. 遍历当前从缓存中拷贝过来的Job副本，检验每个Job是否合法，剔除掉不满足调度要求的Job, 譬如对于晟腾NPU来说说，不同硬件
	// 型号的NPU的数量申请是有要求，就譬如注解，标签设置错误等等
	ssn := openSession(cache)
	ssn.Tiers = tiers
	ssn.Configurations = configurations
	// 两种表示NodeInfo数据结构之间的转换
	ssn.NodeMap = GenerateNodeMapAndSlice(ssn.Nodes)
	// 从Job中分理出Pod
	ssn.PodLister = NewPodLister(ssn)

	// TODO 为什么要设计为两层的结构？
	for _, tier := range tiers {
		for _, plugin := range tier.Plugins {
			// 加载插件
			if pb, found := GetPluginBuilder(plugin.Name); !found {
				klog.Errorf("Failed to get plugin %s.", plugin.Name)
			} else {
				plugin := pb(plugin.Arguments)
				ssn.plugins[plugin.Name()] = plugin
				onSessionOpenStart := time.Now()
				// OpenSession的时候，会执行用户配置的各个Plugin的OnOpenSession方法
				// TODO Plugin是如何影响整个调度流程的？其实Plugin并没有影响Session, Cache中元数据，各个插件的实现原理是向Session中
				// 注册回调函数，在执行调度的过程当中会执行这些回调函数，从而影响调度过程
				plugin.OnSessionOpen(ssn)
				metrics.UpdatePluginDuration(plugin.Name(), metrics.OnSessionOpen, metrics.Duration(onSessionOpenStart))
			}
		}
	}
	return ssn
}

// CloseSession close the session
func CloseSession(ssn *Session) {
	for _, plugin := range ssn.plugins {
		onSessionCloseStart := time.Now()
		plugin.OnSessionClose(ssn)
		metrics.UpdatePluginDuration(plugin.Name(), metrics.OnSessionClose, metrics.Duration(onSessionCloseStart))
	}

	closeSession(ssn)
}
