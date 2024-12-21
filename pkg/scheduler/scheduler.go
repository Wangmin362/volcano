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

package scheduler

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/filewatcher"
	schedcache "volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
)

// Scheduler represents a "Volcano Scheduler".
// Scheduler watches for new unscheduled pods(PodGroup) in Volcano.
// It attempts to find nodes that can accommodate these pods and writes the binding information back to the API server.
// 1、Volcano调度的基本单元是PodGroup资源
type Scheduler struct {
	// 缓存，缓存pod, node, queue相关资源
	cache schedcache.Cache
	// 调度器的配置文件路径
	schedulerConf string
	// 调度器配置文件路径监听器，以实现热加载
	fileWatcher filewatcher.FileWatcher
	// 调度周期时间间隔
	schedulePeriod time.Duration
	once           sync.Once

	mutex sync.Mutex
	// TODO 如何自定义Action?
	// 1. volcano的配置文件一般如下所示
	/*
	   actions: "enqueue, allocate, backfill"
	   tiers:
	   - plugins:
	     - name: priority
	       enableNodeOrder: false
	     - name: gang
	       enableNodeOrder: false
	     - name: conformance
	       enableNodeOrder: false
	     - name: volcano-npu_v6.0.RC1_linux-x86_64
	   - plugins:
	     - name: drf
	       enableNodeOrder: false
	     - name: predicates
	       enableNodeOrder: false
	       arguments:
	         predicate.GPUSharingEnable: false
	         predicate.GPUNumberEnable: false
	     - name: proportion
	       enableNodeOrder: false
	     - name: nodeorder
	     - name: binpack
	       enableNodeOrder: false
	   configurations:
	     - name: init-params
	       arguments: {"grace-over-time":"900","presetVirtualDevice":"true","nslb-version":"1.0","shared-tor-num":"2",
	   "useClusterInfoManager":"true","super-pod-size": "48", "reserve-nodes": "2"}
	*/
	actions []framework.Action
	plugins []conf.Tier
	// Action的配置
	configurations []conf.Configuration
	// TODO 指标的配置文件
	metricsConf map[string]string
	// TODO 类似于linux的core dump，估计是用来debug使用的
	dumper schedcache.Dumper
}

// NewScheduler returns a Scheduler
// 1. 如果给Scheduler配置的配置文件，那么需要监听这个配置文件，以实现热加载
// 2. 实例化缓存，这个缓存中缓存的内容比较丰富，缓存了Job, Queue, Nodes, PV, PVC, CSINode, PriorityClass等等所有可以影响调度相关的信息
// 3. 实例化一个调度器，这个调度在在初始化的过程中没有做特别的初始化
func NewScheduler(config *rest.Config, opt *options.ServerOption) (*Scheduler, error) {
	var watcher filewatcher.FileWatcher
	// 如果给Scheduler配置的配置文件，那么需要监听这个配置文件，以实现热加载
	if opt.SchedulerConf != "" {
		var err error
		path := filepath.Dir(opt.SchedulerConf)
		watcher, err = filewatcher.NewFileWatcher(path)
		if err != nil {
			return nil, fmt.Errorf("failed creating filewatcher for %s: %v", opt.SchedulerConf, err)
		}
	}

	// 这里的缓存其实就是缓存了Job, Queue, Nodes, PV, PVC, CSINode, PriorityClass等等所有可以影响调度相关的信息
	cache := schedcache.New(
		config,
		opt.SchedulerNames,
		opt.DefaultQueue,
		opt.NodeSelector, // 如果指定了标签，那么Volcano只会给打了特定标签的节点上调度资源
		opt.NodeWorkerThreads,
		opt.IgnoredCSIProvisioners,
	)
	scheduler := &Scheduler{
		schedulerConf:  opt.SchedulerConf,  // 调度器配置文件
		fileWatcher:    watcher,            // 配置文件监听器，以实现热加载
		cache:          cache,              // 缓存pod, node, queue相关信息
		schedulePeriod: opt.SchedulePeriod, // 调度周期
		dumper:         schedcache.Dumper{Cache: cache, RootDir: opt.CacheDumpFileDir},
	}

	return scheduler, nil
}

// Run initializes and starts the Scheduler. It loads the configuration,
// initializes the cache, and begins the scheduling process.
// 1. 加载Scheduler默认配置文件，Scheduler默认配置文件启用了部分Action,以及Plugin类型的插件
// 2. 监听Scheduler配置文件，如果发生变化了，Scheduler需要热加载这个配置文件
// 3. 启动Cache, 内部会启动各种Informer用于从APIServer同步Cache关心的各种信息
// 4. 等待Cache同步资源完成，其实就是等待Informer同步完成
// 5. 每秒执行一次runOnce函数一次，该函数中其实就是打开一个Session,完成一次调度的过程
func (pc *Scheduler) Run(stopCh <-chan struct{}) {
	// 1、加载Scheduler默认配置文件，Scheduler默认配置文件启用了部分Action,以及Plugin类型的插件
	// 2、之所以提供默认配置文件，主要是因为用户很有可能不是特别熟悉Scheduler，所以想使用默认的
	// 3、如果用户配置了Scheduler配置文件，就加载配置文件然后，用户配置文件中可能存在Action, Plugin, ActionConfiguration, MetricConfiguration
	pc.loadSchedulerConf()
	// 监听Scheduler配置文件，如果发生变化了，Scheduler需要热加载这个配置文件
	go pc.watchSchedulerConf(stopCh)

	// Start cache for policy.
	pc.cache.SetMetricsConf(pc.metricsConf)
	// 启动Cache,等待Informer中的数据同步完成 TODO 分析内部细节
	pc.cache.Run(stopCh)
	// 等待K8S资源以及Volcano定义的资源同步完成，当然这里同步的资源肯定不是所有的资源，而是Scheduler关心的资源
	// TODO 这里尤其是需要注意，若需要Informer同步某些资源，但是忘记给Pod添加RBAC权限，Informer同步资源将会被卡住，这里将会被卡死
	pc.cache.WaitForCacheSync(stopCh)
	klog.V(2).Infof("Scheduler completes Initialization and start to run")

	// 1. schedulePeriod默认为1秒，也就是volcano默认调度器每秒钟执行一次调度
	go wait.Until(pc.runOnce, pc.schedulePeriod, stopCh)

	// TODO 这里应该类似于kernel dump, 主要目的应该是为了排查问题
	if options.ServerOpts.EnableCacheDumper {
		pc.dumper.ListenForSignal(stopCh)
	}

	// TODO 这玩意干嘛的？
	go runSchedulerSocket()
}

// runOnce executes a single scheduling cycle. This function is called periodically
// as defined by the Scheduler's schedule period.
func (pc *Scheduler) runOnce() {
	// 通过这两个日志，就可以确定Scheduler一个调度周期花费的时间
	klog.V(4).Infof("Start scheduling ...")
	scheduleStartTime := time.Now()
	defer klog.V(4).Infof("End scheduling ...")

	// 使用当前时刻配置的Action以及Plugin, 因为在调度的过程中Volcano配置文件可能会发生更改
	pc.mutex.Lock()
	actions := pc.actions               // 用户配置的action
	plugins := pc.plugins               // 用于配置的plugin
	configurations := pc.configurations // 用户给action的配置文件
	pc.mutex.Unlock()

	// Load ConfigMap to check which action is enabled.
	conf.EnabledActionMap = make(map[string]bool)
	// TODO 如果用户一分钟前开启了某个Action, 后面删除了这个Action,此Map中中KV将会一直存在, 这里是否会有问题
	for _, action := range actions {
		conf.EnabledActionMap[action.Name()] = true
	}

	// 开启一个调度Session,volcano在一个会话中完成对于当前监听到的PodGroup资源的调度
	ssn := framework.OpenSession(pc.cache, plugins, configurations)
	defer func() {
		framework.CloseSession(ssn) // 完成调度
		metrics.UpdateE2eDuration(metrics.Duration(scheduleStartTime))
	}()

	// volcano目前的配置如下：
	/*
	   actions: "enqueue, allocate, backfill"
	   tiers:
	   - plugins:
	     - name: priority
	       enableNodeOrder: false
	     - name: gang
	       enableNodeOrder: false
	     - name: conformance
	       enableNodeOrder: false
	     - name: volcano-npu_v6.0.RC1_linux-x86_64
	   - plugins:
	     - name: drf
	       enableNodeOrder: false
	     - name: predicates
	       enableNodeOrder: false
	       arguments:
	         predicate.GPUSharingEnable: false
	         predicate.GPUNumberEnable: false
	     - name: proportion
	       enableNodeOrder: false
	     - name: nodeorder
	     - name: binpack
	       enableNodeOrder: false
	   configurations:
	     - name: init-params
	       arguments: {"grace-over-time":"900","presetVirtualDevice":"true","nslb-version":"1.0","shared-tor-num":"2",
	   "useClusterInfoManager":"true","super-pod-size": "48", "reserve-nodes": "2"}
	*/
	for _, action := range actions {
		actionStartTime := time.Now()
		action.Execute(ssn)
		metrics.UpdateActionDuration(action.Name(), metrics.Duration(actionStartTime))
	}
}

// 1、加载Scheduler默认配置文件，Scheduler默认配置文件启用了部分Action,以及Plugin类型的插件
// 2、之所以提供默认配置文件，主要是因为用户很有可能不是特别熟悉Scheduler，所以想使用默认的
// 3、如果用户配置了Scheduler配置文件，就加载配置文件然后，用户配置文件中可能存在Action, Plugin, ActionConfiguration, MetricConfiguration
func (pc *Scheduler) loadSchedulerConf() {
	klog.V(4).Infof("Start loadSchedulerConf ...")
	defer func() {
		actions, plugins := pc.getSchedulerConf()
		klog.V(2).Infof("Successfully loaded Scheduler conf, actions: %v, plugins: %v", actions, plugins)
	}()

	var err error
	pc.once.Do(func() {
		// 加载Scheduler默认配置配置文件，防止用户没有提供任何的配置文件，显然，如果用户配置了Scheduler，那么默认就使用用户配置的配置文件
		pc.actions, pc.plugins, pc.configurations, pc.metricsConf, err = UnmarshalSchedulerConf(DefaultSchedulerConf)
		if err != nil {
			klog.Errorf("unmarshal Scheduler config %s failed: %v", DefaultSchedulerConf, err)
			panic("invalid default configuration")
		}
	})

	var config string
	if len(pc.schedulerConf) != 0 {
		confData, err := os.ReadFile(pc.schedulerConf)
		if err != nil {
			klog.Errorf("Failed to read the Scheduler config in '%s', using previous configuration: %v",
				pc.schedulerConf, err)
			return
		}
		config = strings.TrimSpace(string(confData))
	}

	// 记载用户配置的配置文件
	actions, plugins, configurations, metricsConf, err := UnmarshalSchedulerConf(config)
	if err != nil {
		klog.Errorf("Scheduler config %s is invalid: %v", config, err)
		return
	}

	pc.mutex.Lock()
	pc.actions = actions
	pc.plugins = plugins
	pc.configurations = configurations
	pc.metricsConf = metricsConf
	pc.mutex.Unlock()
}

func (pc *Scheduler) getSchedulerConf() (actions []string, plugins []string) {
	for _, action := range pc.actions {
		actions = append(actions, action.Name())
	}
	for _, tier := range pc.plugins {
		for _, plugin := range tier.Plugins {
			plugins = append(plugins, plugin.Name)
		}
	}
	return
}

func (pc *Scheduler) watchSchedulerConf(stopCh <-chan struct{}) {
	if pc.fileWatcher == nil {
		return
	}
	eventCh := pc.fileWatcher.Events()
	errCh := pc.fileWatcher.Errors()
	for {
		select {
		case event, ok := <-eventCh:
			if !ok {
				return
			}
			klog.V(4).Infof("watch %s event: %v", pc.schedulerConf, event)
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				pc.loadSchedulerConf()
				pc.cache.SetMetricsConf(pc.metricsConf)
			}
		case err, ok := <-errCh:
			if !ok {
				return
			}
			klog.Infof("watch %s error: %v", pc.schedulerConf, err)
		case <-stopCh:
			return
		}
	}
}
