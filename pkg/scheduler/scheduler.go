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
	actions []framework.Action
	// TODO 如何自定义插件
	plugins []conf.Tier
	// Action的配置
	configurations []conf.Configuration
	// TODO 指标的配置文件
	metricsConf map[string]string
	// TODO 类似于linux的core dump，估计是用来debug使用的
	dumper schedcache.Dumper
}

// NewScheduler returns a Scheduler
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

	// TODO 缓存的原理
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
func (pc *Scheduler) Run(stopCh <-chan struct{}) {
	// 1、加载Scheduler默认配置文件，Scheduler默认配置文件启用了部分Action,以及Plugin类型的插件
	// 2、之所以提供默认配置文件，主要是因为用户很有可能不是特别熟悉Scheduler，所以想使用默认的
	// 3、如果用户配置了Scheduler配置文件，就加载配置文件然后，用户配置文件中可能存在Action, Plugin, ActionConfiguration, MetricConfiguration
	pc.loadSchedulerConf()
	// 监听Scheduler配置文件，如果发生变化了，Scheduler需要热加载这个配置文件
	go pc.watchSchedulerConf(stopCh)

	// Start cache for policy.
	pc.cache.SetMetricsConf(pc.metricsConf)
	// 启动Cache
	pc.cache.Run(stopCh)
	// 等待K8S资源以及Volcano定义的资源同步完成，当然这里同步的资源肯定不是所有的资源，而是Scheduler关心的资源
	pc.cache.WaitForCacheSync(stopCh)
	klog.V(2).Infof("Scheduler completes Initialization and start to run")
	// TODO 核心在这里
	go wait.Until(pc.runOnce, pc.schedulePeriod, stopCh)
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

	pc.mutex.Lock()
	actions := pc.actions
	plugins := pc.plugins
	configurations := pc.configurations
	pc.mutex.Unlock()

	// Load ConfigMap to check which action is enabled.
	conf.EnabledActionMap = make(map[string]bool)
	for _, action := range actions {
		conf.EnabledActionMap[action.Name()] = true
	}

	// 开启一个调度Session TODO 如何理解一个Session
	ssn := framework.OpenSession(pc.cache, plugins, configurations)
	defer func() {
		framework.CloseSession(ssn)
		metrics.UpdateE2eDuration(metrics.Duration(scheduleStartTime))
	}()

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
