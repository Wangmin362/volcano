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
package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/spf13/pflag"
	_ "go.uber.org/automaxprocs"

	"k8s.io/apimachinery/pkg/util/wait"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/klog/v2"

	_ "volcano.sh/volcano/pkg/controllers/garbagecollector"
	_ "volcano.sh/volcano/pkg/controllers/job"
	_ "volcano.sh/volcano/pkg/controllers/jobflow"
	_ "volcano.sh/volcano/pkg/controllers/jobtemplate"
	_ "volcano.sh/volcano/pkg/controllers/podgroup"
	_ "volcano.sh/volcano/pkg/controllers/queue"

	"volcano.sh/volcano/cmd/controller-manager/app"
	"volcano.sh/volcano/cmd/controller-manager/app/options"
	"volcano.sh/volcano/pkg/version"
)

// 日志刷新频率，默认是5秒钟刷新一次，估计是调用flush系统调用把缓冲区中的数据写入到磁盘进行持久化
var logFlushFreq = pflag.Duration("log-flush-frequency", 5*time.Second, "Maximum number of seconds between log flushes")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	klog.InitFlags(nil)

	// volcano可以配置的参数初始化
	s := options.NewServerOption()
	s.AddFlags(pflag.CommandLine)

	cliflag.InitFlags()

	if s.PrintVersion {
		version.PrintVersionAndExit()
	}
	// 检查参数
	if err := s.CheckOptionOrDie(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	// 校验证书
	if s.CaCertFile != "" && s.CertFile != "" && s.KeyFile != "" {
		if err := s.ParseCAFiles(nil); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse CA file: %v\n", err)
			os.Exit(1)
		}
	}

	// The default klog flush interval is 30 seconds, which is frighteningly long.
	// 持久化日志
	go wait.Until(klog.Flush, *logFlushFreq, wait.NeverStop)
	// 最后退出的时候，可能内存中还有日志没有刷新到磁盘，因此这里在退出的时候还需要在执行一次，防止日志信息的丢失
	defer klog.Flush()

	// Controller的核心流程在这里,后续会启动gcController, jobController, pgController等等完成对于这些资源的处理
	if err := app.Run(s); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
