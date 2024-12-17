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
package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	// init pprof server
	_ "net/http/pprof"

	"github.com/spf13/pflag"
	_ "go.uber.org/automaxprocs"

	"k8s.io/apimachinery/pkg/util/wait"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/cmd/scheduler/app"
	"volcano.sh/volcano/cmd/scheduler/app/options"

	// Import default actions/plugins.
	_ "volcano.sh/volcano/pkg/scheduler/actions"
	_ "volcano.sh/volcano/pkg/scheduler/plugins"

	// init assert
	_ "volcano.sh/volcano/pkg/scheduler/util/assert"
)

var logFlushFreq = pflag.Duration("log-flush-frequency", 5*time.Second, "Maximum number of seconds between log flushes")

func main() {
	// 一般来说，runtime.GOMAXPROCS默认就是CPU核心的数量，但是这个值可以通过环境变量来设置，因此可能会改变。这里手动设置为CPU的核心数
	runtime.GOMAXPROCS(runtime.NumCPU())

	klog.InitFlags(nil)

	// Scheduler参数初始化
	s := options.NewServerOption()
	s.AddFlags(pflag.CommandLine)
	s.RegisterOptions()

	// 解析参数
	cliflag.InitFlags()
	// 校验参数是否有效
	if err := s.CheckOptionOrDie(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if s.CaCertFile != "" && s.CertFile != "" && s.KeyFile != "" {
		if err := s.ParseCAFiles(nil); err != nil {
			klog.Fatalf("Failed to parse CA file: %v", err)
		}
	}

	// 刷新日志，这里应该是一个落盘操作，底层调用的应该是linux内核的flush接口
	go wait.Until(klog.Flush, *logFlushFreq, wait.NeverStop)
	defer klog.Flush()

	if err := app.Run(s); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
