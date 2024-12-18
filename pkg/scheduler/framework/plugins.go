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
	"fmt"
	"path/filepath"
	"plugin"
	"strings"
	"sync"

	"k8s.io/klog/v2"
)

var pluginMutex sync.RWMutex

// PluginBuilder plugin management
type PluginBuilder = func(Arguments) Plugin

// Plugin management
var pluginBuilders = map[string]PluginBuilder{}

// RegisterPluginBuilder register the plugin
func RegisterPluginBuilder(name string, pc PluginBuilder) {
	pluginMutex.Lock()
	defer pluginMutex.Unlock()

	pluginBuilders[name] = pc
}

// CleanupPluginBuilders cleans up all the plugin
func CleanupPluginBuilders() {
	pluginMutex.Lock()
	defer pluginMutex.Unlock()

	pluginBuilders = map[string]PluginBuilder{}
}

// GetPluginBuilder get the pluginbuilder by name
func GetPluginBuilder(name string) (PluginBuilder, bool) {
	pluginMutex.RLock()
	defer pluginMutex.RUnlock()

	pb, found := pluginBuilders[name]
	return pb, found
}

// LoadCustomPlugins loads custom implement plugins
// 1. 遍历当前配置的插件目录中所有的so文件,
// 2. 判断当前路径的插件是否实现了New方法, 如果实现了就把这个插件强转为一个PluginBuilder类型
// 3. 通过插件的路径名截取出插件的名字,也就是说插件的名字其实就是so文件的文件名
// 4. 把插件注册到全局变量pluginBuilders当中,其实就是一个Map
func LoadCustomPlugins(pluginsDir string) error {
	// 找到指定插件目录中的所有so文件
	pluginPaths, _ := filepath.Glob(fmt.Sprintf("%s/*.so", pluginsDir))
	for _, pluginPath := range pluginPaths {
		// 判断当前路径的插件是否实现了New方法, 如果实现了就把这个插件强转为一个PluginBuilder类型
		pluginBuilder, err := loadPluginBuilder(pluginPath)
		if err != nil {
			return err
		}
		// 通过文件名截取当前插件的名字
		pluginName := getPluginName(pluginPath)
		// 插件的核心就是需要实现 func New(arguments framework.Arguments) framework.Plugin {}方法
		RegisterPluginBuilder(pluginName, pluginBuilder)
		klog.V(4).Infof("Custom plugin %s loaded", pluginName)
	}

	return nil
}

func getPluginName(pluginPath string) string {
	return strings.TrimSuffix(filepath.Base(pluginPath), filepath.Ext(pluginPath))
}

// 判断当前路径的插件是否实现了New方法, 如果实现了就把这个插件强转为一个PluginBuilder类型
func loadPluginBuilder(pluginPath string) (PluginBuilder, error) {
	plug, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, err
	}

	symBuilder, err := plug.Lookup("New")
	if err != nil {
		return nil, err
	}

	builder, ok := symBuilder.(PluginBuilder)
	if !ok {
		return nil, fmt.Errorf("unexpected plugin: %s, failed to convert PluginBuilder `New`", pluginPath)
	}

	return builder, nil
}

// Action management
var actionMap = map[string]Action{}

// RegisterAction register action
func RegisterAction(act Action) {
	pluginMutex.Lock()
	defer pluginMutex.Unlock()

	actionMap[act.Name()] = act
}

// GetAction get the action by name
func GetAction(name string) (Action, bool) {
	pluginMutex.RLock()
	defer pluginMutex.RUnlock()

	act, found := actionMap[name]
	return act, found
}
