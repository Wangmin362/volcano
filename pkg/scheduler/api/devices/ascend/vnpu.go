/*
Copyright 2024 The HAMi Authors.

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

package ascend

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/klog/v2"
)

const (
	hamiResourcePrefix    = "huawei.com"
	volcanoResourcePrefix = "volcano.sh"
)

var (
	hamidevs      []*hamiDevices
	ignoreDevices []string
	once          sync.Once
)

type Config struct {
	VNPUs []VNPUConfig `yaml:"vnpus"`
}

type Template struct {
	Name   string `yaml:"name"`
	Memory int64  `yaml:"memory"`
	AICore int32  `yaml:"aiCore,omitempty"`
	AICPU  int32  `yaml:"aiCPU,omitempty"`
}

type VNPUConfig struct {
	CommonWord         string     `yaml:"commonWord"`
	ChipName           string     `yaml:"chipName"`
	ResourceName       string     `yaml:"resourceName"`
	ResourceMemoryName string     `yaml:"resourceMemoryName"`
	MemoryAllocatable  int64      `yaml:"memoryAllocatable"`
	MemoryCapacity     int64      `yaml:"memoryCapacity"`
	AICore             int32      `yaml:"aiCore"`
	AICPU              int32      `yaml:"aiCPU"`
	Templates          []Template `yaml:"templates"`
}

type hamiDevices struct {
	config           VNPUConfig
	nodeRegisterAnno string
	useUUIDAnno      string
	noUseUUIDAnno    string
	handshakeAnno    string
	allocAnno        string
	inRequestDevices string
	supportDevices   string
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	klog.Infof("read %s config file: \n%s", path, string(data))
	var yamlData Config
	err = yaml.Unmarshal(data, &yamlData)
	if err != nil {
		return nil, err
	}
	return &yamlData, nil
}

func initDevices(config []VNPUConfig) []*hamiDevices {
	var devs []*hamiDevices
	for _, vnpu := range config {
		commonWord := vnpu.CommonWord
		dev := &hamiDevices{
			config:           vnpu,
			nodeRegisterAnno: fmt.Sprintf("volcano.sh/node-register-%s", commonWord),
			useUUIDAnno:      fmt.Sprintf("volcano.sh/use-%s-uuid", commonWord),
			noUseUUIDAnno:    fmt.Sprintf("volcano.sh/no-use-%s-uuid", commonWord),
			handshakeAnno:    fmt.Sprintf("volcano.sh/node-handshake-%s", commonWord),
			allocAnno:        fmt.Sprintf("volcano.sh/%s", commonWord),
			inRequestDevices: fmt.Sprintf("volcano.sh/%s-devices-to-allocate", commonWord),
			supportDevices:   fmt.Sprintf("volcano.sh/%s-devices-allocated", commonWord),
		}
		dev.config.ResourceName =
			strings.ReplaceAll(dev.config.ResourceName, hamiResourcePrefix, volcanoResourcePrefix)
		dev.config.ResourceMemoryName =
			strings.ReplaceAll(dev.config.ResourceMemoryName, hamiResourcePrefix, volcanoResourcePrefix)
		ignoreDevices = append(ignoreDevices, dev.config.ResourceMemoryName)
		sort.Slice(dev.config.Templates, func(i, j int) bool {
			return dev.config.Templates[i].Memory < dev.config.Templates[j].Memory
		})
		devs = append(devs, dev)
		klog.Infof("load ascend vnpu config %s: %v", commonWord, dev.config)
	}
	return devs
}

func initVNPU() {
	once.Do(func() {
		config, err := LoadConfig(AscendVGPUConfigPath)
		if err != nil {
			klog.Errorf("load vnpu config %s failed: %v", AscendVGPUConfigPath, err)
			return
		}
		hamidevs = initDevices(config.VNPUs)
	})
}

func (dev *hamiDevices) GetNodeDevices(n *corev1.Node) (*Devices, error) {
	anno, ok := n.Annotations[dev.nodeRegisterAnno]
	if !ok {
		return &Devices{}, fmt.Errorf("node %s not found %s anno", n.Name, dev.nodeRegisterAnno)
	}
	var nodeDevices []*Device
	if err := json.Unmarshal([]byte(anno), &nodeDevices); err != nil {
		klog.ErrorS(err, "failed to unmarshal node devices", "node", n.Name, "device annotation", anno)
		return &Devices{}, err
	}
	if len(nodeDevices) == 0 {
		klog.InfoS("no gpu device found", "node", n.Name, "device annotation", anno)
		return &Devices{}, errors.New("no device found on node")
	}

	devices := make(map[int]*Device, len(nodeDevices))
	for _, device := range nodeDevices {
		devices[int(device.Index)] = &Device{
			ID:           device.ID,
			Index:        device.Index,
			Count:        device.Count,
			Devmem:       device.Devmem,
			Devcore:      device.Devcore,
			Type:         device.Type,
			Numa:         device.Numa,
			Mode:         device.Mode,
			Health:       device.Health,
			PodMap:       map[string]*corev1.Pod{},
			DeviceVendor: device.DeviceVendor,
		}
	}

	return &Devices{Name: n.Name, Device: devices, hamiDevs: dev}, nil
}
