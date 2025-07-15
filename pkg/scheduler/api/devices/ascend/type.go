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

package ascend

var AscendVGPUEnable bool
var AscendVGPUConfigPath string = "/ascend-config.yaml"

const (
	AssignedTimeAnnotations = "volcano.sh/ascend-vgpu-time"
	AssignedNodeAnnotations = "volcano.sh/ascend-vgpu-node"
	BindTimeAnnotations     = "volcano.sh/ascend-bind-time"
	DeviceBindPhase         = "volcano.sh/ascend-bind-phase"

	// DeviceName used to indicate this device
	DeviceName = "hamivnpu"

	// binpack means the lower device memory remained after this allocation, the better
	binpackPolicy = "binpack"
	// spread means better put this task into an idle GPU card than a shared GPU card
	spreadPolicy = "spread"

	binpackMultiplier = 100
	spreadMultiplier  = 100
)

type ContainerDeviceRequest struct {
	Nums     int32
	Type     string
	Memreq   int32
	Coresreq int32
	Template string
}

type ContainerDevice struct {
	UUID      string `json:"UUID,omitempty"`
	Type      string `json:"type,omitempty"`
	Usedmem   int32  `json:"usedmem,omitempty"`
	Usedcores int32  `json:"usedcores,omitempty"`
	Template  string `json:"temp,omitempty"`
}

type ContainerDevices []ContainerDevice

type RuntimeInfo struct {
	UUID string `json:"UUID,omitempty"`
	Temp string `json:"temp,omitempty"`
}
