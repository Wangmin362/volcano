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

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"volcano.sh/volcano/pkg/scheduler/api/devices"
)

// Device include gpu id, memory and the pods that are sharing it.
type Device struct {
	ID    string `json:"id,omitempty"`
	Index uint   `json:"index,omitempty"`
	// max sharing number
	Count int32 `json:"count,omitempty"`
	// number of allocated
	UsedNum int32
	// memory per card
	Devmem int32 `json:"devmem,omitempty"`
	// number of device memory allocated
	UsedMem int32
	// core per card
	Devcore int32 `json:"devcore,omitempty"`
	// number of core used
	UsedCore int32
	// type of this device
	Type string `json:"type,omitempty"`
	Numa int    `json:"numa,omitempty"`
	Mode string `json:"mode,omitempty"`
	// Health condition of this NPU
	Health       bool   `json:"health,omitempty"`
	DeviceVendor string `json:"devicevendor,omitempty"`
	// The pods that are sharing this GPU
	PodMap map[string]*v1.Pod
}

type Devices struct {
	Name string

	// We cache score in filter step according to schedulePolicy, to avoid recalculating in score
	Score float64

	Device   map[int]*Device
	hamiDevs *hamiDevices
}

// NewDevice creates a device
func NewDevice(id string, mem int32) *Device {
	return &Device{
		ID:       id,
		Devmem:   mem,
		PodMap:   map[string]*v1.Pod{},
		UsedNum:  0,
		UsedMem:  0,
		UsedCore: 0,
	}
}

func NewDevices(name string, node *v1.Node) *Devices {
	initVNPU()
	if node == nil {
		return nil
	}
	if len(hamidevs) == 0 {
		klog.Errorf("hami-vnpu device not found")
		return nil
	}

	var nodedevices *Devices
	for _, dev := range hamidevs {
		var err error
		nodedevices, err = dev.GetNodeDevices(node)
		if err != nil {
			klog.V(5).Infof("hami-vnpu get node %s device failed: %v", node.Name, err)
			continue
		}
		if nodedevices == nil || len(nodedevices.Device) == 0 {
			klog.V(5).Infof("hami-vnpu %s device not found on node %s", dev.config.CommonWord, node.Name)
			continue
		}
		for _, val := range nodedevices.Device {
			klog.V(4).Infoln("name=", nodedevices.Name, "val=", *val)
		}

		handshake := node.Annotations[dev.handshakeAnno]
		// We have to handshake here in order to avoid time-inconsistency between scheduler and nodes
		if strings.Contains(handshake, "Requesting") {
			formertime, _ := time.Parse("2006.01.02 15:04:05", strings.Split(handshake, "_")[1])
			if time.Now().After(formertime.Add(time.Second * 60)) {
				klog.Infof("node %v device %s leave", node.Name, handshake)

				tmppat := make(map[string]string)
				tmppat[dev.handshakeAnno] = "Deleted_" + time.Now().Format("2006.01.02 15:04:05")
				patchNodeAnnotations(node, tmppat)
				return nil
			}
		} else if strings.Contains(handshake, "Deleted") {
			return nil
		} else {
			tmppat := make(map[string]string)
			tmppat[dev.handshakeAnno] = "Requesting_" + time.Now().Format("2006.01.02 15:04:05")
			patchNodeAnnotations(node, tmppat)
		}
		break
	}
	return nodedevices
}

func (gs *Devices) ScoreNode(pod *v1.Pod, schedulePolicy string) float64 {
	/* TODO: we need a base score to be campatable with preemption, it means a node without evicting a task has
	a higher score than those needs to evict a task */

	// Use cached stored in filter state in order to avoid recalculating.
	return gs.Score
}

func (gs *Devices) GetIgnoredDevices() []string {
	return ignoreDevices
}

// AddResource adds the pod to GPU pool if it is assigned
func (gs *Devices) AddResource(pod *v1.Pod) map[string]float64 {
	if gs == nil || gs.hamiDevs == nil {
		return nil
	}
	devs, ok := pod.Annotations[gs.hamiDevs.supportDevices]
	if !ok {
		return nil
	}
	podDev := decodePodDevices(devs)
	var cnt, rawCnt int32
	for _, val := range podDev {
		for _, deviceused := range val {
			for index, gsdevice := range gs.Device {
				if gsdevice.ID == deviceused.UUID {
					gs.Device[index].UsedMem += deviceused.Usedmem
					gs.Device[index].UsedCore += deviceused.Usedcores
					cnt += deviceused.Usedcores / gs.hamiDevs.config.Templates[0].AICore // Calculate the number of consumed vGPUs.
					gs.Device[index].UsedNum += cnt
					rawCnt++
					klog.V(4).Infoln("ascend vnpu recording pod", pod.Name, "device", deviceused, "type",
						gs.hamiDevs.config.CommonWord, "useMem", gs.Device[index].UsedMem, "useCore", gs.Device[index].UsedCore,
						"usedVnpuCount", cnt, "rawCnt", rawCnt)
				}
			}
		}
	}
	gs.GetStatus()
	cnt -= rawCnt
	return map[string]float64{gs.hamiDevs.config.ResourceName: float64(cnt * 1000)}
}

// SubResource frees the gpu hold by the pod
func (gs *Devices) SubResource(pod *v1.Pod) map[string]float64 {
	if gs == nil || gs.hamiDevs == nil {
		return nil
	}
	ids, ok := pod.Annotations[gs.hamiDevs.supportDevices]
	if !ok {
		return nil
	}
	podDev := decodePodDevices(ids)
	var cnt, rawCnt int32
	for _, val := range podDev {
		for _, deviceused := range val {
			for index, gsdevice := range gs.Device {
				if gsdevice.ID == deviceused.UUID {
					gs.Device[index].UsedMem -= deviceused.Usedmem
					gs.Device[index].UsedCore -= deviceused.Usedcores
					cnt += deviceused.Usedcores / gs.hamiDevs.config.Templates[0].AICore // Calculate the number of consumed vGPUs.
					gs.Device[index].UsedNum -= cnt
					rawCnt++
					klog.V(4).Infoln("ascend vnpu recording pod", pod.Name, "device", deviceused, "type",
						gs.hamiDevs.config.CommonWord, "useMem", gs.Device[index].UsedMem, "useCore", gs.Device[index].UsedCore,
						"usedVnpuCount", cnt, "rawCnt", rawCnt)
				}
			}
		}
	}
	cnt -= rawCnt
	return map[string]float64{gs.hamiDevs.config.ResourceName: float64(cnt * 1000)}
}

func (gs *Devices) HasDeviceRequest(pod *v1.Pod) bool {
	if AscendVGPUEnable && gs.checkVGPUResourcesInPod(pod) {
		return true
	}
	return false
}

func (gs *Devices) Release(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	// Nothing needs to be done here
	return nil
}

func (gs *Devices) FilterNode(pod *v1.Pod, schedulePolicy string) (int, string, error) {
	if AscendVGPUEnable {
		klog.V(4).Infoln("hami-vnpu DeviceSharing starts filtering pods", pod.Name)
		fit, _, score, err := gs.checkNodeGPUSharingPredicateAndScore(pod, gs, true, schedulePolicy)
		if err != nil || !fit {
			klog.Errorln("deviceSharing err=", err.Error())
			return devices.Unschedulable, fmt.Sprintf("hami-vnpuDeviceSharing %s", err.Error()), err
		}
		gs.Score = score
		klog.V(4).Infoln("hami-vnpu DeviceSharing successfully filters pods")
	}
	return devices.Success, "", nil
}

func (gs *Devices) Allocate(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	if AscendVGPUEnable {
		klog.V(4).Infoln("hami-vnpu DeviceSharing:Into AllocateToPod", pod.Name)
		fit, device, _, err := gs.checkNodeGPUSharingPredicateAndScore(pod, gs, false, "")
		if err != nil || !fit {
			klog.Errorln("DeviceSharing err=", err.Error())
			return err
		}

		var rtInfo []RuntimeInfo
		for _, dp := range device {
			for _, val := range dp {
				rtInfo = append(rtInfo, RuntimeInfo{UUID: val.UUID, Temp: val.Template})
			}
		}
		bytes, err := json.Marshal(rtInfo)
		if err != nil {
			return err
		}

		annotations := make(map[string]string)
		annotations[AssignedNodeAnnotations] = gs.Name
		annotations[AssignedTimeAnnotations] = strconv.FormatInt(time.Now().Unix(), 10)
		annotations[gs.hamiDevs.allocAnno] = string(bytes)
		annotations[gs.hamiDevs.inRequestDevices] = encodePodDevices(device)
		annotations[gs.hamiDevs.supportDevices] = encodePodDevices(device)
		annotations[DeviceBindPhase] = "allocating"
		annotations[BindTimeAnnotations] = strconv.FormatInt(time.Now().Unix(), 10)
		err = patchPodAnnotations(pod, annotations)
		if err != nil {
			return err
		}
		gs.GetStatus()
		klog.V(3).Infoln("DeviceSharing:Allocate Success")
	}
	return nil
}
