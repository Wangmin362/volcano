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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

var kubeClient kubernetes.Interface

const (
	CardTypeMismatch                  = "CardTypeMismatch"
	CardUUIDMismatch                  = "CardUuidMismatch"
	CardTimeSlicingExhausted          = "CardTimeSlicingExhausted"
	CardComputeUnitsExhausted         = "CardComputeUnitsExhausted"
	CardInsufficientMemory            = "CardInsufficientMemory"
	CardInsufficientCore              = "CardInsufficientCore"
	NumaNotFit                        = "NumaNotFit"
	ExclusiveDeviceAllocateConflict   = "ExclusiveDeviceAllocateConflict"
	CardNotFoundCustomFilterRule      = "CardNotFoundCustomFilterRule"
	NodeInsufficientDevice            = "NodeInsufficientDevice"
	AllocatedCardsInsufficientRequest = "AllocatedCardsInsufficientRequest"
	NodeUnfitPod                      = "NodeUnfitPod"
	NodeFitPod                        = "NodeFitPod"
)

func init() {
	var err error
	kubeClient, err = NewClient()
	if err != nil {
		klog.Errorf("init kubeclient in hamivgpu failed: %s", err.Error())
	} else {
		klog.V(3).Infoln("init kubeclient success")
	}
}

// NewClient connects to an API server
func NewClient() (kubernetes.Interface, error) {
	kubeConfig := os.Getenv("KUBECONFIG")
	if kubeConfig == "" {
		kubeConfig = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		config, err = clientcmd.BuildConfigFromFlags("", kubeConfig)
		if err != nil {
			return nil, err
		}
	}
	client, err := kubernetes.NewForConfig(config)
	kubeClient = client
	return client, err
}

func patchNodeAnnotations(node *v1.Node, annotations map[string]string) error {
	type patchMetadata struct {
		Annotations map[string]string `json:"annotations,omitempty"`
	}
	type patchPod struct {
		Metadata patchMetadata `json:"metadata"`
		//Spec     patchSpec     `json:"spec,omitempty"`
	}

	p := patchPod{}
	p.Metadata.Annotations = annotations

	bytes, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = kubeClient.CoreV1().Nodes().
		Patch(context.Background(), node.Name, k8stypes.StrategicMergePatchType, bytes, metav1.PatchOptions{})
	if err != nil {
		klog.Errorf("patch pod %v failed, %v", node.Name, err)
	}
	return err
}

func encodeContainerDevices(cd []ContainerDevice) string {
	tmp := ""
	for _, val := range cd {
		tmp += val.UUID + "," + val.Type + "," + strconv.Itoa(int(val.Usedmem)) + "," + strconv.Itoa(int(val.Usedcores)) + ":"
	}
	klog.V(4).Infoln("Encoded container Devices=", tmp)
	return tmp
}

func encodePodDevices(pd []ContainerDevices) string {
	var ss []string
	for _, cd := range pd {
		ss = append(ss, encodeContainerDevices(cd))
	}
	return strings.Join(ss, ";")
}

func decodeContainerDevices(str string) ContainerDevices {
	if len(str) == 0 {
		return ContainerDevices{}
	}
	cd := strings.Split(str, ":")
	contdev := ContainerDevices{}
	tmpdev := ContainerDevice{}
	if len(str) == 0 {
		return contdev
	}
	for _, val := range cd {
		if strings.Contains(val, ",") {
			//fmt.Println("cd is ", val)
			tmpstr := strings.Split(val, ",")
			tmpdev.UUID = tmpstr[0]
			tmpdev.Type = tmpstr[1]
			devmem, _ := strconv.ParseInt(tmpstr[2], 10, 32)
			tmpdev.Usedmem = int32(devmem)
			devcores, _ := strconv.ParseInt(tmpstr[3], 10, 32)
			tmpdev.Usedcores = int32(devcores)
			contdev = append(contdev, tmpdev)
		}
	}
	return contdev
}

func decodePodDevices(str string) []ContainerDevices {
	if len(str) == 0 {
		return []ContainerDevices{}
	}
	var pd []ContainerDevices
	for _, s := range strings.Split(str, ";") {
		cd := decodeContainerDevices(s)
		pd = append(pd, cd)
	}
	return pd
}

func (gs *Devices) checkVGPUResourcesInPod(pod *v1.Pod) bool {
	if gs == nil || gs.hamiDevs == nil {
		return false
	}
	for _, container := range pod.Spec.Containers {
		_, ok := container.Resources.Limits[v1.ResourceName(gs.hamiDevs.config.ResourceName)]
		if ok {
			return true
		}
		_, ok = container.Resources.Limits[v1.ResourceName(gs.hamiDevs.config.ResourceMemoryName)]
		if ok {
			return true
		}
	}
	return false
}

func (as *Devices) resourcereqs(pod *v1.Pod) ([]ContainerDeviceRequest, error) {
	resourceName := v1.ResourceName(as.hamiDevs.config.ResourceName)
	resourceMem := v1.ResourceName(as.hamiDevs.config.ResourceMemoryName)
	var counts []ContainerDeviceRequest
	//Count Nvidia GPU
	for i := 0; i < len(pod.Spec.Containers); i++ {
		singledevice := false
		v, ok := pod.Spec.Containers[i].Resources.Limits[resourceName]
		if !ok {
			v, ok = pod.Spec.Containers[i].Resources.Limits[resourceMem]
			singledevice = true
		}
		if ok {
			n := int64(1)
			if !singledevice {
				n, _ = v.AsInt64()
			}
			memnum := 0
			mem, ok := pod.Spec.Containers[i].Resources.Limits[resourceMem]
			if !ok {
				mem, ok = pod.Spec.Containers[i].Resources.Requests[resourceMem]
			}
			if ok {
				memnums, ok := mem.AsInt64()
				if ok {
					memnum = int(memnums)
				}
			}
			if memnum == 0 {
				memnum = int(as.hamiDevs.config.MemoryAllocatable)
				klog.V(4).Infoln("dont`t assign %s, so allocate whole card, set mem to %d",
					as.hamiDevs.config.ResourceMemoryName, as.hamiDevs.config.MemoryAllocatable)
			}
			if n > 1 && memnum < int(as.hamiDevs.config.MemoryAllocatable) {
				return nil, fmt.Errorf("vNPU nor supported for multiple devices")
			}

			memory, aicore, template, err := as.trimMemory(int64(memnum))
			if err != nil {
				return nil, err
			}
			if memnum != int(memory) {
				klog.V(4).Infof("don`t support customized memory size:%d, trim to %d, use template: [%s]", memnum, memory, template)
				memnum = int(memory)
			}
			counts = append(counts, ContainerDeviceRequest{
				Nums:     int32(n),
				Type:     as.hamiDevs.config.CommonWord,
				Memreq:   int32(memnum),
				Coresreq: aicore,
				Template: template,
			})
		}
	}
	klog.V(3).Infoln("counts=", counts)
	return counts, nil
}

func (as *Devices) trimMemory(m int64) (int64, int32, string, error) {
	for i := range as.hamiDevs.config.Templates {
		if m <= as.hamiDevs.config.Templates[i].Memory {
			return as.hamiDevs.config.Templates[i].Memory,
				as.hamiDevs.config.Templates[i].AICore, as.hamiDevs.config.Templates[i].Name, nil
		}
	}
	if m <= as.hamiDevs.config.MemoryCapacity {
		return as.hamiDevs.config.MemoryAllocatable, as.hamiDevs.config.AICore, "", nil
	}

	return 0, 0, "", fmt.Errorf("request memory is too large requestMem=%d maxMem=%d", m, as.hamiDevs.config.MemoryAllocatable)
}

func (as *Devices) checkGPUtype(annos map[string]string, cardtype string) bool {
	inuse, ok := annos[as.hamiDevs.useUUIDAnno]
	if ok {
		if !strings.Contains(inuse, ",") {
			if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(inuse)) {
				return true
			}
		} else {
			for _, val := range strings.Split(inuse, ",") {
				if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(val)) {
					return true
				}
			}
		}
		return false
	}
	nouse, ok := annos[as.hamiDevs.noUseUUIDAnno]
	if ok {
		if !strings.Contains(nouse, ",") {
			if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(nouse)) {
				return true
			}
		} else {
			for _, val := range strings.Split(nouse, ",") {
				if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(val)) {
					return false
				}
			}
		}
		return true
	}
	return true
}

func (as *Devices) checkType(annos map[string]string, d Device, n ContainerDeviceRequest) bool {
	//General type check, NVIDIA->NVIDIA MLU->MLU
	if !strings.Contains(d.Type, n.Type) {
		return false
	}
	if n.Type == as.hamiDevs.config.CommonWord {
		return as.checkGPUtype(annos, d.Type)
	}
	klog.Errorf("Unrecognized device %v", n.Type)
	return false
}

func getGPUDeviceSnapShot(snap *Devices) *Devices {
	ret := Devices{
		Name:     snap.Name,
		Device:   make(map[int]*Device),
		Score:    float64(0),
		hamiDevs: snap.hamiDevs,
	}
	for index, val := range snap.Device {
		if val != nil {
			ret.Device[index] = &Device{
				ID:       val.ID,
				PodMap:   val.PodMap,
				Devmem:   val.Devmem,
				Count:    val.Count,
				Type:     val.Type,
				Health:   val.Health,
				UsedNum:  val.UsedNum,
				UsedMem:  val.UsedMem,
				UsedCore: val.UsedCore,
			}
		}
	}
	return &ret
}

// checkNodeGPUSharingPredicate checks if a pod with gpu requirement can be scheduled on a node.
func (as *Devices) checkNodeGPUSharingPredicateAndScore(pod *v1.Pod, gssnap *Devices, replicate bool, schedulePolicy string) (bool, []ContainerDevices, float64, error) {
	// no gpu sharing request
	score := float64(0)
	if !as.checkVGPUResourcesInPod(pod) {
		return true, []ContainerDevices{}, 0, nil
	}
	ctrReq, err := as.resourcereqs(pod)
	if err != nil {
		return false, []ContainerDevices{}, 0, err
	}
	if len(ctrReq) == 0 {
		return true, []ContainerDevices{}, 0, nil
	}
	var gs *Devices
	if replicate {
		gs = getGPUDeviceSnapShot(gssnap)
	} else {
		gs = gssnap
	}
	var ctrdevs []ContainerDevices
	for _, val := range ctrReq {
		devs := []ContainerDevice{}
		if int(val.Nums) > len(gs.Device) {
			return false, []ContainerDevices{}, 0, fmt.Errorf("no enough npu cards on node %s", gs.Name)
		}
		klog.V(3).InfoS("Allocating device for container", "request", val)

		for i := len(gs.Device) - 1; i >= 0; i-- {
			klog.V(3).InfoS("Scoring pod request", "memReq", val.Memreq, "coresReq", val.Coresreq, "Nums", val.Nums, "Index", i, "ID", gs.Device[i].ID)
			klog.V(3).InfoS("Current Device", "Index", i, "TotalMemory", gs.Device[i].Devmem, "UsedMemory", gs.Device[i].UsedMem, "UsedCores", gs.Device[i].UsedNum)
			if gs.Device[i].Count <= gs.Device[i].UsedNum {
				klog.V(5).InfoS(CardTimeSlicingExhausted, "pod", pod.Name, "node", as.Name, "Index", i, "Count", gs.Device[i].Count, "UsedNum", gs.Device[i].UsedNum)
				continue
			}
			if gs.Device[i].Devmem-gs.Device[i].UsedMem < val.Memreq {
				klog.V(5).InfoS(CardInsufficientMemory, "pod", pod.Name, "node", as.Name, "Index", i, "Devmem", gs.Device[i].Devmem, "UsedMem", gs.Device[i].UsedMem, "Memreq", val.Memreq)
				continue
			}
			if gs.hamiDevs.config.AICore-gs.Device[i].UsedCore < val.Coresreq {
				klog.V(5).InfoS(CardInsufficientCore, "pod", pod.Name, "node", as.Name, "Index", i, "Devcore", gs.Device[i].Devcore, "UsedCore", gs.Device[i].UsedCore, "Coresreq", val.Coresreq)
				continue
			}
			// Coresreq=100 indicates it want this card exclusively
			if val.Coresreq == gs.hamiDevs.config.AICore && gs.Device[i].UsedNum > 0 {
				klog.V(5).InfoS(ExclusiveDeviceAllocateConflict, "pod", pod.Name, "node", as.Name, "Index", i, "UsedNum", gs.Device[i].UsedNum, "TotalAICore", gs.hamiDevs.config.AICore, "reqAICore", val.Coresreq)
				continue
			}
			// You can't allocate core=0 job to an already full GPU
			if gs.Device[i].UsedCore == gs.hamiDevs.config.AICore && val.Coresreq == 0 {
				klog.V(5).InfoS(CardInsufficientCore, "pod", pod.Name, "node", as.Name, "Index", i, "TotalAICore", gs.hamiDevs.config.AICore, "usedAICore", gs.Device[i].UsedCore, "reqAICore", val.Coresreq)
				continue
			}
			if !as.checkType(pod.Annotations, *gs.Device[i], val) {
				klog.Errorln("failed checktype", gs.Device[i].Type, val.Type)
				continue
			}
			if val.Nums > 0 {
				klog.V(3).InfoS("device fitted", "ID", gs.Device[i].ID)
				val.Nums--
				gs.Device[i].UsedNum++
				gs.Device[i].UsedMem += val.Memreq
				gs.Device[i].UsedCore += val.Coresreq
				devs = append(devs, ContainerDevice{
					UUID:      gs.Device[i].ID,
					Type:      val.Type,
					Usedmem:   val.Memreq,
					Usedcores: val.Coresreq,
					Template:  val.Template,
				})
				switch schedulePolicy {
				case binpackPolicy:
					score += binpackMultiplier * (float64(gs.Device[i].UsedMem) / float64(gs.Device[i].Devmem))
				case spreadPolicy:
					if gs.Device[i].UsedNum == 1 {
						score += spreadMultiplier
					}
				default:
					score = float64(0)
				}
			}
			if val.Nums == 0 {
				break
			}
		}
		if val.Nums > 0 {
			return false, []ContainerDevices{}, 0, fmt.Errorf("not enough gpu fitted on this node")
		}
		ctrdevs = append(ctrdevs, devs)
	}
	return true, ctrdevs, score, nil
}

func patchPodAnnotations(pod *v1.Pod, annotations map[string]string) error {
	type patchMetadata struct {
		Annotations map[string]string `json:"annotations,omitempty"`
	}
	type patchPod struct {
		Metadata patchMetadata `json:"metadata"`
		//Spec     patchSpec     `json:"spec,omitempty"`
	}

	p := patchPod{}
	p.Metadata.Annotations = annotations

	bytes, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = kubeClient.CoreV1().Pods(pod.Namespace).
		Patch(context.Background(), pod.Name, k8stypes.StrategicMergePatchType, bytes, metav1.PatchOptions{})
	if err != nil {
		klog.Errorf("patch pod %v failed, %v", pod.Name, err)
	}
	return err
}
