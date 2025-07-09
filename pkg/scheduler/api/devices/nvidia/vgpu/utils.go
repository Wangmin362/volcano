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

package vgpu

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

/*
	str类型

volcano.sh/node-vgpu-register: 'GPU-151ae9c0-83a9-2dd1-8981-b5f44c021a58,1,1126,NVIDIA-NVIDIA

	GeForce GTX 1080 Ti,true:GPU-76f93f2b-8cd8-4dd0-e555-56418bde1457,1,1126,NVIDIA-NVIDIA
	GeForce GTX 1080 Ti,true:'
*/
// 解析hami nvidia-volcano-device-plugin上报上来的信息
func decodeNodeDevices(name string, str string) *GPUDevices {
	// 每个分号就是一个设备的信息描述
	if !strings.Contains(str, ":") {
		return nil
	}
	tmp := strings.Split(str, ":")
	retval := &GPUDevices{
		Name:   name,
		Device: make(map[int]*GPUDevice),
		Score:  float64(0),
	}
	for index, val := range tmp {
		if strings.Contains(val, ",") {
			items := strings.Split(val, ",")
			count, _ := strconv.Atoi(items[1])
			devmem, _ := strconv.Atoi(items[2])
			health, _ := strconv.ParseBool(items[4])
			i := GPUDevice{
				ID:     index,
				UUID:   items[0],
				Number: uint(count),
				Memory: uint(devmem),
				Type:   items[3],
				Health: health,
			}
			retval.Device[index] = &i
		}
	}
	return retval
}

// 当Pod分配好设备之后，会被写入到Pod注解当中，譬如： volcano.sh/vgpu-ids-new: 'GPU-76f93f2b-8cd8-4dd0-e555-56418bde1457,NVIDIA,1126,0:
func encodeContainerDevices(cd []ContainerDevice) string {
	tmp := ""
	for _, val := range cd {
		tmp += val.UUID + "," + val.Type + "," + strconv.Itoa(int(val.Usedmem)) + "," + strconv.Itoa(int(val.Usedcores)) + ":"
	}
	klog.V(4).Infoln("Encoded container Devices=", tmp)
	return tmp
	//return strings.Join(cd, ",")
}

// 一个Pod存在多个容器的情况下，假如多个容器申请了设备，就需要使用这里的方法进行编码，譬如：
// volcano.sh/vgpu-ids-new: 'GPU-76f93f2b-8cd8-4dd0-e555-56418bde1457,NVIDIA,1126,0:;vgpu-ids-new: 'GPU-76f93f2b-8cd8-4dd0-e555-56418bde1457,NVIDIA,1126,0:
func encodePodDevices(pd []ContainerDevices) string {
	var ss []string
	for _, cd := range pd {
		ss = append(ss, encodeContainerDevices(cd))
	}
	return strings.Join(ss, ";")
}

// Pod上的注解格式为： vgpu-ids-new: 'GPU-76f93f2b-8cd8-4dd0-e555-56418bde1457,NVIDIA,1126,0:
func decodeContainerDevices(str string) ContainerDevices {
	if len(str) == 0 {
		return ContainerDevices{}
	}
	cd := strings.Split(str, ":")
	contdev := ContainerDevices{}
	tmpdev := ContainerDevice{}
	//fmt.Println("before container device", str)
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
	//fmt.Println("Decoded container device", contdev)
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

// 只要Pod申请volcano.sh/vgpu-memory或者volcano.sh/vgpu-number，就认为Pod申请了VGPU资源
func checkVGPUResourcesInPod(pod *v1.Pod) bool {
	for _, container := range pod.Spec.Containers {
		_, ok := container.Resources.Limits[VolcanoVGPUMemory]
		if ok {
			return true
		}
		_, ok = container.Resources.Limits[VolcanoVGPUNumber]
		if ok {
			return true
		}
	}
	return false
}

// 从Pod上解析容器申请的资源
func resourcereqs(pod *v1.Pod) []ContainerDeviceRequest {
	resourceName := v1.ResourceName(VolcanoVGPUNumber)
	resourceMem := v1.ResourceName(VolcanoVGPUMemory)
	resourceMemPercentage := v1.ResourceName(VolcanoVGPUMemoryPercentage)
	resourceCores := v1.ResourceName(VolcanoVGPUCores)
	counts := []ContainerDeviceRequest{}
	//Count Nvidia GPU
	for i := 0; i < len(pod.Spec.Containers); i++ {
		singledevice := false
		v, ok := pod.Spec.Containers[i].Resources.Limits[resourceName]
		if !ok {
			// TODO 意思是使用volcano.sh/vgpu-memory资源的情况下，一个容器只能挂一张卡么？
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
			mempnum := int32(101)
			mem, ok = pod.Spec.Containers[i].Resources.Limits[resourceMemPercentage]
			if !ok {
				mem, ok = pod.Spec.Containers[i].Resources.Requests[resourceMemPercentage]
			}
			if ok {
				mempnums, ok := mem.AsInt64()
				if ok {
					mempnum = int32(mempnums)
				}
			}
			if mempnum == 101 && memnum == 0 {
				mempnum = 100
			}
			corenum := 0
			core, ok := pod.Spec.Containers[i].Resources.Limits[resourceCores]
			if !ok {
				core, ok = pod.Spec.Containers[i].Resources.Requests[resourceCores]
			}
			if ok {
				corenums, ok := core.AsInt64()
				if ok {
					corenum = int(corenums)
				}
			}
			counts = append(counts, ContainerDeviceRequest{
				Nums:             int32(n),
				Type:             "NVIDIA",
				Memreq:           int32(memnum),
				MemPercentagereq: int32(mempnum),
				Coresreq:         int32(corenum),
			})
		}
	}
	klog.V(3).Infoln("counts=", counts)
	return counts
}

func checkGPUtype(annos map[string]string, cardtype string) bool {
	inuse, ok := annos[GPUInUse]
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
	nouse, ok := annos[GPUNoUse]
	if ok {
		if !strings.Contains(nouse, ",") {
			if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(nouse)) {
				// TODO 这是不是错了，因该是false
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

// d表示当前节点上的GPU设备信息，n表示Pod上容器的资源申请信息, annos表示Pod上的注解信息
// 判断当前容器申请的卡的类型和节点上卡的类型是否相等，并且看看用户是否配置了需要使用的卡的类型以及禁用的卡的类型
func checkType(annos map[string]string, d GPUDevice, n ContainerDeviceRequest) bool {
	//General type check, NVIDIA->NVIDIA MLU->MLU
	if !strings.Contains(d.Type, n.Type) {
		return false
	}
	if n.Type == NvidiaGPUDevice {
		return checkGPUtype(annos, d.Type)
	}
	klog.Errorf("Unrecognized device %v", n.Type)
	return false
}

func getGPUDeviceSnapShot(snap *GPUDevices) *GPUDevices {
	ret := GPUDevices{
		Name:   snap.Name,
		Device: make(map[int]*GPUDevice),
		Score:  float64(0),
	}
	for index, val := range snap.Device {
		if val != nil {
			ret.Device[index] = &GPUDevice{
				ID:       val.ID,
				UUID:     val.UUID,
				PodMap:   val.PodMap,
				Memory:   val.Memory,
				Number:   val.Number,
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
func checkNodeGPUSharingPredicateAndScore(pod *v1.Pod, gssnap *GPUDevices, replicate bool, schedulePolicy string) (bool, []ContainerDevices, float64, error) {
	// no gpu sharing request
	score := float64(0)
	// 只要Pod申请volcano.sh/vgpu-memory或者volcano.sh/vgpu-number，就认为Pod申请了VGPU资源
	if !checkVGPUResourcesInPod(pod) {
		// 如果Pod没有申请相关的资源，那么就认为这个Pod是否当前Pod调度，因为本身这里只是为了负责调度vgpu
		return true, []ContainerDevices{}, 0, nil
	}
	// 从Pod上解析容器申请的资源
	ctrReq := resourcereqs(pod)
	if len(ctrReq) == 0 {
		// 如果Pod没有申请相关的资源，那么就认为这个Pod是否当前Pod调度，因为本身这里只是为了负责调度vgpu
		return true, []ContainerDevices{}, 0, nil
	}
	var gs *GPUDevices
	if replicate {
		// Q: 这里复制的意义在哪里？ 为了解决什么问题？
		// A: 后续代码可知，再给容器分配卡的时候只能一个一个看，看的过程中如果当前卡满足容器需求，就需要更新信息。但是最终如果如果不能分配
		// 容器所有申请的卡，显然之前假设分配的卡就需要还原，因此这里需要拷贝一份节点卡的信息。后续如果分配失败，不会影响原始节点信息
		gs = getGPUDeviceSnapShot(gssnap)
	} else {
		gs = gssnap
	}
	ctrdevs := []ContainerDevices{}
	for _, val := range ctrReq {
		// 判断当前节点是否满足每一个设备的需求
		devs := []ContainerDevice{}

		// 如果需要申请的设备的数量大于当前节点总共的数量，那当前节点肯定是无法满足调度的
		if int(val.Nums) > len(gs.Device) {
			return false, []ContainerDevices{}, 0, fmt.Errorf("no enough gpu cards on node %s", gs.Name)
		}
		klog.V(3).InfoS("Allocating device for container", "request", val)

		// 从当前节点上的设备倒叙分配
		for i := len(gs.Device) - 1; i >= 0; i-- {
			klog.V(3).InfoS("Scoring pod request", "memReq", val.Memreq, "memPercentageReq", val.MemPercentagereq, "coresReq", val.Coresreq, "Nums", val.Nums, "Index", i, "ID", gs.Device[i].ID)
			klog.V(3).InfoS("Current Device", "Index", i, "TotalMemory", gs.Device[i].Memory, "UsedMemory", gs.Device[i].UsedMem, "UsedCores", gs.Device[i].UsedNum)
			// 如果当前GPU能够被同时使用的数量已经超了，那么当前卡不能分配，因为已经超了
			if gs.Device[i].Number <= uint(gs.Device[i].UsedNum) {
				continue
			}
			// TODO 为啥是101这么一个奇怪的数字？
			if val.MemPercentagereq != 101 && val.Memreq == 0 {
				val.Memreq = int32(gs.Device[i].Memory * uint(val.MemPercentagereq/100))
			}
			// 当前卡的显存不够，也无法分配
			if gs.Device[i].Memory-gs.Device[i].UsedMem < uint(val.Memreq) {
				continue
			}
			// 当前卡的算力不够无法分配
			if 100-gs.Device[i].UsedCore < uint(val.Coresreq) {
				continue
			}
			// Coresreq=100 indicates it want this card exclusively
			// 如果当前容器想要独占一张卡，但是这张卡又被其它容器使用了，那么当前容器也无法分配这张卡
			if val.Coresreq == 100 && gs.Device[i].UsedNum > 0 {
				continue
			}
			// You can't allocate core=0 job to an already full GPU
			// 如果容器没有设置算力的需求，但是这张卡算力已经被用完了，此时也无法分配
			if gs.Device[i].UsedCore == 100 && val.Coresreq == 0 {
				continue
			}
			// 判断当前容器申请的卡的类型和节点上卡的类型是否相等，并且看看用户是否配置了需要使用的卡的类型以及禁用的卡的类型
			if !checkType(pod.Annotations, *gs.Device[i], val) {
				klog.Errorln("failed checktype", gs.Device[i].Type, val.Type)
				continue
			}
			//total += gs.Devices[i].Count
			//free += node.Devices[i].Count - node.Devices[i].Used
			if val.Nums > 0 {
				klog.V(3).InfoS("device fitted", "ID", gs.Device[i].ID)
				// 假设分配这张卡，因此需要更新使用信息
				val.Nums--
				gs.Device[i].UsedNum++
				gs.Device[i].UsedMem += uint(val.Memreq)
				gs.Device[i].UsedCore += uint(val.Coresreq)
				// 记录分配的设备
				devs = append(devs, ContainerDevice{
					UUID:      gs.Device[i].UUID,
					Type:      val.Type,
					Usedmem:   val.Memreq,
					Usedcores: val.Coresreq,
				})
				switch schedulePolicy {
				case binpackPolicy:
					score += binpackMultiplier * (float64(gs.Device[i].UsedMem) / float64(gs.Device[i].Memory))
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
		// 如果容器申请的设备到最后都还没有全部分配到，那就认为当前节点无法满足调度，因为GPU的数量不够
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
	/*
		Can't modify Env of pods here

		patch1 := addGPUIndexPatch()
		_, err = s.kubeClient.CoreV1().Pods(pod.Namespace).
			Patch(context.Background(), pod.Name, k8stypes.JSONPatchType, []byte(patch1), metav1.PatchOptions{})
		if err != nil {
			klog.Infof("Patch1 pod %v failed, %v", pod.Name, err)
		}*/

	return err
}
