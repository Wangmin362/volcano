/*
Copyright 2019 The Volcano Authors.

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

package cache

import (
	v1 "k8s.io/api/core/v1"

	"volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/volcano/pkg/controllers/apis"
)

// Cache Interface.
// TODO job缓存本质上就是缓存了VolcanoJob相关的信息,并且缓存了Job相关的Pod信息
type Cache interface {
	Run(stopCh <-chan struct{})

	Get(key string) (*apis.JobInfo, error)
	GetStatus(key string) (*v1alpha1.JobStatus, error)
	Add(obj *v1alpha1.Job) error
	Update(obj *v1alpha1.Job) error
	Delete(obj *v1alpha1.Job) error

	// AddPod Pod上有一个volcano.sh/job-name注解,用于表明当前的Pod到底是属于哪个Job
	AddPod(pod *v1.Pod) error
	UpdatePod(pod *v1.Pod) error
	DeletePod(pod *v1.Pod) error

	// Task本质上和Pod区别不大,可以把Task映射到Pod上
	TaskCompleted(jobKey, taskName string) bool
	TaskFailed(jobKey, taskName string) bool
}
