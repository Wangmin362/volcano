/*
Copyright 2022 The Volcano Authors.

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

package state

import "volcano.sh/apis/pkg/apis/flow/v1alpha1"

type runningState struct {
	jobFlow *v1alpha1.JobFlow
}

func (p *runningState) Execute(action v1alpha1.Action) error {
	switch action {
	case v1alpha1.SyncJobFlowAction:
		return SyncJobFlow(p.jobFlow, func(status *v1alpha1.JobFlowStatus, allJobList int) {
			// Job处于Running的状态时，可能有些Job已经运行完成了，此时就更新Job的状态为Succeed，表示JobFlow底下所有的Job已经运行完成
			if len(status.CompletedJobs) == allJobList {
				status.State.Phase = v1alpha1.Succeed
			}
		})
	}
	return nil
}
