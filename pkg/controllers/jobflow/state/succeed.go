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

type succeedState struct {
	jobFlow *v1alpha1.JobFlow
}

func (p *succeedState) Execute(action v1alpha1.Action) error {
	switch action {
	case v1alpha1.SyncJobFlowAction:
		// Job已经处于Succeed状态后，说明所有的Job已经运行完成，此时不可能再有变化，因此啥也不用干
		return SyncJobFlow(p.jobFlow, func(status *v1alpha1.JobFlowStatus, allJobList int) {})
	}
	return nil
}
