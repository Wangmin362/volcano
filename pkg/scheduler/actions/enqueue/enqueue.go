/*
Copyright 2019 The Kubernetes Authors.

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

package enqueue

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util"
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (enqueue *Action) Name() string {
	return "enqueue"
}

func (enqueue *Action) Initialize() {}

// Execute
// 1. 遍历所有的Job, 把Job按照其关联的Queue进行优先级排序,如果Job不处于Pending状态,那么就把这个Job剔除掉，因为只有Pending状态的Job才需要被调度
// 2.
func (enqueue *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Enqueue ...")
	defer klog.V(5).Infof("Leaving Enqueue ...")

	// 1、queues本质上是一个小端堆，这里可以看出，不同的队列有不同的优先级，这正式Volcano设计Queue意义之所在，Queue本身就是为了抽象
	// 不同的优先级，本质上就是为了划分集群资源。通过Queue划分集群资源之后，Queue中归属的Job也相当于划分了优先级
	// 2. 队列优先级排序规则为: 若制定了queueOrder函数, 则按照queueOrder函数的优先级排序, 否则按照创建时间和UID排序
	queues := util.NewPriorityQueue(ssn.QueueOrderFn)
	queueSet := sets.NewString()

	// 1、以Queue维度，划分Job，其实就是把Job划分到Job所属Queue当中
	// 2、具体到每个Queue来说看，其中的Job其实也是需要按照优先级进行排序的
	jobsMap := map[api.QueueID]*util.PriorityQueue{}

	for _, job := range ssn.Jobs {
		// 如果Job的调度时间不为空，说明Job已经开始调度
		if job.ScheduleStartTimestamp.IsZero() {
			// 更新Job的调度时间
			ssn.Jobs[job.UID].ScheduleStartTimestamp = metav1.Time{
				Time: time.Now(),
			}
		}

		// 获取Job所在的Queue
		if queue, found := ssn.Queues[job.Queue]; !found {
			klog.Errorf("Failed to find Queue <%s> for Job <%s/%s>",
				job.Queue, job.Namespace, job.Name)
			continue
		} else if !queueSet.Has(string(queue.UID)) {
			klog.V(5).Infof("Added Queue <%s> for Job <%s/%s>",
				queue.Name, job.Namespace, job.Name)

			// 记录当前一共有多少个不同的队列
			queueSet.Insert(string(queue.UID))
			queues.Push(queue)
		}

		// 1. 需要调度的Job就是处于Pending当中的Job，否则处于其它状态的Job肯定已经调度过了
		// 2. Job是否Pending,其实判断的是PodGroup的状态是否Pending
		if job.IsPending() {
			// 初始化优先级队列，这个队列本质上就是一个小端堆，TODO 为什么不设置为一个大端堆
			if _, found := jobsMap[job.Queue]; !found {
				jobsMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
			}

			// 把待调度的Job放入到对应队列
			klog.V(5).Infof("Added Job <%s/%s> into Queue <%s>", job.Namespace, job.Name, job.Queue)
			jobsMap[job.Queue].Push(job)
		}
	}

	klog.V(3).Infof("Try to enqueue PodGroup to %d Queues", len(jobsMap))

	// 1. 按照队列的优先级,依次取出队列
	// 2. 遍历队列中的Job,
	for {
		// 如果为空，说明当前没有需要调度的Job
		if queues.Empty() {
			break
		}

		// 按照优先级从小到大弹出不同的Queue资源
		queue := queues.Pop().(*api.QueueInfo)

		// skip the Queue that has no pending job
		// 获取当前队列所有的Job
		jobs, found := jobsMap[queue.UID]
		if !found || jobs.Empty() {
			continue
		}

		// 从队列当中弹出一个Job,判断当前Job是否可以入队
		job := jobs.Pop().(*api.JobInfo)

		// 如果Job没有设置最小需要的资源，说明这个Job没有特别的要求，可能很小的资源都可以运行
		if job.PodGroup.Spec.MinResources == nil || ssn.JobEnqueueable(job) {
			// TODO 入队一个待调度的Job
			ssn.JobEnqueued(job)

			// 更新Job的状态为入队
			job.PodGroup.Status.Phase = scheduling.PodGroupInqueue
			// 更新Session持有的Job
			ssn.Jobs[job.UID] = job
		}

		// Added Queue back until no job in Queue.
		// 1、重新把刚才弹出来的Queue放入到队列当中，这样子下次堆堆中获取队列，其实还是这个Queue
		// 2、本质上就是想要把这个Queue的所有Job都取出来，判断是否可以进入Session
		queues.Push(queue)
	}
}

func (enqueue *Action) UnInitialize() {}
