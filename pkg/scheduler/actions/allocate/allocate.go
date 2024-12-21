/*
 Copyright 2021 The Volcano Authors.

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

package allocate

import (
	"time"

	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/util"
)

type Action struct {
	session *framework.Session
}

func New() *Action {
	return &Action{}
}

func (alloc *Action) Name() string {
	return "allocate"
}

func (alloc *Action) Initialize() {}

func (alloc *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Allocate ...")
	defer klog.V(5).Infof("Leaving Allocate ...")

	// the allocation for pod may have many stages
	// 1. pick a queue named Q (using ssn.QueueOrderFn)
	// 2. pick a job named J from Q (using ssn.JobOrderFn)
	// 3. pick a task T from J (using ssn.TaskOrderFn)
	// 4. use predicateFn to filter out node that T can not be allocated on.
	// 5. use ssn.NodeOrderFn to judge the best node and assign it to T

	// queues sort queues by QueueOrderFn.
	// 实例化一个优先级队列,本质上还是一个二叉堆
	queues := util.NewPriorityQueue(ssn.QueueOrderFn)
	// jobsMap is used to find job with the highest priority in given queue.
	jobsMap := map[api.QueueID]*util.PriorityQueue{}

	alloc.session = ssn

	// 1. 遍历所有的Job, 检查Job的状态是否处于Pending状态,这里有两种情况
	// 1.1. 若用户配置了Enqueue Action, 若一个Job到了Allocate Action, 此时还是Pending状态,说明是一个异常情况,此时需要跳过这个Job
	// 1.2. 若用户没有配置Enqueue Action,那么默认Job的状态就是Inqueue状态
	// 2. 执行JobValidFns回调函数,判断当前Job是否合法,是否可以继续调度
	alloc.pickUpQueuesAndJobs(queues, jobsMap)

	klog.V(3).Infof("Try to allocate resource to %d Queues", len(jobsMap))
	alloc.allocateResources(queues, jobsMap)
}

// 1. 遍历所有的Job, 检查Job的状态是否处于Pending状态,这里有两种情况
// 1.1. 若用户配置了Enqueue Action, 若一个Job到了Allocate Action, 此时还是Pending状态,说明是一个异常情况,此时需要跳过这个Job
// 1.2. 若用户没有配置Enqueue Action,那么默认Job的状态就是Inqueue状态
// 2. 执行JobValidFns回调函数,判断当前Job是否合法,是否可以继续调度
func (alloc *Action) pickUpQueuesAndJobs(queues *util.PriorityQueue, jobsMap map[api.QueueID]*util.PriorityQueue) {
	ssn := alloc.session
	for _, job := range ssn.Jobs {
		// If not config enqueue action, change Pending pg into Inqueue statue to avoid blocking job scheduling.
		// 1. 如果用户没有配置Enqueue Action，那么Job的状态不会变为Inqueue状态，因此这里默认所有的Job都是Inqueue状态, 防止Job被阻塞无法被调度
		if conf.EnabledActionMap["enqueue"] {
			// 1. Job是否Pending,其实判断的是PodGroup的状态是否Pending
			// 2. 如果进入分配阶段了，此时不在考虑还处于Pending状态的Job，因为再enqueue阶段已经把所有的Job变为了Inqueue状态
			if job.IsPending() {
				klog.V(4).Infof("Job <%s/%s> Queue <%s> skip allocate, reason: job status is pending.",
					job.Namespace, job.Name, job.Queue)
				continue
			}
		} else if job.IsPending() {
			// 如果没有enqueue Action，那么Job的状态不可能变为Inqueue状态，因此这里默认所有的Job都是Inqueue状态
			klog.V(4).Infof("Job <%s/%s> Queue <%s> status update from pending to inqueue, reason: no enqueue action is configured.",
				job.Namespace, job.Name, job.Queue)
			job.PodGroup.Status.Phase = scheduling.PodGroupInqueue
		}

		// 校验当前Job是否合法
		if vr := ssn.JobValid(job); vr != nil && !vr.Pass {
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip allocate, reason: %v, message %v", job.Namespace, job.Name, job.Queue, vr.Reason, vr.Message)
			continue
		}

		// 查找这个Job所在Queue，如果没有找到，直接退出这个Job的调度
		if _, found := ssn.Queues[job.Queue]; !found {
			klog.Warningf("Skip adding Job <%s/%s> because its queue %s is not found",
				job.Namespace, job.Name, job.Queue)
			continue
		}

		// 找到当前Job所在Queue的队列，这个队列当中包含了所有在Queue中的Job
		if _, found := jobsMap[job.Queue]; !found {
			jobsMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
			queues.Push(ssn.Queues[job.Queue])
		}

		klog.V(4).Infof("Added Job <%s/%s> into Queue <%s>", job.Namespace, job.Name, job.Queue)
		jobsMap[job.Queue].Push(job)
	}
}

// allocateResources primarily accomplishes two steps:
// 1. picks up tasks.
// 2. allocates resources to these tasks. (this step is carried out by the allocateResourcesForTasks method.)
// 1. 按照优先级大小,从高优先级遍历所有的队列, 然后遍历队列中的Job
// 2. 找到当前Job所有的处于Pending状态的Task, 如果一个Task的QOS是BestEffort,那么忽略这个Task
func (alloc *Action) allocateResources(queues *util.PriorityQueue, jobsMap map[api.QueueID]*util.PriorityQueue) {
	ssn := alloc.session
	pendingTasks := map[api.JobID]*util.PriorityQueue{}

	allNodes := ssn.NodeList

	// To pick <namespace, queue> tuple for job, we choose to pick namespace firstly.
	// Because we believe that number of queues would less than namespaces in most case.
	// And, this action would make the resource usage among namespace balanced.
	for {
		// 队列已经为空,说明此时没有需要调度的Job,直接退出
		if queues.Empty() {
			break
		}

		// 弹出当前最高优先级的队列																																																																																																																																																																																				o
		queue := queues.Pop().(*api.QueueInfo)

		// TODO 似乎再说当前Queue已经使用到容量的上线了，因此直接忽略这个Queue中Job的调度
		if ssn.Overused(queue) {
			klog.V(3).Infof("Queue <%s> is overused, ignore it.", queue.Name)
			continue
		}

		klog.V(3).Infof("Try to allocate resource to Jobs in Queue <%s>", queue.Name)

		jobs, found := jobsMap[queue.UID]
		if !found || jobs.Empty() {
			klog.V(4).Infof("Can not find jobs for queue %s.", queue.Name)
			continue
		}

		// 弹出一个Job
		job := jobs.Pop().(*api.JobInfo)

		// 找到当前Job的所有Pending的Pod,如果一个Pod的QOS是BestEffort,那么忽略这个Pod
		if _, found = pendingTasks[job.UID]; !found {
			tasks := util.NewPriorityQueue(ssn.TaskOrderFn)
			// 调度处于Pending的Job
			for _, task := range job.TaskStatusIndex[api.Pending] {
				// Skip BestEffort task in 'allocate' action.
				// 如果一个任务的Pod是BestEffort,那么忽略这个Pod
				if task.Resreq.IsEmpty() {
					klog.V(4).Infof("Task <%v/%v> is BestEffort task, skip it.",
						task.Namespace, task.Name)
					continue
				}

				tasks.Push(task)
			}
			pendingTasks[job.UID] = tasks
		}
		tasks := pendingTasks[job.UID]

		// Added Queue back until no job in Namespace.
		queues.Push(queue)

		if tasks.Empty() {
			continue
		}

		klog.V(3).Infof("Try to allocate resource to %d tasks of Job <%v/%v>",
			tasks.Len(), job.Namespace, job.Name)

		// 尝试给所有处于Pending状态的Task分配资源
		alloc.allocateResourcesForTasks(tasks, job, jobs, queue, allNodes)
	}
}

func (alloc *Action) allocateResourcesForTasks(tasks *util.PriorityQueue, job *api.JobInfo, jobs *util.PriorityQueue,
	queue *api.QueueInfo, allNodes []*api.NodeInfo) {
	ssn := alloc.session
	stmt := framework.NewStatement(ssn)
	ph := util.NewPredicateHelper()

	for !tasks.Empty() {
		task := tasks.Pop().(*api.TaskInfo)

		// 判断当前队列中的资源是否还够Task的资源需求,如果不够,忽略这个Task
		if !ssn.Allocatable(queue, task) {
			klog.V(3).Infof("Queue <%s> is overused when considering task <%s>, ignore it.", queue.Name, task.Name)
			continue
		}

		klog.V(3).Infof("There are <%d> nodes for Job <%v/%v>", len(ssn.Nodes), job.Namespace, job.Name)

		// TODO 这里的接口设计返回值设计为什么Error? 如果一个任务不能调度,直接跳过这个任务即可,为啥会直接跳过这个Job的调度?
		if err := ssn.PrePredicateFn(task); err != nil {
			klog.V(3).Infof("PrePredicate for task %s/%s failed for: %v", task.Namespace, task.Name, err)
			fitErrors := api.NewFitErrors()
			for _, ni := range allNodes {
				fitErrors.SetNodeError(ni.Name, err)
			}
			job.NodesFitErrors[task.UID] = fitErrors
			break
		}

		// 1. 为当前Task找到所有适合的节点,也就是K8S调度框架当中的预选,即筛选出所有可以部署当前Task的Node出来
		// 2. 这里面有一个优化技巧,一个Task最终只需要运行在一个节点上,但是这里其实并不需要判断所有的节点都可以部署当前节点,只需要筛选出来
		// 一定量的节点即可,譬如一个集群有3000个节点,但是一个Task只需要最终运行在一个节点上,那么我可能只需要筛选出来50个或者100个合适运行
		// 这个Task的Node节点即可, 不需要把每个节点都判断一次,浪费调度时间.
		predicateNodes, fitErrors := ph.PredicateNodes(task, allNodes, alloc.predicate, true)
		if len(predicateNodes) == 0 {
			job.NodesFitErrors[task.UID] = fitErrors
			break
		}

		// Candidate nodes are divided into two gradients:
		// - the first gradient node: a list of free nodes that satisfy the task resource request;
		// - The second gradient node: the node list whose sum of node idle resources and future idle meets the task resource request;
		// Score the first gradient node first. If the first gradient node meets the requirements, ignore the second gradient node list,
		// otherwise, score the second gradient node and select the appropriate node.
		var candidateNodes [][]*api.NodeInfo
		var idleCandidateNodes []*api.NodeInfo
		var futureIdleCandidateNodes []*api.NodeInfo
		// 主要还是判断预选出来的节点资源是否满足当前Task的资源需求
		for _, n := range predicateNodes {
			if task.InitResreq.LessEqual(n.Idle, api.Zero) { // 节点的资源就满足当前Task的资源需求
				idleCandidateNodes = append(idleCandidateNodes, n)
			} else if task.InitResreq.LessEqual(n.FutureIdle(), api.Zero) { // 节点将来的资源可能满足当前任务
				futureIdleCandidateNodes = append(futureIdleCandidateNodes, n)
			} else {
				klog.V(5).Infof("Predicate filtered node %v, idle: %v and future idle: %v do not meet the requirements of task: %v",
					n.Name, n.Idle, n.FutureIdle(), task.Name)
			}
		}
		candidateNodes = append(candidateNodes, idleCandidateNodes)
		candidateNodes = append(candidateNodes, futureIdleCandidateNodes)

		var bestNode *api.NodeInfo
		// 1. 节点优选的时候,优先从第一个梯度节点开始优选,如果第一个梯度节点没有满足当前Task的资源需求,那么就从第二个梯度节点开始优选
		for index, nodes := range candidateNodes {
			if klog.V(5).Enabled() {
				for _, node := range nodes {
					klog.V(5).Infof("node %v, idle: %v, future idle: %v", node.Name, node.Idle, node.FutureIdle())
				}
			}
			switch {
			case len(nodes) == 0:
				klog.V(5).Infof("Task: %v, no matching node is found in the candidateNodes（index: %d） list.", task.Name, index)
			case len(nodes) == 1: // If only one node after predicate, just use it.
				// 如果当前可用节点只有一个,毋庸置疑,直接使用这个节点即可,不需要再优选节点
				bestNode = nodes[0]
			case len(nodes) > 1: // If more than one node after predicate, using "the best" one
				nodeScores := util.PrioritizeNodes(task, nodes, ssn.BatchNodeOrderFn, ssn.NodeOrderMapFn, ssn.NodeOrderReduceFn)

				bestNode = ssn.BestNodeFn(task, nodeScores)
				if bestNode == nil {
					bestNode = util.SelectBestNode(nodeScores)
				}
			}

			// If a proper node is found in idleCandidateNodes, skip futureIdleCandidateNodes and directly return the node information.
			// 如果已经找到了合适的节点,直接退出
			if bestNode != nil {
				break
			}
		}

		// Allocate idle resource to the task.
		// 再次确认一下当前优选出来的节点是否满足当前Task的资源需求,如果满足,那么就将当前Task分配到这个节点上
		if task.InitResreq.LessEqual(bestNode.Idle, api.Zero) {
			klog.V(3).Infof("Binding Task <%v/%v> to node <%v>",
				task.Namespace, task.Name, bestNode.Name)
			// TODO 这里主要干了啥?
			if err := stmt.Allocate(task, bestNode); err != nil {
				klog.Errorf("Failed to bind Task %v on %v in Session %v, err: %v",
					task.UID, bestNode.Name, ssn.UID, err)
			} else {
				metrics.UpdateE2eSchedulingDurationByJob(job.Name, string(job.Queue), job.Namespace, metrics.Duration(job.CreationTimestamp.Time))
				metrics.UpdateE2eSchedulingLastTimeByJob(job.Name, string(job.Queue), job.Namespace, time.Now())
			}
		} else {
			klog.V(3).Infof("Predicates failed in allocate for task <%s/%s> on node <%s> with limited resources",
				task.Namespace, task.Name, bestNode.Name)

			// Allocate releasing resource to the task if any.
			if task.InitResreq.LessEqual(bestNode.FutureIdle(), api.Zero) {
				klog.V(3).Infof("Pipelining Task <%v/%v> to node <%v> for <%v> on <%v>",
					task.Namespace, task.Name, bestNode.Name, task.InitResreq, bestNode.Releasing)
				// TODO 什么叫做Pipeline?
				if err := stmt.Pipeline(task, bestNode.Name); err != nil {
					klog.Errorf("Failed to pipeline Task %v on %v in Session %v for %v.",
						task.UID, bestNode.Name, ssn.UID, err)
				} else {
					metrics.UpdateE2eSchedulingDurationByJob(job.Name, string(job.Queue), job.Namespace, metrics.Duration(job.CreationTimestamp.Time))
					metrics.UpdateE2eSchedulingLastTimeByJob(job.Name, string(job.Queue), job.Namespace, time.Now())
				}
			}
		}

		if ssn.JobReady(job) && !tasks.Empty() {
			jobs.Push(job)
			break
		}
	}

	if ssn.JobReady(job) {
		stmt.Commit()
	} else {
		if !ssn.JobPipelined(job) {
			stmt.Discard()
		}
	}
}

// 判断当前节点是否适合部署当前Task
func (alloc *Action) predicate(task *api.TaskInfo, node *api.NodeInfo) ([]*api.Status, error) {
	// Check for Resource Predicate
	if ok, resources := task.InitResreq.LessEqualWithResourcesName(node.FutureIdle(), api.Zero); !ok {
		return nil, api.NewFitError(task, node, api.WrapInsufficientResourceReason(resources))
	}
	var statusSets util.StatusSets
	statusSets, err := alloc.session.PredicateFn(task, node)
	if err != nil {
		return nil, api.NewFitError(task, node, err.Error())
	}

	if statusSets.ContainsUnschedulable() || statusSets.ContainsUnschedulableAndUnresolvable() ||
		statusSets.ContainsErrorSkipOrWait() {
		return nil, api.NewFitError(task, node, statusSets.Message())
	}
	return nil, nil
}

func (alloc *Action) UnInitialize() {}
