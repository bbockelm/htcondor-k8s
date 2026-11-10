// Package api holds the names this approach writes into Kubernetes objects.
//
// They live in one place because three components key on them and a typo in
// any one of them is a silent no-op: the webhook stamps SchedulerName, the
// node controller taints the node with TaintKey, and the pod controller
// selects on SchedulerName and owns Finalizer.
package api

// SchedulerName is the value the webhook writes into a claimed Pod template's
// .spec.schedulerName.
//
// It is load-bearing here in a way it is not in the HTCJob approach. The pod
// controller binds the Pod to the virtual node itself, so the only thing
// standing between an unbound Pod and kube-scheduler placing it on a real
// machine is kube-scheduler's own check that the name is not its own. Without
// it there is a race whose loser is a job running in two places.
const SchedulerName = "htcondor"

// NodeName is the Node object this virtual kubelet registers and binds to.
const NodeName = "htcondor-virtual-node"

// TaintKey keeps other people's Pods off the virtual node.
//
// The taint is NoSchedule, deliberately not NoExecute. NoExecute would bring
// the taint manager into the picture, and a node that misses a few heartbeats
// would have its Pods evicted out from under running HTCondor jobs. NoSchedule
// only talks to the scheduler, which is the component we actually need to keep
// away, and our own Pods never consult it because we bind them by hand.
const (
	TaintKey   = "htcondor.org/virtual-node"
	TaintValue = "true"
)

// Finalizer keeps the Pod object alive until the HTCondor job behind it is
// gone. Without it a `kubectl delete pod` returns while the job is still
// running, and nothing is left holding the job id.
const Finalizer = "htcondor.org/job"

// Annotations the pod controller stamps onto a Pod it has taken.
const (
	// AnnotationJobID is the HTCondor cluster.proc, for a human reading
	// `kubectl describe pod` who needs to run condor_q.
	AnnotationJobID = "htcondor.org/job-id"

	// AnnotationSchedd names the access point, because a job id is only
	// unique within one queue.
	AnnotationSchedd = "htcondor.org/schedd"

	// AnnotationSpoolDir is the job's initialdir, where stdout and stderr
	// land. The log endpoint reads it back off the Pod rather than
	// re-deriving it, so a change to the naming scheme cannot strand the
	// output of a Pod submitted before the change.
	AnnotationSpoolDir = "htcondor.org/spool-dir"

	// AnnotationHoldReason carries HTCondor's own words when a job is
	// held, which is the one piece of diagnosis Kubernetes has no field
	// for.
	AnnotationHoldReason = "htcondor.org/hold-reason"
)
