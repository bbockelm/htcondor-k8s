package v1alpha1

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Annotations and labels the operator writes onto the objects it claims.
const (
	// AnnotationClaimed is stamped on a batch/v1 Job by the mutating
	// webhook once translate.AdmitJob has passed. Its presence, together
	// with SchedulerName, is what the Job controller keys on; a Job
	// without it is not ours no matter what else it says.
	AnnotationClaimed = "htcondor.org/claimed"

	// AnnotationClaimedAt records when the claim happened, which is the
	// only way to tell a Job claimed by this build of the webhook from one
	// claimed before a capabilities change.
	AnnotationClaimedAt = "htcondor.org/claimed-at"

	// AnnotationUserSuspend carries the user's own .spec.suspend intent.
	//
	// The webhook forces .spec.suspend true on every claimed Job so the
	// built-in Job controller creates no Pods at all -- otherwise it makes
	// Pods that name a scheduler nobody runs, and they stay Pending
	// forever. That overwrite destroys the field's usual meaning, so the
	// user's value is preserved here and is what Suspend is derived from.
	// Flipping this annotation is how a user resumes a claimed Job.
	AnnotationUserSuspend = "htcondor.org/suspend"

	// AnnotationSkip opts a Job out of claiming entirely. A user who wants
	// the default scheduler for a Job that would otherwise translate sets
	// this, and the webhook leaves the object byte-identical.
	AnnotationSkip = "htcondor.org/skip"

	// SchedulerName is the scheduler a claimed Job's pod template names.
	// Nothing in this operator schedules Pods -- it is a fence, so that if
	// anything ever does create a Pod from a claimed template, kube-scheduler
	// will not also try to place it.
	SchedulerName = "htcondor"

	// LabelOwnerUID lets the HTCJob for a Job be found without a field index.
	LabelOwnerUID = "htcondor.org/owner-uid"
)

// Finalizer guards the job queue: an HTCJob is not removed from etcd until
// its procs have been removed from the schedd, because once the CR is gone
// nothing remembers which cluster to remove.
const Finalizer = "htcondor.org/job-queue"

// HTCJobSpec is the subset of batch/v1 JobSpec that an HTCondor pool can run
// faithfully.
//
// It is written out longhand rather than embedding batchv1.JobSpec on purpose.
// Embedding would accept `podFailurePolicy`, `tolerations`, `initContainers`
// and the rest into etcd and leave a controller to explain at runtime that it
// ignored them. Here they are not fields, so the API server's own structural
// schema rejects them at `kubectl apply` time with a field-level error, and no
// object ever exists that promises something the pool will not do.
type HTCJobSpec struct {
	// Completions is the number of procs to queue in the HTCondor cluster.
	// Unlike batch/v1 there is no Parallelism: HTCondor starts every idle
	// proc as soon as a slot matches, and throttling would mean the
	// controller holding and releasing procs by hand.
	//
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	// +optional
	Completions *int32 `json:"completions,omitempty"`

	// CompletionMode Indexed exports JOB_COMPLETION_INDEX to each proc.
	//
	// +kubebuilder:validation:Enum=NonIndexed;Indexed
	// +kubebuilder:default=NonIndexed
	// +optional
	CompletionMode string `json:"completionMode,omitempty"`

	// BackoffLimit becomes max_retries. It is honored only when the
	// template's restartPolicy is OnFailure.
	//
	// +kubebuilder:validation:Minimum=0
	// +optional
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`

	// ActiveDeadlineSeconds becomes a periodic_remove on wall-clock runtime.
	//
	// +kubebuilder:validation:Minimum=1
	// +optional
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty"`

	// Suspend true means the procs are held. Flipping it releases them; the
	// job keeps its cluster id and its place in the queue either way, which
	// is the one place HTCondor's model is strictly nicer than deleting and
	// recreating Pods.
	//
	// +optional
	Suspend *bool `json:"suspend,omitempty"`

	// Template is the single container and its environment.
	Template HTCPodTemplateSpec `json:"template"`
}

// HTCPodTemplateSpec is a pod template with only the metadata a job ad can
// carry. There is no full ObjectMeta: ownerReferences, finalizers and
// generateName on a template that never produces a Pod would be fiction.
type HTCPodTemplateSpec struct {
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	Spec HTCPodSpec `json:"spec"`
}

// HTCPodSpec is the executable shape of an HTCondor job.
type HTCPodSpec struct {
	// Container is singular. A Pod's containers share a network namespace
	// and a lifecycle; an HTCondor job is one process tree on one slot, so
	// a list here would be a list that may only ever hold one element.
	Container HTCContainer `json:"container"`

	// RestartPolicy Never leaves a failed job failed; OnFailure lets
	// BackoffLimit retry it.
	//
	// +kubebuilder:validation:Enum=Never;OnFailure
	// +kubebuilder:default=Never
	// +optional
	RestartPolicy string `json:"restartPolicy,omitempty"`

	// NodeSelector keys must be ones the pool has been configured to
	// express as a machine ClassAd attribute; an unknown key is rejected at
	// admission rather than dropped.
	//
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Volumes may only be emptyDir, which becomes a subdirectory of the job
	// scratch directory. The field is a list of the one supported source,
	// not a VolumeSource union with every type the kubelet knows.
	//
	// +optional
	Volumes []HTCVolume `json:"volumes,omitempty"`
}

// HTCContainer is one container image and how to run it.
type HTCContainer struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Image is passed to container_image. A bare name is a Docker
	// reference; docker:// and the various osdf/apptainer forms work too.
	//
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// Command overrides the image's ENTRYPOINT, as in a Pod. Left empty,
	// the image's own entrypoint runs: the container universe expresses
	// that with an empty executable, so nothing has to pull the image
	// manifest to find out what it is.
	//
	// +optional
	Command []string `json:"command,omitempty"`

	// +optional
	Args []string `json:"args,omitempty"`

	// +optional
	WorkingDir string `json:"workingDir,omitempty"`

	// Env holds literal values only. There is no ValueFrom: a job runs on a
	// machine with no API server to read a Secret or a field reference from.
	//
	// +optional
	Env []HTCEnvVar `json:"env,omitempty"`

	// Resources uses the Kubernetes types because the quantity parsing and
	// the requests/limits distinction both translate exactly. cpu and
	// memory are required; ephemeral-storage defaults; anything else must
	// be a GPU resource the pool advertises.
	//
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// +optional
	VolumeMounts []HTCVolumeMount `json:"volumeMounts,omitempty"`
}

// HTCEnvVar is a literal environment entry.
type HTCEnvVar struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +optional
	Value string `json:"value,omitempty"`
}

// HTCVolume is a scratch directory. Name is the handle a mount refers to.
type HTCVolume struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// EmptyDir must be present; it is the only source there is. It is a
	// struct rather than a bare bool so that a future SizeLimit does not
	// change the field's type.
	EmptyDir HTCEmptyDirVolumeSource `json:"emptyDir"`
}

// HTCEmptyDirVolumeSource is scratch space on the execute node's disk. There
// is no Medium: emptyDir on tmpfs would eat RAM outside the memory request,
// which HTCondor accounts for and would evict the job over.
type HTCEmptyDirVolumeSource struct {
	// SizeLimit is advisory today and is added to request_disk.
	//
	// +optional
	SizeLimit *int64 `json:"sizeLimitBytes,omitempty"`
}

// HTCVolumeMount binds a volume into the container. There is no SubPath and no
// MountPropagation.
type HTCVolumeMount struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	MountPath string `json:"mountPath"`
	// +optional
	ReadOnly bool `json:"readOnly,omitempty"`
}

// HTCJobPhase is a coarse summary for `kubectl get`.
type HTCJobPhase string

const (
	// PhasePending means the CR exists but nothing is in the queue yet.
	PhasePending HTCJobPhase = "Pending"
	// PhaseSubmitted means a cluster id is known and procs are idle.
	PhaseSubmitted HTCJobPhase = "Submitted"
	// PhaseRunning means at least one proc has a slot.
	PhaseRunning HTCJobPhase = "Running"
	// PhaseSucceeded means every requested completion finished with 0.
	PhaseSucceeded HTCJobPhase = "Succeeded"
	// PhaseFailed means no proc is left and at least one failed.
	PhaseFailed HTCJobPhase = "Failed"
	// PhaseRejected means the spec could not be translated. It is a dead
	// end: nothing will be submitted without the spec changing.
	PhaseRejected HTCJobPhase = "Rejected"
)

// Condition types on an HTCJob.
const (
	// ConditionAdmitted is False with the full rejection list when the
	// controller refused to translate the spec. A CR can exist and be
	// inadmissible -- the API server's schema catches shape errors, but
	// pool capabilities (GPU names, slot sizes, nodeSelector keys) are
	// runtime facts no schema can encode.
	ConditionAdmitted = "Admitted"
	// ConditionSubmitted is True once a cluster id is recorded.
	ConditionSubmitted = "Submitted"
	// ConditionQueueReachable is False when the schedd could not be reached
	// on the last attempt. It is reported separately from Submitted so that
	// "we do not know" is distinguishable from "it is not there".
	ConditionQueueReachable = "QueueReachable"
	// ConditionComplete mirrors batch/v1's JobComplete.
	ConditionComplete = "Complete"
	// ConditionFailed mirrors batch/v1's JobFailed.
	ConditionFailed = "Failed"
)

// HTCJobStatus is what the controller knows about the job queue.
type HTCJobStatus struct {
	// ClusterID is the HTCondor cluster. Its presence is the record that a
	// submission happened; the controller will not submit again while it is
	// set, and reconstructs it by querying on the owner UID when a crash
	// lost the status write.
	//
	// +optional
	ClusterID string `json:"clusterID,omitempty"`

	// +optional
	Phase HTCJobPhase `json:"phase,omitempty"`

	// ObservedGeneration is the .metadata.generation the rest of this
	// status was computed from.
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Active, Succeeded and Failed are proc counts, in the same sense as a
	// batch/v1 JobStatus, so they can be mirrored onto the owning Job.
	//
	// +optional
	Active int32 `json:"active,omitempty"`
	// +optional
	Succeeded int32 `json:"succeeded,omitempty"`
	// +optional
	Failed int32 `json:"failed,omitempty"`

	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// LastQueueSync is when the job ads behind this status were read. A
	// status with an old LastQueueSync is stale, not wrong, and the
	// difference matters when the schedd has been unreachable.
	//
	// +optional
	LastQueueSync *metav1.Time `json:"lastQueueSync,omitempty"`

	// Procs is the per-proc detail. It is capped by the CRD so that a
	// 100k-completion job cannot turn one CR into an object too large for
	// etcd; past the cap only the counts above are maintained.
	//
	// +kubebuilder:validation:MaxItems=256
	// +optional
	Procs []HTCProcStatus `json:"procs,omitempty"`

	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// HTCProcStatus is one proc of the cluster, named the way condor_q names it.
type HTCProcStatus struct {
	ProcID int32 `json:"procID"`

	// JobStatus is the raw HTCondor status code (1 idle, 2 running, 5 held,
	// ...). It is reported unmapped alongside Phase because the mapping is
	// lossy -- a held job is neither Pending nor Failed -- and an operator
	// debugging a pool wants the number condor_q showed them.
	JobStatus int32 `json:"jobStatus"`

	// Phase is the Kubernetes reading of JobStatus.
	//
	// +optional
	Phase string `json:"phase,omitempty"`

	// +optional
	ExitCode *int32 `json:"exitCode,omitempty"`
	// +optional
	HoldReason string `json:"holdReason,omitempty"`
	// +optional
	RemoteHost string `json:"remoteHost,omitempty"`
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
	// Index is the completion index for an Indexed job, or nil.
	// +optional
	Index *int32 `json:"index,omitempty"`
}

// JobID is the cluster.proc identifier condor_rm wants.
func (p HTCProcStatus) JobID(clusterID string) string {
	return clusterID + "." + strconv.Itoa(int(p.ProcID))
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=htcj
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.status.clusterID`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Active",type=integer,JSONPath=`.status.active`
// +kubebuilder:printcolumn:name="Succeeded",type=integer,JSONPath=`.status.succeeded`
// +kubebuilder:printcolumn:name="Failed",type=integer,JSONPath=`.status.failed`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// HTCJob is one HTCondor cluster expressed as a Kubernetes object.
type HTCJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec HTCJobSpec `json:"spec,omitempty"`
	// +optional
	Status HTCJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// HTCJobList is a list of HTCJob.
type HTCJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HTCJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HTCJob{}, &HTCJobList{})
}
