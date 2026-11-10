package translate

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// HTCondor JobStatus values.
const (
	JobStatusIdle      = 1
	JobStatusRunning   = 2
	JobStatusRemoved   = 3
	JobStatusCompleted = 4
	JobStatusHeld      = 5
	JobStatusXferOut   = 6
	JobStatusSuspended = 7
)

// JobAd is the subset of a job ad this package reads. Callers populate it from
// a classad.ClassAd (or an htcondordb row) so that this module stays free of
// any HTCondor client dependency — the two integrations are on different
// classad library versions, and a shared struct keeps that skew out of here.
type JobAd struct {
	ClusterID int
	ProcID    int

	JobStatus int
	// ExitCode and ExitBySignal describe how the job ended. ExitCode is
	// only meaningful once the job has left the queue normally.
	ExitCode       int
	ExitBySignal   bool
	ExitSignal     int
	HoldReason     string
	HoldReasonCode int

	QDate                time.Time
	JobStartDate         time.Time
	EnteredCurrentStatus time.Time
	CompletionDate       time.Time

	RemoteHost   string
	NumJobStarts int

	// Identity as read back from the custom attributes.
	Identity Identity
	// Index is K8sCompletionIndex, or -1 when absent.
	Index int
}

// PodStatus maps a job ad onto the Pod status a kubectl user expects to see.
//
// The hard case is Held. A held job is neither pending nor failed in
// HTCondor's eyes — it is waiting for a human. Kubernetes has no such phase, so
// the mapping depends on why: a hold the system placed because the job cannot
// run (bad image, failed transfer) is surfaced as Failed, because waiting for
// it is waiting forever; a hold a user placed is Pending with an explanatory
// condition, because releasing it is a normal thing to do.
func PodStatus(ad JobAd, containerName string) corev1.PodStatus {
	st := corev1.PodStatus{}
	cs := corev1.ContainerStatus{
		Name:  containerName,
		Image: "",
	}

	switch ad.JobStatus {
	case JobStatusIdle:
		st.Phase = corev1.PodPending
		st.Reason = "Idle"
		st.Message = "waiting for a matching slot"
		cs.State.Waiting = &corev1.ContainerStateWaiting{Reason: "Idle", Message: st.Message}

	case JobStatusRunning, JobStatusXferOut, JobStatusSuspended:
		st.Phase = corev1.PodRunning
		cs.Ready = ad.JobStatus == JobStatusRunning
		cs.Started = boolPtr(true)
		cs.State.Running = &corev1.ContainerStateRunning{
			StartedAt: metav1.NewTime(ad.JobStartDate),
		}
		if ad.JobStatus == JobStatusXferOut {
			st.Reason = "TransferringOutput"
		} else if ad.JobStatus == JobStatusSuspended {
			st.Reason = "Suspended"
		}

	case JobStatusCompleted:
		if ad.ExitBySignal {
			st.Phase = corev1.PodFailed
			st.Reason = "Error"
			st.Message = fmt.Sprintf("job terminated by signal %d", ad.ExitSignal)
		} else if ad.ExitCode == 0 {
			st.Phase = corev1.PodSucceeded
		} else {
			st.Phase = corev1.PodFailed
			st.Reason = "Error"
			st.Message = fmt.Sprintf("job exited with code %d", ad.ExitCode)
		}
		cs.State.Terminated = terminated(ad, st.Reason)

	case JobStatusRemoved:
		st.Phase = corev1.PodFailed
		st.Reason = "Removed"
		st.Message = "job was removed from the queue"
		cs.State.Terminated = terminated(ad, "Removed")

	case JobStatusHeld:
		if HoldIsTerminal(ad.HoldReasonCode) {
			st.Phase = corev1.PodFailed
			st.Reason = "Held"
			st.Message = ad.HoldReason
			cs.State.Terminated = terminated(ad, "Held")
		} else {
			st.Phase = corev1.PodPending
			st.Reason = "Held"
			st.Message = ad.HoldReason
			cs.State.Waiting = &corev1.ContainerStateWaiting{Reason: "Held", Message: ad.HoldReason}
		}

	default:
		st.Phase = corev1.PodUnknown
		st.Reason = fmt.Sprintf("JobStatus%d", ad.JobStatus)
	}

	cs.RestartCount = int32(max(0, ad.NumJobStarts-1))
	st.ContainerStatuses = []corev1.ContainerStatus{cs}
	st.HostIP = ""
	st.StartTime = timePtr(ad.JobStartDate)
	st.Conditions = podConditions(st.Phase, ad)
	return st
}

// HoldIsTerminal reports whether a hold means the job will never run without
// someone changing the submission itself.
//
// The codes are HTCondor's, from condor_holdcodes.h. Only a handful of holds
// are states a job passes through or is routinely released from; the rest mean
// the submission itself is wrong, and reporting Pending for those is reporting
// a wait that will never end.
func HoldIsTerminal(code int) bool {
	switch code {
	case 0:
		// Unspecified: no hold at all.
		return false
	case 1, // UserRequest: somebody held it, somebody can release it.
		15, // SubmittedOnHold: the submitter asked for this.
		16, // SpoolingInput: a routine step of a remote submission.
		21: // StartdHeldJob: the EP held it; another EP may not.
		return false
	case 3, // JobPolicy: the job's own periodic_hold fired.
		4,           // CorruptedCredential
		6,           // FailedToCreateProcess
		7, 8, 9, 10, // unable to open output/input (stream)
		12,     // TransferOutputError
		13,     // TransferInputError
		14,     // IwdError
		19,     // HookPrepareJobFailure
		22,     // UnableToInitUserLog
		23,     // FailedToAccessUserAccount
		24,     // NoCompatibleShadow
		25,     // InvalidCronSettings
		26,     // SystemPolicy: the pool's own SYSTEM_PERIODIC_HOLD fired.
		32, 33, // Max transfer input/output size exceeded
		34,     // JobOutOfResources: it asked for less than it used.
		35,     // InvalidDockerImage: this is ImagePullBackOff by another name.
		36,     // FailedToCheckpoint
		43, 44, // Pre/PostScriptFailed
		45,     // SingularityTestFailed
		46, 47, // JobDuration/JobExecute exceeded
		48, // HookShadowPrepareJobFailure
		49: // CannotSwitchPrimaryGroup
		return true
	default:
		// An unclassified hold is treated as terminal. A hold nobody has
		// a rule for is not going to clear on its own, and a Pod stuck
		// Pending forever is the worse failure mode: it is invisible,
		// whereas a wrong Failed gets reported as a bug and fixed here.
		return true
	}
}

func podConditions(phase corev1.PodPhase, ad JobAd) []corev1.PodCondition {
	now := metav1.NewTime(ad.EnteredCurrentStatus)
	cond := func(t corev1.PodConditionType, ok bool, reason, msg string) corev1.PodCondition {
		s := corev1.ConditionFalse
		if ok {
			s = corev1.ConditionTrue
		}
		return corev1.PodCondition{
			Type: t, Status: s, Reason: reason, Message: msg,
			LastTransitionTime: now,
		}
	}
	scheduled := ad.JobStatus != JobStatusIdle
	running := phase == corev1.PodRunning
	return []corev1.PodCondition{
		cond(corev1.PodScheduled, scheduled, "Matched", ""),
		cond(corev1.PodInitialized, scheduled, "", ""),
		cond(corev1.ContainersReady, running, "", ""),
		cond(corev1.PodReady, running, "", ""),
	}
}

func terminated(ad JobAd, reason string) *corev1.ContainerStateTerminated {
	t := &corev1.ContainerStateTerminated{
		ExitCode:   int32(ad.ExitCode),
		Reason:     reason,
		StartedAt:  metav1.NewTime(ad.JobStartDate),
		FinishedAt: metav1.NewTime(ad.CompletionDate),
	}
	if ad.ExitBySignal {
		t.Signal = int32(ad.ExitSignal)
		// Kubernetes reports a signalled exit as 128+signal.
		t.ExitCode = int32(128 + ad.ExitSignal)
	}
	if reason == "" {
		t.Reason = "Completed"
	}
	return t
}

// JobID is the "cluster.proc" identifier of a job.
func (a JobAd) JobID() string { return fmt.Sprintf("%d.%d", a.ClusterID, a.ProcID) }

// Terminal reports whether the job has reached a state it will not leave on
// its own, which is when a watcher may stop following it.
func (a JobAd) Terminal() bool {
	switch a.JobStatus {
	case JobStatusCompleted, JobStatusRemoved:
		return true
	case JobStatusHeld:
		return HoldIsTerminal(a.HoldReasonCode)
	}
	return false
}

func boolPtr(b bool) *bool { return &b }

func timePtr(t time.Time) *metav1.Time {
	if t.IsZero() {
		return nil
	}
	mt := metav1.NewTime(t)
	return &mt
}
