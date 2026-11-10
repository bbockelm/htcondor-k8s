package podctl

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// desiredStatus is the Pod status this job ad implies.
//
// The mapping itself is translate.PodStatus, shared with the facade and the
// operator so that the same job ad cannot mean two different things depending
// on who read it. What is added here is the handful of fields that are facts
// about *this* architecture rather than about the job:
//
//   - the image, which the shared mapping takes from the ad and the status
//     poll's projection deliberately does not fetch;
//   - a container id, so `kubectl describe pod` names something a human can
//     take to condor_q;
//   - startTime, which must never move;
//   - qosClass, which the API server computed at create and which a status
//     update must not drop;
//   - the reason for a removal we asked for ourselves.
//
// It is worth restating why accuracy here is not the usual "status is only for
// humans". The built-in Job controller counts these phases to decide whether
// the user's Job is complete, whether to retry, and whether backoffLimit is
// exhausted. A phase that is merely approximately right corrupts an object
// this integration never writes and cannot repair.
func (r *PodReconciler) desiredStatus(pod *corev1.Pod, ad translate.JobAd) corev1.PodStatus {
	containerName := ""
	if len(pod.Spec.Containers) > 0 {
		containerName = pod.Spec.Containers[0].Name
	}

	st := translate.PodStatus(ad, containerName)

	if len(st.ContainerStatuses) > 0 {
		cs := &st.ContainerStatuses[0]
		if cs.Image == "" && len(pod.Spec.Containers) > 0 {
			// condor.Projection omits K8sPodSpec on purpose -- pulling
			// a whole PodSpec on every poll of every job would make
			// the poll cost scale with template size -- so the ad has
			// no image to report. The Pod's own spec has it, and it
			// is the same string that was rendered into
			// container_image.
			cs.Image = pod.Spec.Containers[0].Image
		}
		if ad.ClusterID != 0 {
			// Not a real container runtime id, and not pretending to
			// be one: the scheme says where to look it up.
			cs.ContainerID = "htcondor://" + ad.JobID()
		}
	}

	// startTime is the instant Kubernetes believes work began on this Pod,
	// and it is only ever allowed to be set once. translate.PodStatus
	// derives it from JobStartDate, which is nil while a job is idle --
	// that would leave a Pending Pod with no startTime and then move it
	// when the job matched, which is a backwards step for anything
	// measuring from it. QDate is the right instant in this architecture,
	// because a Pod is submitted the moment it is taken: queue entry is
	// when "the node" began working on it.
	switch {
	case pod.Status.StartTime != nil:
		st.StartTime = pod.Status.StartTime
	case !ad.QDate.IsZero():
		t := metav1.NewTime(ad.QDate)
		st.StartTime = &t
	}

	// The API server computes qosClass at create and a status update that
	// omits it is a status update that erases it.
	st.QOSClass = pod.Status.QOSClass

	// A job removed by the periodic_remove this controller rendered for a
	// Pod-level activeDeadlineSeconds is not a generic removal, and
	// DeadlineExceeded is the word every other part of Kubernetes uses for
	// it.
	if ad.JobStatus == translate.JobStatusRemoved && pod.Spec.ActiveDeadlineSeconds != nil {
		deadline := *pod.Spec.ActiveDeadlineSeconds
		if !ad.QDate.IsZero() && !ad.EnteredCurrentStatus.IsZero() &&
			int64(ad.EnteredCurrentStatus.Sub(ad.QDate).Seconds()) >= deadline {
			st.Reason = reasonDeadlineExceeded
			st.Message = fmt.Sprintf("pod exceeded its activeDeadlineSeconds of %d", deadline)
			if len(st.ContainerStatuses) > 0 && st.ContainerStatuses[0].State.Terminated != nil {
				st.ContainerStatuses[0].State.Terminated.Reason = reasonDeadlineExceeded
			}
		}
	}

	// Round every timestamp down to the second before this status is
	// compared with, or written over, what the API server has.
	//
	// metav1.Time serializes as RFC3339 with second precision, so whatever
	// is written comes back truncated. A status computed with the
	// nanoseconds still on it therefore never equals the stored one, and
	// statusEqual below would report a change on every single poll --
	// which is the exact write amplification it exists to prevent, and
	// worse, a watch event waking the Job controller each time.
	truncateTimes(&st)
	return st
}

// truncateTimes makes a computed status comparable with a stored one.
func truncateTimes(st *corev1.PodStatus) {
	trunc := func(t *metav1.Time) {
		if t != nil && !t.IsZero() {
			*t = metav1.NewTime(t.Truncate(time.Second))
		}
	}
	trunc(st.StartTime)
	for i := range st.Conditions {
		trunc(&st.Conditions[i].LastTransitionTime)
		trunc(&st.Conditions[i].LastProbeTime)
	}
	for i := range st.ContainerStatuses {
		cs := &st.ContainerStatuses[i]
		if r := cs.State.Running; r != nil {
			trunc(&r.StartedAt)
		}
		if tm := cs.State.Terminated; tm != nil {
			trunc(&tm.StartedAt)
			trunc(&tm.FinishedAt)
		}
	}
}

// settled reports whether a job ad is complete enough to derive a terminal
// phase from.
//
// A Completed job always has a CompletionDate, and always has either an
// ExitCode or ExitBySignal. An ad that says Completed and carries neither is
// one the schedd has not finished writing, or one a projection failed to
// satisfy -- and translate.JobAd's zero value for ExitCode is 0, which is to
// say "succeeded". That is the one wrong answer this integration cannot afford
// to give, so the ad is not believed until it is whole.
//
// The failure mode of being wrong in this direction is a Pod that stays
// Running one poll longer than it should, which is visible. The failure mode
// of the other direction is a Job that reports a completion that never
// happened, which is not.
func settled(ad translate.JobAd) bool {
	if ad.JobStatus != translate.JobStatusCompleted {
		return true
	}
	return !ad.CompletionDate.IsZero() || ad.ExitCode != 0 || ad.ExitBySignal
}

// statusEqual decides whether a status write is worth making.
//
// Without it every poll writes a status identical to the one already stored,
// which costs an etcd write and a watch event per running Pod per poll
// interval, and wakes the Job controller each time for nothing. The conditions
// carry a LastTransitionTime taken from the job's EnteredCurrentStatus rather
// than from the clock, which is what makes a plain comparison stable: an
// unchanged job produces a byte-identical status.
func statusEqual(a, b corev1.PodStatus) bool {
	return apiequality.Semantic.DeepEqual(a, b)
}
