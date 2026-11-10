package translate

import (
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AdmitJob checks a batch/v1 Job: the pod template against [Admit], plus the
// Job-level fields that change what running it means.
//
// This is the function the mutating webhook calls to decide whether to claim a
// Job for HTCondor. A Job it cannot model is left alone for the default
// scheduler, so the bar here is "can we run this faithfully", never "should we
// try".
func AdmitJob(spec *batchv1.JobSpec, meta *metav1.ObjectMeta, caps Capabilities) Rejections {
	if spec == nil {
		var rs Rejections
		rs.add("spec", ReasonInvalidValue, "job spec is empty")
		return rs
	}
	rs := Admit(&spec.Template.Spec, meta, caps)

	if spec.Parallelism != nil && spec.Completions != nil && *spec.Parallelism != *spec.Completions {
		// HTCondor runs every idle proc of a cluster as soon as a slot
		// matches. Throttling to .spec.parallelism would need the
		// controller to hold and release procs, which is real work we
		// have not done; accepting it silently would overrun the limit.
		rs.add("spec.parallelism", ReasonJobFieldUnsupported,
			"parallelism below completions is not throttled yet; all procs run as slots match")
	}
	if spec.CompletionMode != nil && *spec.CompletionMode == batchv1.IndexedCompletion && spec.Completions == nil {
		rs.add("spec.completions", ReasonInvalidValue, "indexed completion requires completions")
	}
	if spec.PodFailurePolicy != nil {
		rs.add("spec.podFailurePolicy", ReasonJobFieldUnsupported,
			"per-exit-code failure policy is not translated; use backoffLimit")
	}
	if spec.SuccessPolicy != nil {
		rs.add("spec.successPolicy", ReasonJobFieldUnsupported, "success policy is not translated")
	}
	// Only a *manually* set selector is a problem. The Job registry fills
	// in .spec.selector and the batch.kubernetes.io template labels in
	// PrepareForCreate, which runs after mutating admission --- so a
	// webhook sees no selector and claims the Job, while every later look
	// at the stored object sees one. Keying on the presence of the field
	// would make a claimed Job fail admission forever. ManualSelector is
	// the API's own flag for "the user wrote this".
	if spec.ManualSelector != nil && *spec.ManualSelector {
		rs.add("spec.manualSelector", ReasonJobFieldUnsupported,
			"a manually set selector implies adopting existing pods, which do not exist here")
	}
	if spec.TTLSecondsAfterFinished != nil {
		// Harmless to honor later, but today nothing reaps the job from
		// the queue on that schedule, so saying we did would be a lie.
		rs.add("spec.ttlSecondsAfterFinished", ReasonJobFieldUnsupported,
			"finished jobs are reaped by the schedd's own history policy")
	}
	return rs
}

// JobSubmitOptions derives the submit-file knobs from a Job spec. Call it only
// after AdmitJob has passed.
func JobSubmitOptions(spec *batchv1.JobSpec, caps Capabilities) SubmitOptions {
	opts := SubmitOptions{Caps: caps, Count: 1}
	if spec == nil {
		return opts
	}
	if spec.Completions != nil && *spec.Completions > 0 {
		opts.Count = int(*spec.Completions)
	}
	if spec.CompletionMode != nil && *spec.CompletionMode == batchv1.IndexedCompletion {
		opts.Indexed = true
	}
	if spec.BackoffLimit != nil {
		opts.MaxRetries = int(*spec.BackoffLimit)
	}
	if spec.ActiveDeadlineSeconds != nil {
		opts.ActiveDeadlineSeconds = *spec.ActiveDeadlineSeconds
	}
	if spec.Suspend != nil {
		opts.Suspend = *spec.Suspend
	}
	return opts
}

// JobStatus aggregates the per-proc job ads of one cluster into a batch/v1
// JobStatus. completions is .spec.completions.
func JobStatus(ads []JobAd, completions int32, startTime *metav1.Time) batchv1.JobStatus {
	st := batchv1.JobStatus{StartTime: startTime}
	var latestFinish metav1.Time
	for _, ad := range ads {
		switch {
		case ad.JobStatus == JobStatusCompleted && !ad.ExitBySignal && ad.ExitCode == 0:
			st.Succeeded++
			if ft := metav1.NewTime(ad.CompletionDate); ft.After(latestFinish.Time) {
				latestFinish = ft
			}
		case ad.Terminal():
			st.Failed++
			if ft := metav1.NewTime(ad.CompletionDate); ft.After(latestFinish.Time) {
				latestFinish = ft
			}
		case ad.JobStatus == JobStatusRunning || ad.JobStatus == JobStatusXferOut || ad.JobStatus == JobStatusSuspended:
			st.Active++
		default:
			st.Active++
		}
	}

	// Kubernetes 1.31 split the terminal Job conditions in two, and since
	// 1.33 the API server refuses a status that sets Complete or Failed
	// without the interim condition that precedes it. Emitting both keeps
	// a caller's status write from being rejected outright.
	switch {
	case completions > 0 && st.Succeeded >= completions:
		st.CompletionTime = &latestFinish
		st.Conditions = append(st.Conditions,
			jobCondition(batchv1.JobSuccessCriteriaMet, "CompletionsReached",
				"every completion of the HTCondor cluster succeeded"),
			jobCondition(batchv1.JobComplete, "", ""))
	case st.Active == 0 && st.Failed > 0:
		const reason = "BackoffLimitExceeded"
		const msg = "every proc of the HTCondor cluster reached a terminal failure"
		st.Conditions = append(st.Conditions,
			jobCondition(batchv1.JobFailureTarget, reason, msg),
			jobCondition(batchv1.JobFailed, reason, msg))
	}
	return st
}

func jobCondition(t batchv1.JobConditionType, reason, msg string) batchv1.JobCondition {
	return batchv1.JobCondition{
		Type:               t,
		Status:             "True",
		Reason:             reason,
		Message:            msg,
		LastTransitionTime: metav1.Now(),
		LastProbeTime:      metav1.Now(),
	}
}
