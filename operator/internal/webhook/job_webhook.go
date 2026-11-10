// Package webhook holds the mutating webhook that claims batch/v1 Jobs for
// HTCondor.
//
// The webhook's whole job is to answer one question -- is this Job one an
// HTCondor pool can run faithfully? -- and to record the answer on the object
// when it is yes. It creates nothing: a mutating webhook may only return a
// patch for the object under admission, so the HTCJob that mirrors a claimed
// Job has to be created by a controller watching Jobs. That split is not a
// workaround, it is the right shape: webhook admission is synchronous and on
// the critical path of every Job create in the cluster, and anything that can
// block -- a schedd round trip, an API write, a retry -- does not belong there.
//
// The asymmetry that governs everything here: claiming a Job is an opinion,
// and refusing to claim one is not. A Job this webhook does not understand is
// returned untouched for the default scheduler, which means a bug in admission
// costs HTCondor a job it could have run, never Kubernetes a job it could have
// run. That is also why the manifest sets failurePolicy: Ignore -- an operator
// that is down must not stop a cluster from running Jobs.
package webhook

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-logr/logr"
	admissionv1 "k8s.io/api/admission/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/bbockelm/htcondor-k8s/operator/api/v1alpha1"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// Path is where the webhook server serves. It is spelled out rather than
// derived, because the MutatingWebhookConfiguration in config/webhook has to
// agree with it exactly and a mismatch fails open and silently.
const Path = "/mutate-batch-v1-job"

// Verdict is the webhook's decision about one Job, separated from the HTTP
// plumbing so it can be table-tested.
type Verdict struct {
	// Claim is true when the Job should be mutated.
	Claim bool
	// Why explains a decision either way, in one phrase, for the log.
	Why string
	// Rejections is why translation failed, when it did. It is kept so the
	// log can say which field lost the Job -- the user never sees it,
	// because a non-claim is silent by design.
	Rejections translate.Rejections
}

// Decide is the pure claim/don't-claim rule.
//
// op matters. On CREATE the Job has no Pods yet, so claiming it is free. On
// UPDATE it may already have running Pods under the default scheduler, and
// claiming it then would run the same work twice, so an unclaimed Job is never
// claimed on update no matter how well it translates.
func Decide(job *batchv1.Job, op admissionv1.Operation, caps translate.Capabilities) Verdict {
	if job == nil {
		return Verdict{Why: "no object"}
	}
	if _, ok := job.Annotations[v1alpha1.AnnotationSkip]; ok {
		return Verdict{Why: "opted out by annotation"}
	}
	if _, claimed := job.Annotations[v1alpha1.AnnotationClaimed]; claimed {
		// Already ours. Re-assert the claim so that an update which
		// tried to hand the Job back to kube-scheduler, or to let the
		// Job controller start making Pods, does not succeed.
		return Verdict{Claim: true, Why: "already claimed"}
	}
	if op != admissionv1.Create {
		return Verdict{Why: "not claiming an existing Job on update"}
	}
	if sched := job.Spec.Template.Spec.SchedulerName; sched != "" && sched != corev1.DefaultSchedulerName {
		// Somebody else's scheduler asked for this Job first.
		return Verdict{Why: fmt.Sprintf("scheduler %q already requested", sched)}
	}
	if rs := translate.AdmitJob(&job.Spec, &job.ObjectMeta, caps); !rs.OK() {
		return Verdict{Why: "not translatable", Rejections: rs}
	}
	return Verdict{Claim: true, Why: "translatable"}
}

// Claim applies the mutation. It is idempotent: running it on an
// already-claimed Job changes only the fields that drifted.
//
// Two things are written, and they do different work.
//
// schedulerName is a fence. Nothing in this operator schedules Pods, so the
// name is not a promise to schedule -- it is a guarantee that if a Pod is ever
// made from this template, kube-scheduler will leave it alone instead of
// placing it on a node while HTCondor is also running it.
//
// suspend is the actual mechanism. A suspended Job makes no Pods at all, which
// is the only way to stop the built-in Job controller from manufacturing Pod
// objects that will never be scheduled. The user's own suspend intent is moved
// into an annotation first, because the field no longer carries it.
// ClaimOptions selects how strongly a claimed Job is taken away from the
// built-in Job controller.
type ClaimOptions struct {
	// ManagedBy writes .spec.managedBy, which makes the built-in Job
	// controller skip the Job outright. It can only be set at CREATE,
	// because the field is immutable.
	ManagedBy bool

	// SuspendFallback forces .spec.suspend even when ManagedBy was
	// written.
	//
	// It is the answer to "what if the cluster ignores managedBy": on a
	// Kubernetes older than 1.32, or one with the JobManagedBy gate
	// explicitly off, the field is accepted and does nothing, the Job
	// controller makes Pods, and the work runs both here and there. The
	// cost of the belt is the asymmetric resume that suspend has always
	// had; the cost of going without it is running a user's job twice.
	SuspendFallback bool
}

// Claim applies the mutation.
func Claim(job *batchv1.Job, now time.Time) { ClaimWith(job, now, ClaimOptions{SuspendFallback: true}) }

// ClaimWith is Claim with the ownership strategy named explicitly.
//
// managedBy is only written when the field is still empty. It is immutable,
// so writing it on an update to a Job created before this operator existed
// would have the API server reject the whole update -- which, on the claimed
// path, means the user's update fails for a reason that has nothing to do
// with what they changed.
func ClaimWith(job *batchv1.Job, now time.Time, opts ClaimOptions) {
	if opts.ManagedBy && job.Spec.ManagedBy == nil {
		name := v1alpha1.ManagedByName
		job.Spec.ManagedBy = &name
	}
	claim(job, now, opts.SuspendFallback || !managedByUs(job))
}

// ManagedByUs reports whether the built-in Job controller has been told to
// leave this Job alone.
func ManagedByUs(job *batchv1.Job) bool { return job != nil && managedByUs(job) }

func managedByUs(job *batchv1.Job) bool {
	return job.Spec.ManagedBy != nil && *job.Spec.ManagedBy == v1alpha1.ManagedByName
}

func claim(job *batchv1.Job, now time.Time, forceSuspend bool) {
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}

	if _, ok := job.Annotations[v1alpha1.AnnotationClaimed]; !ok {
		job.Annotations[v1alpha1.AnnotationClaimed] = "true"
		job.Annotations[v1alpha1.AnnotationClaimedAt] = now.UTC().Format(time.RFC3339)
	}

	// A write of .spec.suspend=false on a claimed Job is a resume request:
	// it is the only signal a user can send through the field, since we
	// hold the field at true. Re-suspending goes through the annotation,
	// because "suspend is true" is indistinguishable from "we forced it".
	if job.Spec.Suspend != nil && !*job.Spec.Suspend {
		job.Annotations[v1alpha1.AnnotationUserSuspend] = "false"
	} else if _, ok := job.Annotations[v1alpha1.AnnotationUserSuspend]; !ok {
		want := "false"
		if job.Spec.Suspend != nil && *job.Spec.Suspend {
			want = "true"
		}
		job.Annotations[v1alpha1.AnnotationUserSuspend] = want
	}

	job.Spec.Template.Spec.SchedulerName = v1alpha1.SchedulerName
	if forceSuspend {
		t := true
		job.Spec.Suspend = &t
	} else {
		// managedBy already stops Pods being created, so the user's own
		// suspend intent is the only thing this field has to carry --
		// which is the asymmetric-resume problem simply not arising.
		want := UserWantsSuspend(job)
		job.Spec.Suspend = &want
	}
}

// UserWantsSuspend reads back the intent Claim stashed.
func UserWantsSuspend(job *batchv1.Job) bool {
	return job.Annotations[v1alpha1.AnnotationUserSuspend] == "true"
}

// Claimed reports whether a Job carries both halves of the claim. Both are
// required: the annotation alone could be copied by a user into a Job we never
// admitted, and schedulerName alone could belong to somebody else's scheduler
// that happens to be called htcondor.
func Claimed(job *batchv1.Job) bool {
	if job == nil {
		return false
	}
	_, ok := job.Annotations[v1alpha1.AnnotationClaimed]
	return ok && job.Spec.Template.Spec.SchedulerName == v1alpha1.SchedulerName
}

// JobClaimer serves the webhook.
type JobClaimer struct {
	// UseManagedBy writes .spec.managedBy on CREATE so the built-in Job
	// controller skips the Job entirely. See v1alpha1.ManagedByName.
	UseManagedBy bool

	// SuspendFallback forces .spec.suspend even under managedBy, for a
	// cluster that might not honor it. See ClaimOptions.
	SuspendFallback bool

	Caps    translate.Capabilities
	Decoder admission.Decoder
	Log     logr.Logger

	// Now is overridable so the claim timestamp is deterministic in tests.
	Now func() time.Time
}

var _ admission.Handler = (*JobClaimer)(nil)

// Handle decodes the Job, decides, and patches only when claiming.
func (jc *JobClaimer) Handle(_ context.Context, req admission.Request) admission.Response {
	job := &batchv1.Job{}
	if err := jc.Decoder.Decode(req, job); err != nil {
		// A Job we cannot decode is one we cannot claim, and refusing
		// it would take down Job creation for the whole cluster over a
		// decoding bug of ours. Allow it through untouched.
		jc.Log.Error(err, "decoding Job; leaving it to the default scheduler",
			"namespace", req.Namespace, "name", req.Name)
		return admission.Allowed("htcondor-k8s: could not decode, not claimed")
	}

	v := Decide(job, req.Operation, jc.Caps)
	if !v.Claim {
		if len(v.Rejections) > 0 {
			jc.Log.V(1).Info("Job not claimed", "namespace", req.Namespace, "name", req.Name,
				"why", v.Why, "rejections", v.Rejections.Error())
		} else {
			jc.Log.V(1).Info("Job not claimed", "namespace", req.Namespace, "name", req.Name, "why", v.Why)
		}
		// Allowed with no patch: the object reaches etcd byte-identical.
		return admission.Allowed("htcondor-k8s: not claimed (" + v.Why + ")")
	}

	now := time.Now
	if jc.Now != nil {
		now = jc.Now
	}
	ClaimWith(job, now(), ClaimOptions{
		// Immutable, so CREATE is the only chance to set it.
		ManagedBy:       jc.UseManagedBy && req.Operation == admissionv1.Create,
		SuspendFallback: jc.SuspendFallback,
	})

	marshaled, err := jsonMarshal(job)
	if err != nil {
		jc.Log.Error(err, "re-encoding claimed Job", "namespace", req.Namespace, "name", req.Name)
		return admission.Errored(http.StatusInternalServerError, err)
	}
	jc.Log.V(1).Info("Job claimed", "namespace", req.Namespace, "name", req.Name, "why", v.Why)
	return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
}
