// Package webhook holds the mutating webhook that points a translatable
// batch/v1 Job's Pods at HTCondor.
//
// This is deliberately the smallest webhook that can work, and the comparison
// with the HTCJob operator's Job-claiming webhook is the point of the whole
// approach. That one writes schedulerName, .spec.suspend and three
// annotations, and then a controller writes .status; three of those four
// writes are to fields a user or the built-in Job controller also owns, and
// docs/OPERATOR.md records the three injuries that produced -- an asymmetric
// resume that cannot be fixed, a .status mirror that ships off because
// kube-controller-manager wins the fight, and a .spec.selector defaulted after
// mutating admission that made the webhook and the controller disagree about
// the same object.
//
// Every one of those came from taking over something the Job machinery owns.
// So this webhook does not suspend the Job, does not write Job status, does
// not stash user intent in an annotation, and does not remember its own
// decision. The built-in Job controller is left running at full strength: it
// creates real Pods, honors parallelism, completions, backoffLimit and
// activeDeadlineSeconds, and derives .status from the Pods' own statuses --
// which the pod controller writes. The Job's status therefore has exactly one
// writer, and it is the one Kubernetes believes.
//
// What is left is two mutations and a third that is accounting, all on
// .spec.template, which is immutable after create and is not read by anything
// except the Pod it stamps out:
//
//   - schedulerName, so kube-scheduler ignores the Pods it makes;
//   - a toleration, so the Pods are placeable on the tainted virtual node;
//   - podReplacementPolicy: Failed, for the reason in [Decide].
//
// The asymmetry from the operator still governs: stamping a Job is an opinion
// and declining to stamp one is not, so a Job this webhook does not understand
// comes out of admission byte-identical and runs on Kubernetes the way it
// always has.
package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-logr/logr"
	admissionv1 "k8s.io/api/admission/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
)

// Path is where the webhook serves. It is spelled out rather than derived,
// because the MutatingWebhookConfiguration in config/webhook has to agree with
// it exactly and a mismatch fails silently in whichever direction
// failurePolicy points.
const Path = "/mutate-batch-v1-job"

// Verdict is the decision about one Job, separated from the HTTP plumbing so
// it can be table-tested without an API server.
type Verdict struct {
	// Stamp is true when the Job's pod template should be mutated.
	Stamp bool
	// Why explains the decision either way, in one phrase, for the log.
	Why string
	// Rejections is why translation failed, when it did. The user never
	// sees it -- declining is silent by design -- so the log is the only
	// place that can say which field lost the Job.
	Rejections translate.Rejections
}

// Decide is the pure stamp/don't-stamp rule.
//
// op matters for the same reason it does in the operator: on CREATE the Job
// has no Pods, so pointing it at HTCondor is free. On UPDATE it may already
// have Pods running under the default scheduler, and .spec.template is
// immutable anyway, so an unstamped Job is never stamped on update. A Job
// already carrying our schedulerName is re-stamped, which is how a hand-edit
// that removed the toleration is repaired.
func Decide(job *batchv1.Job, op admissionv1.Operation, caps translate.Capabilities) Verdict {
	if job == nil {
		return Verdict{Why: "no object"}
	}
	podSpec := &job.Spec.Template.Spec

	if Stamped(job) {
		return Verdict{Stamp: true, Why: "already stamped"}
	}
	if op != admissionv1.Create {
		return Verdict{Why: "not stamping an existing Job on update"}
	}
	if sched := podSpec.SchedulerName; sched != "" && sched != corev1.DefaultSchedulerName {
		return Verdict{Why: fmt.Sprintf("scheduler %q already requested", sched)}
	}

	// podReplacementPolicy is the one piece of Job-level behavior this
	// approach genuinely cannot live with the default of.
	//
	// The built-in Job controller's isPodFailed counts a Pod with a
	// deletionTimestamp as failed unless its phase is already Succeeded,
	// precisely to catch orphans that will never reach a phase. Every Pod
	// here spends a window in exactly that state: the pod controller holds
	// a finalizer while it condor_rm's the job. Under the default
	// TerminatingOrFailed that window burns a backoffLimit retry and
	// manufactures a replacement Pod -- a second HTCondor submission for
	// work that already ran. Under Failed the controller waits for a real
	// terminal phase, which the pod controller writes from the job ad.
	//
	// An explicit TerminatingOrFailed is therefore refused rather than
	// overridden. Overriding it would be the operator's .spec.suspend
	// injury again in miniature: a user-meaningful field held at a value
	// we chose, with no way left to tell "the user asked for this" from
	// "we forced it". Refusing costs that Job nothing but a different
	// scheduler.
	if p := job.Spec.PodReplacementPolicy; p != nil && *p != batchv1.Failed {
		return Verdict{Why: fmt.Sprintf("podReplacementPolicy %q cannot be honored asynchronously", *p)}
	}

	if rs := admitForPodInterception(&job.Spec, &job.ObjectMeta, caps); !rs.OK() {
		return Verdict{Why: "not translatable", Rejections: rs}
	}
	return Verdict{Stamp: true, Why: "translatable"}
}

// admitForPodInterception is translate.AdmitJob adjusted for the one thing
// that is different about this approach: the built-in Job controller is still
// running, so the Job-level fields it owns are not ours to refuse.
//
// It is a filter rather than an edit to shared/translate on purpose. The
// shared contract exists so the three approaches cannot drift into three
// different Pod/job mappings, and the right place to fix this is upstream --
// see docs/VIRTUALNODE.md and the report accompanying it, which proposes
// splitting AdmitJob into the checks that belong to the translation and the
// checks that belong to a front end that is reimplementing Job semantics
// itself. Until that lands, the override is narrow, named, and keyed on both
// the field path and the reason code so that a new rejection on the same field
// is not silently swallowed.
func admitForPodInterception(spec *batchv1.JobSpec, meta *metav1.ObjectMeta, caps translate.Capabilities) translate.Rejections {
	rs := translate.AdmitJob(spec, meta, caps)

	out := make(translate.Rejections, 0, len(rs))
	for _, r := range rs {
		// parallelism below completions: AdmitJob refuses it because
		// nothing throttles HTCondor proc starts. Here nothing needs
		// to: the Job controller holds back Pod creation itself, and a
		// Pod that does not exist cannot be submitted.
		if r.Field == "spec.parallelism" && r.Reason == translate.ReasonJobFieldUnsupported {
			continue
		}
		out = append(out, r)
	}

	// restartPolicy: OnFailure is refused here although translate.Admit
	// accepts it. See docs/VIRTUALNODE.md; the short version is that
	// OnFailure means the kubelet restarts the container in place, there
	// is no kubelet, and the Job controller's OnFailure backoff accounting
	// keys on containerStatuses[].restartCount -- a number we would have
	// to synthesize from NumJobStarts to make a bound we cannot see from
	// the Pod. Getting it wrong is an unbounded retry loop. Never plus
	// backoffLimit is implemented by the real Job controller, correctly,
	// and is strictly better.
	if spec != nil && spec.Template.Spec.RestartPolicy == corev1.RestartPolicyOnFailure {
		out = append(out, translate.Rejection{
			Field:  "spec.template.spec.restartPolicy",
			Reason: translate.ReasonRestartPolicy,
			Detail: "there is no kubelet to restart a container in place; use restartPolicy Never with backoffLimit, which the built-in Job controller implements",
		})
	}
	return out
}

// Toleration is the one toleration this webhook adds, matching the taint the
// node controller puts on the virtual node.
//
// It has to be added by somebody. The taint keeps other people's Pods off a
// node that cannot really run them, and our own Pods are bound by hand rather
// than by kube-scheduler -- but the API server's own admission still checks a
// binding against the node's taints, and a NoSchedule taint with no matching
// toleration is refused. Adding it in the webhook rather than at bind time
// keeps the Pod honest: what it says about where it can run is true before it
// runs, not patched afterwards.
func Toleration() corev1.Toleration {
	return corev1.Toleration{
		Key:      api.TaintKey,
		Operator: corev1.TolerationOpEqual,
		Value:    api.TaintValue,
		Effect:   corev1.TaintEffectNoSchedule,
	}
}

// Stamp applies the mutation. It is idempotent: running it on an
// already-stamped Job changes only what drifted.
func Stamp(job *batchv1.Job) {
	podSpec := &job.Spec.Template.Spec
	podSpec.SchedulerName = api.SchedulerName

	if !hasToleration(podSpec.Tolerations) {
		podSpec.Tolerations = append(podSpec.Tolerations, Toleration())
	}

	// Only ever written when unset. Decide has already refused an explicit
	// value other than Failed, so this never contradicts the user.
	if job.Spec.PodReplacementPolicy == nil {
		p := batchv1.Failed
		job.Spec.PodReplacementPolicy = &p
	}
}

// Stamped reports whether a Job carries the mutation.
//
// Unlike the operator's Claimed, this reads only fields that are part of the
// mechanism. There is no claim annotation to forge, and nothing downstream
// consults one: the pod controller selects Pods on schedulerName, which is a
// fact about the object the Job controller actually copied, not a note we left
// ourselves.
func Stamped(job *batchv1.Job) bool {
	if job == nil {
		return false
	}
	return job.Spec.Template.Spec.SchedulerName == api.SchedulerName
}

func hasToleration(ts []corev1.Toleration) bool {
	want := Toleration()
	for _, t := range ts {
		if t.Key == want.Key && t.Value == want.Value && t.Effect == want.Effect {
			return true
		}
		// A blanket "tolerate everything" toleration covers ours too,
		// and appending a second one would be noise on the object.
		if t.Operator == corev1.TolerationOpExists && t.Key == "" {
			return true
		}
	}
	return false
}

// JobStamper serves the webhook.
type JobStamper struct {
	Caps    translate.Capabilities
	Decoder admission.Decoder
	Log     logr.Logger
}

var _ admission.Handler = (*JobStamper)(nil)

// Handle decodes the Job, decides, and patches only when stamping.
func (js *JobStamper) Handle(_ context.Context, req admission.Request) admission.Response {
	job := &batchv1.Job{}
	if err := js.Decoder.Decode(req, job); err != nil {
		// A Job we cannot decode is one we cannot stamp. Refusing it
		// would take down Job creation for everything this webhook
		// matches over a decoding bug of ours, so it goes through
		// untouched and runs on Kubernetes.
		js.Log.Error(err, "decoding Job; leaving it to the default scheduler",
			"namespace", req.Namespace, "name", req.Name)
		return admission.Allowed("htcondor-k8s: could not decode, not stamped")
	}

	v := Decide(job, req.Operation, js.Caps)
	if !v.Stamp {
		if len(v.Rejections) > 0 {
			js.Log.V(1).Info("Job not stamped", "namespace", req.Namespace, "name", req.Name,
				"why", v.Why, "rejections", v.Rejections.Error())
		} else {
			js.Log.V(1).Info("Job not stamped", "namespace", req.Namespace, "name", req.Name, "why", v.Why)
		}
		// Allowed with no patch: the object reaches etcd byte-identical.
		return admission.Allowed("htcondor-k8s: not stamped (" + v.Why + ")")
	}

	Stamp(job)

	marshaled, err := json.Marshal(job)
	if err != nil {
		js.Log.Error(err, "re-encoding stamped Job", "namespace", req.Namespace, "name", req.Name)
		return admission.Errored(http.StatusInternalServerError, err)
	}
	js.Log.V(1).Info("Job stamped", "namespace", req.Namespace, "name", req.Name, "why", v.Why)
	return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
}
