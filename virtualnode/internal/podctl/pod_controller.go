// Package podctl is the pod controller: the half of the Virtual Kubelet
// pattern that takes a Pending Pod nobody else will schedule, runs it as an
// HTCondor job, and writes the Pod's status back.
//
// # Why this exists rather than an HTCJob
//
// The HTCJob operator and this controller solve the same problem and differ in
// exactly one decision: who owns the Job's status. docs/OPERATOR.md records
// three injuries from the answer "we do, partly" -- .spec.suspend had to be
// commandeered, producing a resume that cannot be made symmetric; job.status
// has a second writer in kube-controller-manager that always wins, so the
// mirror ships off; and .spec.selector is defaulted after mutating admission,
// so the webhook and the controller judged the same object differently.
//
// Here the answer is "we never touch it". The built-in Job controller runs at
// full strength and makes real Pod objects; kube-scheduler skips them because
// their schedulerName is not its own; this controller submits each one and
// writes *pod* status; and the Job controller derives job.status from those
// Pods the way it does for every other Job in the cluster. The co-ownership
// problem is not worked around, it is dissolved -- there is exactly one writer
// per object.
//
// The bill for that comes due here instead, in three places, and they are the
// things to read this file for:
//
//   - Pod status is now load-bearing in a way HTCJob status never was. The Job
//     controller counts these phases. A wrong Succeeded does not just mislead
//     a human, it silently corrupts the user's Job status and stops a Job that
//     should have retried.
//   - A crash between "submitted" and "recorded the job id" must not orphan a
//     running job, and must not submit it twice. See [PodReconciler.ensureSubmitted].
//   - The Job controller counts a Pod with a deletionTimestamp as failed
//     unless it is already Succeeded, which is exactly the window a finalizer
//     creates. See [PodReconciler.finalize].
package podctl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/condor"
)

// PodReconciler runs Pods addressed to the htcondor scheduler as HTCondor jobs.
type PodReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// Queue is the schedd, behind an interface so this is testable without
	// an access point.
	Queue condor.Queue

	// Caps is the pool profile admission is judged against.
	Caps translate.Capabilities

	// NodeName is the virtual node Pods are bound to. Empty means
	// api.NodeName.
	NodeName string

	// ScheddName is recorded on the Pod, because a cluster.proc id is only
	// unique within one queue.
	ScheddName string

	// SpoolRoot, when set, is a directory this controller and the access
	// point both see. Each Pod gets a subdirectory as its initialdir,
	// which is where stdout, stderr and the user log land -- and therefore
	// the only reason `kubectl logs` can work at all.
	SpoolRoot string

	// PollInterval is how often a live job is re-read. There is no watch on
	// a job queue, so this is the whole status freshness story.
	PollInterval time.Duration

	// RetryInterval is the backoff after the schedd could not be reached.
	RetryInterval time.Duration
}

const (
	defaultPollInterval  = 10 * time.Second
	defaultRetryInterval = 10 * time.Second

	// reasonNotAdmitted and friends are the values that land in
	// .status.reason. They are stable because users will grep for them.
	reasonNotAdmitted      = "NotAdmitted"
	reasonLeftQueue        = "LeftQueue"
	reasonDeadlineExceeded = "DeadlineExceeded"
)

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods/binding,verbs=create
// +kubebuilder:rbac:groups="",resources=pods/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives one Pod toward the job queue and its status back.
func (r *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx)

	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Belt and braces. The watch predicate already filters on this, but a
	// Pod addressed to somebody else's scheduler that reached here through
	// a resync is a Pod we must not submit: whoever owns that name may be
	// running it too.
	if pod.Spec.SchedulerName != api.SchedulerName {
		return ctrl.Result{}, nil
	}

	if !pod.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &pod)
	}

	taken := controllerutil.ContainsFinalizer(&pod, api.Finalizer)
	if !taken {
		// Admission is re-run here rather than trusted from the webhook,
		// and this is the only place it runs.
		//
		// It has to run: a bare Pod created directly with
		// schedulerName: htcondor and no Job above it is a legitimate
		// path through this architecture and no webhook ever saw it.
		// Assuming the webhook ran would mean submitting whatever that
		// Pod said, including the things the whitelist exists to refuse.
		//
		// It runs once, before the finalizer, and not on every pass.
		// A Pod's spec is immutable after create, so a second look at
		// the same object can only produce a different answer if the
		// *pool profile* changed under it -- and tearing down a running
		// job because an operator restarted with different Capabilities
		// would be strictly worse than letting it finish. That is the
		// skew trap docs/OPERATOR.md names; taking the decision once,
		// at the moment of taking the Pod, is how this approach avoids
		// having to detect it.
		if rs := translate.Admit(normalizeForAdmission(&pod, r.nodeName()), &pod.ObjectMeta, r.Caps); !rs.OK() {
			return r.reject(ctx, &pod, rs)
		}

		// The finalizer goes on in a pass of its own, before anything is
		// submitted. Added in the same pass as the submit, a crash in
		// between would leave a job that nothing is obliged to clean up:
		// deleting the Pod would succeed immediately and the job would
		// run on.
		controllerutil.AddFinalizer(&pod, api.Finalizer)
		if err := r.Update(ctx, &pod); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// A Pod we have already reported as terminal is finished work. Nothing
	// moves it again, so stop polling the queue for it; the job id stays on
	// the object and the finalizer stays on until the Pod is deleted.
	if isTerminalPhase(pod.Status.Phase) {
		return ctrl.Result{}, nil
	}

	ad, found, res, err := r.ensureSubmitted(ctx, &pod)
	if err != nil || !found {
		return res, err
	}

	if err := r.recordJob(ctx, &pod, ad); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Bind after the job is in the queue and its id is on the object, not
	// before. A bound Pod with no job behind it reads as "scheduled, and
	// the kubelet has gone quiet" to anyone looking, including the Job
	// controller; an unbound one reads as Pending, which is exactly what
	// it is.
	if pod.Spec.NodeName == "" {
		if err := r.bind(ctx, &pod); err != nil {
			return ctrl.Result{}, err
		}
		lg.V(1).Info("bound Pod to the virtual node", "node", r.nodeName(), "job", ad.JobID())
		r.event(&pod, corev1.EventTypeNormal, "Scheduled",
			fmt.Sprintf("bound to %s running HTCondor job %s", r.nodeName(), ad.JobID()))
		// Re-read: the binding changed .spec.nodeName under us, and the
		// status write below must not be built on a stale object.
		return ctrl.Result{Requeue: true}, nil
	}

	if !settled(ad) {
		// See [settled]: an ad that claims to be Completed but carries
		// neither a completion time nor an exit status is one we would
		// have to guess at, and the available guess is "Succeeded".
		lg.V(1).Info("job ad is not settled yet; not deriving a phase from it", "job", ad.JobID())
		return ctrl.Result{RequeueAfter: r.retryInterval()}, nil
	}

	if err := r.writeStatus(ctx, &pod, ad); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if ad.Terminal() {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: r.pollInterval()}, nil
}

// ensureSubmitted gets exactly one HTCondor job into the queue for this Pod
// and returns its ad.
//
// The hazard: a reconcile that submits and then dies before recording the job
// id leaves a job running that nothing points at, and a next pass that trusted
// the missing annotation would submit it again -- running the user's work
// twice.
//
// The fix is to stop trusting the annotation and make the queue itself the
// record. Every submit stamps +K8sPodUID with the Pod's UID, which the API
// server assigned before this controller ever saw the object, which never
// changes, and which a replacement Pod of the same name does not inherit. So
// the order is always look, then submit:
//
//  1. Query(K8sPodUID == "<uid>"). Non-empty -> adopt it, whatever we thought.
//  2. Otherwise submit, and record the id immediately.
//
// A submit whose *outcome* is unknown -- the connection dropped during the
// commit -- resolves on the next pass through step 1, because a committed
// qmgmt transaction is visible to the very next query and an aborted one left
// nothing behind. There is no window in which both answers are possible.
//
// Which leaves one window that this cannot close, and it is worth naming
// rather than hiding: a submit whose result was lost *and* whose job then left
// the queue before the next reconcile looks indistinguishable from a Pod that
// was never submitted, and would be submitted again. Closing it needs a
// history query (condor_history, or the job-epoch ads) on the Pod UID, and the
// Queue interface has no method for that. docs/OPERATOR.md reaches the same
// conclusion from the other direction and calls it the most likely source of
// wrong answers in production. It is the same gap.
func (r *PodReconciler) ensureSubmitted(ctx context.Context, pod *corev1.Pod) (translate.JobAd, bool, ctrl.Result, error) {
	lg := log.FromContext(ctx)
	constraint := PodConstraint(pod.UID)

	ads, err := r.Queue.Query(ctx, constraint)
	if err != nil {
		lg.Error(err, "reading the job queue")
		r.event(pod, corev1.EventTypeWarning, "QueueUnreachable", err.Error())
		// Status is left exactly as it was. It is stale, not wrong, and
		// zeroing it would hand the Job controller a phase change that
		// never happened.
		return translate.JobAd{}, false, ctrl.Result{RequeueAfter: r.retryInterval()}, nil
	}

	if len(ads) > 0 {
		if len(ads) > 1 {
			// One Pod is one cluster of one proc. More than one means
			// the window above was lost at some point; the oldest is
			// the one the Pod has been reporting, so keep reporting it.
			lg.Info("more than one job carries this Pod UID; using the lowest cluster",
				"count", len(ads), "pod", pod.Name)
		}
		best := ads[0]
		for _, a := range ads[1:] {
			if a.ClusterID < best.ClusterID {
				best = a
			}
		}
		return best, true, ctrl.Result{}, nil
	}

	if id := pod.Annotations[api.AnnotationJobID]; id != "" {
		// We have seen this job in the queue before and now it is gone.
		// Completed jobs are reaped into history, so this is the normal
		// end of a job whose terminal transition we missed between two
		// polls.
		//
		// There is no honest way to tell "succeeded" from "failed" from
		// here, and the choice is not symmetric. A wrong Failed makes
		// the Job controller create a replacement Pod, which runs the
		// work again -- bad, visible, and reported as a bug. A wrong
		// Succeeded makes it count a completion that never happened,
		// which silently corrupts the user's Job status and is reported
		// as nothing at all. So this fails.
		return translate.JobAd{}, false, ctrl.Result{}, r.markFailed(ctx, pod, reasonLeftQueue,
			fmt.Sprintf("HTCondor job %s is no longer in the queue and its outcome was not observed", id))
	}

	return r.submit(ctx, pod)
}

func (r *PodReconciler) submit(ctx context.Context, pod *corev1.Pod) (translate.JobAd, bool, ctrl.Result, error) {
	lg := log.FromContext(ctx)

	spec := normalizeForAdmission(pod, r.nodeName())
	submitText, iwd, err := r.renderSubmit(pod, spec)
	if err != nil {
		// A render failure on an admitted Pod is our bug, not the
		// user's, but the Pod still has to stop being Pending forever.
		r.event(pod, corev1.EventTypeWarning, "SubmitRenderFailed", err.Error())
		return translate.JobAd{}, false, ctrl.Result{}, r.markFailed(ctx, pod, "SubmitRenderFailed", err.Error())
	}
	if iwd != "" {
		if err := os.MkdirAll(iwd, 0o750); err != nil {
			r.event(pod, corev1.EventTypeWarning, "SpoolDirFailed", err.Error())
			return translate.JobAd{}, false, ctrl.Result{RequeueAfter: r.retryInterval()}, nil
		}
	}

	cluster, err := r.Queue.Submit(ctx, submitText)
	if err != nil {
		// Deliberately not an error return. An error retries through
		// controller-runtime's backoff, which is fine -- but the retry
		// has to go back through the query in ensureSubmitted rather
		// than straight to Submit, and the only way to guarantee that
		// is to leave by the front door.
		lg.Error(err, "submitting; the next pass will look for the job before retrying")
		r.event(pod, corev1.EventTypeWarning, "SubmitFailed", err.Error())
		return translate.JobAd{}, false, ctrl.Result{RequeueAfter: r.retryInterval()}, nil
	}

	// Record the id immediately, as its own write, before anything else.
	// The query above is what makes this safe to lose, but every
	// millisecond it is missing is a millisecond in which the query is the
	// only way to find the job -- and the window named in ensureSubmitted's
	// comment is exactly as wide as this write is late.
	if err := r.annotate(ctx, pod, map[string]string{
		api.AnnotationJobID:    cluster + ".0",
		api.AnnotationSchedd:   r.ScheddName,
		api.AnnotationSpoolDir: iwd,
	}); err != nil {
		lg.Error(err, "recording the job id; it is still findable by Pod UID", "cluster", cluster)
	}
	r.event(pod, corev1.EventTypeNormal, "Submitted", "submitted HTCondor cluster "+cluster)
	return translate.JobAd{}, false, ctrl.Result{Requeue: true}, nil
}

// finalize removes the job behind a Pod that is going away, and -- first --
// makes sure the Pod's last status is the truth.
//
// The ordering is not cosmetic. The built-in Job controller's isPodFailed
// counts a Pod carrying a deletionTimestamp as failed unless its phase is
// already Succeeded, deliberately, to catch orphans that will never reach a
// phase of their own. A finalizer puts every Pod here into exactly that state
// for as long as it takes to reach a schedd. So a job that succeeded in the
// window between two polls, on a Pod the user then deleted, would be counted
// as a failure, burn a backoffLimit retry, and have a replacement Pod made for
// work that already ran.
//
// Two things close that. The webhook sets podReplacementPolicy: Failed, which
// makes the Job controller wait for a real terminal phase instead of inferring
// one from the deletionTimestamp. And this reads the queue one last time and
// writes the terminal phase before dropping the finalizer, so that the phase
// is there to be waited for.
//
// Note that the Pod will not disappear when our finalizer comes off: a
// Job-created Pod also carries batch.kubernetes.io/job-tracking, which the Job
// controller removes on its own schedule. Nothing here may assume otherwise.
func (r *PodReconciler) finalize(ctx context.Context, pod *corev1.Pod) (ctrl.Result, error) {
	lg := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(pod, api.Finalizer) {
		return ctrl.Result{}, nil
	}

	constraint := PodConstraint(pod.UID)
	ads, err := r.Queue.Query(ctx, constraint)
	if err != nil {
		// A delete blocks on the finalizer rather than dropping it. A
		// stuck delete is visible; an orphaned job is not.
		lg.Error(err, "reading the job queue while finalizing")
		r.event(pod, corev1.EventTypeWarning, "QueueUnreachable", err.Error())
		return ctrl.Result{RequeueAfter: r.retryInterval()}, nil
	}

	if len(ads) == 1 && ads[0].Terminal() && settled(ads[0]) && !isTerminalPhase(pod.Status.Phase) {
		if err := r.writeStatus(ctx, pod, ads[0]); err != nil && !apierrors.IsConflict(err) {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
	}

	ids := make([]string, 0, len(ads))
	for _, ad := range ads {
		ids = append(ids, ad.JobID())
	}
	if len(ids) > 0 {
		if err := r.Queue.Remove(ctx, ids, "the Kubernetes Pod was deleted"); err != nil {
			lg.Error(err, "removing jobs", "ids", ids)
			r.event(pod, corev1.EventTypeWarning, "RemoveFailed", err.Error())
			return ctrl.Result{RequeueAfter: r.retryInterval()}, nil
		}
	} else if pod.Annotations[api.AnnotationJobID] != "" {
		// Nothing in the queue, but we know there was something. Already
		// reaped into history; there is nothing left to remove.
		lg.V(1).Info("no jobs left to remove", "pod", pod.Name)
	} else {
		// Deleted between taking the Pod and submitting it. The
		// constraint is the only handle that would ever have existed,
		// so sweep on it once in case a submit landed after the query.
		if err := r.Queue.RemoveByConstraint(ctx, constraint, "the Kubernetes Pod was deleted"); err != nil {
			lg.Error(err, "removing by constraint")
			return ctrl.Result{RequeueAfter: r.retryInterval()}, nil
		}
	}

	controllerutil.RemoveFinalizer(pod, api.Finalizer)
	if err := r.Update(ctx, pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	return ctrl.Result{}, r.completeDeletion(ctx, pod)
}

// completeDeletion does the last thing a kubelet does for a deleted Pod, which
// nothing else here would do.
//
// Deleting a *bound* Pod is a two-step conversation. The API server marks it
// with a deletionTimestamp and a grace period and then waits; the object is
// only really removed when the node it is bound to says the containers are
// gone, which a kubelet does by issuing its own DELETE with
// gracePeriodSeconds: 0. There is no kubelet on this node, so without this the
// Pod sits in Terminating forever -- finalizers all removed, HTCondor job long
// since gone, object still there. envtest found it immediately; it is invisible
// to a fake client, which deletes as soon as the last finalizer comes off.
//
// Leaving a Pod Terminating is not cosmetic. The built-in Job controller
// tracks terminating Pods separately from active ones and holds its own
// batch.kubernetes.io/job-tracking finalizer until the Pod is accounted for,
// so a Job whose Pods never finish terminating never finishes either.
//
// The UID precondition is the guard that matters: between reading this object
// and issuing the delete, the Job controller may have created a replacement
// Pod with the same name, and deleting that one would destroy work that is
// running.
func (r *PodReconciler) completeDeletion(ctx context.Context, pod *corev1.Pod) error {
	if pod.Spec.NodeName == "" {
		// Never bound, so nothing was ever waiting on a node: the API
		// server removed it the moment the last finalizer came off.
		return nil
	}
	uid := pod.UID
	err := r.Delete(ctx, pod, client.GracePeriodSeconds(0), client.Preconditions{UID: &uid})
	return client.IgnoreNotFound(err)
}

// reject puts an inadmissible Pod into a terminal state with a message naming
// the fields.
//
// It does not bind the Pod and does not add the finalizer: there is no job, so
// there is nothing to clean up and nothing to be bound to. The Pod goes
// straight to Failed, which under a Job counts against backoffLimit and
// eventually fails the Job -- the right outcome, because every replacement Pod
// will be made from the same template and refused the same way. The message is
// on .status.message and on an Event, because an unbound Failed Pod is
// something a user has to be able to explain from kubectl alone.
func (r *PodReconciler) reject(ctx context.Context, pod *corev1.Pod, rs translate.Rejections) (ctrl.Result, error) {
	r.event(pod, corev1.EventTypeWarning, reasonNotAdmitted, rs.Error())
	return ctrl.Result{}, r.markFailed(ctx, pod, reasonNotAdmitted,
		"this pod cannot be modeled as an HTCondor job: "+rs.Error())
}

func (r *PodReconciler) markFailed(ctx context.Context, pod *corev1.Pod, reason, msg string) error {
	if isTerminalPhase(pod.Status.Phase) {
		return nil
	}
	pod.Status.Phase = corev1.PodFailed
	pod.Status.Reason = reason
	pod.Status.Message = msg
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.State.Terminated == nil {
			cs.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode:   1,
				Reason:     reason,
				Message:    msg,
				FinishedAt: metav1.Now(),
			}}
		}
		cs.Ready = false
	}
	err := r.Status().Update(ctx, pod)
	return client.IgnoreNotFound(err)
}

// bind binds the Pod to the virtual node through the pods/binding
// subresource, which is the only way to do it.
//
// Writing .spec.nodeName with an ordinary update does not work and is not a
// style preference: the Pod strategy's ValidatePodUpdate allows a Pod's spec
// to change in a short allowlist of ways -- container images, tolerations
// added, activeDeadlineSeconds shortened, terminationGracePeriodSeconds -- and
// nodeName is not one of them. The API server refuses it.
//
// The subresource is also the right thing for reasons beyond "it is permitted".
// It is atomic: a second binding of an already-bound Pod is a conflict, not a
// silent overwrite, so two controllers cannot quietly disagree about where a
// Pod ran. It is what kube-scheduler uses, so scheduler admission plugins, the
// audit log and anything watching for a real scheduling decision see this as
// one. And it is separately authorizable, so the RBAC for "may place Pods on
// nodes" is a verb on pods/binding rather than blanket write access to every
// Pod spec in the cluster.
func (r *PodReconciler) bind(ctx context.Context, pod *corev1.Pod) error {
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pod.Namespace,
			Name:      pod.Name,
			UID:       pod.UID,
		},
		Target: corev1.ObjectReference{Kind: "Node", Name: r.nodeName()},
	}
	err := r.SubResource("binding").Create(ctx, pod, binding)
	switch {
	case err == nil:
		return nil
	case apierrors.IsConflict(err), apierrors.IsAlreadyExists(err):
		// Somebody bound it first. Since the only binder here is us,
		// that is a previous pass of this reconcile whose result we
		// did not see; the next Get will show the node name.
		return nil
	case apierrors.IsNotFound(err):
		return nil
	default:
		return err
	}
}

// recordJob keeps the job id, the schedd name, the spool directory and the
// hold reason on the Pod.
//
// All four are for humans and for the log endpoint, never for control flow:
// ensureSubmitted finds the job by constraint, not by reading these back. The
// hold reason is the one piece of HTCondor's diagnosis that Kubernetes has no
// field for -- .status.message carries it too, but it is overwritten by the
// next phase, and a user asking "why did this sit Pending for an hour" is
// asking after the fact.
func (r *PodReconciler) recordJob(ctx context.Context, pod *corev1.Pod, ad translate.JobAd) error {
	want := map[string]string{
		api.AnnotationJobID:  ad.JobID(),
		api.AnnotationSchedd: r.ScheddName,
	}
	if iwd := r.spoolDir(pod); iwd != "" {
		want[api.AnnotationSpoolDir] = iwd
	}
	if ad.HoldReason != "" {
		want[api.AnnotationHoldReason] = ad.HoldReason
	}
	return r.annotate(ctx, pod, want)
}

func (r *PodReconciler) annotate(ctx context.Context, pod *corev1.Pod, want map[string]string) error {
	changed := false
	for k, v := range want {
		if v == "" {
			continue
		}
		if pod.Annotations[k] != v {
			changed = true
		}
	}
	if !changed {
		return nil
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	for k, v := range want {
		if v != "" {
			pod.Annotations[k] = v
		}
	}
	return r.Update(ctx, pod)
}

func (r *PodReconciler) writeStatus(ctx context.Context, pod *corev1.Pod, ad translate.JobAd) error {
	next := r.desiredStatus(pod, ad)
	if statusEqual(pod.Status, next) {
		return nil
	}
	pod.Status = next
	return r.Status().Update(ctx, pod)
}

// renderSubmit builds the submit-file text, and returns the initialdir it used.
func (r *PodReconciler) renderSubmit(pod *corev1.Pod, spec *corev1.PodSpec) (string, string, error) {
	opts := translate.SubmitOptions{Caps: r.Caps, Count: 1}

	// Pod-level activeDeadlineSeconds is enforced here, by HTCondor, and
	// nowhere else.
	//
	// Normally the kubelet enforces it, and there is no kubelet. Of the two
	// places it could go, a timer in this controller or a periodic_remove
	// expression on the job, the job wins on every count: it survives a
	// controller outage, it is evaluated by the schedd against the job's
	// own clock rather than against ours, and it leaves exactly one
	// enforcer so there is no pair of clocks to disagree.
	//
	// The *Job*-level activeDeadlineSeconds is deliberately not here. The
	// Job controller owns it and enforces it by deleting active Pods, which
	// arrives at this controller as a deletionTimestamp and comes out as a
	// condor_rm. Setting a periodic_remove for it as well would be two
	// enforcers with two clocks for one deadline.
	//
	// One honest divergence to know about: translate renders the deadline
	// as (time() - QDate), measured from when the job was queued, whereas a
	// kubelet measures from .status.startTime, which is after scheduling.
	// In this architecture those are the same instant -- a Pod is submitted
	// the moment it is taken, and writeStatus reports QDate as startTime
	// for exactly that reason -- but on a pool where jobs sit idle for a
	// long time, queued time counts against the deadline here and would not
	// on a real node.
	if ads := pod.Spec.ActiveDeadlineSeconds; ads != nil && *ads > 0 {
		opts.ActiveDeadlineSeconds = *ads
	}

	iwd := r.spoolDir(pod)
	opts.IWD = iwd

	opts.ExtraAttributes = map[string]string{
		AttrVirtualNode: classAdLiteral(r.nodeName()),
	}
	if idx, ok := completionIndex(pod); ok {
		opts.ExtraAttributes[translate.AttrIndex] = strconv.Itoa(idx)
	}

	text, err := translate.PodToSubmit(spec, identityFor(pod), opts)
	return text, iwd, err
}

func (r *PodReconciler) spoolDir(pod *corev1.Pod) string {
	if r.SpoolRoot == "" {
		return ""
	}
	return filepath.Join(r.SpoolRoot, pod.Namespace, pod.Name)
}

func (r *PodReconciler) event(pod *corev1.Pod, kind, reason, msg string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Event(pod, kind, reason, msg)
}

func (r *PodReconciler) nodeName() string {
	if r.NodeName != "" {
		return r.NodeName
	}
	return api.NodeName
}

func (r *PodReconciler) pollInterval() time.Duration {
	if r.PollInterval > 0 {
		return r.PollInterval
	}
	return defaultPollInterval
}

func (r *PodReconciler) retryInterval() time.Duration {
	if r.RetryInterval > 0 {
		return r.RetryInterval
	}
	return defaultRetryInterval
}

// SetupWithManager wires the reconciler to a watch on Pods addressed to this
// scheduler.
//
// The predicate matters at scale rather than for correctness: without it the
// informer caches every Pod in the cluster, and the one thing this approach
// cannot afford is to be more expensive per Pod than the Pods it does not run.
func (r *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	ours := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		pod, ok := obj.(*corev1.Pod)
		return ok && pod.Spec.SchedulerName == api.SchedulerName
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named("htcondor-pod").
		For(&corev1.Pod{}, builder.WithPredicates(ours)).
		Complete(r)
}

// PodConstraint is the deterministic handle from a Pod to its HTCondor job,
// and the thing that makes submission idempotent.
//
// The Pod's UID is assigned by the API server before this controller ever runs,
// never changes, and is not reused -- a replacement Pod the Job controller
// makes after a failure has a different one, which is what stops it adopting
// the dead Pod's job.
func PodConstraint(uid types.UID) string {
	return fmt.Sprintf("%s == %s", translate.AttrPodUID, classAdLiteral(string(uid)))
}

// NodeConstraint matches every job this virtual node submitted. It is what the
// reaper sweeps on.
func NodeConstraint(nodeName string) string {
	return fmt.Sprintf("%s == %s", AttrVirtualNode, classAdLiteral(nodeName))
}

// classAdLiteral renders a Go string as a ClassAd string literal.
//
// Deliberately not translate.ClassAdString, which is for submit-file text and
// therefore ends by escaping dollar signs into $(DOLLAR) so that user text
// cannot expand a submit macro. A query constraint is handed to the schedd's
// ClassAd evaluator and never passes through condor_submit, so that escaping
// would turn a literal "$" in a value into six characters that match nothing.
func classAdLiteral(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

func isTerminalPhase(p corev1.PodPhase) bool {
	return p == corev1.PodSucceeded || p == corev1.PodFailed
}
