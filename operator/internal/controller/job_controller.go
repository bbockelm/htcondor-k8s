// Package controller holds the two reconcilers.
//
// They are split along the line of what they can fail at. The Job reconciler
// only ever touches the API server, so its failures are API failures and its
// retries are free. The HTCJob reconciler is the only thing that talks to a
// schedd, so every failure mode involving the network, authentication or the
// job queue is contained in one file. Collapsing them into one reconciler
// would mean an unreachable access point also stalls the mirroring of Jobs
// that have nothing to do with the broken submission.
package controller

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/bbockelm/htcondor-k8s/operator/api/v1alpha1"
	"github.com/bbockelm/htcondor-k8s/operator/internal/adapter"
	htcwebhook "github.com/bbockelm/htcondor-k8s/operator/internal/webhook"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// JobReconciler turns a claimed batch/v1 Job into an HTCJob and carries the
// HTCJob's status back onto it.
type JobReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// Caps must be the same profile the webhook used. If they differ, a Job
	// claimed by the webhook can be refused here, which shows up as a Job
	// that is suspended forever with a NotAdmitted event on it.
	Caps translate.Capabilities

	// MirrorStatus copies the HTCJob's counts onto the Job's .status.
	//
	// It is a switch because in a cluster running kube-controller-manager
	// the built-in Job controller also owns that status and recomputes it
	// from Pods -- of which a claimed Job has none. The two writers will
	// fight, and the built-in one is the one Kubernetes believes. The
	// HTCJob is the authoritative status either way; this only decides
	// whether `kubectl get job` is given a useful lie or an honest zero.
	MirrorStatus bool
}

// guardManagedBy catches the one way .spec.managedBy can fail quietly.
//
// The field is accepted by any API server that knows the schema, but it only
// *does* anything where the JobManagedBy feature gate is on -- beta since
// 1.32, GA since 1.35. On an older cluster, or one with the gate explicitly
// off, the Job controller ignores it and makes Pods, and the work then runs
// both on Kubernetes and on HTCondor. Nothing reports that; the Job simply
// looks like it is going well in two places.
//
// Pods are the unambiguous evidence, because this operator never creates any.
// One is enough: suspend the Job, which is what stops the built-in controller
// when managedBy cannot, and say so loudly enough to be actionable.
func (r *JobReconciler) guardManagedBy(ctx context.Context, job *batchv1.Job) error {
	if !htcwebhook.ManagedByUs(job) {
		return nil
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(job.Namespace),
		client.MatchingLabels{"batch.kubernetes.io/job-name": job.Name},
		client.Limit(1)); err != nil {
		// Not fatal: without the list we cannot prove anything is
		// wrong, and refusing to reconcile would strand a Job that is
		// almost certainly fine.
		log.FromContext(ctx).V(1).Info("could not check for pods under a managedBy Job", "error", err.Error())
		return nil
	}
	if len(pods.Items) == 0 {
		return nil
	}

	r.event(job, "Warning", "ManagedByIgnored",
		fmt.Sprintf("this cluster created Pods for a Job with .spec.managedBy=%s, so the "+
			"JobManagedBy feature gate is off and the work would run twice; suspending the Job",
			v1alpha1.ManagedByName))
	if job.Spec.Suspend == nil || !*job.Spec.Suspend {
		t := true
		job.Spec.Suspend = &t
		if err := r.Update(ctx, job); err != nil {
			return fmt.Errorf("suspending a Job whose managedBy was ignored: %w", err)
		}
	}
	return nil
}

// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get;update;patch

// Reconcile mirrors one Job.
func (r *JobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx)

	var job batchv1.Job
	if err := r.Get(ctx, req.NamespacedName, &job); err != nil {
		// The HTCJob is owned by the Job, so garbage collection deletes
		// it, and its own finalizer takes the cluster out of the queue.
		// Nothing to do here.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !htcwebhook.Claimed(&job) {
		return ctrl.Result{}, nil
	}
	if !job.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Admission runs again here, against the same capabilities, because the
	// webhook may have been bypassed: failurePolicy is Ignore, a webhook
	// that was down lets Jobs through unclaimed, but an operator restored
	// from a backup or a hand-edited object can carry the annotation
	// without ever having been admitted.
	// The stored Job carries fields the API server added after the webhook
	// saw it; admission has to be re-run against the object as the webhook
	// judged it, not as etcd holds it.
	spec := adapter.NormalizeStoredJob(&job.Spec)
	if rs := translate.AdmitJob(spec, &job.ObjectMeta, r.Caps); !rs.OK() {
		lg.Info("claimed Job is not translatable; leaving it suspended",
			"rejections", rs.Error())
		r.event(&job, "Warning", "NotAdmitted", rs.Error())
		return ctrl.Result{}, nil
	}

	if err := r.guardManagedBy(ctx, &job); err != nil {
		return ctrl.Result{}, err
	}

	hj, err := r.ensureHTCJob(ctx, &job, spec)
	if err != nil {
		return ctrl.Result{}, err
	}

	if r.MirrorStatus {
		if err := r.mirrorStatus(ctx, &job, hj); err != nil {
			// A rejected status write must not stall the mirroring
			// of the spec, which is the part that makes jobs run.
			lg.V(1).Info("could not mirror status onto the Job", "error", err.Error())
		}
	}
	return ctrl.Result{}, nil
}

// ensureHTCJob creates or updates the HTCJob for a Job.
//
// Name and namespace are the Job's own. That makes the mapping obvious to a
// human (`kubectl get htcjob <same name>`) and makes the create idempotent
// without a lookup by label: a second create attempt collides on the name.
func (r *JobReconciler) ensureHTCJob(ctx context.Context, job *batchv1.Job, spec *batchv1.JobSpec) (*v1alpha1.HTCJob, error) {
	desired := adapter.JobToHTCJobSpec(spec)
	// .spec.suspend on the Job is held at true by the webhook so the
	// built-in Job controller makes no Pods, so it cannot carry the user's
	// intent. The annotation does.
	suspend := htcwebhook.UserWantsSuspend(job)
	desired.Suspend = &suspend

	hj := &v1alpha1.HTCJob{
		ObjectMeta: metav1.ObjectMeta{Name: job.Name, Namespace: job.Namespace},
	}
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, hj, func() error {
		if hj.Labels == nil {
			hj.Labels = map[string]string{}
		}
		hj.Labels[v1alpha1.LabelOwnerUID] = string(job.UID)

		// The owner reference is what makes `kubectl delete job` take
		// the HTCondor cluster with it: GC deletes the HTCJob, whose
		// finalizer removes the jobs.
		if err := controllerutil.SetControllerReference(job, hj, r.Scheme); err != nil {
			return err
		}

		// .spec.template of a Job is immutable, so in practice only
		// suspend moves. Assigning the whole spec anyway means a
		// change to the projection in adapter is picked up on the next
		// pass instead of only for new Jobs.
		hj.Spec = desired
		return nil
	})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			// A name collision with an HTCJob somebody else owns.
			// Taking it over would be worse than refusing.
			r.event(job, "Warning", "HTCJobConflict",
				"an HTCJob named "+job.Name+" already exists and is not owned by this Job")
			return nil, nil
		}
		return nil, err
	}
	if op != controllerutil.OperationResultNone {
		r.event(job, "Normal", "HTCJob"+string(op), fmt.Sprintf("HTCJob %s/%s", hj.Namespace, hj.Name))
	}
	return hj, nil
}

// mirrorStatus carries the HTCJob's counts back onto the Job.
//
// Only the fields a batch/v1 JobStatus can hold honestly are written. The
// conditions come from the shared translation so that the facade's view of a
// finished job and this one agree on what "complete" means.
func (r *JobReconciler) mirrorStatus(ctx context.Context, job *batchv1.Job, hj *v1alpha1.HTCJob) error {
	if hj == nil {
		return nil
	}
	want := JobStatusFromHTCJob(hj)
	if equality.Semantic.DeepEqual(statusShape(job.Status), statusShape(want)) {
		return nil
	}

	fresh := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, fresh); err != nil {
		return err
	}
	fresh.Status.Active = want.Active
	fresh.Status.Succeeded = want.Succeeded
	fresh.Status.Failed = want.Failed
	fresh.Status.StartTime = want.StartTime
	fresh.Status.CompletionTime = want.CompletionTime
	for _, c := range want.Conditions {
		upsertJobCondition(&fresh.Status.Conditions, c)
	}
	return r.Status().Update(ctx, fresh)
}

// JobStatusFromHTCJob is the HTCJob's status in batch/v1 terms.
func JobStatusFromHTCJob(hj *v1alpha1.HTCJob) batchv1.JobStatus {
	st := batchv1.JobStatus{
		Active:         hj.Status.Active,
		Succeeded:      hj.Status.Succeeded,
		Failed:         hj.Status.Failed,
		StartTime:      hj.Status.StartTime,
		CompletionTime: hj.Status.CompletionTime,
	}
	// Terminal conditions have to satisfy the built-in Job state machine,
	// not just say the right word. The API server refuses Complete=True
	// without a completionTime and without a preceding SuccessCriteriaMet,
	// and Failed=True without a preceding FailureTarget -- these are
	// interim conditions the real Job controller sets on its way through,
	// and anything writing a Job status has to reproduce the whole
	// sequence. That coupling tracks whatever the running Kubernetes
	// version's Job validation requires, and is a standing cost of
	// mirroring onto an object somebody else's controller owns.
	switch hj.Status.Phase {
	case v1alpha1.PhaseSucceeded:
		st.Conditions = append(st.Conditions,
			jobCondition(batchv1.JobSuccessCriteriaMet, "AllProcsSucceeded", ""),
			jobCondition(batchv1.JobComplete, "AllProcsSucceeded", ""))
	case v1alpha1.PhaseFailed:
		msg := fmt.Sprintf("%d procs failed", hj.Status.Failed)
		st.Conditions = append(st.Conditions,
			jobCondition(batchv1.JobFailureTarget, "ProcsFailed", msg),
			jobCondition(batchv1.JobFailed, "ProcsFailed", msg))
	}

	// Kubernetes requires a startTime once a Job has ever been running or
	// finished, and a completionTime on a complete one. The queue does not
	// always give both -- a job ad whose CompletionDate the schedd has not
	// written yet is normal -- so the gap is filled rather than left to be
	// rejected.
	if st.CompletionTime == nil && hj.Status.Phase == v1alpha1.PhaseSucceeded {
		now := metav1.Now()
		st.CompletionTime = &now
	}
	if st.StartTime == nil && (st.Active > 0 || st.Succeeded > 0 || st.Failed > 0) {
		if st.CompletionTime != nil {
			st.StartTime = st.CompletionTime
		} else {
			now := metav1.Now()
			st.StartTime = &now
		}
	}
	return st
}

func jobCondition(t batchv1.JobConditionType, reason, msg string) batchv1.JobCondition {
	now := metav1.Now()
	return batchv1.JobCondition{
		Type: t, Status: "True", Reason: reason, Message: msg,
		LastProbeTime: now, LastTransitionTime: now,
	}
}

func upsertJobCondition(conds *[]batchv1.JobCondition, c batchv1.JobCondition) {
	for i := range *conds {
		if (*conds)[i].Type == c.Type {
			if (*conds)[i].Status != c.Status {
				(*conds)[i] = c
			}
			return
		}
	}
	*conds = append(*conds, c)
}

// statusShape reduces a JobStatus to the parts this controller owns, so the
// comparison that decides whether to write ignores whatever else is in there.
func statusShape(st batchv1.JobStatus) [3]int32 {
	return [3]int32{st.Active, st.Succeeded, st.Failed}
}

func (r *JobReconciler) event(job *batchv1.Job, kind, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Event(job, kind, reason, msg)
	}
}

// SetupWithManager watches Jobs, and HTCJobs through their owner reference so
// that a status change on an HTCJob wakes the Job that owns it.
func (r *JobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.Job{}).
		Owns(&v1alpha1.HTCJob{}).
		Named("job").
		Complete(r)
}
