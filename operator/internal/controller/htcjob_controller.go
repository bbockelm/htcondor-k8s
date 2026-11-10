package controller

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/bbockelm/htcondor-k8s/operator/api/v1alpha1"
	"github.com/bbockelm/htcondor-k8s/operator/internal/adapter"
	"github.com/bbockelm/htcondor-k8s/operator/internal/condor"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// HTCJobReconciler owns the relationship between an HTCJob and the HTCondor
// job queue. It is the only thing in this operator that talks to a schedd.
type HTCJobReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// Queue is the schedd. Behind an interface so the reconciler is
	// testable without an access point.
	Queue condor.Queue

	// Caps is the pool profile admission is judged against.
	Caps translate.Capabilities

	// SpoolRoot, when set, is a directory the operator and the access point
	// both see; each HTCJob gets a subdirectory as its initialdir, which is
	// where stdout, stderr and the user log land. Left empty, the jobs run
	// in the schedd's default working directory and there is nowhere to
	// read their output from.
	SpoolRoot string

	// PollInterval is how often a non-terminal HTCJob is re-read from the
	// queue. There is no watch on a job queue, so this is the whole status
	// freshness story.
	PollInterval time.Duration

	// RetryInterval is the backoff after the schedd could not be reached.
	RetryInterval time.Duration
}

const (
	defaultPollInterval  = 15 * time.Second
	defaultRetryInterval = 10 * time.Second

	// maxReportedProcs bounds .status.procs. A cluster larger than this
	// still gets accurate counts; it just does not get a row per proc,
	// because an HTCJob with 100k rows in its status is an object no etcd
	// wants and no kubectl can print.
	maxReportedProcs = 256
)

// +kubebuilder:rbac:groups=htcondor.org,resources=htcjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=htcondor.org,resources=htcjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=htcondor.org,resources=htcjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives one HTCJob toward the job queue.
func (r *HTCJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx)

	var hj v1alpha1.HTCJob
	if err := r.Get(ctx, req.NamespacedName, &hj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !hj.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &hj)
	}

	// The finalizer goes on before anything is submitted, and the
	// reconcile ends there. If it were added in the same pass as the
	// submit, a crash in between would leave a cluster in the queue that
	// nothing is obliged to clean up -- deleting the CR would then succeed
	// immediately and the jobs would run on, orphaned.
	if !controllerutil.ContainsFinalizer(&hj, v1alpha1.Finalizer) {
		controllerutil.AddFinalizer(&hj, v1alpha1.Finalizer)
		if err := r.Update(ctx, &hj); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	jobSpec := adapter.HTCJobToJobSpec(&hj.Spec)
	if rs := translate.AdmitJob(jobSpec, &hj.ObjectMeta, r.Caps); !rs.OK() {
		// A CR can be structurally valid and still inadmissible: slot
		// sizes, GPU resource names and nodeSelector keys are facts
		// about the pool, not about the schema. This is terminal until
		// the spec or the pool profile changes, so there is no requeue.
		r.event(&hj, "Warning", "NotAdmitted", rs.Error())
		setCondition(&hj.Status.Conditions, hj.Generation, v1alpha1.ConditionAdmitted,
			metav1.ConditionFalse, "Untranslatable", rs.Error())
		hj.Status.Phase = v1alpha1.PhaseRejected
		return ctrl.Result{}, r.writeStatus(ctx, &hj)
	}
	setCondition(&hj.Status.Conditions, hj.Generation, v1alpha1.ConditionAdmitted,
		metav1.ConditionTrue, "Translatable", "the spec maps onto a single HTCondor cluster")

	constraint := condor.OwnerConstraint(string(hj.UID))

	if hj.Status.ClusterID == "" {
		res, err := r.ensureSubmitted(ctx, &hj, jobSpec, constraint)
		if err != nil || res.RequeueAfter > 0 {
			return res, err
		}
	}

	ads, err := r.Queue.Query(ctx, condor.ClusterConstraint(hj.Status.ClusterID))
	if err != nil {
		lg.Error(err, "reading job queue", "cluster", hj.Status.ClusterID)
		setCondition(&hj.Status.Conditions, hj.Generation, v1alpha1.ConditionQueueReachable,
			metav1.ConditionFalse, "QueryFailed", err.Error())
		// The previous counts stay as they were. They are stale, not
		// wrong, and .status.lastQueueSync says how stale.
		if werr := r.writeStatus(ctx, &hj); werr != nil {
			return ctrl.Result{}, werr
		}
		return ctrl.Result{RequeueAfter: r.retryInterval()}, nil
	}
	setCondition(&hj.Status.Conditions, hj.Generation, v1alpha1.ConditionQueueReachable,
		metav1.ConditionTrue, "Queried", "")

	if err := r.reconcileSuspend(ctx, &hj, ads, constraint); err != nil {
		return ctrl.Result{}, err
	}

	terminal := r.applyQueueStatus(&hj, ads)
	if err := r.writeStatus(ctx, &hj); err != nil {
		return ctrl.Result{}, err
	}
	if terminal {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: r.pollInterval()}, nil
}

// ensureSubmitted gets exactly one cluster into the queue for this CR.
//
// The hazard it exists for: a reconcile that submits and then dies before
// writing .status.clusterID leaves jobs running that nothing points at, and
// the next reconcile -- seeing an empty clusterID -- would submit them again.
//
// The fix is to make the queue itself the record. Every submit stamps
// +K8sOwnerUID with the CR's UID, which the API server assigned before this
// controller ever saw the object and which no other object will ever have. So
// before submitting, ask the queue whether this CR's jobs are already there.
// A submit whose outcome is unknown -- the connection dropped during the
// commit -- is resolved the same way on the next pass, because a committed
// qmgmt transaction is visible to the very next query and an aborted one left
// nothing behind. There is no window in which both answers are possible.
func (r *HTCJobReconciler) ensureSubmitted(ctx context.Context, hj *v1alpha1.HTCJob, jobSpec *batchv1.JobSpec, constraint string) (ctrl.Result, error) {
	lg := log.FromContext(ctx)

	existing, err := r.Queue.Query(ctx, constraint)
	if err != nil {
		setCondition(&hj.Status.Conditions, hj.Generation, v1alpha1.ConditionQueueReachable,
			metav1.ConditionFalse, "QueryFailed", err.Error())
		if werr := r.writeStatus(ctx, hj); werr != nil {
			return ctrl.Result{}, werr
		}
		return ctrl.Result{RequeueAfter: r.retryInterval()}, nil
	}
	if len(existing) > 0 {
		cluster := strconv.Itoa(existing[0].ClusterID)
		lg.Info("adopting a cluster this CR already owns", "cluster", cluster)
		r.event(hj, "Normal", "Adopted", "adopted existing HTCondor cluster "+cluster)
		return ctrl.Result{}, r.recordCluster(ctx, hj, cluster)
	}

	submitText, err := r.renderSubmit(hj, jobSpec)
	if err != nil {
		r.event(hj, "Warning", "SubmitRenderFailed", err.Error())
		setCondition(&hj.Status.Conditions, hj.Generation, v1alpha1.ConditionAdmitted,
			metav1.ConditionFalse, "RenderFailed", err.Error())
		hj.Status.Phase = v1alpha1.PhaseRejected
		return ctrl.Result{}, r.writeStatus(ctx, hj)
	}

	cluster, err := r.Queue.Submit(ctx, submitText)
	if err != nil {
		// Deliberately not an error return: an error return retries with
		// controller-runtime's backoff, and the retry must go through
		// the query above rather than straight back to Submit.
		lg.Error(err, "submitting to the schedd; the next pass will look for the cluster before retrying")
		r.event(hj, "Warning", "SubmitFailed", err.Error())
		setCondition(&hj.Status.Conditions, hj.Generation, v1alpha1.ConditionQueueReachable,
			metav1.ConditionFalse, "SubmitFailed", err.Error())
		if werr := r.writeStatus(ctx, hj); werr != nil {
			return ctrl.Result{}, werr
		}
		return ctrl.Result{RequeueAfter: r.retryInterval()}, nil
	}

	r.event(hj, "Normal", "Submitted", "submitted HTCondor cluster "+cluster)
	return ctrl.Result{}, r.recordCluster(ctx, hj, cluster)
}

// recordCluster writes the cluster id down as its own status update, before
// any other status work. If this write fails the jobs are still findable by
// owner UID, but the sooner the id is in etcd the shorter the window in which
// a query is the only way to find them.
func (r *HTCJobReconciler) recordCluster(ctx context.Context, hj *v1alpha1.HTCJob, cluster string) error {
	hj.Status.ClusterID = cluster
	if hj.Status.Phase == "" || hj.Status.Phase == v1alpha1.PhasePending {
		hj.Status.Phase = v1alpha1.PhaseSubmitted
	}
	setCondition(&hj.Status.Conditions, hj.Generation, v1alpha1.ConditionSubmitted,
		metav1.ConditionTrue, "Queued", "HTCondor cluster "+cluster)
	return r.writeStatus(ctx, hj)
}

// renderSubmit builds the submit-file text. The identity it stamps names the
// HTCJob, not the Job above it: the HTCJob is what this controller owns and
// what the finalizer protects, and its UID is the join key every query here
// uses.
func (r *HTCJobReconciler) renderSubmit(hj *v1alpha1.HTCJob, jobSpec *batchv1.JobSpec) (string, error) {
	opts := translate.JobSubmitOptions(jobSpec, r.Caps)
	opts.Suspend = adapter.Suspended(&hj.Spec)
	if r.SpoolRoot != "" {
		opts.IWD = filepath.Join(r.SpoolRoot, hj.Namespace, hj.Name)
	}

	id := translate.Identity{
		Name:        hj.Name,
		Namespace:   hj.Namespace,
		UID:         string(hj.UID),
		OwnerKind:   "HTCJob",
		OwnerName:   hj.Name,
		OwnerUID:    string(hj.UID),
		Labels:      hj.Spec.Template.Labels,
		Annotations: hj.Spec.Template.Annotations,
	}
	return translate.PodToSubmit(&jobSpec.Template.Spec, id, opts)
}

// reconcileSuspend holds or releases the cluster to match .spec.suspend.
//
// The decision is made from the queue's own state rather than from a flag in
// status, so a hold placed or released by hand with condor_hold is converged
// back rather than fought over blindly on every pass.
func (r *HTCJobReconciler) reconcileSuspend(ctx context.Context, hj *v1alpha1.HTCJob, ads []translate.JobAd, constraint string) error {
	want := adapter.Suspended(&hj.Spec)

	var holdable, releasable int
	for _, ad := range ads {
		switch {
		case ad.JobStatus == translate.JobStatusHeld && ad.HoldReasonCode == 1:
			// Hold code 1 is "a user asked for this", which is the
			// only hold this controller may undo. A system hold --
			// a failed image pull, a memory overrun -- is a fact
			// about the job, and releasing it would just put it
			// back on hold.
			releasable++
		case ad.JobStatus == translate.JobStatusIdle || ad.JobStatus == translate.JobStatusRunning:
			holdable++
		}
	}

	switch {
	case want && holdable > 0:
		if err := r.Queue.Hold(ctx, constraint, "suspended by HTCJob "+hj.Namespace+"/"+hj.Name); err != nil {
			r.event(hj, "Warning", "HoldFailed", err.Error())
			return err
		}
		r.event(hj, "Normal", "Suspended", fmt.Sprintf("held %d procs", holdable))
	case !want && releasable > 0:
		if err := r.Queue.Release(ctx, constraint, "resumed by HTCJob "+hj.Namespace+"/"+hj.Name); err != nil {
			r.event(hj, "Warning", "ReleaseFailed", err.Error())
			return err
		}
		r.event(hj, "Normal", "Resumed", fmt.Sprintf("released %d procs", releasable))
	}
	return nil
}

// applyQueueStatus folds the job ads into status and reports whether the job
// has reached a state it will not leave on its own.
func (r *HTCJobReconciler) applyQueueStatus(hj *v1alpha1.HTCJob, ads []translate.JobAd) bool {
	now := metav1.Now()
	hj.Status.LastQueueSync = &now
	hj.Status.ObservedGeneration = hj.Generation

	if len(ads) == 0 {
		// The cluster is gone from the queue. It either finished and was
		// reaped into history, or somebody ran condor_rm. The last
		// counts we saw are the best answer available here. Telling
		// the two apart needs a history query on the owner UID, which
		// the Queue interface has no method for yet, so a cluster that
		// finishes between two polls is reported as a failure.
		if hj.Status.Phase == v1alpha1.PhaseRunning || hj.Status.Phase == v1alpha1.PhaseSubmitted {
			hj.Status.Active = 0
			hj.Status.Phase = v1alpha1.PhaseFailed
			setCondition(&hj.Status.Conditions, hj.Generation, v1alpha1.ConditionFailed,
				metav1.ConditionTrue, "LeftQueue",
				"the cluster is no longer in the queue and its outcome was not observed")
		}
		return true
	}

	completions := adapter.Completions(&hj.Spec)
	js := translate.JobStatus(ads, completions, hj.Status.StartTime)
	hj.Status.Active = js.Active
	hj.Status.Succeeded = js.Succeeded
	hj.Status.Failed = js.Failed

	containerName := hj.Spec.Template.Spec.Container.Name
	procs := make([]v1alpha1.HTCProcStatus, 0, min(len(ads), maxReportedProcs))
	var earliestStart, latestFinish *metav1.Time
	for i, ad := range ads {
		if ad.JobStartDate.IsZero() {
			// not started
		} else if t := metav1.NewTime(ad.JobStartDate); earliestStart == nil || t.Before(earliestStart) {
			earliestStart = &t
		}
		if !ad.CompletionDate.IsZero() {
			if t := metav1.NewTime(ad.CompletionDate); latestFinish == nil || latestFinish.Before(&t) {
				latestFinish = &t
			}
		}
		if i >= maxReportedProcs {
			continue
		}
		procs = append(procs, procStatus(ad, containerName))
	}
	hj.Status.Procs = procs
	if hj.Status.StartTime == nil && earliestStart != nil {
		hj.Status.StartTime = earliestStart
	}

	terminal := true
	for _, ad := range ads {
		if !ad.Terminal() {
			terminal = false
			break
		}
	}

	switch {
	case js.Succeeded >= completions:
		hj.Status.Phase = v1alpha1.PhaseSucceeded
		hj.Status.CompletionTime = latestFinish
		setCondition(&hj.Status.Conditions, hj.Generation, v1alpha1.ConditionComplete,
			metav1.ConditionTrue, "AllProcsSucceeded", "")
	case terminal && js.Failed > 0:
		hj.Status.Phase = v1alpha1.PhaseFailed
		hj.Status.CompletionTime = latestFinish
		setCondition(&hj.Status.Conditions, hj.Generation, v1alpha1.ConditionFailed,
			metav1.ConditionTrue, "ProcsFailed",
			fmt.Sprintf("%d of %d procs failed", js.Failed, len(ads)))
	case js.Active > 0 && anyRunning(ads):
		hj.Status.Phase = v1alpha1.PhaseRunning
	default:
		hj.Status.Phase = v1alpha1.PhaseSubmitted
	}
	return terminal
}

func anyRunning(ads []translate.JobAd) bool {
	for _, ad := range ads {
		if ad.JobStatus == translate.JobStatusRunning || ad.JobStatus == translate.JobStatusXferOut {
			return true
		}
	}
	return false
}

func procStatus(ad translate.JobAd, containerName string) v1alpha1.HTCProcStatus {
	// PodStatus is the shared mapping from a job ad to Kubernetes terms --
	// including the awkward cases, like a system hold being Failed while a
	// user hold is Pending. Reusing it here is what keeps the CRD's phase
	// and the facade's Pod phase from drifting apart.
	ps := translate.PodStatus(ad, containerName)
	out := v1alpha1.HTCProcStatus{
		ProcID:     int32(ad.ProcID),
		JobStatus:  int32(ad.JobStatus),
		Phase:      string(ps.Phase),
		HoldReason: ad.HoldReason,
		RemoteHost: ad.RemoteHost,
	}
	if ad.Index >= 0 {
		idx := int32(ad.Index)
		out.Index = &idx
	}
	if !ad.JobStartDate.IsZero() {
		t := metav1.NewTime(ad.JobStartDate)
		out.StartTime = &t
	}
	if !ad.CompletionDate.IsZero() {
		t := metav1.NewTime(ad.CompletionDate)
		out.CompletionTime = &t
	}
	if len(ps.ContainerStatuses) > 0 && ps.ContainerStatuses[0].State.Terminated != nil {
		code := ps.ContainerStatuses[0].State.Terminated.ExitCode
		out.ExitCode = &code
	}
	return out
}

// finalize removes the cluster from the queue, then lets the CR go.
func (r *HTCJobReconciler) finalize(ctx context.Context, hj *v1alpha1.HTCJob) (ctrl.Result, error) {
	lg := log.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(hj, v1alpha1.Finalizer) {
		return ctrl.Result{}, nil
	}

	if err := r.removeFromQueue(ctx, hj); err != nil {
		lg.Error(err, "removing jobs from the queue; not releasing the finalizer")
		r.event(hj, "Warning", "RemoveFailed", err.Error())
		// Holding the finalizer blocks the delete. That is the point:
		// dropping it here would strand running jobs, and a stuck
		// delete is visible where an orphaned cluster is not.
		return ctrl.Result{RequeueAfter: r.retryInterval()}, nil
	}

	controllerutil.RemoveFinalizer(hj, v1alpha1.Finalizer)
	if err := r.Update(ctx, hj); err != nil && !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *HTCJobReconciler) removeFromQueue(ctx context.Context, hj *v1alpha1.HTCJob) error {
	if ids := procIDs(hj); len(ids) > 0 {
		return r.Queue.Remove(ctx, ids, "HTCJob deleted")
	}
	// No proc list: either the status write never landed or the cluster is
	// larger than status reports. The owner UID constraint covers both,
	// and covers the case where no cluster id was ever recorded.
	return r.Queue.RemoveByConstraint(ctx, condor.OwnerConstraint(string(hj.UID)), "HTCJob deleted")
}

func procIDs(hj *v1alpha1.HTCJob) []string {
	if hj.Status.ClusterID == "" {
		return nil
	}
	if int32(len(hj.Status.Procs)) < adapter.Completions(&hj.Spec) {
		// status.procs is capped and may be short; falling back to the
		// constraint is the only way to be sure every proc goes.
		return nil
	}
	ids := make([]string, 0, len(hj.Status.Procs))
	for _, p := range hj.Status.Procs {
		ids = append(ids, p.JobID(hj.Status.ClusterID))
	}
	return ids
}

func (r *HTCJobReconciler) writeStatus(ctx context.Context, hj *v1alpha1.HTCJob) error {
	err := r.Status().Update(ctx, hj)
	if errors.IsConflict(err) {
		// Somebody else wrote the object between our read and this
		// write. Nothing here is a delta -- the next pass recomputes
		// the whole status from the queue -- so a conflict is a reason
		// to go around again, not to fail.
		return nil
	}
	return client.IgnoreNotFound(err)
}

func (r *HTCJobReconciler) event(hj *v1alpha1.HTCJob, kind, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Event(hj, kind, reason, msg)
	}
}

func (r *HTCJobReconciler) pollInterval() time.Duration {
	if r.PollInterval > 0 {
		return r.PollInterval
	}
	return defaultPollInterval
}

func (r *HTCJobReconciler) retryInterval() time.Duration {
	if r.RetryInterval > 0 {
		return r.RetryInterval
	}
	return defaultRetryInterval
}

// SetupWithManager registers the reconciler.
func (r *HTCJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.HTCJob{}).
		Named("htcjob").
		Complete(r)
}

// setCondition writes a condition, leaving LastTransitionTime alone when
// nothing changed so that a status poll does not churn the object.
func setCondition(conds *[]metav1.Condition, gen int64, condType string, status metav1.ConditionStatus, reason, msg string) {
	for i := range *conds {
		c := &(*conds)[i]
		if c.Type != condType {
			continue
		}
		if c.Status == status && c.Reason == reason && c.Message == msg && c.ObservedGeneration == gen {
			return
		}
		if c.Status != status {
			c.LastTransitionTime = metav1.Now()
		}
		c.Status, c.Reason, c.Message, c.ObservedGeneration = status, reason, msg, gen
		return
	}
	if reason == "" {
		reason = "Unknown"
	}
	*conds = append(*conds, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: gen,
		LastTransitionTime: metav1.Now(),
	})
}
