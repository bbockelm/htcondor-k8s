package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/bbockelm/htcondor-k8s/operator/api/v1alpha1"
	"github.com/bbockelm/htcondor-k8s/operator/internal/condor"
	htcwebhook "github.com/bbockelm/htcondor-k8s/operator/internal/webhook"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	require.NoError(t, v1alpha1.AddToScheme(s))
	return s
}

func claimedJob(name string) *batchv1.Job {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default",
			UID: types.UID("job-uid-" + name),
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:    "work",
						Image:   "docker://busybox:latest",
						Command: []string{"/bin/sh", "-c", "echo hi"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("2"),
								corev1.ResourceMemory: resource.MustParse("512Mi"),
							},
						},
					}},
				},
			},
		},
	}
	htcwebhook.Claim(job, time.Now())
	return job
}

func htcJob(name string, completions int32) *v1alpha1.HTCJob {
	return &v1alpha1.HTCJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default", UID: types.UID("htcjob-uid-" + name),
		},
		Spec: v1alpha1.HTCJobSpec{
			Completions: &completions,
			Template: v1alpha1.HTCPodTemplateSpec{
				Spec: v1alpha1.HTCPodSpec{
					RestartPolicy: "Never",
					Container: v1alpha1.HTCContainer{
						Name:    "work",
						Image:   "docker://busybox:latest",
						Command: []string{"/bin/sh", "-c", "echo hi"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1"),
								corev1.ResourceMemory: resource.MustParse("256Mi"),
							},
						},
					},
				},
			},
		},
	}
}

func newClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.HTCJob{}, &batchv1.Job{}).
		Build()
}

func reqFor(o client.Object) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Name: o.GetName(), Namespace: o.GetNamespace()}}
}

// reconcileUntilStable drives the reconciler the way the manager would, up to
// a bound, so a test never depends on how many passes the implementation needs
// to settle. It is a loop rather than a sleep: nothing here is asynchronous.
func reconcileUntilStable(t *testing.T, r reconciler, req ctrl.Request, max int) ctrl.Result {
	t.Helper()
	var res ctrl.Result
	for i := 0; i < max; i++ {
		var err error
		res, err = r.Reconcile(context.Background(), req)
		require.NoError(t, err, "reconcile pass %d", i)
		if !res.Requeue {
			return res
		}
	}
	t.Fatalf("reconciler did not settle in %d passes", max)
	return res
}

type reconciler interface {
	Reconcile(context.Context, ctrl.Request) (ctrl.Result, error)
}

func TestJobReconcilerCreatesOwnedHTCJob(t *testing.T) {
	job := claimedJob("mirror")
	c := newClient(t, job)
	r := &JobReconciler{Client: c, Scheme: testScheme(t), Caps: translate.DefaultCapabilities()}

	_, err := r.Reconcile(context.Background(), reqFor(job))
	require.NoError(t, err)

	var hj v1alpha1.HTCJob
	require.NoError(t, c.Get(context.Background(), reqFor(job).NamespacedName, &hj))

	require.Equal(t, "docker://busybox:latest", hj.Spec.Template.Spec.Container.Image)
	require.Equal(t, []string{"/bin/sh", "-c", "echo hi"}, hj.Spec.Template.Spec.Container.Command)
	require.Equal(t, int32(1), *hj.Spec.Completions)
	require.Equal(t, "Never", hj.Spec.Template.Spec.RestartPolicy)
	require.Equal(t, string(job.UID), hj.Labels[v1alpha1.LabelOwnerUID])

	require.Len(t, hj.OwnerReferences, 1)
	require.Equal(t, "Job", hj.OwnerReferences[0].Kind)
	require.Equal(t, job.UID, hj.OwnerReferences[0].UID)
	require.True(t, *hj.OwnerReferences[0].Controller)

	// The Job's own .spec.suspend is held at true by the webhook, so the
	// HTCJob must take its suspend from the annotation instead.
	require.True(t, *job.Spec.Suspend)
	require.False(t, *hj.Spec.Suspend)
}

func TestJobReconcilerIgnoresUnclaimedJob(t *testing.T) {
	job := claimedJob("plain")
	job.Annotations = nil
	job.Spec.Template.Spec.SchedulerName = ""

	c := newClient(t, job)
	r := &JobReconciler{Client: c, Scheme: testScheme(t), Caps: translate.DefaultCapabilities()}
	_, err := r.Reconcile(context.Background(), reqFor(job))
	require.NoError(t, err)

	var list v1alpha1.HTCJobList
	require.NoError(t, c.List(context.Background(), &list))
	require.Empty(t, list.Items, "an unclaimed Job must not produce an HTCJob")
}

func TestJobReconcilerSuspendAnnotationReachesTheHTCJob(t *testing.T) {
	job := claimedJob("suspendable")
	job.Annotations[v1alpha1.AnnotationUserSuspend] = "true"

	c := newClient(t, job)
	r := &JobReconciler{Client: c, Scheme: testScheme(t), Caps: translate.DefaultCapabilities()}
	_, err := r.Reconcile(context.Background(), reqFor(job))
	require.NoError(t, err)

	var hj v1alpha1.HTCJob
	require.NoError(t, c.Get(context.Background(), reqFor(job).NamespacedName, &hj))
	require.True(t, *hj.Spec.Suspend)

	// Resuming flips the annotation; the next pass must carry it through.
	job.Annotations[v1alpha1.AnnotationUserSuspend] = "false"
	require.NoError(t, c.Update(context.Background(), job))
	_, err = r.Reconcile(context.Background(), reqFor(job))
	require.NoError(t, err)
	require.NoError(t, c.Get(context.Background(), reqFor(job).NamespacedName, &hj))
	require.False(t, *hj.Spec.Suspend)
}

func TestJobReconcilerRefusesUntranslatableClaimedJob(t *testing.T) {
	// A Job carrying the claim annotation that was never actually admitted
	// -- restored from a backup, or hand-edited.
	job := claimedJob("forged")
	job.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 8080}}

	c := newClient(t, job)
	r := &JobReconciler{Client: c, Scheme: testScheme(t), Caps: translate.DefaultCapabilities()}
	_, err := r.Reconcile(context.Background(), reqFor(job))
	require.NoError(t, err)

	var list v1alpha1.HTCJobList
	require.NoError(t, c.List(context.Background(), &list))
	require.Empty(t, list.Items)
}

func newHTCJobReconciler(t *testing.T, c client.Client, q condor.Queue) *HTCJobReconciler {
	return &HTCJobReconciler{
		Client: c, Scheme: testScheme(t), Queue: q,
		Caps:          translate.DefaultCapabilities(),
		PollInterval:  time.Second,
		RetryInterval: time.Millisecond,
	}
}

func TestHTCJobReconcilerSubmitsExactlyOnce(t *testing.T) {
	hj := htcJob("once", 3)
	c := newClient(t, hj)
	q := condor.NewFakeQueue()
	r := newHTCJobReconciler(t, c, q)

	reconcileUntilStable(t, r, reqFor(hj), 5)
	require.Equal(t, 1, q.SubmitCount())

	var got v1alpha1.HTCJob
	require.NoError(t, c.Get(context.Background(), reqFor(hj).NamespacedName, &got))
	require.Equal(t, "100", got.Status.ClusterID)
	require.Equal(t, v1alpha1.PhaseSubmitted, got.Status.Phase)
	require.Len(t, got.Status.Procs, 3)

	// Three more passes, as a crash loop or a resync would produce.
	for i := 0; i < 3; i++ {
		_, err := r.Reconcile(context.Background(), reqFor(hj))
		require.NoError(t, err)
	}
	require.Equal(t, 1, q.SubmitCount(), "a resync must not submit again")

	// The submit file carries the join key the idempotency relies on.
	require.Contains(t, q.Submissions[0], "+"+translate.AttrOwnerUID+" = \""+string(hj.UID)+"\"")
	require.Contains(t, q.Submissions[0], "queue 3")
	require.Contains(t, q.Submissions[0], "request_cpus = 1")
}

// TestHTCJobReconcilerDoesNotResubmitAfterALostSubmitResult is the hazard the
// owner-UID query exists for: the jobs really are in the queue and the
// controller never learned their cluster id.
func TestHTCJobReconcilerDoesNotResubmitAfterALostSubmitResult(t *testing.T) {
	hj := htcJob("lost", 2)
	c := newClient(t, hj)
	q := condor.NewFakeQueue()
	q.LoseSubmitResult = true
	r := newHTCJobReconciler(t, c, q)

	// Pass 1 adds the finalizer; pass 2 submits and loses the answer.
	_, err := r.Reconcile(context.Background(), reqFor(hj))
	require.NoError(t, err)
	res, err := r.Reconcile(context.Background(), reqFor(hj))
	require.NoError(t, err)
	require.NotZero(t, res.RequeueAfter, "a failed submit must be retried, not dropped")

	var got v1alpha1.HTCJob
	require.NoError(t, c.Get(context.Background(), reqFor(hj).NamespacedName, &got))
	require.Empty(t, got.Status.ClusterID, "nothing was recorded, which is the whole problem")
	require.Equal(t, 1, q.SubmitCount())

	// The schedd comes back. The next pass must find the orphaned cluster
	// rather than queue a second one.
	q.LoseSubmitResult = false
	reconcileUntilStable(t, r, reqFor(hj), 5)
	require.Equal(t, 1, q.SubmitCount(), "the existing cluster must be adopted, not duplicated")

	require.NoError(t, c.Get(context.Background(), reqFor(hj).NamespacedName, &got))
	require.Equal(t, "100", got.Status.ClusterID)
	require.Len(t, got.Status.Procs, 2)
}

func TestHTCJobReconcilerTracksQueueStatus(t *testing.T) {
	hj := htcJob("status", 2)
	c := newClient(t, hj)
	q := condor.NewFakeQueue()
	r := newHTCJobReconciler(t, c, q)
	reconcileUntilStable(t, r, reqFor(hj), 5)

	started := time.Now().Add(-time.Minute).Truncate(time.Second)
	q.SetProcStatus("100", 0, translate.JobStatusRunning, func(ad *translate.JobAd) {
		ad.JobStartDate = started
		ad.RemoteHost = "slot1@ep1.example.edu"
	})
	res, err := r.Reconcile(context.Background(), reqFor(hj))
	require.NoError(t, err)
	require.Equal(t, time.Second, res.RequeueAfter, "a live job must be polled again")

	var got v1alpha1.HTCJob
	require.NoError(t, c.Get(context.Background(), reqFor(hj).NamespacedName, &got))
	require.Equal(t, v1alpha1.PhaseRunning, got.Status.Phase)
	require.Equal(t, int32(2), got.Status.Active)
	require.NotNil(t, got.Status.StartTime)
	require.NotNil(t, got.Status.LastQueueSync)
	require.Equal(t, "slot1@ep1.example.edu", got.Status.Procs[0].RemoteHost)
	require.Equal(t, string(corev1.PodRunning), got.Status.Procs[0].Phase)

	finished := time.Now().Truncate(time.Second)
	for p := 0; p < 2; p++ {
		q.SetProcStatus("100", p, translate.JobStatusCompleted, func(ad *translate.JobAd) {
			ad.ExitCode = 0
			ad.CompletionDate = finished
		})
	}
	res, err = r.Reconcile(context.Background(), reqFor(hj))
	require.NoError(t, err)
	require.Zero(t, res.RequeueAfter, "a finished job must stop being polled")

	require.NoError(t, c.Get(context.Background(), reqFor(hj).NamespacedName, &got))
	require.Equal(t, v1alpha1.PhaseSucceeded, got.Status.Phase)
	require.Equal(t, int32(2), got.Status.Succeeded)
	require.Equal(t, int32(0), got.Status.Active)
	require.NotNil(t, got.Status.CompletionTime)
	require.True(t, hasCondition(got.Status.Conditions, v1alpha1.ConditionComplete, metav1.ConditionTrue))
}

func TestHTCJobReconcilerReportsFailure(t *testing.T) {
	hj := htcJob("failing", 2)
	c := newClient(t, hj)
	q := condor.NewFakeQueue()
	r := newHTCJobReconciler(t, c, q)
	reconcileUntilStable(t, r, reqFor(hj), 5)

	q.SetProcStatus("100", 0, translate.JobStatusCompleted, func(ad *translate.JobAd) { ad.ExitCode = 0 })
	q.SetProcStatus("100", 1, translate.JobStatusCompleted, func(ad *translate.JobAd) { ad.ExitCode = 17 })
	_, err := r.Reconcile(context.Background(), reqFor(hj))
	require.NoError(t, err)

	var got v1alpha1.HTCJob
	require.NoError(t, c.Get(context.Background(), reqFor(hj).NamespacedName, &got))
	require.Equal(t, v1alpha1.PhaseFailed, got.Status.Phase)
	require.Equal(t, int32(1), got.Status.Succeeded)
	require.Equal(t, int32(1), got.Status.Failed)
	require.Equal(t, int32(17), *got.Status.Procs[1].ExitCode)
}

// A system hold is a terminal state, not a Pending one, or the controller
// polls a job that will never move for as long as the CR exists.
func TestHTCJobReconcilerTreatsSystemHoldAsTerminal(t *testing.T) {
	hj := htcJob("held", 1)
	c := newClient(t, hj)
	q := condor.NewFakeQueue()
	r := newHTCJobReconciler(t, c, q)
	reconcileUntilStable(t, r, reqFor(hj), 5)

	q.SetProcStatus("100", 0, translate.JobStatusHeld, func(ad *translate.JobAd) {
		ad.HoldReasonCode = 26 // failed to pull the container image
		ad.HoldReason = "Error from slot1@ep1: image not found"
	})
	res, err := r.Reconcile(context.Background(), reqFor(hj))
	require.NoError(t, err)
	require.Zero(t, res.RequeueAfter)

	var got v1alpha1.HTCJob
	require.NoError(t, c.Get(context.Background(), reqFor(hj).NamespacedName, &got))
	require.Equal(t, v1alpha1.PhaseFailed, got.Status.Phase)
	require.Equal(t, string(corev1.PodFailed), got.Status.Procs[0].Phase)
	require.Contains(t, got.Status.Procs[0].HoldReason, "image not found")
}

func TestHTCJobReconcilerSuspendAndResume(t *testing.T) {
	hj := htcJob("suspend", 2)
	c := newClient(t, hj)
	q := condor.NewFakeQueue()
	r := newHTCJobReconciler(t, c, q)
	reconcileUntilStable(t, r, reqFor(hj), 5)
	require.False(t, q.IsHeld("100"))

	var got v1alpha1.HTCJob
	require.NoError(t, c.Get(context.Background(), reqFor(hj).NamespacedName, &got))
	tr := true
	got.Spec.Suspend = &tr
	require.NoError(t, c.Update(context.Background(), &got))

	_, err := r.Reconcile(context.Background(), reqFor(hj))
	require.NoError(t, err)
	require.True(t, q.IsHeld("100"), "suspend must hold the cluster")

	// A second pass must not re-hold: the ads already say held.
	_, err = r.Reconcile(context.Background(), reqFor(hj))
	require.NoError(t, err)

	require.NoError(t, c.Get(context.Background(), reqFor(hj).NamespacedName, &got))
	f := false
	got.Spec.Suspend = &f
	require.NoError(t, c.Update(context.Background(), &got))
	_, err = r.Reconcile(context.Background(), reqFor(hj))
	require.NoError(t, err)
	require.False(t, q.IsHeld("100"), "clearing suspend must release the cluster")
}

func TestHTCJobReconcilerSubmitsSuspendedJobOnHold(t *testing.T) {
	hj := htcJob("born-suspended", 1)
	tr := true
	hj.Spec.Suspend = &tr
	c := newClient(t, hj)
	q := condor.NewFakeQueue()
	r := newHTCJobReconciler(t, c, q)
	reconcileUntilStable(t, r, reqFor(hj), 5)

	require.Contains(t, q.Submissions[0], "hold = True")
	require.True(t, q.IsHeld("100"))
}

func TestHTCJobReconcilerRemovesFromQueueOnDelete(t *testing.T) {
	hj := htcJob("deleteme", 2)
	c := newClient(t, hj)
	q := condor.NewFakeQueue()
	r := newHTCJobReconciler(t, c, q)
	reconcileUntilStable(t, r, reqFor(hj), 5)
	require.False(t, q.IsRemoved("100"))

	var got v1alpha1.HTCJob
	require.NoError(t, c.Get(context.Background(), reqFor(hj).NamespacedName, &got))
	require.Contains(t, got.Finalizers, v1alpha1.Finalizer)
	require.NoError(t, c.Delete(context.Background(), &got))

	_, err := r.Reconcile(context.Background(), reqFor(hj))
	require.NoError(t, err)
	require.True(t, q.IsRemoved("100"), "deleting the CR must take the cluster out of the queue")

	err = c.Get(context.Background(), reqFor(hj).NamespacedName, &got)
	require.Error(t, err, "the finalizer must be released once the queue is clean")
}

// A delete that cannot reach the schedd must block, because dropping the
// finalizer would leave jobs running that nothing points at any more.
func TestHTCJobReconcilerHoldsFinalizerWhenTheScheddIsUnreachable(t *testing.T) {
	hj := htcJob("stuck", 1)
	c := newClient(t, hj)
	q := condor.NewFakeQueue()
	r := newHTCJobReconciler(t, c, q)
	reconcileUntilStable(t, r, reqFor(hj), 5)

	var got v1alpha1.HTCJob
	require.NoError(t, c.Get(context.Background(), reqFor(hj).NamespacedName, &got))
	require.NoError(t, c.Delete(context.Background(), &got))

	q.ActionErr = errUnreachable
	res, err := r.Reconcile(context.Background(), reqFor(hj))
	require.NoError(t, err)
	require.NotZero(t, res.RequeueAfter)
	require.NoError(t, c.Get(context.Background(), reqFor(hj).NamespacedName, &got),
		"the object must still exist")
	require.Contains(t, got.Finalizers, v1alpha1.Finalizer)

	q.ActionErr = nil
	_, err = r.Reconcile(context.Background(), reqFor(hj))
	require.NoError(t, err)
	require.True(t, q.IsRemoved("100"))
}

func TestHTCJobReconcilerKeepsStaleStatusWhenTheScheddIsUnreachable(t *testing.T) {
	hj := htcJob("stale", 1)
	c := newClient(t, hj)
	q := condor.NewFakeQueue()
	r := newHTCJobReconciler(t, c, q)
	reconcileUntilStable(t, r, reqFor(hj), 5)

	q.SetProcStatus("100", 0, translate.JobStatusRunning, func(ad *translate.JobAd) {
		ad.JobStartDate = time.Now().Add(-time.Minute)
	})
	_, err := r.Reconcile(context.Background(), reqFor(hj))
	require.NoError(t, err)

	q.QueryErr = errUnreachable
	res, err := r.Reconcile(context.Background(), reqFor(hj))
	require.NoError(t, err, "an unreachable schedd is not a reconcile failure")
	require.NotZero(t, res.RequeueAfter)

	var got v1alpha1.HTCJob
	require.NoError(t, c.Get(context.Background(), reqFor(hj).NamespacedName, &got))
	require.Equal(t, v1alpha1.PhaseRunning, got.Status.Phase, "the last known state is kept")
	require.Equal(t, int32(1), got.Status.Active)
	require.True(t, hasCondition(got.Status.Conditions, v1alpha1.ConditionQueueReachable, metav1.ConditionFalse))
}

func TestHTCJobReconcilerRejectsASpecThePoolCannotRun(t *testing.T) {
	hj := htcJob("too-big", 1)
	hj.Spec.Template.Spec.Container.Resources.Requests[corev1.ResourceCPU] = resource.MustParse("64")

	c := newClient(t, hj)
	q := condor.NewFakeQueue()
	r := newHTCJobReconciler(t, c, q)
	r.Caps.MaxCPU = 16

	reconcileUntilStable(t, r, reqFor(hj), 5)
	require.Equal(t, 0, q.SubmitCount())

	var got v1alpha1.HTCJob
	require.NoError(t, c.Get(context.Background(), reqFor(hj).NamespacedName, &got))
	require.Equal(t, v1alpha1.PhaseRejected, got.Status.Phase)
	require.True(t, hasCondition(got.Status.Conditions, v1alpha1.ConditionAdmitted, metav1.ConditionFalse))
}

func TestJobStatusFromHTCJob(t *testing.T) {
	hj := htcJob("mapped", 3)
	start := metav1.NewTime(time.Now().Add(-time.Hour))
	done := metav1.NewTime(time.Now())
	hj.Status = v1alpha1.HTCJobStatus{
		Phase: v1alpha1.PhaseSucceeded, Succeeded: 3,
		StartTime: &start, CompletionTime: &done,
	}
	st := JobStatusFromHTCJob(hj)
	require.Equal(t, int32(3), st.Succeeded)
	require.Equal(t, &start, st.StartTime)

	// The API server refuses Complete=True without SuccessCriteriaMet=True
	// and without a completionTime, so the mirror has to reproduce the
	// built-in Job controller's whole condition sequence, not just its
	// conclusion.
	require.Equal(t, []batchv1.JobConditionType{batchv1.JobSuccessCriteriaMet, batchv1.JobComplete},
		conditionTypes(st.Conditions))

	// A status with counts but no observed times must still be valid
	// Kubernetes: a complete Job without a completionTime is rejected, and
	// one with counts and no startTime is too.
	hj.Status.StartTime = nil
	hj.Status.CompletionTime = nil
	st = JobStatusFromHTCJob(hj)
	require.NotNil(t, st.StartTime)
	require.NotNil(t, st.CompletionTime)

	hj.Status = v1alpha1.HTCJobStatus{Phase: v1alpha1.PhaseFailed, Failed: 2}
	st = JobStatusFromHTCJob(hj)
	require.Equal(t, []batchv1.JobConditionType{batchv1.JobFailureTarget, batchv1.JobFailed},
		conditionTypes(st.Conditions))
	require.NotNil(t, st.StartTime)
}

func conditionTypes(conds []batchv1.JobCondition) []batchv1.JobConditionType {
	out := make([]batchv1.JobConditionType, 0, len(conds))
	for _, c := range conds {
		out = append(out, c.Type)
	}
	return out
}

func TestSubmitFileCarriesTheKubernetesIdentity(t *testing.T) {
	hj := htcJob("ident", 1)
	c := newClient(t, hj)
	q := condor.NewFakeQueue()
	r := newHTCJobReconciler(t, c, q)
	r.SpoolRoot = "/var/lib/htcondor-k8s"
	reconcileUntilStable(t, r, reqFor(hj), 5)

	sf := q.Submissions[0]
	for _, want := range []string{
		`+` + translate.AttrOwnerKind + ` = "HTCJob"`,
		`+` + translate.AttrOwnerName + ` = "ident"`,
		`+` + translate.AttrPodNamespace + ` = "default"`,
		"initialdir = /var/lib/htcondor-k8s/default/ident",
		"universe = container",
	} {
		require.True(t, strings.Contains(sf, want), "submit file missing %q:\n%s", want, sf)
	}
}

func hasCondition(conds []metav1.Condition, t string, s metav1.ConditionStatus) bool {
	for _, c := range conds {
		if c.Type == t {
			return c.Status == s
		}
	}
	return false
}

var errUnreachable = &unreachableError{}

type unreachableError struct{}

func (*unreachableError) Error() string { return "schedd unreachable" }
