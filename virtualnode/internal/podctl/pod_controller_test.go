package podctl

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/webhook"
)

// --- harness ------------------------------------------------------------

// bindingShim teaches the controller-runtime fake client the one subresource
// it does not know: pods/binding.
//
// The fake supports eviction and token and nothing else, and the real binding
// path is exercised against a real API server in test/podctl. What is wanted
// here is only that a bound Pod ends up with a node name, so the rest of the
// reconcile can be driven without an apiserver.
type bindingShim struct{ client.Client }

func (b bindingShim) SubResource(s string) client.SubResourceClient {
	if s == "binding" {
		return bindingWriter{b.Client}
	}
	return b.Client.SubResource(s)
}

type bindingWriter struct{ client.Client }

func (w bindingWriter) Get(ctx context.Context, obj client.Object, sub client.Object, _ ...client.SubResourceGetOption) error {
	return w.Client.Get(ctx, client.ObjectKeyFromObject(obj), sub)
}

func (w bindingWriter) Create(ctx context.Context, obj client.Object, sub client.Object, _ ...client.SubResourceCreateOption) error {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	binding, ok := sub.(*corev1.Binding)
	if !ok {
		return nil
	}
	var live corev1.Pod
	if err := w.Client.Get(ctx, client.ObjectKeyFromObject(pod), &live); err != nil {
		return err
	}
	live.Spec.NodeName = binding.Target.Name
	return w.Client.Update(ctx, &live)
}

func (w bindingWriter) Update(context.Context, client.Object, ...client.SubResourceUpdateOption) error {
	return nil
}

func (w bindingWriter) Patch(context.Context, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
	return nil
}

type harness struct {
	r     *PodReconciler
	c     client.Client
	q     *FakeQueue
	t     *testing.T
	key   types.NamespacedName
	clock time.Time
}

func newHarness(t *testing.T, pods ...*corev1.Pod) *harness {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	objs := make([]client.Object, 0, len(pods))
	for _, p := range pods {
		objs = append(objs, p)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&corev1.Pod{}).
		Build()

	h := &harness{c: c, q: NewFakeQueue(), t: t, clock: time.Now()}
	h.q.Now = func() time.Time { return h.clock }
	h.r = &PodReconciler{
		Client:        bindingShim{c},
		Scheme:        scheme,
		Recorder:      record.NewFakeRecorder(100),
		Queue:         h.q,
		Caps:          translate.DefaultCapabilities(),
		ScheddName:    "ap.example.org",
		PollInterval:  time.Millisecond,
		RetryInterval: time.Millisecond,
	}
	if len(pods) > 0 {
		h.key = types.NamespacedName{Namespace: pods[0].Namespace, Name: pods[0].Name}
	}
	return h
}

// run drives the reconcile loop to a fixed point, the way a manager would, so
// a test asserts on an outcome rather than on a particular number of passes.
func (h *harness) run(n int) {
	h.t.Helper()
	for i := 0; i < n; i++ {
		res, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: h.key})
		require.NoErrorf(h.t, err, "reconcile %d", i)
		if !res.Requeue && res.RequeueAfter == 0 {
			return
		}
	}
}

func (h *harness) pod() *corev1.Pod {
	h.t.Helper()
	var p corev1.Pod
	require.NoError(h.t, h.c.Get(context.Background(), h.key, &p))
	return &p
}

func newPod(name string, uid types.UID) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default", UID: uid,
			// Every Pod a Job makes already carries the Job
			// controller's own finalizer. Nothing here may assume it
			// holds the only one, or that removing ours deletes the
			// object.
			Finalizers: []string{"batch.kubernetes.io/job-tracking"},
			Labels: map[string]string{
				"app":          "demo",
				labelJobName:   "work",
				legacyJobName_: "work",
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName: api.SchedulerName,
			RestartPolicy: corev1.RestartPolicyNever,
			// What the webhook stamps: the virtual node's taint plus
			// the not-ready/unreachable pair.
			Tolerations: webhook.Tolerations(),
			Containers: []corev1.Container{{
				Name:    "work",
				Image:   "docker://busybox:latest",
				Command: []string{"/bin/sh", "-c", "echo hi"},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
				},
			}},
		},
	}
}

const legacyJobName_ = legacyLabelJobName

// --- the happy path -----------------------------------------------------

func TestSubmitsBindsAndReportsSucceeded(t *testing.T) {
	h := newHarness(t, newPod("work-abc", "uid-1"))
	h.run(10)

	require.Equal(t, 1, h.q.SubmitCount())
	pod := h.pod()
	require.Equal(t, api.NodeName, pod.Spec.NodeName, "the Pod must be bound to the virtual node")
	require.Equal(t, "100.0", pod.Annotations[api.AnnotationJobID])
	require.Equal(t, "ap.example.org", pod.Annotations[api.AnnotationSchedd])
	require.Contains(t, pod.Finalizers, api.Finalizer)
	require.Equal(t, corev1.PodPending, pod.Status.Phase)

	// The pool runs it.
	h.q.SetStatus("100.0", translate.JobStatusRunning, nil)
	h.run(5)
	pod = h.pod()
	require.Equal(t, corev1.PodRunning, pod.Status.Phase)
	require.NotNil(t, pod.Status.StartTime)
	require.Len(t, pod.Status.ContainerStatuses, 1)
	require.True(t, pod.Status.ContainerStatuses[0].Ready)
	require.Equal(t, "docker://busybox:latest", pod.Status.ContainerStatuses[0].Image)
	require.Equal(t, "htcondor://100.0", pod.Status.ContainerStatuses[0].ContainerID)
	requireCondition(t, pod, corev1.PodScheduled, corev1.ConditionTrue)
	requireCondition(t, pod, corev1.PodReady, corev1.ConditionTrue)

	// And finishes.
	h.q.Complete("100.0", 0)
	h.run(5)
	pod = h.pod()
	require.Equal(t, corev1.PodSucceeded, pod.Status.Phase)
	term := pod.Status.ContainerStatuses[0].State.Terminated
	require.NotNil(t, term)
	require.EqualValues(t, 0, term.ExitCode)
	require.False(t, term.FinishedAt.IsZero())
	require.False(t, term.StartedAt.IsZero())
}

func TestNonZeroExitIsFailedWithTheRealCode(t *testing.T) {
	h := newHarness(t, newPod("work-abc", "uid-1"))
	h.run(10)
	h.q.SetStatus("100.0", translate.JobStatusRunning, nil)
	h.q.Complete("100.0", 17)
	h.run(5)

	pod := h.pod()
	require.Equal(t, corev1.PodFailed, pod.Status.Phase)
	require.EqualValues(t, 17, pod.Status.ContainerStatuses[0].State.Terminated.ExitCode)
}

func requireCondition(t *testing.T, pod *corev1.Pod, ct corev1.PodConditionType, want corev1.ConditionStatus) {
	t.Helper()
	for _, c := range pod.Status.Conditions {
		if c.Type == ct {
			require.Equalf(t, want, c.Status, "condition %s", ct)
			return
		}
	}
	t.Fatalf("no %s condition on the Pod", ct)
}

// --- crash safety -------------------------------------------------------

// TestCrashBetweenSubmitAndRecordDoesNotResubmit is the test the whole
// find-before-submit design exists for.
//
// The fake queues the job and *then* reports an error, which is a connection
// that dropped after the qmgmt commit: the work is really running and the
// controller has no id for it. Nothing on the Pod records the submission, so a
// controller that trusted its own annotations would submit a second cluster
// and run the user's job twice.
//
// Mutation check: deleting the `if len(ads) > 0` adoption branch in
// ensureSubmitted makes this fail with 2 submissions, not with a timeout.
func TestCrashBetweenSubmitAndRecordDoesNotResubmit(t *testing.T) {
	h := newHarness(t, newPod("work-abc", "uid-1"))
	h.q.LoseSubmitResult = true

	// Two passes exactly: one to add the finalizer, one to submit. The
	// second is the crash -- the job is in the queue and the error came
	// back, so nothing was recorded on the Pod.
	for i := 0; i < 2; i++ {
		_, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: h.key})
		require.NoError(t, err)
	}
	require.Equal(t, 1, h.q.SubmitCount())
	require.Equal(t, 1, h.q.LiveCount(), "the job really is in the queue")
	require.Empty(t, h.pod().Annotations[api.AnnotationJobID],
		"the submit reported an error, so nothing should have been recorded")

	// The connection comes back. Every later pass must find the job that is
	// already there rather than making another one.
	h.q.LoseSubmitResult = false
	h.run(10)

	require.Equal(t, 1, h.q.SubmitCount(), "the job was submitted twice")
	require.Equal(t, 1, h.q.LiveCount())
	pod := h.pod()
	require.Equal(t, "100.0", pod.Annotations[api.AnnotationJobID], "the orphaned job was not adopted")
	require.Equal(t, api.NodeName, pod.Spec.NodeName)
}

// TestFinalizerIsAddedBeforeAnySubmission: a crash between the submit and the
// finalizer write would leave a job nothing is obliged to clean up, because
// deleting the Pod would then succeed immediately.
func TestFinalizerIsAddedBeforeAnySubmission(t *testing.T) {
	h := newHarness(t, newPod("work-abc", "uid-1"))

	res, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: h.key})
	require.NoError(t, err)
	require.True(t, res.Requeue)
	require.Contains(t, h.pod().Finalizers, api.Finalizer)
	require.Equal(t, 0, h.q.SubmitCount(), "nothing may be submitted before the finalizer lands")
}

// --- admission ----------------------------------------------------------

// TestBarePodIsAdmittedByTheControllerNotTrusted: a Pod created directly with
// schedulerName: htcondor never passed through the webhook, so the controller
// has to run the whitelist itself.
func TestBarePodIsAdmittedByTheControllerNotTrusted(t *testing.T) {
	pod := newPod("hand-written", "uid-2")
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "sidecar", Image: "busybox"})
	pod.Spec.Volumes = []corev1.Volume{{
		Name:         "data",
		VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/etc"}},
	}}

	h := newHarness(t, pod)
	h.run(5)

	got := h.pod()
	require.Equal(t, corev1.PodFailed, got.Status.Phase)
	require.Equal(t, reasonNotAdmitted, got.Status.Reason)
	require.Contains(t, got.Status.Message, "spec.containers")
	require.Contains(t, got.Status.Message, "spec.volumes[0]")
	require.Empty(t, got.Spec.NodeName, "an inadmissible Pod must not be bound")
	require.NotContains(t, got.Finalizers, api.Finalizer, "there is no job to finalize")
	require.Equal(t, 0, h.q.SubmitCount())
}

func TestPodForAnotherSchedulerIsIgnored(t *testing.T) {
	pod := newPod("not-ours", "uid-3")
	pod.Spec.SchedulerName = "default-scheduler"
	h := newHarness(t, pod)
	h.run(3)

	got := h.pod()
	require.Equal(t, 0, h.q.SubmitCount())
	require.NotContains(t, got.Finalizers, api.Finalizer)
	require.Empty(t, got.Status.Phase)
}

// --- deletion -----------------------------------------------------------

func TestDeletionRemovesTheJobThenTheFinalizer(t *testing.T) {
	h := newHarness(t, newPod("work-abc", "uid-1"))
	h.run(10)
	require.Equal(t, 1, h.q.LiveCount())

	pod := h.pod()
	require.NoError(t, h.c.Delete(context.Background(), pod))
	h.run(5)

	require.True(t, h.q.IsRemoved("100.0"), "the HTCondor job must be removed")
	after := h.pod()
	require.NotContains(t, after.Finalizers, api.Finalizer,
		"our finalizer must come off once the job is gone")
	require.Contains(t, after.Finalizers, "batch.kubernetes.io/job-tracking",
		"and only ours: the Job controller's finalizer is not ours to remove")
}

// TestDeletionWritesTheTerminalPhaseBeforeDroppingTheFinalizer guards a race
// in the built-in Job controller, not in this one.
//
// isPodFailed counts a Pod carrying a deletionTimestamp as failed unless its
// phase is already Succeeded -- deliberately, to catch orphans that never
// reach a phase. A finalizer puts every Pod here in exactly that state. So a
// job that succeeds in the window between two polls, on a Pod that is then
// deleted, would be counted as a failure and have a replacement Pod made for
// work that already ran.
//
// The queue is read one last time on the way out, and the terminal phase
// written, before the finalizer is dropped.
func TestDeletionWritesTheTerminalPhaseBeforeDroppingTheFinalizer(t *testing.T) {
	h := newHarness(t, newPod("work-abc", "uid-1"))
	h.run(10)
	require.Equal(t, corev1.PodPending, h.pod().Status.Phase)

	// The job succeeds and the Pod is deleted before the next poll sees it.
	h.q.SetStatus("100.0", translate.JobStatusRunning, nil)
	h.q.Complete("100.0", 0)
	require.NoError(t, h.c.Delete(context.Background(), h.pod()))

	h.run(5)

	after := h.pod()
	require.Equal(t, corev1.PodSucceeded, after.Status.Phase,
		"the success has to be recorded before the finalizer comes off, or the Job controller counts a failure")
}

// --- the job that left the queue ----------------------------------------

// TestJobThatLeftTheQueueFailsRatherThanSucceeds: completed jobs are reaped
// into history, and a poll that misses the terminal transition sees nothing.
// There is no honest way to tell success from failure from there, and the two
// wrong answers are not equally bad: a wrong Failed makes a replacement Pod
// (visible, reported), a wrong Succeeded corrupts the user's Job status
// silently.
func TestJobThatLeftTheQueueFailsRatherThanSucceeds(t *testing.T) {
	h := newHarness(t, newPod("work-abc", "uid-1"))
	h.run(10)
	require.Equal(t, "100.0", h.pod().Annotations[api.AnnotationJobID])

	h.q.Drop("100.0")
	h.run(5)

	pod := h.pod()
	require.Equal(t, corev1.PodFailed, pod.Status.Phase)
	require.Equal(t, reasonLeftQueue, pod.Status.Reason)
}

// --- deadlines ----------------------------------------------------------

func TestPodLevelActiveDeadlineBecomesPeriodicRemove(t *testing.T) {
	pod := newPod("work-abc", "uid-1")
	d := int64(120)
	pod.Spec.ActiveDeadlineSeconds = &d

	h := newHarness(t, pod)
	h.run(10)

	require.Len(t, h.q.Submissions, 1)
	require.Contains(t, h.q.Submissions[0], "periodic_remove = (time() - QDate) > 120",
		"a Pod-level deadline has no kubelet to enforce it, so HTCondor must")
}

// TestNoDeadlineMeansNoPeriodicRemove is the other half: the *Job*-level
// activeDeadlineSeconds is the Job controller's, enforced by deleting Pods,
// and this controller must not add a second enforcer with a second clock.
// Nothing about a Job's deadline reaches a Pod spec, so the test is that a Pod
// with no deadline of its own produces no expression at all.
func TestNoDeadlineMeansNoPeriodicRemove(t *testing.T) {
	h := newHarness(t, newPod("work-abc", "uid-1"))
	h.run(10)
	require.Len(t, h.q.Submissions, 1)
	require.NotContains(t, h.q.Submissions[0], "periodic_remove")
}

func TestRemovedPastItsDeadlineReportsDeadlineExceeded(t *testing.T) {
	pod := newPod("work-abc", "uid-1")
	d := int64(60)
	pod.Spec.ActiveDeadlineSeconds = &d

	h := newHarness(t, pod)
	h.run(10)

	h.clock = h.clock.Add(90 * time.Second)
	h.q.SetStatus("100.0", translate.JobStatusRemoved, nil)
	h.run(5)

	got := h.pod()
	require.Equal(t, corev1.PodFailed, got.Status.Phase)
	require.Equal(t, reasonDeadlineExceeded, got.Status.Reason)
}

// --- the submit file ----------------------------------------------------

func TestSubmitCarriesThePodIdentityAndTheNodeStamp(t *testing.T) {
	pod := newPod("work-abc", "uid-1")
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "batch/v1", Kind: "Job", Name: "work",
		UID: "job-uid-9", Controller: boolp(true),
	}}
	pod.Annotations = map[string]string{annotationCompletionIndex: "3"}

	h := newHarness(t, pod)
	h.run(10)

	text := h.q.Submissions[0]
	require.Contains(t, text, `+K8sPodUID = "uid-1"`)
	require.Contains(t, text, `+K8sPodName = "work-abc"`)
	require.Contains(t, text, `+K8sOwnerKind = "Job"`)
	require.Contains(t, text, `+K8sOwnerUID = "job-uid-9"`)
	require.Contains(t, text, `+K8sVirtualNode = "`+api.NodeName+`"`)
	require.Contains(t, text, `+K8sCompletionIndex = 3`)

	// The bookkeeping labels the Job controller stamps on its Pods are the
	// controller's join keys, not user intent, and must not reach the ad
	// looking like something a user could select on.
	require.Contains(t, text, `+K8sLabels = `)
	require.NotContains(t, text, legacyLabelJobName+`\":\"work`)

	// And the recorded PodSpec is the Pod as its author wrote it: no
	// toleration we added, no node we bound it to.
	require.NotContains(t, text, api.TaintKey)
}

func boolp(b bool) *bool { return &b }

// --- normalization ------------------------------------------------------

func TestNormalizeRemovesOnlyWhatWeAdded(t *testing.T) {
	pod := newPod("work-abc", "uid-1")
	pod.Spec.NodeName = api.NodeName
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{Key: "spot"})

	spec := normalizeForAdmission(pod, api.NodeName)
	require.Empty(t, spec.NodeName)
	require.Equal(t, []corev1.Toleration{{Key: "spot"}}, spec.Tolerations,
		"every toleration the webhook adds must come back off, and only those")

	// Which is what makes the round trip work: the shared whitelist
	// rejects both fields outright, and re-running it on a bound Pod would
	// otherwise refuse a Pod we admitted ourselves a moment earlier -- the
	// same shape as the operator's .spec.selector trap.
	rs := translate.Admit(spec, &pod.ObjectMeta, translate.DefaultCapabilities())
	require.False(t, rs.OK(), "a toleration the user wrote is still refused: %v", rs)
	require.Contains(t, rs.Error(), "spec.tolerations")

	pod.Spec.Tolerations = pod.Spec.Tolerations[:1]
	require.True(t, translate.Admit(normalizeForAdmission(pod, api.NodeName), &pod.ObjectMeta,
		translate.DefaultCapabilities()).OK())
}

// TestNormalizeKeepsAUserNotReadyToleration: ours is tolerated forever
// (tolerationSeconds nil). A user's 300-second one is a different thing with
// the same key, and is not ours to delete -- it still gets refused, loudly,
// the way any other toleration does.
func TestNormalizeKeepsAUserNotReadyToleration(t *testing.T) {
	pod := newPod("work-abc", "uid-1")
	secs := int64(300)
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{
		Key:               corev1.TaintNodeNotReady,
		Operator:          corev1.TolerationOpExists,
		Effect:            corev1.TaintEffectNoExecute,
		TolerationSeconds: &secs,
	})
	spec := normalizeForAdmission(pod, api.NodeName)
	require.Len(t, spec.Tolerations, 1)
	require.NotNil(t, spec.Tolerations[0].TolerationSeconds)
}

// TestAStampedPodIsStillAdmissible is the round trip the whole normalization
// exists for: everything the webhook writes must survive a second look by the
// shared whitelist, which rejects tolerations and nodeName outright.
func TestAStampedPodIsStillAdmissible(t *testing.T) {
	pod := newPod("work-abc", "uid-1")
	pod.Spec.NodeName = api.NodeName
	rs := translate.Admit(normalizeForAdmission(pod, api.NodeName), &pod.ObjectMeta,
		translate.DefaultCapabilities())
	require.True(t, rs.OK(), "a Pod this integration made is not admissible to it: %v", rs)
}

func TestNormalizeLeavesAHandSetNodeNameAlone(t *testing.T) {
	pod := newPod("work-abc", "uid-1")
	pod.Spec.NodeName = "node-17"
	spec := normalizeForAdmission(pod, api.NodeName)
	require.Equal(t, "node-17", spec.NodeName, "only our own node name is ours to remove")
}

// --- settled ------------------------------------------------------------

// TestUnsettledCompletedAdIsNotBelieved: translate.JobAd's zero ExitCode is 0,
// which reads as "succeeded". An ad that claims Completed and carries neither
// a completion time nor an exit status is one the schedd has not finished
// writing, and believing it is the one wrong answer this integration cannot
// afford.
func TestUnsettledCompletedAdIsNotBelieved(t *testing.T) {
	require.False(t, settled(translate.JobAd{JobStatus: translate.JobStatusCompleted}))
	require.True(t, settled(translate.JobAd{JobStatus: translate.JobStatusCompleted, ExitCode: 1}))
	require.True(t, settled(translate.JobAd{JobStatus: translate.JobStatusCompleted, ExitBySignal: true}))
	require.True(t, settled(translate.JobAd{
		JobStatus: translate.JobStatusCompleted, CompletionDate: time.Now()}))
	require.True(t, settled(translate.JobAd{JobStatus: translate.JobStatusRunning}))
}

func TestUnsettledAdLeavesThePodWhereItWas(t *testing.T) {
	h := newHarness(t, newPod("work-abc", "uid-1"))
	h.run(10)
	h.q.SetStatus("100.0", translate.JobStatusRunning, nil)
	h.run(5)
	require.Equal(t, corev1.PodRunning, h.pod().Status.Phase)

	// A half-written completion: status says Completed, nothing else does.
	h.q.SetStatus("100.0", translate.JobStatusCompleted, func(a *translate.JobAd) {
		a.CompletionDate = time.Time{}
		a.ExitCode = 0
	})
	h.run(3)
	require.Equal(t, corev1.PodRunning, h.pod().Status.Phase,
		"a Pod that stalls Running is visible; a Pod wrongly reported Succeeded is not")
}

// --- status details -----------------------------------------------------

func TestStartTimeNeverMoves(t *testing.T) {
	h := newHarness(t, newPod("work-abc", "uid-1"))
	h.run(10)
	first := h.pod().Status.StartTime
	require.NotNil(t, first, "a submitted Pod has started, as far as Kubernetes is concerned")

	h.clock = h.clock.Add(time.Hour)
	h.q.SetStatus("100.0", translate.JobStatusRunning, nil)
	h.run(5)
	require.Equal(t, first, h.pod().Status.StartTime)
}

func TestHeldJobReportsTheReason(t *testing.T) {
	h := newHarness(t, newPod("work-abc", "uid-1"))
	h.run(10)

	h.q.SetStatus("100.0", translate.JobStatusHeld, func(a *translate.JobAd) {
		a.HoldReasonCode = 35 // InvalidDockerImage: ImagePullBackOff by another name.
		a.HoldReason = "Error from image pull"
	})
	h.run(5)

	pod := h.pod()
	require.Equal(t, corev1.PodFailed, pod.Status.Phase, "a hold nobody can release is terminal")
	require.Equal(t, "Error from image pull", pod.Annotations[api.AnnotationHoldReason])
}

func TestUserHeldJobStaysPending(t *testing.T) {
	h := newHarness(t, newPod("work-abc", "uid-1"))
	h.run(10)

	h.q.SetStatus("100.0", translate.JobStatusHeld, func(a *translate.JobAd) {
		a.HoldReasonCode = 1 // UserRequest: somebody can release it.
		a.HoldReason = "held by hand"
	})
	h.run(5)
	require.Equal(t, corev1.PodPending, h.pod().Status.Phase)
}

func TestStatusIsNotRewrittenWhenNothingChanged(t *testing.T) {
	h := newHarness(t, newPod("work-abc", "uid-1"))
	h.run(10)
	before := h.pod().ResourceVersion

	h.run(3)
	require.Equal(t, before, h.pod().ResourceVersion,
		"an unchanged job must not cost an etcd write and a Job-controller wakeup per poll")
}

// --- unreachable schedd -------------------------------------------------

func TestUnreachableScheddKeepsStaleStatusRatherThanZeroingIt(t *testing.T) {
	h := newHarness(t, newPod("work-abc", "uid-1"))
	h.run(10)
	h.q.SetStatus("100.0", translate.JobStatusRunning, nil)
	h.run(5)
	require.Equal(t, corev1.PodRunning, h.pod().Status.Phase)

	h.q.QueryErr = context.DeadlineExceeded
	res, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: h.key})
	require.NoError(t, err)
	require.Positive(t, res.RequeueAfter)
	require.Equal(t, corev1.PodRunning, h.pod().Status.Phase,
		"status is stale, not wrong; a phase change that never happened would reach the Job controller")
}

func TestDeleteBlocksRatherThanOrphaningWhenTheScheddIsDown(t *testing.T) {
	h := newHarness(t, newPod("work-abc", "uid-1"))
	h.run(10)
	require.NoError(t, h.c.Delete(context.Background(), h.pod()))

	h.q.ActionErr = context.DeadlineExceeded
	res, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: h.key})
	require.NoError(t, err)
	require.Positive(t, res.RequeueAfter)
	require.Contains(t, h.pod().Finalizers, api.Finalizer,
		"a stuck delete is visible; an orphaned job is not")
}

// --- the reaper ---------------------------------------------------------

func newReaper(t *testing.T, q *FakeQueue, pods ...client.Object) *Reaper {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	return &Reaper{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build(),
		Queue:  q,
		Grace:  time.Minute,
	}
}

// submitFor queues a job for a Pod without going through the reconciler, so a
// reaper test can set up a queue that Kubernetes never agreed to.
func submitFor(t *testing.T, q *FakeQueue, pod *corev1.Pod) string {
	t.Helper()
	r := &PodReconciler{Caps: translate.DefaultCapabilities()}
	text, _, err := r.renderSubmit(pod, normalizeForAdmission(pod, api.NodeName))
	require.NoError(t, err)
	cluster, err := q.Submit(context.Background(), text)
	require.NoError(t, err)
	return cluster + ".0"
}

func TestReaperRemovesAJobWhosePodIsGone(t *testing.T) {
	q := NewFakeQueue()
	id := submitFor(t, q, newPod("vanished", "uid-9"))
	q.AgeAll(2 * time.Minute)

	r := newReaper(t, q) // no Pods at all
	require.NoError(t, r.Sweep(context.Background()))
	require.True(t, q.IsRemoved(id))
}

func TestReaperKeepsAJobWhosePodIsAlive(t *testing.T) {
	q := NewFakeQueue()
	pod := newPod("alive", "uid-9")
	id := submitFor(t, q, pod)
	q.AgeAll(2 * time.Minute)

	r := newReaper(t, q, pod)
	require.NoError(t, r.Sweep(context.Background()))
	require.False(t, q.IsRemoved(id))
}

// TestReaperComparesUIDsNotNames: restartPolicy Never means the Job controller
// replaces a failed Pod, and for a non-indexed Job the replacement can reuse
// the name. Matching on name alone would see the replacement, conclude the
// original is fine, and leave its job running forever beside the new one.
func TestReaperComparesUIDsNotNames(t *testing.T) {
	q := NewFakeQueue()
	old := newPod("work-abc", "uid-old")
	id := submitFor(t, q, old)
	q.AgeAll(2 * time.Minute)

	replacement := newPod("work-abc", "uid-new")
	r := newReaper(t, q, replacement)
	require.NoError(t, r.Sweep(context.Background()))
	require.True(t, q.IsRemoved(id), "the dead Pod's job must go even though the name is taken again")
}

// TestReaperRespectsItsGracePeriod: between Submit returning and the
// annotation write landing there is a job whose Pod a stale cache may not
// show. Reaping it would delete healthy work.
func TestReaperRespectsItsGracePeriod(t *testing.T) {
	q := NewFakeQueue()
	id := submitFor(t, q, newPod("just-submitted", "uid-9"))

	r := newReaper(t, q)
	require.NoError(t, r.Sweep(context.Background()))
	require.False(t, q.IsRemoved(id), "a job younger than the grace period is not an orphan")
}

func TestReaperIgnoresAnotherNodesJobs(t *testing.T) {
	q := NewFakeQueue()
	pod := newPod("theirs", "uid-9")
	rr := &PodReconciler{Caps: translate.DefaultCapabilities(), NodeName: "some-other-virtual-node"}
	text, _, err := rr.renderSubmit(pod, normalizeForAdmission(pod, "some-other-virtual-node"))
	require.NoError(t, err)
	cluster, err := q.Submit(context.Background(), text)
	require.NoError(t, err)
	q.AgeAll(2 * time.Minute)

	r := newReaper(t, q)
	require.NoError(t, r.Sweep(context.Background()))
	require.False(t, q.IsRemoved(cluster+".0"))
}

// --- constraints --------------------------------------------------------

// TestPodConstraintDoesNotEscapeSubmitMacros guards a subtle cross-wiring:
// translate.ClassAdString ends by turning every dollar sign into $(DOLLAR) so
// that user text cannot expand a submit macro, which is right for submit-file
// text and wrong for a query constraint -- the schedd's evaluator never sees
// condor_submit, so the escape would match nothing.
func TestPodConstraintDoesNotEscapeSubmitMacros(t *testing.T) {
	got := PodConstraint(types.UID(`a"b$c`))
	require.Equal(t, `K8sPodUID == "a\"b$c"`, got)
	require.NotContains(t, got, "$(DOLLAR)")
	require.True(t, strings.HasPrefix(got, translate.AttrPodUID))
}
