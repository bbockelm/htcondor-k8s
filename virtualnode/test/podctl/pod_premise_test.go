package podctl_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/podctl"
)

// jobControllerSim is kube-controller-manager's Job controller, reduced to the
// behavior this approach depends on.
//
// It exists because envtest cannot run the real one; see this package's doc
// comment for why, and for how much weaker that makes the evidence. Every
// rule here was read out of kubernetes v1.37.0 pkg/controller/job rather than
// recalled, and the two that matter are written out in full so a reader can
// check them:
//
//   - isPodFailed: a Pod is failed if its phase is Failed, or --- unless
//     onlyReplaceFailedPods --- if it carries a deletionTimestamp and its
//     phase is not Succeeded. That second clause is the reason the webhook
//     sets podReplacementPolicy.
//   - onlyReplaceFailedPods: true iff .spec.podReplacementPolicy is Failed.
//
// What it does not model: backoff delays, the tracking finalizer's removal
// schedule, indexed completion bookkeeping, podFailurePolicy, or any of the
// interim condition machinery beyond SuccessCriteriaMet and FailureTarget.
type jobControllerSim struct {
	c   client.Client
	job *batchv1.Job

	// MaxConcurrent records the high-water mark of active Pods, which is
	// how the parallelism claim is checked.
	MaxConcurrent int
	Created       int
}

func onlyReplaceFailedPods(job *batchv1.Job) bool {
	return job.Spec.PodReplacementPolicy != nil && *job.Spec.PodReplacementPolicy == batchv1.Failed
}

func isPodFailed(pod *corev1.Pod, job *batchv1.Job) bool {
	if pod.Status.Phase == corev1.PodFailed {
		return true
	}
	if onlyReplaceFailedPods(job) {
		return false
	}
	return pod.DeletionTimestamp != nil && pod.Status.Phase != corev1.PodSucceeded
}

func (s *jobControllerSim) pods(ctx context.Context) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := s.c.List(ctx, &list, client.InNamespace(s.job.Namespace),
		client.MatchingLabels{"batch.kubernetes.io/job-name": s.job.Name}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// Step is one pass of the controller's loop: count the Pods, create however
// many more parallelism allows, and write the Job's status from what the Pods
// say.
func (s *jobControllerSim) Step(ctx context.Context) error {
	pods, err := s.pods(ctx)
	if err != nil {
		return err
	}

	var active, succeeded, failed int32
	for i := range pods {
		p := &pods[i]
		switch {
		case p.Status.Phase == corev1.PodSucceeded:
			succeeded++
		case isPodFailed(p, s.job):
			failed++
		case p.DeletionTimestamp == nil:
			active++
		}
	}
	if int(active) > s.MaxConcurrent {
		s.MaxConcurrent = int(active)
	}

	completions := int32(1)
	if s.job.Spec.Completions != nil {
		completions = *s.job.Spec.Completions
	}
	parallelism := completions
	if s.job.Spec.Parallelism != nil {
		parallelism = *s.job.Spec.Parallelism
	}
	backoffLimit := int32(6)
	if s.job.Spec.BackoffLimit != nil {
		backoffLimit = *s.job.Spec.BackoffLimit
	}

	done := succeeded >= completions || failed > backoffLimit
	if !done {
		want := completions - succeeded - active
		if want > parallelism-active {
			want = parallelism - active
		}
		for i := int32(0); i < want; i++ {
			if err := s.createPod(ctx); err != nil {
				return err
			}
			active++
			s.Created++
		}
		if int(active) > s.MaxConcurrent {
			s.MaxConcurrent = int(active)
		}
	}

	st := batchv1.JobStatus{Active: active, Succeeded: succeeded, Failed: failed}
	if s.job.Status.StartTime == nil {
		now := metav1.Now()
		st.StartTime = &now
	} else {
		st.StartTime = s.job.Status.StartTime
	}
	switch {
	case succeeded >= completions:
		now := metav1.Now()
		st.CompletionTime = &now
		st.Conditions = []batchv1.JobCondition{
			cond(batchv1.JobSuccessCriteriaMet), cond(batchv1.JobComplete),
		}
	case failed > backoffLimit:
		st.Conditions = []batchv1.JobCondition{
			cond(batchv1.JobFailureTarget), cond(batchv1.JobFailed),
		}
	}
	s.job.Status = st
	return s.c.Status().Update(ctx, s.job)
}

func cond(t batchv1.JobConditionType) batchv1.JobCondition {
	return batchv1.JobCondition{
		Type: t, Status: corev1.ConditionTrue,
		Reason: "Simulated", Message: "written by jobControllerSim",
		LastTransitionTime: metav1.Now(), LastProbeTime: metav1.Now(),
	}
}

// createPod makes a Pod the way the real controller does: from the (stamped)
// template, with the controller's own labels, an owner reference and the
// job-tracking finalizer.
func (s *jobControllerSim) createPod(ctx context.Context) error {
	tmpl := s.job.Spec.Template.DeepCopy()
	labels := map[string]string{}
	for k, v := range tmpl.Labels {
		labels[k] = v
	}
	labels["batch.kubernetes.io/job-name"] = s.job.Name
	labels["batch.kubernetes.io/controller-uid"] = string(s.job.UID)

	t := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: s.job.Name + "-",
			Namespace:    s.job.Namespace,
			Labels:       labels,
			Finalizers:   []string{"batch.kubernetes.io/job-tracking"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "batch/v1", Kind: "Job",
				Name: s.job.Name, UID: s.job.UID,
				Controller: &t, BlockOwnerDeletion: &t,
			}},
		},
		Spec: tmpl.Spec,
	}
	return s.c.Create(ctx, pod)
}

// --- the premise --------------------------------------------------------

// TestPodInterceptionDrivesAJobToCompletion is the whole approach, end to end,
// against a real API server: a user applies an ordinary batch/v1 Job, the
// webhook stamps the template, Pods appear, this controller submits and binds
// and reports them, and the Job's status comes out right -- written entirely
// by the Job controller from Pod statuses, with nothing here ever touching a
// Job object after admission.
//
// The Job controller's half is simulated. See the package comment.
func TestPodInterceptionDrivesAJobToCompletion(t *testing.T) {
	skipWithoutAssets(t)
	mutatingWebhookInstalled(t)
	ctx := context.Background()

	job := translatableJob("premise")
	job.Spec.Completions = i32(4)
	job.Spec.Parallelism = i32(2)
	job.Spec.BackoffLimit = i32(2)
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatalf("creating the Job: %v", err)
	}

	// 1. The webhook stamped the template on the way into etcd.
	stored := getJob(t, "premise")
	if stored.Spec.Template.Spec.SchedulerName != api.SchedulerName {
		t.Fatalf("schedulerName is %q; the webhook did not stamp this Job",
			stored.Spec.Template.Spec.SchedulerName)
	}
	if stored.Spec.PodReplacementPolicy == nil || *stored.Spec.PodReplacementPolicy != batchv1.Failed {
		t.Fatalf("podReplacementPolicy is %v; the Job controller would count our finalizer window as a failure",
			stored.Spec.PodReplacementPolicy)
	}
	if stored.Spec.Suspend != nil && *stored.Spec.Suspend {
		t.Fatal("this approach must never suspend a Job: that is the operator's injury")
	}
	if !hasOurToleration(stored.Spec.Template.Spec.Tolerations) {
		t.Fatal("the pod template has no toleration for the virtual node's taint")
	}

	sim := &jobControllerSim{c: k8sClient, job: stored}

	// 2. Drive both halves until the Job is Complete: the simulated Job
	//    controller makes Pods, the real pod controller submits and binds
	//    them, and the pool (the fake queue) runs them.
	deadline := time.Now().Add(eventually)
	for time.Now().Before(deadline) {
		if err := sim.Step(ctx); err != nil {
			t.Fatalf("job controller step: %v", err)
		}
		runTheQueue(t)
		if jobComplete(sim.job) {
			break
		}
		time.Sleep(tick)
	}
	if !jobComplete(sim.job) {
		t.Fatalf("the Job never completed: %+v", sim.job.Status)
	}

	// 3. The Job's status is right, and nothing here wrote it.
	if sim.job.Status.Succeeded != 4 {
		t.Fatalf("succeeded = %d, want 4", sim.job.Status.Succeeded)
	}
	if sim.job.Status.Failed != 0 {
		t.Fatalf("failed = %d, want 0", sim.job.Status.Failed)
	}

	// 4. parallelism was honored -- by the Job controller, for free. This
	//    is the thing neither of the other two approaches can do: the
	//    facade and the HTCJob operator both refuse parallelism below
	//    completions outright, because HTCondor starts every idle proc of
	//    a cluster as soon as a slot matches and nothing throttles it.
	if sim.MaxConcurrent > 2 {
		t.Fatalf("%d Pods were active at once; parallelism is 2", sim.MaxConcurrent)
	}
	if sim.Created != 4 {
		t.Fatalf("%d Pods were created; completions is 4 and none should have failed", sim.Created)
	}

	// 5. One HTCondor job per Pod, no duplicates.
	pods, err := sim.pods(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pods) != 4 {
		t.Fatalf("%d Pods exist, want 4", len(pods))
	}
	seen := map[string]bool{}
	for i := range pods {
		p := &pods[i]
		if p.Status.Phase != corev1.PodSucceeded {
			t.Fatalf("pod %s is %s, want Succeeded", p.Name, p.Status.Phase)
		}
		if p.Spec.NodeName != api.NodeName {
			t.Fatalf("pod %s is bound to %q, want the virtual node", p.Name, p.Spec.NodeName)
		}
		id := p.Annotations[api.AnnotationJobID]
		if id == "" {
			t.Fatalf("pod %s has no HTCondor job id", p.Name)
		}
		if seen[id] {
			t.Fatalf("two Pods claim HTCondor job %s", id)
		}
		seen[id] = true
		if len(p.Status.ContainerStatuses) != 1 ||
			p.Status.ContainerStatuses[0].State.Terminated == nil ||
			p.Status.ContainerStatuses[0].State.Terminated.ExitCode != 0 {
			t.Fatalf("pod %s has no terminated container status with exit 0: %+v",
				p.Name, p.Status.ContainerStatuses)
		}
	}
	if n := len(seen); n != 4 {
		t.Fatalf("%d distinct HTCondor jobs for 4 Pods", n)
	}
}

// TestAFailingJobCountsAgainstBackoffLimit is the other direction, and it is
// the one a wrong Succeeded would break: a Job whose work fails must fail,
// after exactly backoffLimit+1 attempts, with the Job controller doing the
// counting from the Pod phases this controller wrote.
func TestAFailingJobCountsAgainstBackoffLimit(t *testing.T) {
	skipWithoutAssets(t)
	ctx := context.Background()

	job := translatableJob("failing")
	job.Spec.Completions = i32(1)
	job.Spec.Parallelism = i32(1)
	job.Spec.BackoffLimit = i32(2)
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatalf("creating the Job: %v", err)
	}
	sim := &jobControllerSim{c: k8sClient, job: getJob(t, "failing")}

	deadline := time.Now().Add(eventually)
	for time.Now().Before(deadline) {
		if err := sim.Step(ctx); err != nil {
			t.Fatalf("job controller step: %v", err)
		}
		failTheQueue(t)
		if jobFailed(sim.job) {
			break
		}
		time.Sleep(tick)
	}
	if !jobFailed(sim.job) {
		t.Fatalf("the Job never failed: %+v", sim.job.Status)
	}
	if sim.job.Status.Failed != 3 {
		t.Fatalf("failed = %d, want 3 (backoffLimit 2 means three attempts)", sim.job.Status.Failed)
	}
	if sim.Created != 3 {
		t.Fatalf("%d Pods were created, want 3", sim.Created)
	}
}

func jobComplete(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// runTheQueue is the HTCondor pool: it advances every idle job to running and
// every running job to a successful completion.
func runTheQueue(t *testing.T) {
	t.Helper()
	ads, err := fakeQueue.Query(context.Background(), podctl.NodeConstraint(api.NodeName))
	if err != nil {
		t.Fatal(err)
	}
	for _, ad := range ads {
		switch ad.JobStatus {
		case translate.JobStatusIdle:
			fakeQueue.SetStatus(ad.JobID(), translate.JobStatusRunning, nil)
		case translate.JobStatusRunning:
			fakeQueue.Complete(ad.JobID(), 0)
		}
	}
}

func failTheQueue(t *testing.T) {
	t.Helper()
	ads, err := fakeQueue.Query(context.Background(), podctl.NodeConstraint(api.NodeName))
	if err != nil {
		t.Fatal(err)
	}
	for _, ad := range ads {
		switch ad.JobStatus {
		case translate.JobStatusIdle:
			fakeQueue.SetStatus(ad.JobID(), translate.JobStatusRunning, nil)
		case translate.JobStatusRunning:
			fakeQueue.Complete(ad.JobID(), 1)
		}
	}
}

func hasOurToleration(ts []corev1.Toleration) bool {
	for _, tol := range ts {
		if tol.Key == api.TaintKey && tol.Value == api.TaintValue {
			return true
		}
	}
	return false
}

func i32(v int32) *int32 { return &v }

var _ = fmt.Sprintf
