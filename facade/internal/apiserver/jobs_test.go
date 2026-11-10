package apiserver

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

func demoJob(name, ns string, completions int32) *batchv1.Job {
	indexed := batchv1.IndexedCompletion
	backoff := int32(2)
	pod := demoPod(name, ns)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{"team": "physics"}},
		Spec: batchv1.JobSpec{
			Completions:    &completions,
			Parallelism:    &completions,
			CompletionMode: &indexed,
			BackoffLimit:   &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "demo"}},
				Spec:       pod.Spec,
			},
		},
	}
}

func TestCreateJobQueuesOneClusterPerJob(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx := context.Background()

	created, err := cs.BatchV1().Jobs("research").Create(ctx, demoJob("sweep", "research", 3), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating a job: %v", err)
	}
	if created.Name != "sweep" {
		t.Fatalf("got %q", created.Name)
	}
	if created.Status.Active != 3 {
		t.Errorf("status.active = %d, want 3", created.Status.Active)
	}

	// One cluster, three procs: the Job's completions became the queue
	// count rather than three separate submissions.
	for proc := 0; proc < 3; proc++ {
		ad := f.Fake.Ad(jobKey(1, proc))
		if ad == nil {
			t.Fatalf("no job 1.%d in the queue", proc)
		}
		if owner, _ := ad.EvaluateAttrString(translate.AttrOwnerName); owner != "sweep" {
			t.Errorf("1.%d: K8sOwnerName = %q", proc, owner)
		}
		if idx, ok := ad.EvaluateAttrInt(translate.AttrIndex); !ok || int(idx) != proc {
			t.Errorf("1.%d: completion index = %d (present=%v), want %d", proc, idx, ok, proc)
		}
	}
	if f.Fake.Ad("1.3") != nil {
		t.Error("a fourth proc was queued")
	}
}

func TestJobPodsAreListableAsPods(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx := context.Background()
	if _, err := cs.BatchV1().Jobs("research").Create(ctx, demoJob("sweep", "research", 2), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.sync()

	pods, err := cs.CoreV1().Pods("research").List(ctx, metav1.ListOptions{LabelSelector: "job-name=sweep"})
	if err != nil {
		t.Fatalf("listing the job's pods: %v", err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("got %d pods, want 2", len(pods.Items))
	}
	for i, pod := range pods.Items {
		want := "sweep-" + string(rune('0'+i))
		if pod.Name != want {
			t.Errorf("pod %d is named %q, want %q", i, pod.Name, want)
		}
		if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].Kind != "Job" {
			t.Errorf("%s has no Job owner reference", pod.Name)
		}
	}
	// Two pods of one cluster must not share a UID, or every client that
	// caches by UID collapses them into one.
	if pods.Items[0].UID == pods.Items[1].UID {
		t.Errorf("both pods have UID %q", pods.Items[0].UID)
	}

	// A single pod of the job is reachable by its derived name.
	if _, err := cs.CoreV1().Pods("research").Get(ctx, "sweep-1", metav1.GetOptions{}); err != nil {
		t.Errorf("getting one of a job's pods: %v", err)
	}
}

func TestJobStatusAggregatesItsProcs(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx := context.Background()
	if _, err := cs.BatchV1().Jobs("research").Create(ctx, demoJob("sweep", "research", 2), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	f.Fake.SetStatus("1.0", translate.JobStatusCompleted)
	f.Fake.SetStatus("1.1", translate.JobStatusRunning)
	f.sync()

	job, err := cs.BatchV1().Jobs("research").Get(ctx, "sweep", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if job.Status.Succeeded != 1 || job.Status.Active != 1 {
		t.Fatalf("succeeded=%d active=%d, want 1 and 1", job.Status.Succeeded, job.Status.Active)
	}
	// The Job spec round-trips through the job ad, which is the only place
	// it is stored.
	if job.Spec.Completions == nil || *job.Spec.Completions != 2 {
		t.Errorf("spec.completions did not survive: %v", job.Spec.Completions)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 2 {
		t.Errorf("spec.backoffLimit did not survive: %v", job.Spec.BackoffLimit)
	}

	f.Fake.SetStatus("1.1", translate.JobStatusCompleted)
	f.sync()
	job, err = cs.BatchV1().Jobs("research").Get(ctx, "sweep", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var complete bool
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			complete = true
		}
	}
	if !complete {
		t.Errorf("a job whose procs all succeeded must report Complete; conditions: %+v", job.Status.Conditions)
	}
}

func TestCreateJobRejectionNamesTheJobFields(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)

	bad := demoJob("bad", "default", 4)
	one := int32(1)
	bad.Spec.Parallelism = &one
	ttl := int32(30)
	bad.Spec.TTLSecondsAfterFinished = &ttl

	_, err := cs.BatchV1().Jobs("default").Create(context.Background(), bad, metav1.CreateOptions{})
	if err == nil {
		t.Fatal("a job with fields that cannot be honored must be rejected")
	}
	status, ok := err.(apierrors.APIStatus)
	if !ok {
		t.Fatalf("not a Status: %T", err)
	}
	fields := map[string]bool{}
	for _, c := range status.Status().Details.Causes {
		fields[c.Field] = true
	}
	for _, want := range []string{"spec.parallelism", "spec.ttlSecondsAfterFinished"} {
		if !fields[want] {
			t.Errorf("no cause for %s; got %v", want, fields)
		}
	}
	_ = f
}

func TestDeleteJobRemovesEveryProc(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx := context.Background()
	if _, err := cs.BatchV1().Jobs("research").Create(ctx, demoJob("sweep", "research", 3), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.sync()
	if err := cs.BatchV1().Jobs("research").Delete(ctx, "sweep", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	recs, _ := f.Fake.Query(ctx, "true")
	if len(recs) != 0 {
		t.Fatalf("%d jobs survived the delete", len(recs))
	}
	pods, err := cs.CoreV1().Pods("research").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("deleting a job must take its pods with it; %d remain", len(pods.Items))
	}
}

func jobKey(cluster, proc int) string {
	return string(rune('0'+cluster)) + "." + string(rune('0'+proc))
}
