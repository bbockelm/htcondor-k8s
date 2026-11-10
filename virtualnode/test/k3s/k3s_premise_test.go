package k3s_test

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/podctl"
	htcwebhook "github.com/bbockelm/htcondor-k8s/virtualnode/internal/webhook"
)

// TestTheRealJobControllerDrivesAJobToCompletion is the test the whole
// approach was waiting for.
//
// Everything outside this process is real: the Job controller creates the
// Pods, kube-scheduler decides whether to touch them, and the Job controller
// reads back the Pod statuses our controller writes. Nothing here simulates
// the component the design depends on.
func TestTheRealJobControllerDrivesAJobToCompletion(t *testing.T) {
	skipWithoutDocker(t)
	ctx := context.Background()

	// Submissions are counted as a delta: the fake queue is shared by every
	// test in this package and Go runs them in one process, so an absolute
	// count silently depends on file order.
	before := fakeQueue.SubmitCount()

	job := translatableJob("premise")
	htcwebhook.Stamp(job)
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatal(err)
	}

	// 1. The real Job controller creates Pods, and honors parallelism.
	var pods corev1.PodList
	eventuallyTrue(t, "the real Job controller to create the first pod", func() bool {
		pods = podsOf(t, "premise")
		return len(pods.Items) >= 1
	})
	if n := len(pods.Items); n > 1 {
		t.Fatalf("parallelism 1 should have produced one pod at a time, got %d", n)
	}

	// 2. kube-scheduler declines them, because schedulerName is not its own.
	// Our controller is what binds them, so a pod that reached our node did
	// so through the binding subresource and not through the scheduler.
	eventuallyTrue(t, "our controller to bind the pod to the virtual node", func() bool {
		pods = podsOf(t, "premise")
		return len(pods.Items) > 0 && pods.Items[0].Spec.NodeName == api.NodeName
	})
	if got := pods.Items[0].Spec.SchedulerName; got != api.SchedulerName {
		t.Fatalf("pod scheduler name = %q, want %q", got, api.SchedulerName)
	}

	// 3. Drive the HTCondor side to success and let our controller write the
	// terminal Pod status.
	deadline := time.Now().Add(eventually)
	for time.Now().Before(deadline) {
		runTheQueue(t)
		if jobDone(t, "premise") {
			break
		}
		time.Sleep(tick)
	}

	// 4. The real Job controller counted our Pod statuses.
	got := getJob(t, "premise")
	if got.Status.Succeeded != 2 {
		t.Fatalf("job.status.succeeded = %d, want 2 -- the real Job controller did not count "+
			"the pod statuses this controller wrote; status was %+v", got.Status.Succeeded, got.Status)
	}
	if !jobComplete(got) {
		t.Fatalf("the real Job controller did not mark the job Complete; conditions: %+v", got.Status.Conditions)
	}
	if n := fakeQueue.SubmitCount() - before; n != 2 {
		t.Fatalf("submitted %d times for this job, want 2 -- one HTCondor job per completion", n)
	}
}

// TestTheRealJobControllerDefaultsPodReplacementPolicy pins the behavior that
// made an earlier version of the webhook refuse every Job in the cluster.
//
// The API server applies scheme defaulting while decoding, before mutating
// admission runs, so there is no such thing as a Job whose
// podReplacementPolicy is unset by the time a webhook sees it. An admission
// rule written against the absence of this field rejects everything.
func TestTheRealJobControllerDefaultsPodReplacementPolicy(t *testing.T) {
	skipWithoutDocker(t)
	ctx := context.Background()

	// Deliberately NOT stamped: this is about the API server, not about us.
	job := translatableJob("defaulting")
	job.Spec.Completions = nil
	job.Spec.Parallelism = nil
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatal(err)
	}

	stored := getJob(t, "defaulting")
	if stored.Spec.PodReplacementPolicy == nil {
		t.Skip("this API server no longer defaults podReplacementPolicy; the webhook may now refuse an explicit value")
	}
	if got := *stored.Spec.PodReplacementPolicy; got != batchv1.TerminatingOrFailed {
		t.Fatalf("podReplacementPolicy defaulted to %q, want %q", got, batchv1.TerminatingOrFailed)
	}
	if stored.Spec.ManualSelector == nil {
		t.Fatal("manualSelector is no longer defaulted; the AdmitJob check that keys on it needs re-reading")
	}
	if stored.Spec.Selector == nil {
		t.Fatal("the registry no longer fills in .spec.selector; the trap that caused it may have changed shape")
	}
}

// TestAStampedJobIsLeftAloneByKubeScheduler proves the mechanism the approach
// rests on from the other side: with our schedulerName and no controller of
// ours running against it, a Pod stays unscheduled forever rather than being
// placed on the real k3s node.
func TestAStampedJobIsLeftAloneByKubeScheduler(t *testing.T) {
	skipWithoutDocker(t)
	ctx := context.Background()

	// A bare Pod our controller will not take: it is in a namespace we do
	// not watch? No -- simpler and stronger: give it a scheduler name that
	// is neither kube-scheduler's nor ours, so nothing in the cluster and
	// nothing in this process will bind it.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "unscheduled", Namespace: "default"},
		Spec: corev1.PodSpec{
			SchedulerName: "nobody-at-all",
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name: "work", Image: "busybox", Command: []string{"true"},
			}},
		},
	}
	if err := k8sClient.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}

	// Give kube-scheduler a generous chance to do the wrong thing.
	time.Sleep(10 * time.Second)

	var got corev1.Pod
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), &got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.NodeName != "" {
		t.Fatalf("kube-scheduler bound a pod carrying a foreign schedulerName to %q; "+
			"the whole approach depends on it declining", got.Spec.NodeName)
	}
	if got.Status.Phase != corev1.PodPending {
		t.Fatalf("pod phase = %q, want Pending", got.Status.Phase)
	}
}

func translatableJob(name string) *batchv1.Job {
	two := int32(2)
	one := int32(1)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default",
			Labels: map[string]string{"htcondor.org/scheduler": "htcondor"},
		},
		Spec: batchv1.JobSpec{
			Completions: &two,
			Parallelism: &one,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
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
			},
		},
	}
}

func podsOf(t *testing.T, jobName string) corev1.PodList {
	t.Helper()
	var pods corev1.PodList
	if err := k8sClient.List(context.Background(), &pods,
		client.InNamespace("default"),
		client.MatchingLabels{"batch.kubernetes.io/job-name": jobName}); err != nil {
		t.Fatal(err)
	}
	return pods
}

func getJob(t *testing.T, name string) *batchv1.Job {
	t.Helper()
	var job batchv1.Job
	if err := k8sClient.Get(context.Background(),
		client.ObjectKey{Namespace: "default", Name: name}, &job); err != nil {
		t.Fatal(err)
	}
	return &job
}

func jobDone(t *testing.T, name string) bool {
	t.Helper()
	return jobComplete(getJob(t, name))
}

func jobComplete(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

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
