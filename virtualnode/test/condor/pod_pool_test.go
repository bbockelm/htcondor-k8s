// Package condor_test runs the pod controller against a real schedd.
//
// The unit tests cover the reconciler against a fake queue and the envtest
// suite covers it against a real API server. Neither touches the two things
// that can only be wrong against a real access point, and the second of them
// is load-bearing for the entire crash-safety design:
//
//  1. whether the submit file shared/translate renders for a Pod is one a
//     schedd accepts, and whether the ad that comes back parses into the
//     fields the status mapping reads;
//  2. whether `K8sPodUID == "..."` is a constraint a real schedd can evaluate
//     against a custom attribute. Submission here is idempotent only because
//     that query works. If it silently matched nothing, every reconcile would
//     submit again and the unit tests -- whose fake resolves the constraint by
//     construction -- would all still pass.
//
// It skips without condor_master on PATH, which is the normal case on a
// developer laptop:
//
//	PATH=/path/to/release_dir/bin:/path/to/release_dir/sbin:$PATH go test ./test/condor/...
package condor_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/condor"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/podctl"
	htcwebhook "github.com/bbockelm/htcondor-k8s/virtualnode/internal/webhook"
)

func TestPodSubmitAndFindAgainstARealSchedd(t *testing.T) {
	// Created before the harness so that cleanup (which is LIFO) removes
	// it after the daemons have shut down. A t.TempDir() here races the
	// shadow still writing into the job's initialdir.
	spool, err := os.MkdirTemp("", "virtualnode-spool")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(spool) })

	h := htcondor.SetupCondorHarness(t) // skips without condor_master
	defer func() {
		if t.Failed() {
			h.PrintScheddLog()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	collector := htcondor.NewCollector(h.GetCollectorAddr())
	ads, err := collector.QueryAds(ctx, "ScheddAd", "")
	if err != nil || len(ads) == 0 {
		t.Skipf("no schedd in the harness pool: %v", err)
	}
	addr, ok := ads[0].EvaluateAttrString("MyAddress")
	if !ok {
		t.Fatal("the schedd ad has no MyAddress")
	}
	name, _ := ads[0].EvaluateAttrString("Name")
	queue := condor.NewQueue(htcondor.NewSchedd(name, addr))

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	const podUID = "real-pod-uid-0001"
	deadline := int64(3600)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "real-pod", Namespace: "default", UID: types.UID(podUID),
			Labels: map[string]string{"app": "demo"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "batch/v1", Kind: "Job", Name: "real",
				UID: "real-job-uid-0001", Controller: boolp(true),
			}},
		},
		Spec: corev1.PodSpec{
			SchedulerName: api.SchedulerName,
			RestartPolicy: corev1.RestartPolicyNever,
			Tolerations:   htcwebhook.Tolerations(),
			// Pre-bound so the reconcile skips the binding subresource,
			// which the controller-runtime fake client does not
			// implement and which the envtest suite covers properly.
			NodeName:              api.NodeName,
			ActiveDeadlineSeconds: &deadline,
			Containers: []corev1.Container{{
				Name:    "work",
				Image:   "docker://busybox:latest",
				Command: []string{"/bin/sleep"},
				Args:    []string{"600"},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("64Mi"),
					},
				},
			}},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pod).
		WithStatusSubresource(&corev1.Pod{}).
		Build()

	r := &podctl.PodReconciler{
		Client: c, Scheme: scheme, Queue: queue,
		Caps:       translate.DefaultCapabilities(),
		ScheddName: name,
		// Without an initialdir the job's log lands in the test's own
		// working directory, which is the source tree.
		SpoolRoot:     spool,
		PollInterval:  time.Second,
		RetryInterval: time.Second,
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "real-pod", Namespace: "default"}}

	// Clean up whatever reaches the queue, whether or not the test passes:
	// a test that leaves jobs in a queue it did not create is a bad test.
	t.Cleanup(func() {
		_ = queue.RemoveByConstraint(context.Background(),
			podctl.PodConstraint(types.UID(podUID)), "test cleanup")
	})

	var got corev1.Pod
	for i := 0; i < 10; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
		if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Annotations[api.AnnotationJobID] != "" {
			break
		}
	}
	jobID := got.Annotations[api.AnnotationJobID]
	if jobID == "" {
		t.Fatal("no job id was recorded; the schedd did not accept the submit file")
	}

	// (2) The constraint the whole idempotency design rests on, evaluated
	// by a real schedd against a real custom attribute.
	found, err := queue.Query(ctx, podctl.PodConstraint(types.UID(podUID)))
	if err != nil {
		t.Fatalf("querying by Pod UID: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("K8sPodUID == %q matched %d jobs, want 1; "+
			"without this query every reconcile would submit again", podUID, len(found))
	}

	// (1) And the attributes the status mapping reads survive the round
	// trip through the queue.
	ad := found[0]
	if ad.Identity.UID != podUID {
		t.Fatalf("the Pod UID did not survive the round trip: %q", ad.Identity.UID)
	}
	if ad.Identity.Name != "real-pod" || ad.Identity.Namespace != "default" {
		t.Fatalf("the identity did not survive the round trip: %+v", ad.Identity)
	}
	if ad.Identity.OwnerKind != "Job" || ad.Identity.OwnerUID != "real-job-uid-0001" {
		t.Fatalf("the owner did not survive the round trip: %+v", ad.Identity)
	}
	if ad.JobStatus == 0 {
		t.Fatal("JobStatus did not parse")
	}
	if ad.QDate.IsZero() {
		t.Fatal("QDate did not parse; startTime is derived from it")
	}
	if ad.JobID() != jobID {
		t.Fatalf("the recorded job id %q is not the queue's %q", jobID, ad.JobID())
	}

	// The reaper's constraint has to work too, on the attribute this
	// controller invents rather than one shared/translate defines.
	byNode, err := queue.Query(ctx, podctl.NodeConstraint(api.NodeName))
	if err != nil {
		t.Fatalf("querying by virtual node: %v", err)
	}
	if len(byNode) != 1 {
		t.Fatalf("%s == %q matched %d jobs, want 1; the orphan sweep would be blind",
			podctl.AttrVirtualNode, api.NodeName, len(byNode))
	}

	// A Pod-level activeDeadlineSeconds became a periodic_remove the
	// schedd parsed rather than rejected, which is the only check there is
	// that the expression is well formed.
	if _, err := queue.Query(ctx, "ClusterId == "+strconv.Itoa(ad.ClusterID)); err != nil {
		t.Fatalf("the cluster is not queryable: %v", err)
	}

	// And a second pass must not submit a second cluster.
	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("second-pass reconcile %d: %v", i, err)
		}
	}
	again, err := queue.Query(ctx, podctl.PodConstraint(types.UID(podUID)))
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 {
		t.Fatalf("%d jobs carry this Pod UID after re-reconciling; the user's work would run twice", len(again))
	}

	// Deleting the Pod removes the job.
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, &got); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("finalize reconcile %d: %v", i, err)
		}
	}
	left, err := queue.Query(ctx, podctl.PodConstraint(types.UID(podUID)))
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range left {
		if a.JobStatus != translate.JobStatusRemoved && a.JobStatus != translate.JobStatusCompleted {
			t.Fatalf("job %s is still in state %d after its Pod was deleted", a.JobID(), a.JobStatus)
		}
	}
}

// TestPodDeadlineBecomesAnExpressionTheScheddParsed checks the one thing a
// rendered periodic_remove can be wrong about and still look fine: whether
// HTCondor accepted the expression.
//
// A Pod-level activeDeadlineSeconds has no kubelet to enforce it, so it is
// rendered as `periodic_remove = (time() - QDate) > N`. If that text did not
// parse, the submit would still succeed with no expression at all and the
// deadline would simply never fire -- silently, and indistinguishably from a
// job that finished in time.
func TestPodDeadlineBecomesAnExpressionTheScheddParsed(t *testing.T) {
	h := htcondor.SetupCondorHarness(t)
	defer func() {
		if t.Failed() {
			h.PrintScheddLog()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	collector := htcondor.NewCollector(h.GetCollectorAddr())
	ads, err := collector.QueryAds(ctx, "ScheddAd", "")
	if err != nil || len(ads) == 0 {
		t.Skipf("no schedd in the harness pool: %v", err)
	}
	addr, _ := ads[0].EvaluateAttrString("MyAddress")
	name, _ := ads[0].EvaluateAttrString("Name")
	schedd := htcondor.NewSchedd(name, addr)
	queue := condor.NewQueue(schedd)

	const podUID = "deadline-pod-uid-0001"
	deadline := int64(900)
	pod := podFor("deadline-pod", podUID)
	pod.Spec.ActiveDeadlineSeconds = &deadline

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pod).WithStatusSubresource(&corev1.Pod{}).Build()
	r := &podctl.PodReconciler{
		Client: c, Scheme: scheme, Queue: queue,
		Caps: translate.DefaultCapabilities(), ScheddName: name,
		SpoolRoot: t.TempDir(), PollInterval: time.Second, RetryInterval: time.Second,
	}
	t.Cleanup(func() {
		_ = queue.RemoveByConstraint(context.Background(),
			podctl.PodConstraint(types.UID(podUID)), "test cleanup")
	})

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "deadline-pod", Namespace: "default"}}
	for i := 0; i < 6; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}

	raw, err := schedd.Query(ctx, podctl.PodConstraint(types.UID(podUID)), []string{"PeriodicRemove"})
	if err != nil {
		t.Fatalf("querying PeriodicRemove: %v", err)
	}
	if len(raw) != 1 {
		t.Fatalf("expected one job, got %d", len(raw))
	}
	expr, ok := raw[0].Lookup("PeriodicRemove")
	if !ok || expr == nil {
		t.Fatal("the job has no PeriodicRemove; a Pod-level deadline would never fire")
	}
	if text := expr.String(); !strings.Contains(text, "QDate") || !strings.Contains(text, "900") {
		t.Fatalf("PeriodicRemove is %q, which is not the deadline we rendered", text)
	}
}

// podFor is the Pod the webhook would have stamped, pre-bound so the reconcile
// skips the binding subresource the fake client does not implement.
func podFor(name, uid string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default", UID: types.UID(uid),
		},
		Spec: corev1.PodSpec{
			SchedulerName: api.SchedulerName,
			RestartPolicy: corev1.RestartPolicyNever,
			Tolerations:   htcwebhook.Tolerations(),
			NodeName:      api.NodeName,
			Containers: []corev1.Container{{
				Name:    "work",
				Image:   "docker://busybox:latest",
				Command: []string{"/bin/sleep"},
				Args:    []string{"600"},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("64Mi"),
					},
				},
			}},
		},
	}
}

func boolp(b bool) *bool { return &b }
