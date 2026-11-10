// Package condor_test runs the HTCJob reconciler against a real schedd.
//
// The unit tests cover the reconciler's logic against a fake queue, and the
// envtest suite covers it against a real API server. Neither of them touches
// the one piece that can only be wrong in production: whether the submit file
// shared/translate renders is one a schedd accepts, and whether the job ad
// that comes back parses into the fields the status mapping reads. That is
// what this file is for.
//
// It skips without condor_master on PATH, which is the normal case on a
// developer laptop. Point PATH at an HTCondor install to run it:
//
//	PATH=/path/to/release_dir/bin:/path/to/release_dir/sbin:$PATH go test ./test/condor/...
package condor_test

import (
	"context"
	"os"
	"strconv"
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

	"github.com/bbockelm/htcondor-k8s/operator/api/v1alpha1"
	"github.com/bbockelm/htcondor-k8s/operator/internal/condor"
	"github.com/bbockelm/htcondor-k8s/operator/internal/controller"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

func TestSubmitAndTrackAgainstARealSchedd(t *testing.T) {
	// Created before the harness so that cleanup (which is LIFO) removes it
	// after the daemons have been shut down. A t.TempDir() here races the
	// shadow still writing into the job's initialdir.
	spool, err := os.MkdirTemp("", "htcjob-spool")
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
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	hj := &v1alpha1.HTCJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "real", Namespace: "default", UID: types.UID("real-pool-uid-1"),
		},
		Spec: v1alpha1.HTCJobSpec{
			Completions: ptr(int32(2)),
			Template: v1alpha1.HTCPodTemplateSpec{
				Spec: v1alpha1.HTCPodSpec{
					Container: v1alpha1.HTCContainer{
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
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(hj).
		WithStatusSubresource(&v1alpha1.HTCJob{}).
		Build()

	r := &controller.HTCJobReconciler{
		Client: c, Scheme: scheme, Queue: queue,
		Caps: translate.DefaultCapabilities(),
		// Without an initialdir the job's log lands in the test's own
		// working directory, which is the source tree.
		SpoolRoot:     spool,
		PollInterval:  time.Second,
		RetryInterval: time.Second,
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "real", Namespace: "default"}}

	// Reconcile until a cluster id lands, then clean up no matter what:
	// a test that leaves jobs in a queue it did not create is a bad test,
	// even when the queue is a temporary one.
	var got v1alpha1.HTCJob
	for i := 0; i < 10; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
		if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Status.ClusterID != "" {
			break
		}
	}
	if got.Status.ClusterID == "" {
		t.Fatal("no cluster id was recorded; the schedd did not accept the submit file")
	}
	t.Cleanup(func() {
		_ = queue.RemoveByConstraint(context.Background(),
			condor.OwnerConstraint(string(hj.UID)), "test cleanup")
	})

	// This is the real assertion: the attributes the status mapping reads
	// survive a round trip through the job queue.
	ads2, err := queue.Query(ctx, condor.ClusterConstraint(got.Status.ClusterID))
	if err != nil {
		t.Fatalf("querying the queue: %v", err)
	}
	if len(ads2) != 2 {
		t.Fatalf("expected 2 procs, got %d", len(ads2))
	}
	for _, ad := range ads2 {
		if ad.Identity.OwnerUID != string(hj.UID) {
			t.Fatalf("the owner UID did not survive the round trip: %q", ad.Identity.OwnerUID)
		}
		if ad.Identity.Namespace != "default" || ad.Identity.Name != "real" {
			t.Fatalf("the identity did not survive the round trip: %+v", ad.Identity)
		}
		if ad.JobStatus == 0 {
			t.Fatal("JobStatus did not parse")
		}
		if ad.QDate.IsZero() {
			t.Fatal("QDate did not parse")
		}
	}

	// Status must reflect the queue.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("status reconcile: %v", err)
	}
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Procs) != 2 {
		t.Fatalf("expected 2 procs in status, got %d", len(got.Status.Procs))
	}

	// And a second pass must not submit a second cluster.
	before := got.Status.ClusterID
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	all, err := queue.Query(ctx, condor.OwnerConstraint(string(hj.UID)))
	if err != nil {
		t.Fatal(err)
	}
	for _, ad := range all {
		if id := itoa(ad.ClusterID); id != before {
			t.Fatalf("a second cluster %s appeared beside %s", id, before)
		}
	}

	// Hold and release through the same path .spec.suspend uses.
	if err := queue.Hold(ctx, condor.OwnerConstraint(string(hj.UID)), "test"); err != nil {
		t.Fatalf("holding: %v", err)
	}
	held, err := queue.Query(ctx, condor.ClusterConstraint(before))
	if err != nil {
		t.Fatal(err)
	}
	for _, ad := range held {
		if ad.JobStatus != translate.JobStatusHeld {
			t.Fatalf("expected proc %d to be held, got status %d", ad.ProcID, ad.JobStatus)
		}
		if ad.HoldReasonCode != 1 {
			t.Fatalf("expected a user hold (code 1), got %d; suspend/resume keys on this",
				ad.HoldReasonCode)
		}
	}
}

func ptr[T any](v T) *T { return &v }

func itoa(i int) string { return strconv.Itoa(i) }
