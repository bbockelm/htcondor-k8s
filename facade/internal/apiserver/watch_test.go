package apiserver

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/bbockelm/htcondor-k8s/facade/internal/store"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// Watch is asserted through client-go's own StreamWatcher, which is the code
// an informer runs. It is strict about the framing in a way a test that reads
// lines of JSON is not: a WatchEvent whose embedded object is missing its kind
// decodes to nothing and the stream silently stalls.
func nextEvent(t *testing.T, w watch.Interface) watch.Event {
	t.Helper()
	select {
	case ev, ok := <-w.ResultChan():
		if !ok {
			t.Fatal("the watch stream closed unexpectedly")
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a watch event")
		return watch.Event{}
	}
}

func TestWatchSendsInitialStateThenChanges(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := cs.CoreV1().Pods("default").Create(ctx, demoPod("first", "default"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.sync()

	w, err := cs.CoreV1().Pods("default").Watch(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer w.Stop()

	ev := nextEvent(t, w)
	if ev.Type != watch.Added {
		t.Fatalf("the first event for an existing object must be ADDED, got %s", ev.Type)
	}
	pod, ok := ev.Object.(*corev1.Pod)
	if !ok {
		t.Fatalf("the event object did not decode as a Pod: %T", ev.Object)
	}
	if pod.Name != "first" {
		t.Fatalf("got pod %q", pod.Name)
	}
	firstRV := pod.ResourceVersion

	f.Fake.SetStatus("1.0", translate.JobStatusRunning)
	f.sync()
	ev = nextEvent(t, w)
	if ev.Type != watch.Modified {
		t.Fatalf("a status change must be MODIFIED, got %s", ev.Type)
	}
	pod = ev.Object.(*corev1.Pod)
	if pod.Status.Phase != corev1.PodRunning {
		t.Errorf("phase = %s, want Running", pod.Status.Phase)
	}
	if pod.ResourceVersion == firstRV {
		t.Error("a changed object must carry a new resourceVersion")
	}

	if err := cs.CoreV1().Pods("default").Delete(ctx, "first", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	ev = nextEvent(t, w)
	if ev.Type != watch.Deleted {
		t.Fatalf("a removed job must be DELETED, got %s", ev.Type)
	}
	if ev.Object.(*corev1.Pod).Name != "first" {
		t.Error("a DELETED event must name the object that went away")
	}
}

func TestWatchResumesFromAListsResourceVersion(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := cs.CoreV1().Pods("default").Create(ctx, demoPod("one", "default"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.sync()
	list, err := cs.CoreV1().Pods("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Everything after this point happened after the list, so a correct
	// resume reports all of it and nothing before it. This is the gap that
	// silently loses objects when list and watch come from different
	// snapshots.
	if _, err := cs.CoreV1().Pods("default").Create(ctx, demoPod("two", "default"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.Fake.SetStatus("1.0", translate.JobStatusRunning)
	f.sync()

	w, err := cs.CoreV1().Pods("default").Watch(ctx, metav1.ListOptions{ResourceVersion: list.ResourceVersion})
	if err != nil {
		t.Fatalf("resuming the watch: %v", err)
	}
	defer w.Stop()

	seen := map[string]watch.EventType{}
	for i := 0; i < 2; i++ {
		ev := nextEvent(t, w)
		seen[ev.Object.(*corev1.Pod).Name] = ev.Type
	}
	if seen["two"] != watch.Added {
		t.Errorf("the pod created after the list should be ADDED, got %q", seen["two"])
	}
	if seen["one"] != watch.Modified {
		t.Errorf("the pod that changed after the list should be MODIFIED, got %q", seen["one"])
	}
}

func TestWatchFromAnUnusableVersionIs410(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx := context.Background()

	// A version from a generation this server never served is exactly what
	// a client holds after the facade restarts.
	stale := store.Version{Gen: 0, Seq: 1}.String()
	_, err := cs.CoreV1().Pods("default").Watch(ctx, metav1.ListOptions{ResourceVersion: stale})
	if err == nil {
		t.Fatal("resuming from an unusable version must fail, not quietly start from now")
	}
	if !apierrors.IsResourceExpired(err) && !apierrors.IsGone(err) {
		t.Fatalf("want a 410 Gone so the client re-lists, got %v", err)
	}

	if _, err := cs.CoreV1().Pods("default").Watch(ctx, metav1.ListOptions{ResourceVersion: "not-a-version"}); err == nil {
		t.Error("a malformed resourceVersion must be refused")
	}
	_ = f
}

func TestWatchRespectsALabelSelector(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := cs.CoreV1().Pods("default").Watch(ctx, metav1.ListOptions{LabelSelector: "app=demo"})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	unmatched := demoPod("ignored", "default")
	unmatched.Labels = map[string]string{"app": "something-else"}
	if _, err := cs.CoreV1().Pods("default").Create(ctx, unmatched, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CoreV1().Pods("default").Create(ctx, demoPod("matched", "default"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.sync()

	ev := nextEvent(t, w)
	if got := ev.Object.(*corev1.Pod).Name; got != "matched" {
		t.Fatalf("the selector let %q through", got)
	}
}
