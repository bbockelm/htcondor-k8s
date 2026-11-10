package store

import (
	"testing"
	"time"

	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

func rec(cluster, proc, status int, name string) backend.JobRecord {
	var r backend.JobRecord
	r.Ad.ClusterID = cluster
	r.Ad.ProcID = proc
	r.Ad.JobStatus = status
	r.Ad.Identity.Name = name
	r.Ad.Identity.Namespace = "default"
	return r
}

func drain(t *testing.T, w *Watcher, want int) []Event {
	t.Helper()
	var got []Event
	deadline := time.After(2 * time.Second)
	for len(got) < want {
		select {
		case ev, ok := <-w.Events():
			if !ok {
				t.Fatalf("watcher closed after %d of %d events (expired=%v)", len(got), want, w.Expired())
			}
			got = append(got, ev)
		case <-deadline:
			t.Fatalf("timed out after %d of %d events", len(got), want)
		}
	}
	return got
}

func TestReplaceAssignsMonotonicVersions(t *testing.T) {
	s := New(7)
	s.Replace([]backend.JobRecord{rec(1, 0, translate.JobStatusIdle, "a"), rec(2, 0, translate.JobStatusIdle, "b")})

	va, ok := s.VersionOf("1.0")
	if !ok {
		t.Fatal("1.0 is missing from the store")
	}
	vb, _ := s.VersionOf("2.0")
	if !va.Before(vb) {
		t.Fatalf("versions are not increasing: %s then %s", va, vb)
	}
	if got := s.Version(); got != vb {
		t.Fatalf("collection version %s should be the newest object version %s", got, vb)
	}

	// An identical snapshot must not invent a change. An informer that is
	// woken once per poll for every object is an informer nobody can run.
	before := s.Version()
	s.Replace([]backend.JobRecord{rec(1, 0, translate.JobStatusIdle, "a"), rec(2, 0, translate.JobStatusIdle, "b")})
	if s.Version() != before {
		t.Fatalf("an unchanged snapshot bumped the version from %s to %s", before, s.Version())
	}
}

func TestReplaceEmitsAddedModifiedDeleted(t *testing.T) {
	s := New(1)
	w, err := s.Watch(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	s.Replace([]backend.JobRecord{rec(1, 0, translate.JobStatusIdle, "a")})
	s.Replace([]backend.JobRecord{rec(1, 0, translate.JobStatusRunning, "a")})
	s.Replace(nil)

	got := drain(t, w, 3)
	want := []EventType{Added, Modified, Deleted}
	for i, ev := range got {
		if ev.Type != want[i] {
			t.Errorf("event %d: got %s, want %s", i, ev.Type, want[i])
		}
	}
	if got[2].Record.Ad.Identity.Name != "a" {
		t.Error("a DELETED event must still identify which object went away")
	}
}

func TestRemovedJobLooksDeleted(t *testing.T) {
	s := New(1)
	s.Replace([]backend.JobRecord{rec(1, 0, translate.JobStatusIdle, "a")})
	w, err := s.Watch(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	s.Upsert(rec(1, 0, translate.JobStatusRemoved, "a"))
	got := drain(t, w, 1)
	if got[0].Type != Deleted {
		t.Fatalf("a job moving to JobStatus 3 must read as a deletion, got %s", got[0].Type)
	}
	if _, _, ok := s.Item("1.0"); ok {
		t.Error("the removed job is still in the store")
	}
}

func TestWatchResumesFromAVersion(t *testing.T) {
	s := New(1)
	s.Replace([]backend.JobRecord{rec(1, 0, translate.JobStatusIdle, "a")})
	snapshot, at := s.Snapshot(nil)
	if len(snapshot) != 1 {
		t.Fatalf("snapshot has %d records", len(snapshot))
	}

	// Changes that land after the snapshot was taken but before the watch
	// starts are exactly the ones a naive implementation drops.
	s.Upsert(rec(2, 0, translate.JobStatusIdle, "b"))
	s.Upsert(rec(1, 0, translate.JobStatusRunning, "a"))

	w, err := s.Watch(&at)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	got := drain(t, w, 2)
	if got[0].Record.Ad.Identity.Name != "b" || got[0].Type != Added {
		t.Errorf("first replayed event: got %s %s", got[0].Type, got[0].Record.Ad.Identity.Name)
	}
	if got[1].Record.Ad.Identity.Name != "a" || got[1].Type != Modified {
		t.Errorf("second replayed event: got %s %s", got[1].Type, got[1].Record.Ad.Identity.Name)
	}
}

func TestWatchFromAnotherGenerationIsTooOld(t *testing.T) {
	s := New(5)
	s.Replace([]backend.JobRecord{rec(1, 0, translate.JobStatusIdle, "a")})
	stale := Version{Gen: 4, Seq: 1}
	if _, err := s.Watch(&stale); err != ErrTooOld {
		t.Fatalf("a version from an older generation must be refused, got %v", err)
	}
}

func TestResyncStartsANewGenerationAndExpiresWatchers(t *testing.T) {
	s := New(1)
	s.Replace([]backend.JobRecord{rec(1, 0, translate.JobStatusIdle, "a")})
	w, err := s.Watch(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	s.Resync(2, []backend.JobRecord{rec(1, 0, translate.JobStatusIdle, "a")})

	select {
	case _, open := <-w.Events():
		if open {
			t.Fatal("the watcher should have been closed, not fed an event")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a resync must end every open watch")
	}
	if !w.Expired() {
		t.Error("the watcher should report that it expired, so the handler stops the stream")
	}
	if s.Generation() != 2 {
		t.Fatalf("generation is %d, want 2", s.Generation())
	}
	// A generation that went backwards would make versions compare wrong
	// forever, so the store must refuse one.
	s.Resync(1, nil)
	if s.Generation() != 3 {
		t.Fatalf("a backwards resync must still move forward; generation is %d", s.Generation())
	}
}

func TestVersionRoundTripAndOrder(t *testing.T) {
	v := Version{Gen: 1700000000000, Seq: 42}
	parsed, err := ParseVersion(v.String())
	if err != nil || parsed != v {
		t.Fatalf("round trip of %s gave %v, %v", v, parsed, err)
	}
	if v.String() == "0" || v.String() == "" {
		t.Fatal(`a resourceVersion must never render as "" or "0"`)
	}
	if !(Version{Gen: 1, Seq: 9}).Before(Version{Gen: 2, Seq: 0}) {
		t.Error("a later generation must sort after an earlier one regardless of sequence")
	}
	if _, err := ParseVersion("nonsense"); err == nil {
		t.Error("a malformed resourceVersion must be rejected, not silently treated as zero")
	}
}

func TestWatchFromBeforeTheReplayBufferIsTooOld(t *testing.T) {
	s := New(1)
	s.histCap = 4
	s.Replace([]backend.JobRecord{rec(1, 0, translate.JobStatusIdle, "a")})
	early, _ := s.VersionOf("1.0")

	// Overrun the buffer. Each Upsert of a changed record is one event, so
	// well past histCap events have been evicted by the end of this.
	for i := 0; i < 10; i++ {
		s.Upsert(rec(1, 0, translate.JobStatusIdle+i%2, "a"))
	}
	if _, err := s.Watch(&early); err != ErrTooOld {
		t.Fatalf("a version evicted from the replay buffer must be refused, got %v", err)
	}

	// The oldest version the buffer still covers is resumable: everything
	// strictly after it is retained, so there is no hole to hide.
	s.mu.RLock()
	floor := s.oldest
	s.mu.RUnlock()
	w, err := s.Watch(&floor)
	if err != nil {
		t.Fatalf("a resume from the buffer's own floor must succeed, got %v", err)
	}
	w.Stop()
}
