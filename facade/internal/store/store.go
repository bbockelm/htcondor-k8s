package store

import (
	"encoding/json"
	"errors"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
)

// EventType is a Kubernetes watch event type.
type EventType string

const (
	Added    EventType = "ADDED"
	Modified EventType = "MODIFIED"
	Deleted  EventType = "DELETED"
)

// Event is one observed change, already stamped with the version at which it
// happened. Record carries the last known state even for a Deleted event,
// because a client that only ever saw the deletion still has to be able to
// tell *which* Pod went away, and in what namespace.
type Event struct {
	Type    EventType
	Version Version
	Record  backend.JobRecord
}

// ErrTooOld is the 410 Gone case: the caller asked to resume from a version
// this store can no longer reconstruct, either because the generation changed
// or because the event fell out of the replay buffer.
var ErrTooOld = errors.New("resourceVersion is too old")

// defaultHistory is how many recent events a watcher may fall behind by and
// still resume. A full-queue poll of a busy schedd can produce thousands of
// events in one tick, so this is sized for "a client reconnected within a few
// seconds", not for "a client was away for a minute": past it, re-listing is
// cheaper than replaying anyway.
const defaultHistory = 4096

// Store is the facade's object cache: every job the facade can see, the
// version at which each last changed, and a bounded replay buffer so a watch
// can be resumed.
//
// Reads are served from here rather than from the schedd. That makes a list
// consistent with the watch stream that follows it -- a list and a watch that
// disagree is the bug that makes informers lose objects forever -- at the cost
// of serving state as fresh as the last source tick. Writes compensate by
// injecting their own result synchronously, so a create is visible to the get
// that follows it.
type Store struct {
	mu      sync.RWMutex
	gen     uint64
	seq     uint64
	items   map[string]*entry
	history []Event
	// histBase is the number of events already evicted from history, so a
	// resume can tell "before my buffer" from "nothing happened since".
	oldest   Version
	watchers map[uint64]*watcher
	nextID   uint64
	histCap  int
}

type entry struct {
	rec     backend.JobRecord
	version Version
	hash    uint64
}

// New returns an empty store in generation gen.
//
// The caller picks the generation, because only it knows whether the sequence
// space it is about to fill is a continuation of something or a fresh start.
// A facade restart must pass a larger gen than last time or clients will see
// versions go backwards; the clock is the only thing available that does that
// without persistent state, which is a real limitation and not a detail.
func New(gen uint64) *Store {
	return &Store{
		gen:      gen,
		items:    map[string]*entry{},
		watchers: map[uint64]*watcher{},
		histCap:  defaultHistory,
		oldest:   Version{Gen: gen, Seq: 0},
	}
}

// Version is the store's current version, which is what a list reports as the
// collection's resourceVersion.
func (s *Store) Version() Version {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Version{Gen: s.gen, Seq: s.seq}
}

// Generation returns the current generation.
func (s *Store) Generation() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gen
}

// Replace installs a complete snapshot, emitting one event per difference.
//
// This is the polling source's only operation and the htcondordb source's
// recovery path. Diffing a snapshot rather than trusting a delta stream is
// what makes a missed update self-heal: the worst a lost change can do is be
// reported one tick late, never be lost.
func (s *Store) Replace(records []backend.JobRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seen := make(map[string]bool, len(records))
	// Sort so a snapshot that changes many objects assigns versions in a
	// stable order. Nothing depends on *which* order, but a reproducible
	// one makes a failing watch test say the same thing twice.
	sort.Slice(records, func(i, j int) bool { return records[i].Key() < records[j].Key() })

	var out []Event
	for _, rec := range records {
		if !rec.Visible() {
			continue
		}
		seen[rec.Key()] = true
		if ev, ok := s.upsertLocked(rec); ok {
			out = append(out, ev)
		}
	}
	for key, it := range s.items {
		if seen[key] {
			continue
		}
		s.seq++
		v := Version{Gen: s.gen, Seq: s.seq}
		delete(s.items, key)
		out = append(out, Event{Type: Deleted, Version: v, Record: it.rec})
	}
	s.recordLocked(out)
}

// Upsert installs or updates one record, used by a write path to make its own
// result visible without waiting for the next source tick.
func (s *Store) Upsert(recs ...backend.JobRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Event
	for _, rec := range recs {
		if !rec.Visible() {
			// A record that stopped being visible -- a removed job --
			// is a deletion, not an update to ignore.
			out = append(out, s.deleteLocked(rec.Key())...)
			continue
		}
		if ev, ok := s.upsertLocked(rec); ok {
			out = append(out, ev)
		}
	}
	s.recordLocked(out)
}

// Delete removes records by key, used by the delete path for the same reason.
func (s *Store) Delete(keys ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Event
	for _, key := range keys {
		out = append(out, s.deleteLocked(key)...)
	}
	s.recordLocked(out)
}

// Resync starts a new generation and installs a snapshot into it. Every
// outstanding watch is terminated with ErrTooOld, which is the honest answer:
// the source told us it cannot account for what happened in between.
func (s *Store) Resync(gen uint64, records []backend.JobRecord) {
	s.mu.Lock()
	if gen <= s.gen {
		// A generation must never go backwards; if a source hands us a
		// stale one, step past it rather than serve versions that
		// compare wrong.
		gen = s.gen + 1
	}
	s.gen = gen
	s.seq = 0
	s.oldest = Version{Gen: gen, Seq: 0}
	s.history = nil
	s.items = map[string]*entry{}
	ws := make([]*watcher, 0, len(s.watchers))
	for id, w := range s.watchers {
		ws = append(ws, w)
		delete(s.watchers, id)
	}
	s.mu.Unlock()

	for _, w := range ws {
		w.expire()
	}
	s.Replace(records)
}

// upsertLocked returns the event to emit, or ok=false when nothing changed.
func (s *Store) upsertLocked(rec backend.JobRecord) (Event, bool) {
	h := hashRecord(rec)
	key := rec.Key()
	if old, ok := s.items[key]; ok {
		if old.hash == h {
			return Event{}, false
		}
		s.seq++
		v := Version{Gen: s.gen, Seq: s.seq}
		s.items[key] = &entry{rec: rec, version: v, hash: h}
		return Event{Type: Modified, Version: v, Record: rec}, true
	}
	s.seq++
	v := Version{Gen: s.gen, Seq: s.seq}
	s.items[key] = &entry{rec: rec, version: v, hash: h}
	return Event{Type: Added, Version: v, Record: rec}, true
}

func (s *Store) deleteLocked(key string) []Event {
	it, ok := s.items[key]
	if !ok {
		return nil
	}
	s.seq++
	delete(s.items, key)
	return []Event{{Type: Deleted, Version: Version{Gen: s.gen, Seq: s.seq}, Record: it.rec}}
}

func (s *Store) recordLocked(events []Event) {
	if len(events) == 0 {
		return
	}
	s.history = append(s.history, events...)
	if over := len(s.history) - s.histCap; over > 0 {
		// Remember the last version evicted. A resume from it is still
		// safe --- everything after it is retained --- so it is the
		// floor, and only a resume from before it must re-list.
		s.oldest = s.history[over-1].Version
		s.history = append([]Event(nil), s.history[over:]...)
	}
	// A watcher that could not keep up is closed and must leave the
	// registry in the same breath; sending to it again would panic on a
	// closed channel and take every other watcher with it.
	for id, w := range s.watchers {
		if !w.send(events) {
			delete(s.watchers, id)
		}
	}
}

// Item returns one record and its version.
func (s *Store) Item(key string) (backend.JobRecord, Version, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	it, ok := s.items[key]
	if !ok {
		return backend.JobRecord{}, Version{}, false
	}
	return it.rec, it.version, true
}

// Snapshot returns every record matching keep, in key order, together with the
// version the snapshot was taken at. A watch started from that version is
// guaranteed to report every change the snapshot does not already contain.
func (s *Store) Snapshot(keep func(backend.JobRecord) bool) ([]backend.JobRecord, Version) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]backend.JobRecord, 0, len(s.items))
	for _, it := range s.items {
		if keep == nil || keep(it.rec) {
			out = append(out, it.rec)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out, Version{Gen: s.gen, Seq: s.seq}
}

// VersionOf returns the version at which a record last changed, which is the
// object's own resourceVersion.
func (s *Store) VersionOf(key string) (Version, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	it, ok := s.items[key]
	if !ok {
		return Version{}, false
	}
	return it.version, true
}

// hashRecord decides what counts as a change.
//
// It hashes the JSON of the whole decoded record, so any attribute the facade
// surfaces produces an event and any attribute it does not surface produces
// none. That is the property that matters: a job ad changes constantly
// (ImageSize, DiskUsage, the schedd's own bookkeeping) and emitting a
// MODIFIED for each would turn every informer into a hot loop, while missing a
// JobStatus transition would lose a Pod phase change forever.
func hashRecord(rec backend.JobRecord) uint64 {
	h := fnv.New64a()
	enc := json.NewEncoder(h)
	// A marshal failure here would have to come from a value the API
	// server's own decoder produced, so treat it as "changed" rather than
	// risk suppressing a real event.
	if enc.Encode(rec) != nil {
		return 0
	}
	return h.Sum64()
}
