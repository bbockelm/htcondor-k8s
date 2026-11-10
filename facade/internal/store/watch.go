package store

import "sync"

// Watcher is one open `?watch=true` request.
//
// A watcher that stops reading is closed rather than allowed to block the
// store: a single slow kubectl must not stall the source that every other
// watcher depends on. Closing with Expired set tells the handler to end the
// stream, which makes the client re-list -- the same recovery the API server
// uses for a 410, and the only one that cannot lose an event.
type Watcher struct {
	ch      chan Event
	store   *Store
	id      uint64
	once    sync.Once
	expired bool
	mu      sync.Mutex
}

// Events is the stream. It is closed when the watcher ends.
func (w *Watcher) Events() <-chan Event { return w.ch }

// Expired reports whether the stream ended because the watcher could not be
// kept up to date, rather than because the caller stopped it.
func (w *Watcher) Expired() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.expired
}

// Stop releases the watcher. It is safe to call more than once.
func (w *Watcher) Stop() {
	w.store.mu.Lock()
	delete(w.store.watchers, w.id)
	w.store.mu.Unlock()
	w.once.Do(func() { close(w.ch) })
}

type watcher = Watcher

// watchBuffer is how far behind a watcher may fall before it is expired. A
// full-queue resync can produce one event per job at once, so this is sized to
// absorb a small pool's worth in a single tick.
const watchBuffer = 1024

// send reports whether the watcher is still usable. A false answer means the
// caller must deregister it before any further send: the channel is closed,
// and a second send on it would take the whole store down.
func (w *Watcher) send(events []Event) bool {
	for _, ev := range events {
		select {
		case w.ch <- ev:
		default:
			w.expire()
			return false
		}
	}
	return true
}

func (w *Watcher) expire() {
	w.mu.Lock()
	w.expired = true
	w.mu.Unlock()
	w.once.Do(func() { close(w.ch) })
}

// Watch returns a stream of changes after from.
//
// from == nil means "start from now": the caller is expected to have taken a
// Snapshot first and to synthesize the ADDED events itself, which is how the
// handler keeps a list and the watch that follows it consistent.
//
// A non-nil from is honored exactly: every event the store still remembers
// with a greater version is replayed before any live event. If the store
// cannot prove it remembers all of them -- a different generation, or a
// version older than the replay buffer's floor -- it returns ErrTooOld rather
// than silently skipping, because a watch with a hole in it is worse than no
// watch at all.
func (s *Store) Watch(from *Version) (*Watcher, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var replay []Event
	if from != nil {
		if from.Gen != s.gen {
			return nil, ErrTooOld
		}
		// s.oldest is the version of the last event evicted from the
		// replay buffer, so everything strictly after it is still
		// remembered. A resume *at* the floor therefore has no hole and
		// is honored; only a resume from before it has to re-list.
		if from.Before(s.oldest) {
			return nil, ErrTooOld
		}
		for _, ev := range s.history {
			if from.Before(ev.Version) {
				replay = append(replay, ev)
			}
		}
		if len(replay) > watchBuffer {
			// The backlog alone would overrun the watcher; a re-list
			// is both cheaper and what the client would end up doing.
			return nil, ErrTooOld
		}
	}

	s.nextID++
	w := &Watcher{ch: make(chan Event, watchBuffer), store: s, id: s.nextID}
	s.watchers[w.id] = w
	for _, ev := range replay {
		w.ch <- ev
	}
	return w, nil
}
