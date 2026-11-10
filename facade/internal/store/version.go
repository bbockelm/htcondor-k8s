// Package store is the facade's answer to "where is the object store".
//
// There is no etcd here. The schedd's job queue holds the objects, but a job
// queue gives a Kubernetes client none of what it needs from a store: no
// monotonic revision counter, no "give me everything that changed after X",
// and no way to tell "nothing changed" from "I stopped looking". This package
// supplies those on top of whatever a JobEventSource can observe, and is
// therefore the single place where the correctness of `kubectl get -w` and of
// every informer built on this API server is decided.
package store

import (
	"fmt"
	"strconv"
	"strings"
)

// Version is a resourceVersion: a generation and a sequence within it.
//
// Kubernetes requires resourceVersion to be opaque to clients, monotonically
// increasing while the server keeps serving, and resumable. A schedd offers
// nothing with those properties -- a job ad has no revision, and the one
// counter in the system (the job queue log's offset) resets whenever the
// schedd rotates the log. So the facade keeps the counter itself, and makes
// the reset explicit instead of pretending it cannot happen:
//
//   - Seq is a facade-assigned counter, bumped once per observed change. It is
//     monotone within a generation, and it is what makes "resume after X"
//     answerable at all.
//
//   - Gen changes whenever the sequence space restarts: the facade restarted,
//     the event source lost its place, or the source itself reported a resync.
//     A watch resuming across a generation boundary is answered with 410 Gone,
//     which is exactly what the Kubernetes contract wants a server to say when
//     it can no longer honor a cursor. Clients re-list; nothing silently
//     misses an event.
//
// Ordering compares Gen first, so a version from an older generation is always
// older. That is only sound because a new generation is always given a larger
// Gen than the one it replaces, which is the one invariant every
// JobEventSource must uphold.
type Version struct {
	Gen uint64
	Seq uint64
}

// String renders the wire form. It is never empty and never "0": both are
// reserved by ListOptions.ResourceVersion to mean something else ("the most
// recent" and "any, a cache is fine"), and handing a client a version it will
// read as one of those is a bug that only shows up under load.
func (v Version) String() string {
	return strconv.FormatUint(v.Gen, 10) + "." + strconv.FormatUint(v.Seq, 10)
}

// Before reports whether v is strictly older than other.
func (v Version) Before(other Version) bool {
	if v.Gen != other.Gen {
		return v.Gen < other.Gen
	}
	return v.Seq < other.Seq
}

// ParseVersion reads a version off the wire.
func ParseVersion(s string) (Version, error) {
	gen, seq, ok := strings.Cut(s, ".")
	if !ok {
		return Version{}, fmt.Errorf("resourceVersion %q is not in this server's format", s)
	}
	g, err := strconv.ParseUint(gen, 10, 64)
	if err != nil {
		return Version{}, fmt.Errorf("resourceVersion %q has a bad generation: %w", s, err)
	}
	q, err := strconv.ParseUint(seq, 10, 64)
	if err != nil {
		return Version{}, fmt.Errorf("resourceVersion %q has a bad sequence: %w", s, err)
	}
	return Version{Gen: g, Seq: q}, nil
}
