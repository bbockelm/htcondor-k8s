// Package source supplies the facade with changes to the job queue.
//
// A Kubernetes watch is only as good as the thing telling the API server that
// something happened, and HTCondor offers two very different answers. Polling
// the schedd works anywhere a query works and is bounded by the poll interval;
// htcondordb's change feed mirrors job_queue.log and reports a change within a
// few hundred milliseconds, at the cost of a second daemon on the AP. Both are
// behind one interface so the choice is a flag, not a fork in the code.
package source

import (
	"context"

	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
)

// Sink is the half of the store a source writes to. *store.Store implements it.
type Sink interface {
	// Replace installs a complete snapshot in the current generation,
	// emitting one event per difference.
	Replace(records []backend.JobRecord)
	// Upsert and Delete apply single incremental changes.
	Upsert(records ...backend.JobRecord)
	Delete(keys ...string)
	// Resync starts a new generation. A source calls it when it can no
	// longer account for everything that happened since the last event it
	// delivered, which is what turns an unprovable gap into an honest 410
	// for every open watch rather than a silently missing event.
	Resync(gen uint64, records []backend.JobRecord)
	// Generation is the generation the sink is currently in.
	Generation() uint64
}

// JobEventSource keeps a Sink up to date with the AP's job queue.
//
// Run blocks until ctx is done or it hits an error it cannot recover from. A
// source that can recover must do so internally rather than return: the store
// above it has no way to distinguish "the source is reconnecting" from "the
// source is gone", and guessing wrong in either direction is a correctness
// bug.
type JobEventSource interface {
	Name() string
	Run(ctx context.Context, sink Sink) error
}
