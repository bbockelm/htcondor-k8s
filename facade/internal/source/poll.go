package source

import (
	"context"
	"log/slog"
	"time"

	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
)

// Poll is the source that always works: a full-queue query on a timer.
//
// It has no incremental path on purpose. A delta stream derived from polling
// would be a delta stream with gaps in it, and the store's own diff already
// turns a snapshot into exactly the events that changed. The cost is that
// every tick transfers every job the facade can see; for a PoC against one AP
// that is fine, and the projection in backend.Projection is what keeps it from
// being absurd. It is the wrong answer for a schedd with a million jobs in it.
//
// The observable consequence for a client is latency, not correctness: a Pod
// phase change is reported within one Interval, and `kubectl get pods -w`
// behaves like a Kubernetes watch that happens to be slow.
type Poll struct {
	Backend  backend.Backend
	Interval time.Duration
	Log      *slog.Logger
}

func (p *Poll) Name() string { return "schedd-poll" }

func (p *Poll) Run(ctx context.Context, sink Sink) error {
	interval := p.Interval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	p.tick(ctx, sink)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			p.tick(ctx, sink)
		}
	}
}

func (p *Poll) tick(ctx context.Context, sink Sink) {
	recs, err := p.Backend.Query(ctx, backend.Constraint(""))
	if err != nil {
		// A failed poll is not a reason to tell clients their objects
		// vanished: hold the last snapshot and try again. A schedd that
		// stays unreachable shows up as staleness, which the facade
		// reports through /healthz rather than by lying in a list.
		if p.Log != nil && ctx.Err() == nil {
			p.Log.Warn("job queue poll failed; serving the previous snapshot", "err", err)
		}
		return
	}
	sink.Replace(recs)
}
