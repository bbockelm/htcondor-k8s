package backend

import (
	"context"
	"fmt"
	"strconv"

	htcondor "github.com/bbockelm/golang-htcondor"
)

// Schedd is the real backend: a live HTCondor access point.
type Schedd struct {
	schedd *htcondor.Schedd
}

// NewSchedd wraps an already-located schedd.
func NewSchedd(s *htcondor.Schedd) *Schedd { return &Schedd{schedd: s} }

// Locate finds a schedd through a collector. name may be empty for the pool's
// only schedd.
func Locate(ctx context.Context, collectorAddr, name string) (*Schedd, error) {
	col := htcondor.NewCollector(collectorAddr)
	loc, err := col.LocateDaemon(ctx, "Schedd", name)
	if err != nil {
		return nil, fmt.Errorf("locating schedd %q via collector %s: %w", name, collectorAddr, err)
	}
	return &Schedd{schedd: htcondor.NewSchedd(loc.Name, loc.Address)}, nil
}

// Name is the schedd's own name, which the facade folds into the identity of
// every object it serves: a Pod is only unique within one job queue.
func (s *Schedd) Name() string { return s.schedd.Name() }

func (s *Schedd) Submit(ctx context.Context, submitFile string) (int, error) {
	clusterStr, err := s.schedd.Submit(ctx, submitFile)
	if err != nil {
		return 0, err
	}
	cluster, err := strconv.Atoi(clusterStr)
	if err != nil {
		return 0, fmt.Errorf("schedd returned a non-numeric cluster id %q: %w", clusterStr, err)
	}
	return cluster, nil
}

func (s *Schedd) Query(ctx context.Context, constraint string) ([]JobRecord, error) {
	ads, err := s.schedd.Query(ctx, constraint, Projection)
	if err != nil {
		return nil, err
	}
	out := make([]JobRecord, 0, len(ads))
	for _, ad := range ads {
		r := FromClassAd(ad)
		if r.Usable() {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *Schedd) Remove(ctx context.Context, ids []string, reason string) error {
	if len(ids) == 0 {
		return nil
	}
	results, err := s.schedd.RemoveJobsByID(ctx, ids, reason)
	if err != nil {
		return err
	}
	// A remove that matched nothing is not an error at this layer: the
	// REST layer already established the object existed, and a job that
	// left the queue between the two calls is a deletion that succeeded.
	_ = results
	return nil
}

func (s *Schedd) Peek(ctx context.Context, cluster, proc int, stderr bool, maxBytes int64) ([]byte, error) {
	req := htcondor.PeekRequest{MaxBytes: maxBytes}
	if stderr {
		req.Stderr = true
		req.StderrOffset = -1
	} else {
		req.Stdout = true
		req.StdoutOffset = -1
	}
	res, err := s.schedd.PeekJobOutput(ctx, cluster, proc, req)
	if err != nil {
		return nil, err
	}
	stream := res.Stdout
	if stderr {
		stream = res.Stderr
	}
	if stream == nil {
		return nil, nil
	}
	return stream.Bytes, nil
}
