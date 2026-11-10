package backend

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/PelicanPlatform/classad/classad"
	htcondor "github.com/bbockelm/golang-htcondor"
)

// Fake is an in-memory job queue.
//
// It is not a mock: submit files go through golang-htcondor's real submit
// parser and the ads it produces are the ads the facade reads back. That makes
// the unit tests catch the failure mode that actually bites -- submit text
// that parses but does not say what the Pod said -- without a schedd. What it
// cannot test is anything the schedd itself decides: ownership, matchmaking,
// and whether the C++ submit path agrees with the Go one.
type Fake struct {
	mu        sync.Mutex
	nextClust int
	ads       map[string]*classad.ClassAd // "cluster.proc"
	// Output is the canned stdout/stderr Peek returns, keyed "cluster.proc".
	Output map[string][]byte

	// SubmitErr, if set, is returned by the next Submit. Tests use it to
	// drive the "the schedd said no" path.
	SubmitErr error
}

func NewFake() *Fake {
	return &Fake{nextClust: 1, ads: map[string]*classad.ClassAd{}, Output: map[string][]byte{}}
}

func (f *Fake) Submit(_ context.Context, submitFile string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.SubmitErr != nil {
		err := f.SubmitErr
		f.SubmitErr = nil
		return 0, err
	}
	parsed, err := htcondor.ParseSubmitFile(strings.NewReader(submitFile))
	if err != nil {
		return 0, fmt.Errorf("fake schedd: %w", err)
	}
	cluster := f.nextClust
	f.nextClust++
	result, err := parsed.Submit(cluster)
	if err != nil {
		return 0, fmt.Errorf("fake schedd: %w", err)
	}
	for i, ad := range result.ProcAds {
		// A real schedd fills these in at submit time; without them the
		// job looks like it has no status at all.
		_ = ad.Set("ClusterId", int64(cluster))
		_ = ad.Set("ProcId", int64(i))
		if _, ok := ad.EvaluateAttrInt("JobStatus"); !ok {
			_ = ad.Set("JobStatus", int64(1))
		}
		_ = ad.Set("Owner", "faker")
		f.ads[fmt.Sprintf("%d.%d", cluster, i)] = ad
	}
	return cluster, nil
}

func (f *Fake) Query(_ context.Context, constraint string) ([]JobRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	expr, err := classad.ParseExpr(constraint)
	if err != nil {
		return nil, fmt.Errorf("fake schedd: bad constraint %q: %w", constraint, err)
	}
	var out []JobRecord
	for _, ad := range f.ads {
		b, err := ad.EvaluateExprWithTarget(expr, nil).BoolValue()
		if err != nil || !b {
			continue
		}
		r := FromClassAd(ad)
		if r.Usable() {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *Fake) Remove(_ context.Context, ids []string, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		delete(f.ads, id)
	}
	return nil
}

func (f *Fake) Peek(_ context.Context, cluster, proc int, stderr bool, _ int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fmt.Sprintf("%d.%d", cluster, proc)
	if stderr {
		key += ".err"
	}
	return f.Output[key], nil
}

// SetStatus drives a queued job through its lifecycle, which is the only way
// a test can reach the status-mapping and watch-event code paths.
func (f *Fake) SetStatus(id string, status int, mutate ...func(*classad.ClassAd)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ad, ok := f.ads[id]
	if !ok {
		return
	}
	_ = ad.Set("JobStatus", int64(status))
	for _, m := range mutate {
		m(ad)
	}
}

// Ad exposes a stored ad so a test can assert on what the submit file really
// produced, rather than on the facade's own reading of it.
func (f *Fake) Ad(id string) *classad.ClassAd {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ads[id]
}
