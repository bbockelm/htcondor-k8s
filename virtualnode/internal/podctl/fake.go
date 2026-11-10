package podctl

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/condor"
)

// FakeQueue is an in-memory job queue for testing this controller.
//
// It lives in the non-test build because the envtest suite uses it too: what
// is under test there is this controller's interaction with a real API server,
// and requiring a real access point as well would make the one test that must
// always run depend on a pool being present.
//
// It is a second fake beside internal/condor.FakeQueue, which is not an
// accident worth hiding. That one serves the HTCJob controllers and cannot
// serve this one for two concrete reasons: its constraint resolver understands
// only `K8sOwnerUID == "..."` and `ClusterId == N`, while the Pod controller's
// whole idempotency argument rests on `K8sPodUID == "..."` and its reaper on
// `K8sVirtualNode == "..."`; and its Submit never reads +K8sPodUID back out of
// the submit file, so its ads carry no Identity.UID at all -- which is the
// exact field the adoption path and the orphan sweep compare on. A fake that
// cannot express the thing under test would make every one of these tests pass
// vacuously. Folding the two together is worth doing once the Queue interface
// settles; see the report.
//
// Its constraint support is still deliberately narrow -- equality on one of a
// handful of named attributes -- for the same reason the other fake's is. A
// fake that evaluated arbitrary ClassAd expressions would be a second, worse
// implementation of the evaluator, and its first bug would look like a
// controller bug.
type FakeQueue struct {
	mu sync.Mutex

	// Submissions records every submit-file text in order, so a test can
	// assert that a reconcile loop submitted exactly once.
	Submissions []string

	// SubmitErr, QueryErr and ActionErr, when set, fail the corresponding
	// call. They model an unreachable schedd.
	SubmitErr error
	QueryErr  error
	ActionErr error

	// LoseSubmitResult makes Submit queue the job and *then* report an
	// error. This is the crash the idempotency design exists for: the work
	// is really in the queue and the controller does not know it.
	LoseSubmitResult bool

	// Now is the clock, overridable so a test can age a job past the
	// reaper's grace period without sleeping.
	Now func() time.Time

	nextCluster int
	clusters    map[string]*fakeCluster
	order       []string
}

type fakeCluster struct {
	attrs   map[string]string
	procs   []*translate.JobAd
	removed bool
}

// NewFakeQueue returns an empty fake whose first cluster id is 100.
func NewFakeQueue() *FakeQueue {
	return &FakeQueue{nextCluster: 100, clusters: map[string]*fakeCluster{}}
}

var _ condor.Queue = (*FakeQueue)(nil)

func (f *FakeQueue) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

func (f *FakeQueue) Submit(_ context.Context, submitFile string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.SubmitErr != nil {
		return "", f.SubmitErr
	}
	f.Submissions = append(f.Submissions, submitFile)

	cluster := strconv.Itoa(f.nextCluster)
	f.nextCluster++

	attrs := submitAttrs(submitFile)
	id := translate.Identity{
		Name:      attrs[translate.AttrPodName],
		Namespace: attrs[translate.AttrPodNamespace],
		UID:       attrs[translate.AttrPodUID],
		OwnerKind: attrs[translate.AttrOwnerKind],
		OwnerName: attrs[translate.AttrOwnerName],
		OwnerUID:  attrs[translate.AttrOwnerUID],
	}
	index := -1
	if v, err := strconv.Atoi(attrs[translate.AttrIndex]); err == nil {
		index = v
	}

	fc := &fakeCluster{attrs: attrs}
	n := queueCount(submitFile)
	for p := 0; p < n; p++ {
		fc.procs = append(fc.procs, &translate.JobAd{
			ClusterID: f.nextCluster - 1, ProcID: p,
			JobStatus: translate.JobStatusIdle,
			QDate:     f.now(), EnteredCurrentStatus: f.now(),
			Identity: id, Index: index,
		})
	}
	f.clusters[cluster] = fc
	f.order = append(f.order, cluster)

	if f.LoseSubmitResult {
		return "", fmt.Errorf("fake: the connection dropped after the commit")
	}
	return cluster, nil
}

func (f *FakeQueue) Query(_ context.Context, constraint string) ([]translate.JobAd, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.QueryErr != nil {
		return nil, f.QueryErr
	}
	var out []translate.JobAd
	for _, id := range f.order {
		fc := f.clusters[id]
		if fc.removed || !f.matches(id, fc, constraint) {
			continue
		}
		for _, p := range fc.procs {
			out = append(out, *p)
		}
	}
	return out, nil
}

func (f *FakeQueue) Remove(_ context.Context, ids []string, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ActionErr != nil {
		return f.ActionErr
	}
	for _, id := range ids {
		if cluster, _, ok := strings.Cut(id, "."); ok {
			if fc := f.clusters[cluster]; fc != nil {
				fc.removed = true
			}
		}
	}
	return nil
}

func (f *FakeQueue) RemoveByConstraint(_ context.Context, constraint, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ActionErr != nil {
		return f.ActionErr
	}
	for _, id := range f.order {
		if fc := f.clusters[id]; f.matches(id, fc, constraint) {
			fc.removed = true
		}
	}
	return nil
}

func (f *FakeQueue) Hold(_ context.Context, constraint, reason string) error {
	return f.setStatus(constraint, translate.JobStatusHeld, reason)
}

func (f *FakeQueue) Release(_ context.Context, constraint, _ string) error {
	return f.setStatus(constraint, translate.JobStatusIdle, "")
}

func (f *FakeQueue) setStatus(constraint string, status int, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ActionErr != nil {
		return f.ActionErr
	}
	for _, id := range f.order {
		fc := f.clusters[id]
		if fc.removed || !f.matches(id, fc, constraint) {
			continue
		}
		for _, p := range fc.procs {
			p.JobStatus = status
			p.HoldReason = reason
			if status == translate.JobStatusHeld {
				p.HoldReasonCode = 1
			}
			p.EnteredCurrentStatus = f.now()
		}
	}
	return nil
}

// matches evaluates the narrow constraint grammar the controllers emit:
// `<Attr> == "<literal>"` or `ClusterId == <n>`.
func (f *FakeQueue) matches(cluster string, fc *fakeCluster, constraint string) bool {
	attr, want, ok := strings.Cut(constraint, " == ")
	if !ok {
		return false
	}
	attr = strings.TrimSpace(attr)
	want = strings.Trim(strings.TrimSpace(want), `"`)
	if attr == "ClusterId" {
		return cluster == want
	}
	v, ok := fc.attrs[attr]
	return ok && v == want
}

// --- test drivers -------------------------------------------------------

// SetStatus drives a job through a state change the way a pool would.
func (f *FakeQueue) SetStatus(jobID string, status int, mutate func(*translate.JobAd)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cluster, procS, _ := strings.Cut(jobID, ".")
	proc, _ := strconv.Atoi(procS)
	fc := f.clusters[cluster]
	if fc == nil {
		return
	}
	for _, p := range fc.procs {
		if p.ProcID != proc {
			continue
		}
		p.JobStatus = status
		p.EnteredCurrentStatus = f.now()
		if status == translate.JobStatusRunning && p.JobStartDate.IsZero() {
			p.JobStartDate = f.now()
			p.NumJobStarts++
		}
		if status == translate.JobStatusCompleted {
			p.CompletionDate = f.now()
		}
		if mutate != nil {
			mutate(p)
		}
	}
}

// Complete finishes a job with an exit code, which is the state transition
// every terminal assertion in these tests turns on.
func (f *FakeQueue) Complete(jobID string, exitCode int) {
	f.SetStatus(jobID, translate.JobStatusCompleted, func(a *translate.JobAd) {
		a.ExitCode = exitCode
	})
}

// Drop removes a cluster from the queue without a condor_rm, the way the
// schedd's own history reaping does.
func (f *FakeQueue) Drop(jobID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cluster, _, _ := strings.Cut(jobID, ".")
	if fc := f.clusters[cluster]; fc != nil {
		fc.removed = true
	}
}

// SubmitCount is how many submit files have been accepted.
func (f *FakeQueue) SubmitCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Submissions)
}

// LiveCount is how many clusters are still in the queue.
func (f *FakeQueue) LiveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, fc := range f.clusters {
		if !fc.removed {
			n++
		}
	}
	return n
}

// IsRemoved reports whether the cluster behind a job id has been removed.
func (f *FakeQueue) IsRemoved(jobID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	cluster, _, _ := strings.Cut(jobID, ".")
	fc := f.clusters[cluster]
	return fc == nil || fc.removed
}

// AgeAll moves every queued job's QDate backwards, so a test can reach the
// reaper's grace period without sleeping through it.
func (f *FakeQueue) AgeAll(by time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, fc := range f.clusters {
		for _, p := range fc.procs {
			p.QDate = p.QDate.Add(-by)
		}
	}
}

// --- submit-file reading ------------------------------------------------

// submitAttrs reads the `+Attr = "value"` lines back out of a submit file.
// Only the custom attributes matter here; everything else a real schedd would
// do with the file is somebody else's test.
func submitAttrs(submitFile string) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(submitFile, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "+") {
			continue
		}
		name, value, ok := strings.Cut(line[1:], " = ")
		if !ok {
			continue
		}
		out[name] = strings.Trim(strings.TrimSpace(value), `"`)
	}
	return out
}

func queueCount(submitFile string) int {
	n := 1
	for _, line := range strings.Split(submitFile, "\n") {
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), "queue")
		if !ok {
			continue
		}
		rest = strings.TrimSpace(rest)
		if rest == "" {
			n = 1
			continue
		}
		if v, err := strconv.Atoi(rest); err == nil && v > 0 {
			n = v
		}
	}
	return n
}
