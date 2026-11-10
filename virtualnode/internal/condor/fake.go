package condor

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// FakeQueue is an in-memory job queue for tests.
//
// It lives in the non-test build because the envtest integration uses it too:
// the thing under test there is the controller's interaction with a real API
// server, and bringing up a real pool as well would make the one test that
// must always run depend on an access point being present.
//
// Its constraint support is deliberately only what the controllers emit --
// `K8sOwnerUID == "..."` and `ClusterId == N`. A fake that pretended to
// evaluate arbitrary ClassAd expressions would be a second, worse
// implementation of the classad evaluator, and the first bug in it would look
// like a controller bug.
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

	// LoseSubmitResult makes Submit queue the cluster and then report an
	// error, which is the failure the idempotency design exists for: the
	// jobs are really in the queue and the controller does not know it.
	LoseSubmitResult bool

	// PeekOut is the tail Peek returns, keyed by "cluster.proc.stream"
	// where stream is "out" or "err". A missing key returns no bytes,
	// which is what a job that has not written yet looks like.
	PeekOut map[string][]byte

	// PeekErr, when set, fails every Peek. It models a starter that
	// cannot be reached, which is the common case rather than an exotic
	// one: the job has to be running and the AP has to answer.
	PeekErr error

	nextCluster int
	jobs        map[string][]*translate.JobAd // cluster id -> procs
	ownerOf     map[string]string             // owner uid -> cluster id
	held        map[string]bool
	removed     map[string]bool
}

// NewFakeQueue returns an empty fake whose first cluster id is 100.
func NewFakeQueue() *FakeQueue {
	return &FakeQueue{
		nextCluster: 100,
		PeekOut:     map[string][]byte{},
		jobs:        map[string][]*translate.JobAd{},
		ownerOf:     map[string]string{},
		held:        map[string]bool{},
		removed:     map[string]bool{},
	}
}

var _ Queue = (*FakeQueue)(nil)

func (f *FakeQueue) Submit(_ context.Context, submitFile string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.SubmitErr != nil {
		return "", f.SubmitErr
	}
	f.Submissions = append(f.Submissions, submitFile)

	cluster := strconv.Itoa(f.nextCluster)
	f.nextCluster++

	count := queueCount(submitFile)
	owner := submitAttr(submitFile, translate.AttrOwnerUID)
	id := translate.Identity{
		Name:      submitAttr(submitFile, translate.AttrPodName),
		Namespace: submitAttr(submitFile, translate.AttrPodNamespace),
		OwnerKind: submitAttr(submitFile, translate.AttrOwnerKind),
		OwnerName: submitAttr(submitFile, translate.AttrOwnerName),
		OwnerUID:  owner,
	}
	held := strings.Contains(submitFile, "hold = True")
	for p := 0; p < count; p++ {
		status := translate.JobStatusIdle
		if held {
			status = translate.JobStatusHeld
		}
		f.jobs[cluster] = append(f.jobs[cluster], &translate.JobAd{
			ClusterID: atoi(cluster), ProcID: p,
			JobStatus: status, HoldReasonCode: 1,
			Identity: id, Index: -1,
		})
	}
	f.held[cluster] = held
	if owner != "" {
		f.ownerOf[owner] = cluster
	}
	if f.LoseSubmitResult {
		return "", fmt.Errorf("fake: connection dropped after commit")
	}
	return cluster, nil
}

func (f *FakeQueue) Query(_ context.Context, constraint string) ([]translate.JobAd, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.QueryErr != nil {
		return nil, f.QueryErr
	}
	cluster, ok := f.resolve(constraint)
	if !ok || f.removed[cluster] {
		return nil, nil
	}
	out := make([]translate.JobAd, 0, len(f.jobs[cluster]))
	for _, j := range f.jobs[cluster] {
		out = append(out, *j)
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
			f.removed[cluster] = true
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
	if cluster, ok := f.resolve(constraint); ok {
		f.removed[cluster] = true
	}
	return nil
}

func (f *FakeQueue) Hold(_ context.Context, constraint, _ string) error {
	return f.setHold(constraint, true)
}

func (f *FakeQueue) Release(_ context.Context, constraint, _ string) error {
	return f.setHold(constraint, false)
}

func (f *FakeQueue) setHold(constraint string, hold bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ActionErr != nil {
		return f.ActionErr
	}
	cluster, ok := f.resolve(constraint)
	if !ok {
		return nil
	}
	f.held[cluster] = hold
	for _, j := range f.jobs[cluster] {
		if hold && j.JobStatus == translate.JobStatusIdle {
			j.JobStatus = translate.JobStatusHeld
			j.HoldReasonCode = 1
			j.HoldReason = "suspended by htcondor-k8s"
		} else if !hold && j.JobStatus == translate.JobStatusHeld {
			j.JobStatus = translate.JobStatusIdle
			j.HoldReason = ""
		}
	}
	return nil
}

// SetProcStatus drives a proc through a state change, the way the pool would.
func (f *FakeQueue) SetProcStatus(cluster string, proc int, status int, mutate func(*translate.JobAd)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, j := range f.jobs[cluster] {
		if j.ProcID == proc {
			j.JobStatus = status
			if mutate != nil {
				mutate(j)
			}
		}
	}
}

// SubmitCount is how many submit files have been accepted.
func (f *FakeQueue) SubmitCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Submissions)
}

// IsHeld reports the hold state of a cluster.
func (f *FakeQueue) IsHeld(cluster string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.held[cluster]
}

// IsRemoved reports whether a cluster has been removed from the queue.
func (f *FakeQueue) IsRemoved(cluster string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.removed[cluster]
}

// resolve maps one of the two constraints the controllers build onto a
// cluster. Callers hold the lock.
func (f *FakeQueue) resolve(constraint string) (string, bool) {
	if rest, ok := strings.CutPrefix(constraint, translate.AttrOwnerUID+" == "); ok {
		uid := strings.Trim(strings.TrimSpace(rest), `"`)
		c, ok := f.ownerOf[uid]
		return c, ok
	}
	if rest, ok := strings.CutPrefix(constraint, "ClusterId == "); ok {
		c := strings.TrimSpace(rest)
		_, ok := f.jobs[c]
		return c, ok
	}
	return "", false
}

// queueCount reads the proc count off the trailing `queue N` line.
func queueCount(submitFile string) int {
	n := 1
	for _, line := range strings.Split(submitFile, "\n") {
		line = strings.TrimSpace(line)
		rest, ok := strings.CutPrefix(line, "queue")
		if !ok {
			continue
		}
		rest = strings.TrimSpace(rest)
		if rest == "" {
			n = 1
			continue
		}
		if v, err := strconv.Atoi(rest); err == nil {
			n = v
		}
	}
	if n < 1 {
		n = 1
	}
	return n
}

// submitAttr reads a `+Attr = "value"` line back out of a submit file.
func submitAttr(submitFile, attr string) string {
	prefix := "+" + attr + " = "
	for _, line := range strings.Split(submitFile, "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), prefix); ok {
			return strings.Trim(rest, `"`)
		}
	}
	return ""
}

func atoi(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

// Peek returns whatever PeekOut holds for this proc and stream.
func (f *FakeQueue) Peek(_ context.Context, cluster, proc int, stderr bool, maxBytes int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.PeekErr != nil {
		return nil, f.PeekErr
	}
	stream := "out"
	if stderr {
		stream = "err"
	}
	b := f.PeekOut[fmt.Sprintf("%d.%d.%s", cluster, proc, stream)]
	// The real starter returns a bounded tail, and a caller that assumes
	// it got the whole stream is wrong in a way only a large job reveals.
	if maxBytes > 0 && int64(len(b)) > maxBytes {
		b = b[int64(len(b))-maxBytes:]
	}
	return b, nil
}
