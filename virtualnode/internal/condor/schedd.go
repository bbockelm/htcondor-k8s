// Package condor is the controller's view of a schedd.
//
// The controller talks to the job queue through the Queue interface rather
// than through *htcondor.Schedd directly, for two reasons. The first is
// testability: a reconciler that needs a live access point to test is a
// reconciler nobody tests. The second is that the interface is where the
// translation between a classad.ClassAd and translate.JobAd lives, which keeps
// the classad library -- and the version skew that comes with it -- out of the
// reconcilers entirely.
package condor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// Queue is the job-queue operations the controllers need.
//
// Every method is expected to be safe to retry: Submit is the one that is not
// naturally idempotent, which is why FindCluster exists beside it rather than
// Submit trying to deduplicate on its own.
type Queue interface {
	// Submit queues a cluster and returns its id.
	Submit(ctx context.Context, submitFile string) (clusterID string, err error)

	// Query returns the job ads matching a ClassAd constraint.
	Query(ctx context.Context, constraint string) ([]translate.JobAd, error)

	// Remove removes jobs by "cluster.proc" id. Ids that are no longer in
	// the queue are not an error: a job that already left is removed.
	Remove(ctx context.Context, ids []string, reason string) error

	// RemoveByConstraint is the fallback for when no proc ids are known --
	// a CR deleted before its first status write still has to have its
	// cluster removed, and the owner UID is the only handle left.
	RemoveByConstraint(ctx context.Context, constraint, reason string) error

	// Hold and Release implement .spec.suspend.
	Hold(ctx context.Context, constraint, reason string) error
	Release(ctx context.Context, constraint, reason string) error

	// Peek asks a running job's starter for the tail of its output.
	//
	// It exists for the case the spool directory cannot cover: an access
	// point that does not share a filesystem with this process. Reading
	// the file is always better when it is possible -- it works for a
	// finished job and costs one open() -- so Peek is the fallback, not
	// the primary path. It only answers while the job is running, and it
	// returns a bounded tail rather than the whole stream.
	Peek(ctx context.Context, cluster, proc int, stderr bool, maxBytes int64) ([]byte, error)
}

// Projection is the attribute set the controller reads back. Asking for only
// these matters: a job ad carries the whole K8sPodSpec JSON blob, and pulling
// it on every status poll of every job would make the poll cost scale with the
// size of the pod templates rather than the number of jobs.
var Projection = []string{
	"ClusterId", "ProcId", "JobStatus",
	"ExitCode", "ExitBySignal", "ExitSignal",
	"HoldReason", "HoldReasonCode",
	"QDate", "JobStartDate", "EnteredCurrentStatus", "CompletionDate",
	"RemoteHost", "LastRemoteHost", "NumJobStarts",
	translate.AttrPodName, translate.AttrPodNamespace, translate.AttrPodUID,
	translate.AttrOwnerKind, translate.AttrOwnerName, translate.AttrOwnerUID,
	translate.AttrIndex,
}

// scheddQueue is the real implementation.
type scheddQueue struct {
	schedd *htcondor.Schedd
}

// NewQueue wraps a located schedd.
func NewQueue(s *htcondor.Schedd) Queue { return &scheddQueue{schedd: s} }

func (q *scheddQueue) Submit(ctx context.Context, submitFile string) (string, error) {
	return q.schedd.Submit(ctx, submitFile)
}

func (q *scheddQueue) Query(ctx context.Context, constraint string) ([]translate.JobAd, error) {
	ads, err := q.schedd.Query(ctx, constraint, Projection)
	if err != nil {
		return nil, err
	}
	out := make([]translate.JobAd, 0, len(ads))
	for _, ad := range ads {
		out = append(out, ParseJobAd(ad))
	}
	return out, nil
}

func (q *scheddQueue) Remove(ctx context.Context, ids []string, reason string) error {
	if len(ids) == 0 {
		return nil
	}
	res, err := q.schedd.RemoveJobsByID(ctx, ids, reason)
	if err != nil {
		return err
	}
	// NotFound is the expected answer for a job that already completed and
	// left the queue, so it is not a failure. PermissionDenied is: it means
	// the operator's credential does not own these jobs, and retrying will
	// not help, but silently dropping the finalizer would leak them.
	if res != nil && res.PermissionDenied > 0 {
		return fmt.Errorf("permission denied removing %d of %d jobs", res.PermissionDenied, len(ids))
	}
	return nil
}

func (q *scheddQueue) RemoveByConstraint(ctx context.Context, constraint, reason string) error {
	res, err := q.schedd.RemoveJobs(ctx, constraint, reason)
	if err != nil {
		return err
	}
	if res != nil && res.PermissionDenied > 0 {
		return fmt.Errorf("permission denied removing %d jobs", res.PermissionDenied)
	}
	return nil
}

func (q *scheddQueue) Hold(ctx context.Context, constraint, reason string) error {
	_, err := q.schedd.HoldJobs(ctx, constraint, reason)
	return err
}

func (q *scheddQueue) Release(ctx context.Context, constraint, reason string) error {
	_, err := q.schedd.ReleaseJobs(ctx, constraint, reason)
	return err
}

func (q *scheddQueue) Peek(ctx context.Context, cluster, proc int, stderr bool, maxBytes int64) ([]byte, error) {
	req := htcondor.PeekRequest{Stdout: !stderr, Stderr: stderr, MaxBytes: maxBytes}
	res, err := q.schedd.PeekJobOutput(ctx, cluster, proc, req)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
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

// OwnerConstraint is the deterministic handle from a Kubernetes object to its
// jobs. It is what makes submission idempotent: the UID is assigned by the API
// server before the controller ever runs, it never changes, and it is not
// reused when an object of the same name is recreated.
func OwnerConstraint(ownerUID string) string {
	return fmt.Sprintf("%s == %s", translate.AttrOwnerUID, classAdString(ownerUID))
}

// ClusterConstraint narrows to one cluster, for status polls once the cluster
// id is known.
func ClusterConstraint(clusterID string) string {
	return fmt.Sprintf("ClusterId == %s", clusterID)
}

// ParseJobAd reads the attributes this integration cares about out of a job ad.
//
// Missing attributes are normal rather than exceptional: a job that has never
// run has no JobStartDate, a job still in the queue has no CompletionDate, and
// a projection the schedd could not satisfy simply omits the attribute. So
// every read here is best-effort and the zero value is meaningful.
func ParseJobAd(ad *classad.ClassAd) translate.JobAd {
	out := translate.JobAd{Index: -1}
	if ad == nil {
		return out
	}

	out.ClusterID = int(adInt(ad, "ClusterId"))
	out.ProcID = int(adInt(ad, "ProcId"))
	out.JobStatus = int(adInt(ad, "JobStatus"))
	out.ExitCode = int(adInt(ad, "ExitCode"))
	out.ExitBySignal = adBool(ad, "ExitBySignal")
	out.ExitSignal = int(adInt(ad, "ExitSignal"))
	out.HoldReason = adString(ad, "HoldReason")
	out.HoldReasonCode = int(adInt(ad, "HoldReasonCode"))
	out.NumJobStarts = int(adInt(ad, "NumJobStarts"))

	out.QDate = adTime(ad, "QDate")
	out.JobStartDate = adTime(ad, "JobStartDate")
	out.EnteredCurrentStatus = adTime(ad, "EnteredCurrentStatus")
	out.CompletionDate = adTime(ad, "CompletionDate")

	out.RemoteHost = adString(ad, "RemoteHost")
	if out.RemoteHost == "" {
		// Once a job stops running the schedd moves the slot name to
		// LastRemoteHost; for a completed job that is the only record of
		// where it ran, and it is what the user wants to see.
		out.RemoteHost = adString(ad, "LastRemoteHost")
	}

	out.Identity = translate.Identity{
		Name:      adString(ad, translate.AttrPodName),
		Namespace: adString(ad, translate.AttrPodNamespace),
		UID:       adString(ad, translate.AttrPodUID),
		OwnerKind: adString(ad, translate.AttrOwnerKind),
		OwnerName: adString(ad, translate.AttrOwnerName),
		OwnerUID:  adString(ad, translate.AttrOwnerUID),
	}
	if v, ok := ad.EvaluateAttrInt(translate.AttrIndex); ok {
		out.Index = int(v)
	}
	return out
}

func adInt(ad *classad.ClassAd, name string) int64 {
	v, _ := ad.EvaluateAttrInt(name)
	return v
}

func adBool(ad *classad.ClassAd, name string) bool {
	v, _ := ad.EvaluateAttrBool(name)
	return v
}

func adString(ad *classad.ClassAd, name string) string {
	v, _ := ad.EvaluateAttrString(name)
	return v
}

// adTime reads a job-ad timestamp. HTCondor stores them as Unix seconds, and
// zero means "never happened" rather than 1970.
func adTime(ad *classad.ClassAd, name string) time.Time {
	v, ok := ad.EvaluateAttrInt(name)
	if !ok || v <= 0 {
		return time.Time{}
	}
	return time.Unix(v, 0).UTC()
}

// classAdString renders a Go string as a ClassAd string literal. Constraints
// are built from object UIDs, which are machine-generated and could not
// contain a quote -- but a constraint assembled by concatenation is exactly
// the kind of thing that stops being true later.
func classAdString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
