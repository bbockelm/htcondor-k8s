// Package backend is the facade's view of an HTCondor access point.
//
// Everything above this package manipulates Kubernetes objects; everything
// below it speaks ClassAds and CEDAR. The interface exists so the REST layer
// can be exercised against an in-memory queue in unit tests without a schedd,
// and because the two things the facade needs from an AP -- a job queue it can
// write to and read back, and the bytes a job wrote to stdout -- are a much
// smaller surface than the whole golang-htcondor client.
package backend

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// Facade-local job-ad attributes, alongside the ones shared/translate stamps.
const (
	// AttrJobSpec holds the admitted batchv1.JobSpec as JSON on every proc
	// of a Job's cluster. translate stamps the PodSpec but not the Job
	// fields around it, and without them `kubectl get job -o yaml` cannot
	// round-trip: there is nowhere else to keep .spec.completions.
	AttrJobSpec = "K8sJobSpec"

	// AttrIWD is the spool directory the facade created for this object.
	// It is also the job's initialdir, so it is where stdout and stderr
	// land; the log handler needs it and deriving it from the ad is more
	// honest than recomputing the path and hoping the rules never change.
	AttrIWD = "K8sSpoolDir"
)

// Projection is the attribute set every facade query asks for. A job ad has
// hundreds of attributes and the facade reads a few dozen; naming them keeps
// the per-ad cost of a full-queue poll roughly flat as pools grow.
//
// It deliberately includes K8sPodSpec, which is the largest attribute by far.
// The facade is the object store, so a list that omitted the spec would be a
// list that cannot answer `kubectl get pod -o yaml`.
var Projection = []string{
	"ClusterId", "ProcId", "JobStatus", "ExitCode", "ExitBySignal", "ExitSignal",
	"HoldReason", "HoldReasonCode", "QDate", "JobStartDate", "EnteredCurrentStatus",
	"CompletionDate", "RemoteHost", "LastRemoteHost", "NumJobStarts", "Owner",
	translate.AttrPodName, translate.AttrPodNamespace, translate.AttrPodUID,
	translate.AttrOwnerKind, translate.AttrOwnerName, translate.AttrOwnerUID,
	translate.AttrLabels, translate.AttrAnnotations, translate.AttrPodSpec,
	translate.AttrIndex, AttrJobSpec, AttrIWD,
}

// JobRecord is one proc of one cluster, decoded far enough that nothing above
// this package has to know what a ClassAd is.
type JobRecord struct {
	Ad translate.JobAd

	// PodSpec is the spec the Pod was admitted with, read back from
	// K8sPodSpec. Nil means the attribute was missing or unparseable,
	// which for a facade-created job should not happen and for anything
	// else means the job is not ours.
	PodSpec *corev1.PodSpec

	// JobSpecJSON is the raw K8sJobSpec, empty for a standalone Pod.
	JobSpecJSON string

	// IWD is the job's spool directory, where stdout and stderr land.
	IWD string

	// Owner is the HTCondor user the job runs as. Surfaced so the facade
	// can say in a Pod annotation who really owns the work, which is not
	// the same thing as the Kubernetes namespace it claims to be in.
	Owner string
}

// Key is the "cluster.proc" identifier, which is the facade's primary key for
// everything: there is no other durable, unique name in a job queue.
func (r JobRecord) Key() string { return r.Ad.JobID() }

// Backend is an HTCondor access point.
//
// Every method takes the namespace-scoped constraint already built by the
// caller: namespace is not an HTCondor concept, so there is no place below
// this line that could enforce it.
type Backend interface {
	// Submit queues a cluster and returns its ClusterId.
	Submit(ctx context.Context, submitFile string) (int, error)

	// Query returns every job matching constraint, decoded.
	Query(ctx context.Context, constraint string) ([]JobRecord, error)

	// Remove removes jobs by "cluster.proc" id.
	Remove(ctx context.Context, ids []string, reason string) error

	// Peek returns up to maxBytes from the tail of a *running* job's
	// stdout or stderr. It is the only way to see output when the facade
	// does not share a filesystem with the schedd.
	Peek(ctx context.Context, cluster, proc int, stderr bool, maxBytes int64) ([]byte, error)
}

// Constraint builds a ClassAd constraint for the facade's own jobs.
//
// Every clause is anchored on K8sPodName being defined, so the facade never
// reports a job it did not create as a Pod. That is not a security boundary
// -- a user can set +K8sPodName by hand -- but it does keep an ordinary
// condor_submit from showing up in `kubectl get pods`.
func Constraint(namespace string, extra ...string) string {
	parts := []string{translate.AttrPodName + " =!= undefined"}
	if namespace != "" {
		parts = append(parts, fmt.Sprintf("%s == %s", translate.AttrPodNamespace, adString(namespace)))
	}
	// A removed job is a deleted object: condor_rm is the only way a Pod
	// goes away here, so leaving JobStatus 3 visible would mean `kubectl
	// delete pod` appeared not to work for as long as the schedd took to
	// drain it.
	parts = append(parts, "JobStatus =!= 3")
	parts = append(parts, extra...)
	return strings.Join(parts, " && ")
}

// adString renders a Go string as a ClassAd string literal. shared/translate
// has the same function unexported; duplicating ten lines is cheaper than
// widening a contract two implementations depend on.
func adString(s string) string {
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
		case '\t':
			b.WriteString(`\t`)
		case '\r':
			b.WriteString(`\r`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// AdString is the exported form, for callers that have to build a constraint
// or an ExtraAttributes value themselves.
func AdString(s string) string { return adString(s) }
