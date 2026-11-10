package condor

import (
	"strconv"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/stretchr/testify/require"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

func parse(t *testing.T, text string) *classad.ClassAd {
	t.Helper()
	ad, err := classad.Parse(text)
	require.NoError(t, err)
	return ad
}

func TestParseJobAdReadsTheQueueAttributes(t *testing.T) {
	start := time.Now().Add(-time.Hour).Truncate(time.Second)
	ad := parse(t, `[
		ClusterId = 4211;
		ProcId = 7;
		JobStatus = 2;
		NumJobStarts = 3;
		JobStartDate = `+itoa(start.Unix())+`;
		RemoteHost = "slot1_2@ep7.chtc.wisc.edu";
		K8sPodName = "sim";
		K8sPodNamespace = "physics";
		K8sOwnerKind = "HTCJob";
		K8sOwnerUID = "6f1b-uid";
		K8sCompletionIndex = 7
	]`)

	got := ParseJobAd(ad)
	require.Equal(t, 4211, got.ClusterID)
	require.Equal(t, 7, got.ProcID)
	require.Equal(t, translate.JobStatusRunning, got.JobStatus)
	require.Equal(t, 3, got.NumJobStarts)
	require.Equal(t, start.UTC(), got.JobStartDate)
	require.Equal(t, "slot1_2@ep7.chtc.wisc.edu", got.RemoteHost)
	require.Equal(t, "physics", got.Identity.Namespace)
	require.Equal(t, "6f1b-uid", got.Identity.OwnerUID)
	require.Equal(t, 7, got.Index)
	require.Equal(t, "4211.7", got.JobID())
}

// Every attribute is optional in practice -- a projection the schedd could not
// satisfy simply omits it -- so the parser must never depend on one being set.
func TestParseJobAdToleratesMissingAttributes(t *testing.T) {
	got := ParseJobAd(parse(t, `[ ClusterId = 1; ProcId = 0; JobStatus = 1 ]`))
	require.Equal(t, translate.JobStatusIdle, got.JobStatus)
	require.True(t, got.JobStartDate.IsZero(), "a job that never ran has no start date, not 1970")
	require.True(t, got.CompletionDate.IsZero())
	require.Equal(t, -1, got.Index, "no completion index must read as -1, not proc 0")
	require.Equal(t, "", got.RemoteHost)

	require.Equal(t, translate.JobAd{Index: -1}, ParseJobAd(nil))
}

func TestParseJobAdFallsBackToLastRemoteHost(t *testing.T) {
	got := ParseJobAd(parse(t, `[
		ClusterId = 9; ProcId = 0; JobStatus = 4;
		LastRemoteHost = "slot1@ep3.example.edu"
	]`))
	require.Equal(t, "slot1@ep3.example.edu", got.RemoteHost,
		"a completed job's only record of where it ran is LastRemoteHost")
}

func TestParseJobAdReadsSignalExit(t *testing.T) {
	got := ParseJobAd(parse(t, `[
		ClusterId = 9; ProcId = 0; JobStatus = 4;
		ExitBySignal = true; ExitSignal = 9
	]`))
	require.True(t, got.ExitBySignal)
	require.Equal(t, 9, got.ExitSignal)
	ps := translate.PodStatus(got, "work")
	require.Equal(t, int32(137), ps.ContainerStatuses[0].State.Terminated.ExitCode,
		"Kubernetes reports a signalled exit as 128+signal")
}

func TestOwnerConstraintQuotesTheUID(t *testing.T) {
	c := OwnerConstraint("6f1b-uid")
	require.Equal(t, `K8sOwnerUID == "6f1b-uid"`, c)

	// The constraint goes onto the wire as a ClassAd expression; a value
	// that could close the string literal must not be able to. The quotes
	// stay escaped, so the whole thing remains one string literal.
	c = OwnerConstraint(`a" || true || "`)
	require.Equal(t, `K8sOwnerUID == "a\" || true || \""`, c)
	expr, err := classad.ParseExpr(c)
	require.NoError(t, err)
	require.NotNil(t, expr)
}

func TestFakeQueueResolvesBothConstraintForms(t *testing.T) {
	q := NewFakeQueue()
	cluster, err := q.Submit(t.Context(), `+K8sOwnerUID = "abc"
queue 2`)
	require.NoError(t, err)

	byOwner, err := q.Query(t.Context(), OwnerConstraint("abc"))
	require.NoError(t, err)
	require.Len(t, byOwner, 2)

	byCluster, err := q.Query(t.Context(), ClusterConstraint(cluster))
	require.NoError(t, err)
	require.Len(t, byCluster, 2)

	none, err := q.Query(t.Context(), OwnerConstraint("nobody"))
	require.NoError(t, err)
	require.Empty(t, none)
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }
