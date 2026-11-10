package backend

import (
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

func parse(t *testing.T, old string) *classad.ClassAd {
	t.Helper()
	ad, err := classad.ParseOld(old)
	if err != nil {
		t.Fatalf("parsing the test ad: %v", err)
	}
	return ad
}

func TestFromClassAdReadsTheIdentityAttributes(t *testing.T) {
	ad := parse(t, `ClusterId = 42
ProcId = 3
JobStatus = 2
QDate = 1700000000
JobStartDate = 1700000100
NumJobStarts = 3
RemoteHost = "slot1_2@node7.example.org"
Owner = "alice"
K8sPodName = "sweep"
K8sPodNamespace = "research"
K8sPodUID = "abc-123"
K8sOwnerKind = "Job"
K8sOwnerName = "sweep"
K8sOwnerUID = "abc-123"
K8sCompletionIndex = 3
K8sLabels = "{\"app\":\"demo\"}"
K8sPodSpec = "{\"containers\":[{\"name\":\"main\",\"image\":\"docker://busybox\"}]}"
K8sSpoolDir = "/var/spool/research/sweep"
`)
	rec := FromClassAd(ad)
	if !rec.Usable() || !rec.Visible() {
		t.Fatal("a running job of ours should be both usable and visible")
	}
	if rec.Key() != "42.3" {
		t.Errorf("key = %q", rec.Key())
	}
	if rec.Ad.Identity.Namespace != "research" || rec.Ad.Identity.OwnerName != "sweep" {
		t.Errorf("identity = %+v", rec.Ad.Identity)
	}
	if rec.Ad.Index != 3 {
		t.Errorf("completion index = %d", rec.Ad.Index)
	}
	if rec.Ad.Identity.Labels["app"] != "demo" {
		t.Errorf("labels = %v", rec.Ad.Identity.Labels)
	}
	if rec.PodSpec == nil || len(rec.PodSpec.Containers) != 1 {
		t.Fatalf("the pod spec did not decode: %+v", rec.PodSpec)
	}
	if rec.IWD != "/var/spool/research/sweep" || rec.Owner != "alice" {
		t.Errorf("iwd=%q owner=%q", rec.IWD, rec.Owner)
	}
	if !rec.Ad.QDate.Equal(time.Unix(1700000000, 0).UTC()) {
		t.Errorf("QDate = %v", rec.Ad.QDate)
	}
	// NumJobStarts is how a Pod reports restarts, and one start is zero
	// restarts; shared/translate does the arithmetic, so the only thing to
	// get wrong here is losing the attribute.
	if rec.Ad.NumJobStarts != 3 {
		t.Errorf("NumJobStarts = %d", rec.Ad.NumJobStarts)
	}
}

func TestFromClassAdFallsBackToLastRemoteHost(t *testing.T) {
	// A finished job has no RemoteHost, but a Pod that can still name the
	// machine it ran on is strictly more useful than one that cannot.
	rec := FromClassAd(parse(t, `ClusterId = 1
ProcId = 0
JobStatus = 4
LastRemoteHost = "slot1@node3.example.org"
K8sPodName = "done"
`))
	if rec.Ad.RemoteHost != "slot1@node3.example.org" {
		t.Fatalf("RemoteHost = %q", rec.Ad.RemoteHost)
	}
}

func TestFromClassAdIgnoresJobsThatAreNotOurs(t *testing.T) {
	rec := FromClassAd(parse(t, "ClusterId = 1\nProcId = 0\nJobStatus = 1\nOwner = \"bob\"\n"))
	if rec.Usable() {
		t.Fatal("a job with no K8sPodName must not be surfaced as a Pod")
	}
}

func TestRemovedJobIsNotVisible(t *testing.T) {
	rec := FromClassAd(parse(t, "ClusterId = 1\nProcId = 0\nJobStatus = 3\nK8sPodName = \"gone\"\n"))
	if !rec.Usable() {
		t.Fatal("the record is still one of ours")
	}
	if rec.Visible() {
		t.Fatal("a removed job must read as a deleted object, or kubectl delete looks like it failed")
	}
}

func TestMalformedAttributesDegradeRatherThanFail(t *testing.T) {
	// One job carrying a mangled attribute must not break a whole list.
	rec := FromClassAd(parse(t, `ClusterId = 1
ProcId = 0
JobStatus = 1
K8sPodName = "ok"
K8sLabels = "not json"
K8sPodSpec = "{{{"
`))
	if !rec.Usable() {
		t.Fatal("the job should still be listable")
	}
	if rec.Ad.Identity.Labels != nil || rec.PodSpec != nil {
		t.Fatal("unparseable attributes should be dropped, not half-decoded")
	}
}

func TestConstraintIsAnchoredAndEscaped(t *testing.T) {
	c := Constraint(`re"search`)
	if !strings.Contains(c, translate.AttrPodName+" =!= undefined") {
		t.Errorf("the constraint does not exclude jobs we did not create: %s", c)
	}
	if !strings.Contains(c, "JobStatus =!= 3") {
		t.Errorf("the constraint does not exclude removed jobs: %s", c)
	}
	if !strings.Contains(c, `"re\"search"`) {
		t.Errorf("a namespace with a quote in it was not escaped: %s", c)
	}
	// It must still be a parseable ClassAd expression after escaping.
	if _, err := classad.ParseExpr(c); err != nil {
		t.Fatalf("the constraint does not parse: %v\n%s", err, c)
	}
}
