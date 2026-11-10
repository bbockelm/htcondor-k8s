//go:build integration

// Package integration drives the facade against a real HTCondor access point
// and, where the binary is present, against the real kubectl.
//
// Everything else in this repo is tested against an in-memory queue, which
// proves the facade is self-consistent. It cannot prove the two things that
// actually decide whether this works: that the C++ schedd accepts the submit
// file shared/translate produces, and that the job ad it stores reads back as
// the Pod that was submitted. Those are what this file is for.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bbockelm/htcondor-k8s/facade/internal/apiserver"
	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
	"github.com/bbockelm/htcondor-k8s/facade/internal/source"
	"github.com/bbockelm/htcondor-k8s/facade/internal/store"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// liveFacade starts a personal HTCondor and a facade in front of it. The
// harness skips the whole test when condor_master is not on PATH.
type liveFacade struct {
	t       *testing.T
	harness *htcondor.CondorTestHarness
	schedd  *backend.Schedd
	http    *httptest.Server
}

func startFacade(t *testing.T) *liveFacade {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping the integration test in short mode")
	}
	if _, err := exec.LookPath("condor_master"); err != nil {
		t.Skip("condor_master is not on PATH; skipping the integration test")
	}

	harness := htcondor.SetupCondorHarness(t)
	if err := harness.WaitForDaemons(); err != nil {
		t.Fatalf("the personal condor did not start: %v", err)
	}
	sched, err := backend.Locate(context.Background(), harness.GetCollectorAddr(), "")
	if err != nil {
		t.Fatalf("locating the schedd: %v", err)
	}

	st := store.New(uint64(time.Now().UnixMilli()))
	src := &source.Poll{Backend: sched, Interval: 250 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = src.Run(ctx, st) }()

	srv := apiserver.New(&apiserver.Server{
		Backend:    sched,
		Store:      st,
		Caps:       translate.DefaultCapabilities(),
		SpoolRoot:  t.TempDir(),
		ScheddName: sched.Name(),
	})
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return &liveFacade{t: t, harness: harness, schedd: sched, http: ts}
}

// waitFor polls a predicate. No fixed sleeps anywhere in here: a schedd's
// response time depends on what else the machine is doing.
func waitFor(t *testing.T, what string, timeout time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

const integrationPod = `{
  "apiVersion": "v1",
  "kind": "Pod",
  "metadata": {"name": "live", "namespace": "research", "labels": {"app": "demo"}},
  "spec": {
    "restartPolicy": "Never",
    "containers": [{
      "name": "main",
      "image": "docker://busybox:latest",
      "command": ["/bin/sh", "-c", "echo hello from htcondor"],
      "resources": {"requests": {"cpu": "2", "memory": "512Mi", "ephemeral-storage": "2Gi"}}
    }]
  }
}`

// TestFacadeSubmitsToARealSchedd is the claim this whole approach rests on:
// a Pod posted to the facade becomes a job in a real C++ schedd's queue, with
// the resources the Pod asked for and the identity attributes the facade needs
// to read it back.
func TestFacadeSubmitsToARealSchedd(t *testing.T) {
	f := startFacade(t)
	ctx := context.Background()

	resp, body := post(t, f.http.URL+"/api/v1/namespaces/research/pods", integrationPod)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("creating the pod: %d %s", resp.StatusCode, body)
	}
	var created corev1.Pod
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("decoding the created pod: %v", err)
	}
	jobID := created.Annotations[apiserver.AnnoJobID]
	if jobID == "" {
		t.Fatal("the created pod does not name the job behind it")
	}
	t.Logf("pod research/live is HTCondor job %s", jobID)

	// Ask the schedd directly rather than the facade: the facade reading
	// back what the facade wrote would prove nothing about the schedd.
	cluster := strings.SplitN(jobID, ".", 2)[0]
	ads, err := f.schedd.Query(ctx, "ClusterId == "+cluster)
	if err != nil {
		t.Fatalf("querying the schedd: %v", err)
	}
	if len(ads) != 1 {
		t.Fatalf("the schedd has %d jobs in cluster %s, want 1", len(ads), cluster)
	}
	rec := ads[0]
	if rec.Ad.Identity.Name != "live" || rec.Ad.Identity.Namespace != "research" {
		t.Errorf("the job ad identifies %s/%s", rec.Ad.Identity.Namespace, rec.Ad.Identity.Name)
	}
	if rec.PodSpec == nil || len(rec.PodSpec.Containers) != 1 {
		t.Fatal("K8sPodSpec did not survive a round trip through the real schedd")
	}
	if got := rec.PodSpec.Containers[0].Image; got != "docker://busybox:latest" {
		t.Errorf("image came back as %q", got)
	}

	// request_cpus and request_memory are read with condor_q, so the
	// assertion is about what HTCondor itself believes it provisioned,
	// not about what the facade thinks it asked for.
	raw := condorQ(t, f.harness, cluster, "RequestCpus", "RequestMemory", "RequestDisk", "JobUniverse", "WantContainer", "ContainerImage")
	if raw["RequestCpus"] != "2" {
		t.Errorf("RequestCpus = %q, want 2", raw["RequestCpus"])
	}
	if raw["RequestMemory"] != "512" {
		t.Errorf("RequestMemory = %q, want 512 (MB)", raw["RequestMemory"])
	}
	if raw["RequestDisk"] != fmt.Sprint(2*1024*1024) {
		t.Errorf("RequestDisk = %q, want %d (KB)", raw["RequestDisk"], 2*1024*1024)
	}
	// HTCondor's container universe is vanilla (JobUniverse 5) plus
	// WantContainer and ContainerImage; there is no separate universe
	// number for it. Asserting on the universe alone would have passed for
	// a plain vanilla job that ignored the image entirely.
	if raw["JobUniverse"] != "5" {
		t.Errorf("JobUniverse = %q, want 5 (vanilla, which is what container universe is)", raw["JobUniverse"])
	}
	if raw["WantContainer"] != "true" {
		t.Errorf("WantContainer = %q; the container universe did not survive", raw["WantContainer"])
	}
	if raw["ContainerImage"] != "docker://busybox:latest" {
		t.Errorf("ContainerImage = %q", raw["ContainerImage"])
	}

	// The facade's own list must agree, once the poll has run.
	waitFor(t, "the pod to appear in a list", 15*time.Second, func() bool {
		var list corev1.PodList
		getJSON(t, f.http.URL+"/api/v1/namespaces/research/pods", &list)
		return len(list.Items) == 1 && list.Items[0].Name == "live"
	})

	// Deleting through the facade must remove the job from the real queue.
	del, _ := request(t, http.MethodDelete, f.http.URL+"/api/v1/namespaces/research/pods/live", "")
	if del.StatusCode != http.StatusOK {
		t.Fatalf("deleting the pod: %d", del.StatusCode)
	}
	waitFor(t, "the job to leave the schedd's queue", 30*time.Second, func() bool {
		ads, err := f.schedd.Query(ctx, "ClusterId == "+cluster+" && JobStatus != 3")
		return err == nil && len(ads) == 0
	})
}

// TestKubectlDrivesTheFacade runs the real client. Everything else can be
// right and kubectl can still refuse to talk to the server, and only kubectl
// can settle that.
func TestKubectlDrivesTheFacade(t *testing.T) {
	kubectl := kubectlPath(t)
	f := startFacade(t)
	dir := t.TempDir()
	manifest := filepath.Join(dir, "pod.json")
	if err := os.WriteFile(manifest, []byte(integrationPod), 0o644); err != nil {
		t.Fatal(err)
	}

	run := func(args ...string) string {
		t.Helper()
		full := append(kubectlFlags(t, f.http.URL), args...)
		cmd := exec.Command(kubectl, full...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("kubectl %s failed: %v\n%s", strings.Join(args, " "), err, out)
		}
		return string(out)
	}

	if out := run("apply", "-f", manifest); !strings.Contains(out, "live") {
		t.Errorf("kubectl apply said: %s", out)
	}
	waitFor(t, "kubectl get pods to show the pod", 15*time.Second, func() bool {
		return strings.Contains(run("get", "pods", "-n", "research"), "live")
	})
	t.Logf("kubectl get pods -n research:\n%s", run("get", "pods", "-n", "research"))
	t.Logf("kubectl get pods -o wide:\n%s", run("get", "pods", "-n", "research", "-o", "wide"))

	// -o yaml round-trips the spec out of the job ad, which is the whole
	// claim that the job queue can serve as the object store.
	yaml := run("get", "pod", "live", "-n", "research", "-o", "yaml")
	for _, want := range []string{"docker://busybox:latest", "resourceVersion:", "uid:"} {
		if !strings.Contains(yaml, want) {
			t.Errorf("kubectl get -o yaml is missing %q:\n%s", want, yaml)
		}
	}

	if out := run("get", "pods", "-n", "research", "-l", "app=demo"); !strings.Contains(out, "live") {
		t.Errorf("a label selector lost the pod: %s", out)
	}
	if out := run("describe", "pod", "live", "-n", "research"); !strings.Contains(out, "research") {
		t.Logf("kubectl describe (informational): %s", out)
	}
	if out := run("delete", "pod", "live", "-n", "research"); !strings.Contains(out, "deleted") {
		t.Errorf("kubectl delete said: %s", out)
	}
}

// TestKubectlRejectionIsReadable checks the message a user gets for a Pod that
// cannot be modeled. The facade's value over "just use condor_submit" is
// mostly in this message being specific.
func TestKubectlRejectionIsReadable(t *testing.T) {
	kubectl := kubectlPath(t)
	f := startFacade(t)
	dir := t.TempDir()
	bad := strings.ReplaceAll(integrationPod, `"restartPolicy": "Never"`, `"restartPolicy": "Always", "hostNetwork": true`)
	manifest := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(manifest, []byte(bad), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(kubectl, append(kubectlFlags(t, f.http.URL), "apply", "-f", manifest)...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("kubectl accepted an unrunnable pod:\n%s", out)
	}
	t.Logf("kubectl apply on a rejected pod:\n%s", out)
	for _, want := range []string{"restartPolicy", "hostNetwork"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("the message a user sees does not mention %q:\n%s", want, out)
		}
	}
}

// TestJobRunsAndLogsComeBack is the end-to-end case: a Pod that actually runs
// on the personal condor's startd, and whose output the facade can serve.
func TestJobRunsAndLogsComeBack(t *testing.T) {
	f := startFacade(t)
	// The vanilla universe is what a personal condor can run without a
	// container runtime; the container universe this facade emits needs
	// Docker or Apptainer on the execute node, which a test box may not
	// have. Submit directly to prove the log path end to end.
	spool := t.TempDir()
	//
	// transfer_executable = false matters on macOS: a copy of a
	// system binary loses its code signature and is SIGKILLed the moment
	// it execs, so a job that transfers /bin/echo dies with signal 9 and
	// produces no output at all.
	submit := fmt.Sprintf(`universe = vanilla
executable = /bin/echo
arguments = "hello from htcondor"
transfer_executable = false
initialdir = %s
output = stdout
error = stderr
log = job.log
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
stream_output = True
request_cpus = 1
request_memory = 64
request_disk = 1024
+%s = "logger"
+%s = "default"
+%s = "%s"
queue
`, spool, translate.AttrPodName, translate.AttrPodNamespace, backend.AttrIWD, spool)

	if err := f.harness.WaitForStartd(60 * time.Second); err != nil {
		t.Skipf("no startd advertised a slot, so nothing can run here: %v", err)
	}
	cluster, err := f.schedd.Submit(context.Background(), submit)
	if err != nil {
		t.Fatalf("submitting: %v", err)
	}
	defer func() {
		if t.Failed() {
			ads, _ := f.schedd.Query(context.Background(), fmt.Sprintf("ClusterId == %d", cluster))
			for _, ad := range ads {
				t.Logf("job %s: JobStatus=%d hold=%q (%d) host=%q",
					ad.Key(), ad.Ad.JobStatus, ad.Ad.HoldReason, ad.Ad.HoldReasonCode, ad.Ad.RemoteHost)
			}
			f.harness.PrintStartdLog()
			f.harness.PrintShadowLog()
		}
	}()
	waitFor(t, "the job to finish and its output to arrive", 120*time.Second, func() bool {
		data, err := os.ReadFile(filepath.Join(spool, "stdout"))
		return err == nil && strings.Contains(string(data), "hello from htcondor")
	})

	waitFor(t, "the facade to see the pod", 20*time.Second, func() bool {
		var list corev1.PodList
		getJSON(t, f.http.URL+"/api/v1/namespaces/default/pods", &list)
		return len(list.Items) == 1
	})
	resp, body := request(t, http.MethodGet, f.http.URL+"/api/v1/namespaces/default/pods/logger/log", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("logs: %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "hello from htcondor") {
		t.Fatalf("the facade served %q", body)
	}
	t.Logf("cluster %d produced: %s", cluster, bytes.TrimSpace(body))
}

// kubectlFlags points kubectl at the facade and at nothing else.
//
// An empty kubeconfig is not optional: a developer's own ~/.kube/config
// supplies a cluster's CA and credentials, and kubectl merges those with
// --server, so it ends up speaking TLS to a plain HTTP port and reporting
// "connection refused". Anyone pointing kubectl at this facade by hand hits
// the same thing.
func kubectlFlags(t *testing.T, url string) []string {
	t.Helper()
	empty := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(empty, []byte("apiVersion: v1\nkind: Config\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return []string{"--kubeconfig=" + empty, "--server=" + url}
}

func kubectlPath(t *testing.T) string {
	t.Helper()
	if p, err := exec.LookPath("kubectl"); err == nil {
		return p
	}
	home, _ := os.UserHomeDir()
	p := filepath.Join(home, ".local", "bin", "kubectl")
	if _, err := os.Stat(p); err == nil {
		return p
	}
	t.Skip("kubectl is not installed; skipping the client-compatibility test")
	return ""
}

// condorQ shells out to the real condor_q, so the assertion is about what
// HTCondor reports rather than what the Go client parsed.
func condorQ(t *testing.T, h *htcondor.CondorTestHarness, cluster string, attrs ...string) map[string]string {
	t.Helper()
	// No -name: CONDOR_CONFIG points condor_q at the harness's own schedd,
	// and the harness's schedd name is not a resolvable hostname.
	args := append([]string{"-af"}, attrs...)
	args = append(args, cluster)
	cmd := exec.Command("condor_q", args...)
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+h.GetConfigFile())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("condor_q: %v\n%s", err, out)
	}
	fields := strings.Fields(strings.TrimSpace(string(out)))
	got := map[string]string{}
	for i, a := range attrs {
		if i < len(fields) {
			got[a] = fields[i]
		}
	}
	return got
}

func request(t *testing.T, method, url, body string) (*http.Response, []byte) {
	t.Helper()
	var rdr *strings.Reader = strings.NewReader(body)
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	return resp, buf.Bytes()
}

func post(t *testing.T, url, body string) (*http.Response, []byte) {
	t.Helper()
	return request(t, http.MethodPost, url, body)
}

func getJSON(t *testing.T, url string, out any) {
	t.Helper()
	resp, body := request(t, http.MethodGet, url, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %d %s", url, resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, out); err != nil {
		t.Fatalf("decoding %s: %v", url, err)
	}
}

var _ = metav1.ObjectMeta{}
