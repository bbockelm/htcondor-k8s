// Package kubelet_test drives the kubelet API the way the Kubernetes API
// server does: over a real TLS connection to a real net/http server, against
// real files on disk.
//
// Using httptest.NewRequest against the handler would have been easier and
// would have missed the half of this that matters. `kubectl logs` reaches us
// through a TLS listener whose certificate nobody verifies, with query
// parameters the API server rewrites, and `kubectl logs -f` depends on the
// response being flushed rather than buffered until the handler returns --
// none of which a direct handler call exercises.
package kubelet_test

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/kubelet"
)

// --- fixtures ----------------------------------------------------------

// pods is a stand-in for the controller-runtime cache the real server reads
// from. It is mutable because `kubectl logs -f` has to see the Pod reach a
// terminal phase while the request is in flight.
type pods struct {
	mu  sync.Mutex
	all map[string]*corev1.Pod
}

func newPods() *pods { return &pods{all: map[string]*corev1.Pod{}} }

func (p *pods) GetPod(_ context.Context, ns, name string) (*corev1.Pod, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pod, ok := p.all[ns+"/"+name]
	if !ok {
		return nil, apierrors.NewNotFound(
			schema.GroupResource{Resource: "pods"}, name)
	}
	return pod.DeepCopy(), nil
}

func (p *pods) put(pod *corev1.Pod) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.all[pod.Namespace+"/"+pod.Name] = pod
}

func (p *pods) setPhase(ns, name string, phase corev1.PodPhase) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.all[ns+"/"+name].Status.Phase = phase
}

func podWithSpool(name, spool string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Annotations: map[string]string{
				api.AnnotationSpoolDir: spool,
				api.AnnotationJobID:    "42.0",
			},
		},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "work"}}},
		Status: corev1.PodStatus{Phase: phase},
	}
}

// peeker stands in for the CEDAR tail of a running job's output.
type peeker struct {
	data  []byte
	err   error
	calls int
}

func (p *peeker) Peek(_ context.Context, cluster, proc int, _ bool, _ int64) ([]byte, error) {
	p.calls++
	if p.err != nil {
		return nil, p.err
	}
	return append([]byte(fmt.Sprintf("job %d.%d:\n", cluster, proc)), p.data...), nil
}

// serve starts the real thing on a real port and returns its base URL.
func serve(t *testing.T, cfg kubelet.Config) (string, *http.Client) {
	t.Helper()
	cfg.Address = "127.0.0.1"
	cfg.Port = 0
	if cfg.FollowInterval == 0 {
		cfg.FollowInterval = 20 * time.Millisecond
	}

	s, err := kubelet.New(cfg)
	require.NoError(t, err)

	ln, err := s.Listen()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("the kubelet server did not stop")
		}
	})

	// InsecureSkipVerify is not laziness here, it is the behavior under
	// test: an API server started without --kubelet-certificate-authority
	// does exactly this, which is why the self-signed certificate works
	// and why that is worth knowing.
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}}
	return "https://" + ln.Addr().String(), client
}

func logURL(base, pod, query string) string {
	u := base + "/containerLogs/default/" + pod + "/work"
	if query != "" {
		u += "?" + query
	}
	return u
}

func get(t *testing.T, client *http.Client, url string) (int, string, http.Header) {
	t.Helper()
	resp, err := client.Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body), resp.Header
}

func spoolWith(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
	}
	return dir
}

// --- the tests ---------------------------------------------------------

func TestContainerLogsServesTheJobsSpoolFile(t *testing.T) {
	spool := spoolWith(t, map[string]string{"stdout": "hello from the pool\n"})
	p := newPods()
	p.put(podWithSpool("job-0", spool, corev1.PodRunning))

	base, client := serve(t, kubelet.Config{Pods: p})

	code, body, _ := get(t, client, logURL(base, "job-0", ""))
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "hello from the pool\n", body)
}

func TestTheSpoolDirectoryComesFromThePodNotFromAFormula(t *testing.T) {
	// The whole point of reading api.AnnotationSpoolDir off the Pod is
	// that a Pod submitted under an older naming scheme keeps working.
	// Two Pods, two unrelated directories, no shared prefix.
	a := spoolWith(t, map[string]string{"stdout": "from A\n"})
	b := spoolWith(t, map[string]string{"stdout": "from B\n"})
	p := newPods()
	p.put(podWithSpool("pod-a", a, corev1.PodSucceeded))
	p.put(podWithSpool("pod-b", b, corev1.PodSucceeded))

	base, client := serve(t, kubelet.Config{Pods: p})

	_, bodyA, _ := get(t, client, logURL(base, "pod-a", ""))
	_, bodyB, _ := get(t, client, logURL(base, "pod-b", ""))
	require.Equal(t, "from A\n", bodyA)
	require.Equal(t, "from B\n", bodyB)
}

func TestTailLinesAndLimitBytes(t *testing.T) {
	spool := spoolWith(t, map[string]string{"stdout": "one\ntwo\nthree\nfour\n"})
	p := newPods()
	p.put(podWithSpool("job-1", spool, corev1.PodSucceeded))

	base, client := serve(t, kubelet.Config{Pods: p})

	_, body, _ := get(t, client, logURL(base, "job-1", "tailLines=2"))
	require.Equal(t, "three\nfour\n", body)

	_, body, _ = get(t, client, logURL(base, "job-1", "limitBytes=3"))
	require.Equal(t, "one", body)

	_, body, _ = get(t, client, logURL(base, "job-1", "tailLines=0"))
	require.Empty(t, body, "--tail=0 asks for no lines")
}

func TestFollowStreamsTheFileAsItGrowsAndEndsWithTheJob(t *testing.T) {
	spool := spoolWith(t, map[string]string{"stdout": "first\n"})
	p := newPods()
	p.put(podWithSpool("job-2", spool, corev1.PodRunning))

	base, client := serve(t, kubelet.Config{Pods: p})

	resp, err := client.Get(logURL(base, "job-2", "follow=true"))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	reader := bufio.NewReader(resp.Body)

	// The first line must arrive before the handler returns, which is
	// what tells us the response is being flushed rather than buffered.
	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "first\n", line)

	f, err := os.OpenFile(filepath.Join(spool, "stdout"), os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = f.WriteString("second\n")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	line, err = reader.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "second\n", line)

	// The job ends. The stream has to end with it, or `kubectl logs -f`
	// hangs forever on a finished job.
	p.setPhase("default", "job-2", corev1.PodSucceeded)
	rest, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Empty(t, string(rest))
}

func TestFollowDeliversTheLinesWrittenAsTheJobExits(t *testing.T) {
	// The race this covers: the job writes its last line and exits
	// between two polls. Returning as soon as the phase is terminal would
	// drop that line, which is the one the user is looking for.
	spool := spoolWith(t, map[string]string{"stdout": "working\n"})
	p := newPods()
	p.put(podWithSpool("job-3", spool, corev1.PodRunning))

	base, client := serve(t, kubelet.Config{Pods: p, FollowInterval: 200 * time.Millisecond})

	resp, err := client.Get(logURL(base, "job-3", "follow=true"))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	f, err := os.OpenFile(filepath.Join(spool, "stdout"), os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = f.WriteString("done\n")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	p.setPhase("default", "job-3", corev1.PodSucceeded)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "working\ndone\n", string(body))
}

func TestPreviousIsRefusedRatherThanAnsweredWithTheWrongRun(t *testing.T) {
	spool := spoolWith(t, map[string]string{"stdout": "this run\n"})
	p := newPods()
	p.put(podWithSpool("job-4", spool, corev1.PodRunning))

	base, client := serve(t, kubelet.Config{Pods: p})

	code, body, _ := get(t, client, logURL(base, "job-4", "previous=true"))
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "truncates its output file")
	require.NotContains(t, body, "this run")
}

func TestParametersWeCannotHonorAreNamedInTheResponse(t *testing.T) {
	spool := spoolWith(t, map[string]string{"stdout": "x\n"})
	p := newPods()
	p.put(podWithSpool("job-5", spool, corev1.PodSucceeded))

	base, client := serve(t, kubelet.Config{Pods: p})

	code, body, header := get(t, client,
		logURL(base, "job-5", "timestamps=true&sinceSeconds=60"))
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "x\n", body, "the log is still served")
	ignored := header.Get("X-HTCondor-Ignored-Parameters")
	require.Contains(t, ignored, "timestamps")
	require.Contains(t, ignored, "sinceSeconds")
}

func TestAPodWithNoJobYetSaysItIsWaitingToStart(t *testing.T) {
	pod := podWithSpool("job-6", "", corev1.PodPending)
	delete(pod.Annotations, api.AnnotationSpoolDir)
	p := newPods()
	p.put(pod)

	base, client := serve(t, kubelet.Config{Pods: p})

	code, body, _ := get(t, client, logURL(base, "job-6", ""))
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "waiting to start")
}

func TestAnUnknownPodOrContainerIsRefused(t *testing.T) {
	spool := spoolWith(t, map[string]string{"stdout": "x\n"})
	p := newPods()
	p.put(podWithSpool("job-7", spool, corev1.PodSucceeded))

	base, client := serve(t, kubelet.Config{Pods: p})

	code, _, _ := get(t, client, logURL(base, "nonesuch", ""))
	require.Equal(t, http.StatusNotFound, code)

	code, body, _ := get(t, client, base+"/containerLogs/default/job-7/sidecar")
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "work", "the message names the container that does exist")
}

func TestAJobOnARemoteAccessPointFallsBackToPeek(t *testing.T) {
	// No spool file on this filesystem: the access point is somewhere
	// else. This is the case the facade's fallback exists for and the one
	// a PoC on one machine never exercises by accident.
	p := newPods()
	p.put(podWithSpool("job-8", filepath.Join(t.TempDir(), "not-here"), corev1.PodRunning))
	peek := &peeker{data: []byte("tail from the starter\n")}

	base, client := serve(t, kubelet.Config{Pods: p, Peek: peek})

	code, body, _ := get(t, client, logURL(base, "job-8", ""))
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, 1, peek.calls)
	require.Contains(t, body, "job 42.0:")
	require.Contains(t, body, "tail from the starter")
}

func TestAJobThatHasNotStartedIsNotPeeked(t *testing.T) {
	// Peeking an idle job asks the schedd for a starter that does not
	// exist, which costs a round trip and returns an error that looks
	// like a malfunction.
	p := newPods()
	p.put(podWithSpool("job-9", filepath.Join(t.TempDir(), "not-here"), corev1.PodPending))
	peek := &peeker{data: []byte("should not be read\n")}

	base, client := serve(t, kubelet.Config{Pods: p, Peek: peek})

	code, body, _ := get(t, client, logURL(base, "job-9", ""))
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, 0, peek.calls)
	require.Empty(t, body)
}

func TestStderrIsReachableAndIsServedWhenStdoutIsAbsent(t *testing.T) {
	spool := spoolWith(t, map[string]string{"stderr": "it broke\n"})
	p := newPods()
	p.put(podWithSpool("job-10", spool, corev1.PodFailed))

	base, client := serve(t, kubelet.Config{Pods: p})

	// A job that wrote only to stderr must not look like it produced
	// nothing: that is the empty log a user will believe.
	code, body, _ := get(t, client, logURL(base, "job-10", ""))
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "it broke\n", body)

	// And once stdout exists, stderr is only reachable by asking.
	require.NoError(t, os.WriteFile(filepath.Join(spool, "stdout"), []byte("fine\n"), 0o600))
	_, body, _ = get(t, client, logURL(base, "job-10", ""))
	require.Equal(t, "fine\n", body)
	_, body, _ = get(t, client, logURL(base, "job-10", "stream=stderr"))
	require.Equal(t, "it broke\n", body)
}

func TestExecAttachAndPortForwardSayWhatIsMissing(t *testing.T) {
	p := newPods()
	base, client := serve(t, kubelet.Config{Pods: p})

	for path, want := range map[string]string{
		"/exec/default/job/work":   "kubectl exec",
		"/attach/default/job/work": "kubectl attach",
		"/portForward/default/job": "kubectl port-forward",
	} {
		code, body, _ := get(t, client, base+path)
		require.Equal(t, http.StatusNotImplemented, code, "path %s", path)
		require.Contains(t, body, want)
		require.Contains(t, body, "schedd_ssh.go",
			"the message points at how it would be implemented")
	}
}

func TestHealthz(t *testing.T) {
	base, client := serve(t, kubelet.Config{Pods: newPods()})
	code, body, _ := get(t, client, base+"/healthz")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "ok\n", body)
}

func TestAClientCertificateIsRequiredWhenAClientCAIsConfigured(t *testing.T) {
	caPEM, caCert, caKey := makeCA(t)
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, caPEM, 0o600))

	spool := spoolWith(t, map[string]string{"stdout": "secret output\n"})
	p := newPods()
	p.put(podWithSpool("job-11", spool, corev1.PodSucceeded))

	cfg := kubelet.Config{Pods: p, ClientCAFile: caFile}
	cfg.Address = "127.0.0.1"
	s, err := kubelet.New(cfg)
	require.NoError(t, err)
	ln, err := s.Listen()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Serve(ctx, ln) }()

	base := "https://" + ln.Addr().String()

	// Without a client certificate the handshake fails. This is the only
	// authentication this server has, so a regression here is a job's
	// output readable by anyone who can reach the port.
	anonymous := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}}
	_, err = anonymous.Get(logURL(base, "job-11", ""))
	require.Error(t, err)
	require.NotContains(t, fmt.Sprint(err), "secret output")

	// With one signed by the configured CA it works.
	authorized := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			Certificates:       []tls.Certificate{clientCert(t, caCert, caKey)},
		},
	}}
	resp, err := authorized.Get(logURL(base, "job-11", ""))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "secret output\n", string(body))
}

func TestAMissingClientCAIsAStartupError(t *testing.T) {
	s, err := kubelet.New(kubelet.Config{
		Pods:         newPods(),
		Address:      "127.0.0.1",
		ClientCAFile: filepath.Join(t.TempDir(), "absent.pem"),
	})
	require.NoError(t, err)
	_, err = s.Listen()
	require.ErrorContains(t, err, "client CA")
}

func TestTheSelfSignedCertificateCoversTheAddressItIsServedOn(t *testing.T) {
	// The API server only checks this when it was started with
	// --kubelet-certificate-authority, but when it is, a certificate that
	// does not name the address in the Node's status breaks `kubectl
	// logs` with an error nobody will connect to this code.
	cert, err := kubelet.SelfSignedCert("10.42.0.9")
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)

	require.NoError(t, leaf.VerifyHostname("10.42.0.9"))
	require.NoError(t, leaf.VerifyHostname("127.0.0.1"))
	require.Contains(t, leaf.Subject.CommonName, "system:node:")
}

// --- certificate helpers -----------------------------------------------

func makeCA(t *testing.T) ([]byte, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-kubelet-client-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), cert, key
}

func clientCert(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		// What a real API server presents, and what a kubelet
		// authorizes against.
		Subject:     pkix.Name{CommonName: "kube-apiserver-kubelet-client"},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	require.NoError(t, err)
	return cert
}
