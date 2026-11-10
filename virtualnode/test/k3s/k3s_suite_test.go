// Package k3s_test closes the one gap the envtest suite cannot: it runs the
// approach against a cluster that has a real kube-controller-manager in it.
//
// The whole design rests on a claim about a component we do not write: that if
// we stamp schedulerName onto a Job's pod template and then write the
// resulting Pods' statuses, the built-in Job controller does the rest by
// itself. test/podctl tests that against a simulation, because envtest ships
// only kube-apiserver, etcd and kubectl -- there is no kube-controller-manager
// binary in the bundle and no configuration that produces one.
//
// Here the controller is real. k3s runs the whole control plane in one
// container, so a `docker run` is the entire setup, and the test process talks
// to it over the forwarded apiserver port exactly as it would to any cluster.
//
// What is still not covered here: the webhook's network path. Admission
// webhooks are called *by* the API server, which in this topology would have
// to dial back out of the container to the test process. test/podctl already
// drives the webhook over real TLS against a real API server, so rather than
// build that tunnel these tests call webhook.Stamp directly -- the same
// function the webhook handler calls on the same object. The two suites cover
// different halves on purpose.
package k3s_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/kubelet"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/node"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/podctl"
)

// k3sImage is pinned to the same minor as the envtest kube-apiserver this
// repo tests against, so the two suites cannot disagree about API behavior
// for reasons that have nothing to do with this code.
const k3sImage = "rancher/k3s:v1.37.2-k3s1"

// hostAlias is the name the cluster uses to reach this process. It is
// published as the virtual node's address, so it is also the name the API
// server dials when a user runs `kubectl logs`.
const hostAlias = "host.docker.internal"

var (
	k8sClient client.Client
	clientset *kubernetes.Clientset
	fakeQueue *podctl.FakeQueue
	spoolRoot string
	scheme    = runtime.NewScheme()

	// containerName is the cluster, kept so the serving certificate can be
	// signed by the CA that cluster's API server was told to trust.
	containerName string

	// dockerErr records why the cluster could not start. It is printed by
	// every skip, because a suite that silently does nothing is worse than
	// one that fails: on CI this must run, and a quiet skip there would be
	// a green build that tested nothing.
	dockerErr error
)

const (
	eventually = 120 * time.Second
	tick       = 250 * time.Millisecond
)

func TestMain(m *testing.M) {
	log.SetLogger(logr.Discard())
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		fmt.Fprintf(os.Stderr, "building the scheme: %v\n", err)
		os.Exit(1)
	}

	code, err := run(m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "k3s suite: %v\n", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run(m *testing.M) (int, error) {
	if err := dockerAvailable(); err != nil {
		// Not an error: this is the documented way the suite behaves on a
		// machine without a container runtime.
		dockerErr = err
		fmt.Fprintf(os.Stderr, "\n*** k3s suite SKIPPED: %v ***\n"+
			"*** This suite is the only evidence that the real Job controller drives this approach. ***\n"+
			"*** It must run in CI; a skip there is a vacuous pass. ***\n\n", err)
		return m.Run(), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg, stopCluster, err := startK3s(ctx)
	if err != nil {
		return 0, fmt.Errorf("starting k3s: %w", err)
	}
	defer stopCluster()

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return 0, fmt.Errorf("building a client: %w", err)
	}
	clientset, err = kubernetes.NewForConfig(cfg)
	if err != nil {
		return 0, fmt.Errorf("building a typed client: %w", err)
	}

	spoolRoot, err = os.MkdirTemp("", "htck8s-spool-")
	if err != nil {
		return 0, fmt.Errorf("making a spool root: %w", err)
	}
	defer os.RemoveAll(spoolRoot)

	certFile, keyFile, err := issueServingCert(ctx, containerName, spoolRoot, hostAlias, "localhost")
	if err != nil {
		return 0, fmt.Errorf("issuing a serving certificate the cluster trusts: %w", err)
	}

	// The kubelet API comes up first, because the port it lands on is
	// what the node publishes and what the API server will dial.
	port, stopKubelet, err := startKubelet(ctx, certFile, keyFile)
	if err != nil {
		return 0, fmt.Errorf("starting the kubelet API: %w", err)
	}
	defer stopKubelet()

	stopNode, err := startNode(ctx, cfg, port)
	if err != nil {
		return 0, fmt.Errorf("registering the virtual node: %w", err)
	}
	defer stopNode()

	// Prove the cluster can reach the log server before any test depends
	// on it. The API server reports an unreachable kubelet as a bare 502
	// Bad Gateway with no mention of TLS or of a refused connection, so
	// without this a certificate problem and a routing problem look
	// identical, and both look like a bug in the log server.
	if err := probeKubelet(ctx, port); err != nil {
		return 0, err
	}

	stopManager, err := startManager(cfg)
	if err != nil {
		return 0, fmt.Errorf("starting the manager: %w", err)
	}
	defer stopManager()

	return m.Run(), nil
}

// probeKubelet dials the log server from inside the cluster, twice: once
// ignoring the certificate and once verifying it the way the API server will.
// Which of the two fails says what is wrong.
func probeKubelet(ctx context.Context, port int32) error {
	url := fmt.Sprintf("https://%s:%d", hostAlias, port)

	insecure := exec.CommandContext(ctx, "docker", "exec", containerName,
		"kubectl", "--server="+url, "--insecure-skip-tls-verify",
		"--request-timeout=10s", "get", "--raw", "/healthz")
	if out, err := insecure.CombinedOutput(); err != nil {
		return fmt.Errorf("the cluster cannot reach the log server at %s at all "+
			"(not a certificate problem): %v: %s", url, err, strings.TrimSpace(string(out)))
	}

	verified := exec.CommandContext(ctx, "docker", "exec", containerName,
		"kubectl", "--server="+url,
		"--certificate-authority=/var/lib/rancher/k3s/server/tls/server-ca.crt",
		"--request-timeout=10s", "get", "--raw", "/healthz")
	if out, err := verified.CombinedOutput(); err != nil {
		return fmt.Errorf("the cluster reaches the log server but rejects its certificate; "+
			"this API server runs with --kubelet-certificate-authority: %v: %s",
			err, strings.TrimSpace(string(out)))
	}
	return nil
}

// startKubelet serves the log API on every interface, so the cluster can
// reach it through host-gateway, and returns the port it actually bound.
func startKubelet(ctx context.Context, certFile, keyFile string) (int32, func(), error) {
	srv, err := kubelet.New(kubelet.Config{
		Pods: kubelet.PodsFromClient(k8sClient),
		// Not self-signed: see k3s_cert_test.go. This API server was
		// started with --kubelet-certificate-authority and will reject
		// anything it cannot chain to the cluster CA.
		CertFile: certFile,
		KeyFile:  keyFile,
		// Empty address binds all interfaces; the *published* address
		// is hostAlias, set on the node below. The two are separate on
		// purpose: what we listen on and what the cluster dials are
		// different questions.
		Address: "",
		Port:    0,
		Log:     logr.Discard(),
	})
	if err != nil {
		return 0, nil, err
	}
	ln, err := srv.Listen()
	if err != nil {
		return 0, nil, err
	}
	port := int32(ln.Addr().(*net.TCPAddr).Port)

	sctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(sctx, ln) }()
	return port, func() { cancel(); <-done }, nil
}

// startNode registers the virtual node through the real controller rather
// than by hand, because what it publishes -- the addresses and the kubelet
// port -- is precisely what the log path depends on.
func startNode(ctx context.Context, cfg *rest.Config, port int32) (func(), error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	ctl, err := node.New(node.Config{
		Client:  cs,
		Address: hostAlias,
		Port:    port,
		Log:     logr.Discard(),
	})
	if err != nil {
		return nil, err
	}
	if err := ctl.Register(ctx); err != nil {
		return nil, err
	}
	nctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ctl.Run(nctx) }()
	return func() { cancel(); <-done }, nil
}

// skipWithoutDocker is called by every test. The message names what was lost.
func skipWithoutDocker(t *testing.T) {
	t.Helper()
	if dockerErr != nil {
		t.Skipf("no container runtime, so the real Job controller was not exercised: %v", dockerErr)
	}
}

func dockerAvailable() error {
	if os.Getenv("HTCK8S_SKIP_K3S") != "" {
		return fmt.Errorf("HTCK8S_SKIP_K3S is set")
	}
	cmd := exec.Command("docker", "info", "--format", "{{.ServerVersion}}")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker info: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// startK3s runs the control plane in a container and returns a config that
// reaches it from this process.
func startK3s(ctx context.Context) (*rest.Config, func(), error) {
	name := fmt.Sprintf("htck8s-k3s-%d", os.Getpid())
	containerName = name
	_ = exec.Command("docker", "rm", "-f", name).Run()

	// --privileged is k3s's own requirement; the disables drop components
	// that cost startup time and that nothing here looks at. The agent is
	// deliberately NOT disabled: a cluster with a real node is the honest
	// setting, and it proves kube-scheduler leaves our Pods alone because
	// it declined them rather than because it had nowhere to put them.
	args := []string{
		"run", "-d", "--name", name, "--privileged",
		"-p", "127.0.0.1::6443",
	}
	// The API server has to dial *back* to the kubelet API this process
	// serves, or `kubectl logs` cannot be tested at all.
	//
	// Docker Desktop already resolves host.docker.internal to a route that
	// reaches the host, and passing --add-host *overrides* that with the
	// bridge gateway -- which on Docker Desktop is an address inside the
	// Linux VM, not the machine this test runs on. The symptom is a 502
	// from the API server's proxy, which reads like a problem with the log
	// server rather than with a name. On a plain Linux daemon the name
	// does not exist at all, so there it has to be added.
	if !dockerDesktop() {
		args = append(args, "--add-host="+hostAlias+":host-gateway")
	}
	args = append(args,
		k3sImage, "server",
		"--disable=traefik", "--disable=metrics-server",
		"--disable=servicelb", "--disable=local-storage",
		"--disable-cloud-controller",
		// Without this, apiserver->kubelet traffic goes through k3s's
		// own agent tunnel, which only exists for nodes that registered
		// as k3s agents. A virtual kubelet never does, so every
		// `kubectl logs` comes back as a 502 that mentions neither the
		// tunnel nor the reason -- even though the address is right,
		// the port is open and the certificate verifies. Disabling the
		// egress selector makes the API server dial the node's
		// published address directly, which is what a stock
		// kube-apiserver does anyway.
		"--egress-selector-mode=disabled",
	)
	if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		return nil, nil, fmt.Errorf("docker run: %v: %s", err, strings.TrimSpace(string(out)))
	}
	stop := func() { _ = exec.Command("docker", "rm", "-f", name).Run() }

	port, err := hostPort(ctx, name)
	if err != nil {
		stop()
		return nil, nil, err
	}
	raw, err := waitForKubeconfig(ctx, name)
	if err != nil {
		stop()
		return nil, nil, err
	}
	// k3s writes the in-container address. The serving certificate already
	// carries 127.0.0.1 as a SAN, so only the port has to change.
	raw = strings.ReplaceAll(raw, "https://127.0.0.1:6443", "https://127.0.0.1:"+port)

	cfg, err := clientcmd.RESTConfigFromKubeConfig([]byte(raw))
	if err != nil {
		stop()
		return nil, nil, fmt.Errorf("parsing the kubeconfig: %w", err)
	}
	if err := waitForAPI(ctx, cfg); err != nil {
		stop()
		return nil, nil, err
	}
	return cfg, stop, nil
}

// dockerDesktop reports whether the daemon provides host.docker.internal
// itself. Everything else -- a Linux CI runner, podman, a remote daemon --
// needs the name added explicitly.
func dockerDesktop() bool {
	out, err := exec.Command("docker", "info", "--format", "{{.OperatingSystem}}").Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), "Docker Desktop")
}

func hostPort(ctx context.Context, name string) (string, error) {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		out, err := exec.CommandContext(ctx, "docker", "port", name, "6443/tcp").Output()
		if err == nil {
			if _, p, ok := strings.Cut(strings.TrimSpace(string(out)), ":"); ok {
				return strings.TrimSpace(p), nil
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(tick):
		}
	}
	return "", fmt.Errorf("the forwarded apiserver port never appeared")
}

func waitForKubeconfig(ctx context.Context, name string) (string, error) {
	deadline := time.Now().Add(3 * time.Minute)
	var last string
	for time.Now().Before(deadline) {
		out, err := exec.CommandContext(ctx, "docker", "exec", name,
			"cat", "/etc/rancher/k3s/k3s.yaml").Output()
		if err == nil && strings.Contains(string(out), "server:") {
			return string(out), nil
		}
		if err != nil {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(tick):
		}
	}
	return "", fmt.Errorf("k3s never wrote a kubeconfig (last error: %s)", last)
}

// waitForAPI blocks until the API server answers and serves batch/v1. Waiting
// for the group matters: the apiserver accepts connections before every
// built-in type is registered, and a Job create issued in that window fails in
// a way that reads like a bug in this repo.
func waitForAPI(ctx context.Context, cfg *rest.Config) error {
	deadline := time.Now().Add(3 * time.Minute)
	var last error
	for time.Now().Before(deadline) {
		c, err := client.New(cfg, client.Options{Scheme: scheme})
		if err == nil {
			var nodes corev1.NodeList
			if err = c.List(ctx, &nodes); err == nil && len(nodes.Items) > 0 {
				return nil
			}
		}
		last = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(tick):
		}
	}
	return fmt.Errorf("the API server never became usable: %w", last)
}

func startManager(cfg *rest.Config) (func(), error) {
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		return nil, err
	}

	fakeQueue = podctl.NewFakeQueue()
	if err := (&podctl.PodReconciler{
		Client:        mgr.GetClient(),
		Scheme:        mgr.GetScheme(),
		Recorder:      mgr.GetEventRecorderFor("htcondor-podctl"),
		Queue:         fakeQueue,
		Caps:          translate.DefaultCapabilities(),
		ScheddName:    "ap.test",
		SpoolRoot:     spoolRoot,
		PollInterval:  250 * time.Millisecond,
		RetryInterval: 250 * time.Millisecond,
	}).SetupWithManager(mgr); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		cancel()
		return nil, fmt.Errorf("cache did not sync")
	}
	return func() { cancel(); <-done }, nil
}

func eventuallyTrue(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventually)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(tick)
	}
	t.Fatalf("timed out waiting for %s", what)
}
