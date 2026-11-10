// Package envtest_test exercises the operator against a real kube-apiserver
// and etcd.
//
// What this proves that the unit tests cannot: that the CRD's structural
// schema really does reject the fields it leaves out, that the mutating
// webhook is reachable over TLS and patches a Job on its way into etcd, and
// that the controllers converge driven by real watches rather than by a test
// calling Reconcile.
//
// What it still cannot prove: envtest runs the API server and etcd and
// nothing else. There is no kube-controller-manager, so the built-in Job
// controller is not here to contest .status, and the garbage collector is not
// here to delete an HTCJob when its owning Job goes away. Both of those are
// real behaviors of a real cluster that this suite is blind to; the k3d script
// in hack/ is where they are checked.
package envtest_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/bbockelm/htcondor-k8s/operator/api/v1alpha1"
	"github.com/bbockelm/htcondor-k8s/operator/internal/condor"
	"github.com/bbockelm/htcondor-k8s/operator/internal/controller"
	htcwebhook "github.com/bbockelm/htcondor-k8s/operator/internal/webhook"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

var (
	testEnv   *envtest.Environment
	k8sClient client.Client
	fakeQueue *condor.FakeQueue
	scheme    = runtime.NewScheme()
)

const (
	// eventually is the ceiling for a convergence that normally takes one
	// watch round trip. It is generous because a loaded CI machine starting
	// etcd is slow, and a flaky timeout reads as a product bug.
	eventually = 30 * time.Second
	tick       = 100 * time.Millisecond
)

func TestMain(m *testing.M) {
	assets, ok := findAssets()
	if !ok {
		fmt.Fprintln(os.Stderr,
			"envtest: no kube-apiserver/etcd binaries found; skipping.\n"+
				"  get them with: go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use -p path")
		// Exit 0 with the suite skipped: a machine without the assets
		// must not fail the build, but every test below announces the
		// skip so an empty run is never mistaken for a passing one.
		os.Setenv("HTCONDOR_K8S_ENVTEST_SKIP", "1")
		os.Exit(m.Run())
	}
	os.Setenv("KUBEBUILDER_ASSETS", assets)

	code, err := run(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "envtest setup:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run(m *testing.M) (int, error) {
	if os.Getenv("HTCONDOR_K8S_ENVTEST_VERBOSE") != "" {
		// Controller logs are off by default because a passing run does
		// not need them; a failing one always does.
		log.SetLogger(zap.New(zap.UseDevMode(true)))
	} else {
		log.SetLogger(logr.Discard())
	}

	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return 0, err
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return 0, err
	}

	root, err := repoRoot()
	if err != nil {
		return 0, err
	}

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(root, "config", "crd")},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			Paths: []string{filepath.Join(root, "config", "webhook")},
		},
	}
	cfg, err := testEnv.Start()
	if err != nil {
		return 0, fmt.Errorf("starting the control plane: %w", err)
	}
	defer func() { _ = testEnv.Stop() }()

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return 0, err
	}

	stop, err := startManager(cfg, testEnv.WebhookInstallOptions)
	if err != nil {
		return 0, err
	}
	defer stop()

	return m.Run(), nil
}

func startManager(cfg *rest.Config, wo envtest.WebhookInstallOptions) (func(), error) {
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		WebhookServer: webhook.NewServer(webhook.Options{
			Host:    wo.LocalServingHost,
			Port:    wo.LocalServingPort,
			CertDir: wo.LocalServingCertDir,
		}),
	})
	if err != nil {
		return nil, err
	}

	caps := translate.DefaultCapabilities()
	mgr.GetWebhookServer().Register(htcwebhook.Path, &admission.Webhook{
		Handler: &htcwebhook.JobClaimer{
			Caps:    caps,
			Decoder: admission.NewDecoder(scheme),
			Log:     logr.Discard(),
		},
	})

	if err := (&controller.JobReconciler{
		Client: mgr.GetClient(), Scheme: mgr.GetScheme(),
		Recorder:     mgr.GetEventRecorderFor("test"),
		Caps:         caps,
		MirrorStatus: true,
	}).SetupWithManager(mgr); err != nil {
		return nil, err
	}

	fakeQueue = condor.NewFakeQueue()
	if err := (&controller.HTCJobReconciler{
		Client: mgr.GetClient(), Scheme: mgr.GetScheme(),
		Recorder:      mgr.GetEventRecorderFor("test"),
		Queue:         fakeQueue,
		Caps:          caps,
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
	if err := waitForWebhook(ctx, wo); err != nil {
		cancel()
		return nil, err
	}
	return func() { cancel(); <-done }, nil
}

// waitForWebhook blocks until the TLS listener answers. Without it the first
// Job races the webhook server's startup -- and because failurePolicy is
// Ignore, losing that race looks exactly like "the webhook decided not to
// claim", which is the one failure this suite must never confuse.
func waitForWebhook(ctx context.Context, wo envtest.WebhookInstallOptions) error {
	addr := fmt.Sprintf("%s:%d", wo.LocalServingHost, wo.LocalServingPort)
	deadline := time.Now().Add(eventually)
	for time.Now().Before(deadline) {
		conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(tick):
		}
	}
	return fmt.Errorf("webhook server never listened on %s", addr)
}

func skipWithoutAssets(t *testing.T) {
	t.Helper()
	if os.Getenv("HTCONDOR_K8S_ENVTEST_SKIP") != "" {
		t.Skip("envtest binaries are not installed")
	}
}

// findAssets locates kube-apiserver and etcd. KUBEBUILDER_ASSETS wins; failing
// that, the newest directory setup-envtest has already populated is used, so
// a developer who ran setup-envtest once never has to set anything.
func findAssets() (string, bool) {
	if p := os.Getenv("KUBEBUILDER_ASSETS"); p != "" {
		if _, err := os.Stat(filepath.Join(p, "kube-apiserver")); err == nil {
			return p, true
		}
		return "", false
	}
	out, err := exec.Command("go", "run",
		"sigs.k8s.io/controller-runtime/tools/setup-envtest@latest", "use", "-i", "-p", "path").Output()
	if err == nil {
		p := strings.TrimSpace(string(out))
		if _, err := os.Stat(filepath.Join(p, "kube-apiserver")); err == nil {
			return p, true
		}
	}
	return "", false
}

func repoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	// .../operator/test/envtest -> .../operator
	return filepath.Abs(filepath.Join(wd, "..", ".."))
}

// --- helpers shared by the tests ---------------------------------------

func translatableJob(name string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:    "work",
						Image:   "docker://busybox:latest",
						Command: []string{"/bin/sh", "-c", "echo hi"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1"),
								corev1.ResourceMemory: resource.MustParse("512Mi"),
							},
						},
					}},
				},
			},
		},
	}
}

func getJob(t *testing.T, name string) *batchv1.Job {
	t.Helper()
	var job batchv1.Job
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Name: name, Namespace: "default"}, &job); err != nil {
		t.Fatalf("getting Job %s: %v", name, err)
	}
	return &job
}

// eventuallyTrue polls until cond returns true. There is no sleep-then-assert
// anywhere in this suite: every wait is a condition with a deadline.
func eventuallyTrue(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventually)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(tick)
	}
	t.Fatalf("timed out after %s waiting for %s", eventually, what)
}

func eventuallyHTCJob(t *testing.T, name string, cond func(*v1alpha1.HTCJob) bool) *v1alpha1.HTCJob {
	t.Helper()
	var hj v1alpha1.HTCJob
	eventuallyTrue(t, "HTCJob "+name, func() bool {
		err := k8sClient.Get(context.Background(),
			types.NamespacedName{Name: name, Namespace: "default"}, &hj)
		if apierrors.IsNotFound(err) {
			return false
		}
		if err != nil {
			t.Fatalf("getting HTCJob %s: %v", name, err)
		}
		return cond(&hj)
	})
	return &hj
}

// mutatingWebhookInstalled is a guard against the whole suite passing
// vacuously because envtest quietly failed to install the configuration.
func mutatingWebhookInstalled(t *testing.T) {
	t.Helper()
	var list admissionv1.MutatingWebhookConfigurationList
	if err := k8sClient.List(context.Background(), &list); err != nil {
		t.Fatalf("listing webhook configurations: %v", err)
	}
	for _, c := range list.Items {
		for _, w := range c.Webhooks {
			if w.Name == "jobs.htcondor.org" {
				return
			}
		}
	}
	t.Fatal("the jobs.htcondor.org mutating webhook is not installed")
}
