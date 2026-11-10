// Package podctl_test exercises the Pod-interception approach against a real
// kube-apiserver and etcd.
//
// # The load-bearing assumption, and the honest state of its evidence
//
// This whole approach rests on one claim: that if we stamp schedulerName onto
// a Job's pod template and then write the resulting Pods' statuses, the
// built-in Job controller does everything else correctly and by itself --
// creates the Pods, honors parallelism and completions and backoffLimit, and
// derives job.status from the Pod statuses we wrote.
//
// **That claim is tested here by simulation, not by the real controller.**
//
// envtest runs kube-apiserver and etcd and nothing else. Its ControlPlane type
// has exactly two fields, APIServer and Etcd, and the asset bundle
// setup-envtest downloads contains kube-apiserver, etcd and kubectl --- no
// kube-controller-manager binary at all. There is no configuration of envtest
// in this repo or any other that makes the Job controller appear. So
// [jobControllerSim] below plays its part: it creates Pods from the stamped
// template and derives a JobStatus from their phases, following the rules the
// real controller follows.
//
// A simulation of the component whose behavior is the premise is weak
// evidence, and it is weak in a specific direction: it proves that the Pod
// controller produces Pod statuses from which a correct Job controller *could*
// derive the right answer, and it proves the webhook's mutation survives a
// real admission round trip. It does not prove that the real controller reads
// them the way this simulation does. Two facts about the real controller were
// read out of kubernetes v1.37.0 job_controller.go and encoded here rather
// than guessed --- isPodFailed counting a deleted non-Succeeded Pod as a
// failure, and onlyReplaceFailedPods short-circuiting that when
// podReplacementPolicy is Failed --- but reading source is not running it.
//
// That gap is now closed elsewhere: test/k3s runs the same flow against a real
// kube-controller-manager inside a k3s container, and is what the premise
// actually rests on. This suite is still worth having -- it is far faster, it
// drives the webhook over real TLS, and it can construct states the real
// controller would not produce -- but when the two disagree, test/k3s wins.
//
// Running it there immediately found something this file structurally could
// not: a real API server's ServiceAccount admission plugin injects a projected
// kube-api-access volume into every Pod, and translate.Admit rejects volumes,
// so this integration refused every Pod on any real cluster. envtest does not
// enable that plugin. A simulation can only be wrong in the ways you thought
// to simulate.
package podctl_test

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
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/podctl"
	htcwebhook "github.com/bbockelm/htcondor-k8s/virtualnode/internal/webhook"
)

var (
	testEnv   *envtest.Environment
	k8sClient client.Client
	fakeQueue *podctl.FakeQueue
	scheme    = runtime.NewScheme()
)

const (
	// eventually is the ceiling for a convergence that normally takes one
	// watch round trip. It is generous because a loaded machine starting
	// etcd is slow, and a flaky timeout reads as a product bug.
	eventually = 60 * time.Second
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
		log.SetLogger(zap.New(zap.UseDevMode(true)))
	} else {
		log.SetLogger(logr.Discard())
	}
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return 0, err
	}

	root, err := repoRoot()
	if err != nil {
		return 0, err
	}

	testEnv = &envtest.Environment{
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
	if err := createVirtualNode(context.Background()); err != nil {
		return 0, err
	}

	stop, err := startManager(cfg, testEnv.WebhookInstallOptions)
	if err != nil {
		return 0, err
	}
	defer stop()

	return m.Run(), nil
}

// createVirtualNode stands in for the node controller, which is the other half
// of this module and is not what these tests are about. What matters here is
// that the Node exists and carries the NoSchedule taint, because binding to a
// node that does not exist and binding to a tainted one are both things the
// API server has an opinion about.
func createVirtualNode(ctx context.Context) error {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   api.NodeName,
			Labels: map[string]string{"type": "virtual-kubelet"},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{{
				Key: api.TaintKey, Value: api.TaintValue, Effect: corev1.TaintEffectNoSchedule,
			}},
		},
	}
	if err := k8sClient.Create(ctx, node); err != nil {
		return err
	}
	node.Status = corev1.NodeStatus{
		Capacity: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1k"),
			corev1.ResourceMemory: resource.MustParse("1Ti"),
			corev1.ResourcePods:   resource.MustParse("10k"),
		},
		Conditions: []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now(),
		}},
	}
	node.Status.Allocatable = node.Status.Capacity
	return k8sClient.Status().Update(ctx, node)
}

func startManager(cfg *rest.Config, wo envtest.WebhookInstallOptions) (func(), error) {
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		WebhookServer: ctrlwebhook.NewServer(ctrlwebhook.Options{
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
		Handler: &htcwebhook.JobStamper{
			Caps:    caps,
			Decoder: admission.NewDecoder(scheme),
			Log:     logr.Discard(),
		},
	})

	fakeQueue = podctl.NewFakeQueue()
	if err := (&podctl.PodReconciler{
		Client:        mgr.GetClient(),
		Scheme:        mgr.GetScheme(),
		Recorder:      mgr.GetEventRecorderFor("htcondor-podctl"),
		Queue:         fakeQueue,
		Caps:          caps,
		ScheddName:    "ap.test",
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

// waitForWebhook blocks until the TLS listener answers. failurePolicy here is
// Fail, so losing this race is a loud error on Job creation rather than the
// silent non-claim it would be under Ignore --- but waiting is still cheaper
// than reading that error.
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
// that, the newest directory setup-envtest has already populated is used.
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
	// .../virtualnode/test/podctl -> .../virtualnode
	return filepath.Abs(filepath.Join(wd, "..", ".."))
}

// --- helpers ------------------------------------------------------------

// translatableJob is the sample Job, including the opt-in label the webhook's
// objectSelector matches. Without the label the webhook never sees it, which
// is a mistake that looks exactly like "the webhook decided not to stamp".
func translatableJob(name string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default",
			Labels: map[string]string{"htcondor.org/scheduler": "htcondor"},
		},
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

func getPod(t *testing.T, name string) *corev1.Pod {
	t.Helper()
	var pod corev1.Pod
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Name: name, Namespace: "default"}, &pod); err != nil {
		t.Fatalf("getting Pod %s: %v", name, err)
	}
	return &pod
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

// mutatingWebhookInstalled guards against the whole suite passing vacuously
// because envtest quietly failed to install the configuration.
func mutatingWebhookInstalled(t *testing.T) {
	t.Helper()
	var list admissionv1.MutatingWebhookConfigurationList
	if err := k8sClient.List(context.Background(), &list); err != nil {
		t.Fatalf("listing webhook configurations: %v", err)
	}
	for _, c := range list.Items {
		for _, w := range c.Webhooks {
			if w.Name == "jobs.virtualnode.htcondor.org" {
				return
			}
		}
	}
	t.Fatal("the jobs.virtualnode.htcondor.org mutating webhook is not installed")
}
