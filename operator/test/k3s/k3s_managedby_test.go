// Package k3s_test checks one claim about a component this repo does not
// write: that setting .spec.managedBy makes the built-in Job controller leave
// a Job alone.
//
// That claim is the whole argument for claiming Jobs this way instead of with
// .spec.suspend, so it is worth more than a reading of the Kubernetes source.
// envtest cannot answer it -- it ships no kube-controller-manager -- so this
// runs a real control plane in a k3s container.
//
// The suite skips itself, loudly, without a container runtime. On CI that
// would be a vacuous pass, so the workflow greps for the named tests.
package k3s_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bbockelm/htcondor-k8s/operator/api/v1alpha1"
	htcwebhook "github.com/bbockelm/htcondor-k8s/operator/internal/webhook"
)

const k3sImage = "rancher/k3s:v1.37.2-k3s1"

var (
	k8sClient client.Client
	scheme    = runtime.NewScheme()
	dockerErr error
)

func TestMain(m *testing.M) {
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		fmt.Fprintf(os.Stderr, "scheme: %v\n", err)
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
		dockerErr = err
		fmt.Fprintf(os.Stderr, "\n*** operator k3s suite SKIPPED: %v ***\n"+
			"*** This is the only check that managedBy actually stops the Job controller. ***\n"+
			"*** It must run in CI; a skip there is a vacuous pass. ***\n\n", err)
		return m.Run(), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg, stop, err := startK3s(ctx)
	if err != nil {
		return 0, err
	}
	defer stop()

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return 0, err
	}
	return m.Run(), nil
}

// TestManagedByStopsTheBuiltInJobController is the claim under test.
//
// The control case matters as much as the subject: "no Pods appeared" is also
// what a broken cluster looks like, so an identical Job without the field has
// to produce Pods in the same window, from the same API server, for the
// negative result to mean anything.
func TestManagedByStopsTheBuiltInJobController(t *testing.T) {
	skipWithoutDocker(t)
	ctx := context.Background()

	claimed := translatableJob("managed")
	htcwebhook.ClaimWith(claimed, time.Now(), htcwebhook.ClaimOptions{ManagedBy: true})
	if claimed.Spec.Suspend != nil && *claimed.Spec.Suspend {
		t.Fatal("this test is meaningless if the Job is suspended; suspend would stop Pods by itself")
	}
	if err := k8sClient.Create(ctx, claimed); err != nil {
		t.Fatal(err)
	}

	control := translatableJob("control")
	if err := k8sClient.Create(ctx, control); err != nil {
		t.Fatal(err)
	}

	// The control proves the cluster is working and bounds the wait: once
	// it has Pods, the Job controller has certainly run.
	deadline := time.Now().Add(90 * time.Second)
	var controlPods int
	for time.Now().Before(deadline) {
		controlPods = len(podsOf(t, "control").Items)
		if controlPods > 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if controlPods == 0 {
		t.Fatal("the control Job never got Pods, so this cluster's Job controller is not running " +
			"and the managedBy result below would mean nothing")
	}

	// Give the claimed Job the same chance again, well past the point the
	// control needed.
	time.Sleep(15 * time.Second)

	if n := len(podsOf(t, "managed").Items); n != 0 {
		t.Fatalf("the Job controller created %d Pods for a Job with .spec.managedBy=%s; "+
			"it is not honoring the field on this cluster", n, v1alpha1.ManagedByName)
	}

	var got batchv1.Job
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: "default", Name: "managed"}, &got); err != nil {
		t.Fatal(err)
	}
	// This is the injury the operator took and wrote up: job.status had a
	// second writer that won. Under managedBy there is no second writer.
	if got.Status.StartTime != nil || got.Status.Active != 0 || len(got.Status.Conditions) != 0 {
		t.Fatalf("the built-in Job controller wrote status onto a managedBy Job: %+v", got.Status)
	}
	if got.Spec.ManagedBy == nil || *got.Spec.ManagedBy != v1alpha1.ManagedByName {
		t.Fatalf("the API server did not store managedBy: %+v", got.Spec.ManagedBy)
	}
}

// managedBy is immutable, which is why the webhook only sets it at CREATE.
func TestManagedByIsImmutable(t *testing.T) {
	skipWithoutDocker(t)
	ctx := context.Background()

	job := translatableJob("immutable")
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	var got batchv1.Job
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: "default", Name: "immutable"}, &got); err != nil {
		t.Fatal(err)
	}
	name := v1alpha1.ManagedByName
	got.Spec.ManagedBy = &name
	if err := k8sClient.Update(ctx, &got); err == nil {
		t.Fatal("the API server accepted a managedBy written after create; " +
			"the webhook could then claim Jobs on update, which it deliberately does not")
	}
}

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
	out, err := exec.Command("docker", "info", "--format", "{{.ServerVersion}}").CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker info: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func startK3s(ctx context.Context) (*rest.Config, func(), error) {
	name := fmt.Sprintf("htck8s-op-k3s-%d", os.Getpid())
	_ = exec.Command("docker", "rm", "-f", name).Run()
	args := []string{
		"run", "-d", "--name", name, "--privileged",
		"-p", "127.0.0.1::6443",
		k3sImage, "server",
		"--disable=traefik", "--disable=metrics-server",
		"--disable=servicelb", "--disable=local-storage",
		"--disable-cloud-controller",
	}
	if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		return nil, nil, fmt.Errorf("docker run: %v: %s", err, strings.TrimSpace(string(out)))
	}
	stop := func() { _ = exec.Command("docker", "rm", "-f", name).Run() }

	var port string
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		out, err := exec.CommandContext(ctx, "docker", "port", name, "6443/tcp").Output()
		if err == nil {
			if _, p, ok := strings.Cut(strings.TrimSpace(string(out)), ":"); ok {
				port = strings.TrimSpace(p)
				break
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	if port == "" {
		stop()
		return nil, nil, fmt.Errorf("the forwarded apiserver port never appeared")
	}

	var raw string
	deadline = time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		out, err := exec.CommandContext(ctx, "docker", "exec", name, "cat", "/etc/rancher/k3s/k3s.yaml").Output()
		if err == nil && strings.Contains(string(out), "server:") {
			raw = string(out)
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if raw == "" {
		stop()
		return nil, nil, fmt.Errorf("k3s never wrote a kubeconfig")
	}
	raw = strings.ReplaceAll(raw, "https://127.0.0.1:6443", "https://127.0.0.1:"+port)

	cfg, err := clientcmd.RESTConfigFromKubeConfig([]byte(raw))
	if err != nil {
		stop()
		return nil, nil, err
	}
	deadline = time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		if c, err := client.New(cfg, client.Options{Scheme: scheme}); err == nil {
			var nodes corev1.NodeList
			if err := c.List(ctx, &nodes); err == nil && len(nodes.Items) > 0 {
				return cfg, stop, nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	stop()
	return nil, nil, fmt.Errorf("the API server never became usable")
}

func translatableJob(name string) *batchv1.Job {
	one := int32(1)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: batchv1.JobSpec{
			Completions: &one,
			Parallelism: &one,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:    "work",
						Image:   "docker://busybox:latest",
						Command: []string{"/bin/sh", "-c", "echo hi"},
						Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("1"),
							corev1.ResourceMemory: resource.MustParse("512Mi"),
						}},
					}},
				},
			},
		},
	}
}

func podsOf(t *testing.T, jobName string) corev1.PodList {
	t.Helper()
	var pods corev1.PodList
	if err := k8sClient.List(context.Background(), &pods,
		client.InNamespace("default"),
		client.MatchingLabels{"batch.kubernetes.io/job-name": jobName}); err != nil {
		t.Fatal(err)
	}
	return pods
}
