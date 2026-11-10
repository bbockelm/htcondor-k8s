package k3s_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/podctl"
	htcwebhook "github.com/bbockelm/htcondor-k8s/virtualnode/internal/webhook"
)

// TestKubectlLogsReachesTheVirtualKubelet is the end-to-end proof that the one
// affordance this approach has over the HTCJob operator actually works.
//
// `kubectl logs` is not a read of any object. The API server looks up the node
// the Pod is bound to, picks an address off node.status.addresses, dials
// node.status.daemonEndpoints.kubeletEndpoint.Port, and proxies. Every hop is
// exercised here: the node this process registered, the address it published,
// the TLS listener it opened, and the job's spool directory on disk. Nothing
// in the path is stubbed, and the request is issued through the same client-go
// call kubectl itself makes.
func TestKubectlLogsReachesTheVirtualKubelet(t *testing.T) {
	skipWithoutDocker(t)
	ctx := context.Background()

	job := translatableJob("logs")
	one := int32(1)
	job.Spec.Completions = &one
	job.Spec.Parallelism = &one
	htcwebhook.Stamp(job)
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatal(err)
	}

	var pod corev1.Pod
	eventuallyTrue(t, "the pod to be bound and carry a spool directory", func() bool {
		pods := podsOf(t, "logs")
		if len(pods.Items) == 0 {
			return false
		}
		pod = pods.Items[0]
		return pod.Spec.NodeName == api.NodeName && pod.Annotations[api.AnnotationSpoolDir] != ""
	})

	// Stand in for the starter: write what the job would have written.
	spool := pod.Annotations[api.AnnotationSpoolDir]
	const want = "hello from an execute node\nsecond line\n"
	if err := os.WriteFile(filepath.Join(spool, "stdout"), []byte(want), 0o600); err != nil {
		t.Fatal(err)
	}

	// A running job is the realistic case, and it is also the one where
	// the API server will not short-circuit the proxy.
	eventuallyTrue(t, "the pod to report Running", func() bool {
		runTheQueueToRunning(t)
		var got corev1.Pod
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(&pod), &got); err != nil {
			return false
		}
		return got.Status.Phase == corev1.PodRunning
	})

	got := podLogs(t, pod.Namespace, pod.Name, &corev1.PodLogOptions{})
	if got != want {
		t.Fatalf("kubectl logs returned %q, want %q", got, want)
	}
}

// TestKubectlLogsHonorsTailLines proves the proxied request keeps its query
// parameters. The API server rebuilds the URL rather than forwarding it, so a
// parameter the log server reads under a different name silently returns the
// whole file -- which looks like success.
func TestKubectlLogsHonorsTailLines(t *testing.T) {
	skipWithoutDocker(t)
	ctx := context.Background()

	job := translatableJob("logs-tail")
	one := int32(1)
	job.Spec.Completions = &one
	job.Spec.Parallelism = &one
	htcwebhook.Stamp(job)
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatal(err)
	}

	var pod corev1.Pod
	eventuallyTrue(t, "the pod to be bound and carry a spool directory", func() bool {
		pods := podsOf(t, "logs-tail")
		if len(pods.Items) == 0 {
			return false
		}
		pod = pods.Items[0]
		return pod.Spec.NodeName == api.NodeName && pod.Annotations[api.AnnotationSpoolDir] != ""
	})

	spool := pod.Annotations[api.AnnotationSpoolDir]
	body := "line one\nline two\nline three\nline four\n"
	if err := os.WriteFile(filepath.Join(spool, "stdout"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	eventuallyTrue(t, "the pod to report Running", func() bool {
		runTheQueueToRunning(t)
		var got corev1.Pod
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(&pod), &got); err != nil {
			return false
		}
		return got.Status.Phase == corev1.PodRunning
	})

	two := int64(2)
	got := podLogs(t, pod.Namespace, pod.Name, &corev1.PodLogOptions{TailLines: &two})
	if n := strings.Count(got, "\n"); n != 2 {
		t.Fatalf("tailLines=2 returned %d lines (%q)", n, got)
	}
	if !strings.Contains(got, "line four") || strings.Contains(got, "line one") {
		t.Fatalf("tailLines=2 returned the wrong end of the file: %q", got)
	}
}

// podLogs issues the request kubectl issues: through the API server, proxied
// to whatever the node says its kubelet is.
func podLogs(t *testing.T, ns, name string, opts *corev1.PodLogOptions) string {
	t.Helper()
	rc, err := clientset.CoreV1().Pods(ns).GetLogs(name, opts).Stream(context.Background())
	if err != nil {
		t.Fatalf("the API server could not proxy to the virtual kubelet: %v", err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading the proxied log stream: %v", err)
	}
	return string(b)
}

func runTheQueueToRunning(t *testing.T) {
	t.Helper()
	ads, err := fakeQueue.Query(context.Background(), podctl.NodeConstraint(api.NodeName))
	if err != nil {
		t.Fatal(err)
	}
	for _, ad := range ads {
		if ad.JobStatus == translate.JobStatusIdle {
			fakeQueue.SetStatus(ad.JobID(), translate.JobStatusRunning, nil)
		}
	}
}
