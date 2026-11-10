package k3s_test

import (
	"bufio"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
	htcwebhook "github.com/bbockelm/htcondor-k8s/virtualnode/internal/webhook"
)

// TestKubectlLogsFollowStreamsThroughTheProxy covers the half of `kubectl
// logs` that the non-following test cannot.
//
// A followed log is a long-lived response with no Content-Length, written in
// pieces. Proving it works needs more than "bytes came back": the bytes have
// to arrive *before the writer finishes*, or the API server has buffered the
// whole response and `kubectl logs -f` would show nothing until the job ends
// -- which looks like a hung job rather than a broken stream. So this reads a
// line, then appends, then reads the appended line, with the writer still
// open throughout.
//
// It also asserts the stream *ends* when the job reaches a terminal state.
// A follow that never closes leaves kubectl hanging after the job is gone.
func TestKubectlLogsFollowStreamsThroughTheProxy(t *testing.T) {
	skipWithoutDocker(t)
	ctx := context.Background()

	job := translatableJob("logs-follow")
	one := int32(1)
	job.Spec.Completions = &one
	job.Spec.Parallelism = &one
	htcwebhook.Stamp(job)
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatal(err)
	}

	var pod corev1.Pod
	eventuallyTrue(t, "the pod to be bound and carry a spool directory", func() bool {
		pods := podsOf(t, "logs-follow")
		if len(pods.Items) == 0 {
			return false
		}
		pod = pods.Items[0]
		return pod.Spec.NodeName == api.NodeName && pod.Annotations[api.AnnotationSpoolDir] != ""
	})

	stdout := filepath.Join(pod.Annotations[api.AnnotationSpoolDir], "stdout")
	f, err := os.OpenFile(stdout, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	appendLine := func(s string) {
		t.Helper()
		if _, err := f.WriteString(s + "\n"); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	appendLine("first")

	eventuallyTrue(t, "the pod to report Running", func() bool {
		runTheQueueToRunning(t)
		var got corev1.Pod
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(&pod), &got); err != nil {
			return false
		}
		return got.Status.Phase == corev1.PodRunning
	})

	streamCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	rc, err := clientset.CoreV1().Pods(pod.Namespace).
		GetLogs(pod.Name, &corev1.PodLogOptions{Follow: true}).Stream(streamCtx)
	if err != nil {
		t.Fatalf("the API server could not open a followed stream: %v", err)
	}
	defer rc.Close()

	lines := make(chan string, 16)
	readErr := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(rc)
		for sc.Scan() {
			lines <- sc.Text()
		}
		err := sc.Err()
		if err == nil || strings.Contains(err.Error(), "context canceled") {
			err = io.EOF
		}
		readErr <- err
		close(lines)
	}()

	nextLine := func(what string) string {
		t.Helper()
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatalf("the stream closed before %s arrived", what)
			}
			return l
		case <-time.After(60 * time.Second):
			t.Fatalf("timed out waiting for %s; the API server may be buffering the whole response", what)
			return ""
		}
	}

	if got := nextLine("the first line"); got != "first" {
		t.Fatalf("first line = %q, want %q", got, "first")
	}

	// The writer is still open. If this arrives, the response really is
	// being streamed rather than assembled and sent at the end.
	appendLine("second")
	if got := nextLine("a line appended while the stream was open"); got != "second" {
		t.Fatalf("second line = %q, want %q", got, "second")
	}

	// A terminal job must end the stream rather than leave kubectl hanging.
	runTheQueue(t)
	select {
	case err := <-readErr:
		if err != nil && err != io.EOF {
			t.Fatalf("the followed stream ended with an error: %v", err)
		}
	case <-time.After(90 * time.Second):
		t.Fatal("the followed stream did not close after the job reached a terminal state")
	}
}

// A followed log of a job that never started must not hang forever waiting
// for bytes that are not coming.
func TestKubectlLogsFollowEndsForATerminalPod(t *testing.T) {
	skipWithoutDocker(t)
	ctx := context.Background()

	job := translatableJob("logs-follow-done")
	one := int32(1)
	job.Spec.Completions = &one
	job.Spec.Parallelism = &one
	htcwebhook.Stamp(job)
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatal(err)
	}

	var pod corev1.Pod
	eventuallyTrue(t, "the pod to be bound", func() bool {
		pods := podsOf(t, "logs-follow-done")
		if len(pods.Items) == 0 {
			return false
		}
		pod = pods.Items[0]
		return pod.Spec.NodeName == api.NodeName && pod.Annotations[api.AnnotationSpoolDir] != ""
	})
	if err := os.WriteFile(filepath.Join(pod.Annotations[api.AnnotationSpoolDir], "stdout"),
		[]byte("all done\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Drive it all the way to a terminal state before following.
	eventuallyTrue(t, "the pod to reach a terminal phase", func() bool {
		runTheQueue(t)
		var got corev1.Pod
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(&pod), &got); err != nil {
			return false
		}
		return got.Status.Phase == corev1.PodSucceeded || got.Status.Phase == corev1.PodFailed
	})

	streamCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	rc, err := clientset.CoreV1().Pods(pod.Namespace).
		GetLogs(pod.Name, &corev1.PodLogOptions{Follow: true}).Stream(streamCtx)
	if err != nil {
		t.Fatalf("the API server could not open a followed stream: %v", err)
	}
	defer rc.Close()

	done := make(chan []byte, 1)
	go func() { b, _ := io.ReadAll(rc); done <- b }()
	select {
	case b := <-done:
		if !strings.Contains(string(b), "all done") {
			t.Fatalf("a followed stream of a finished job returned %q", string(b))
		}
	case <-time.After(60 * time.Second):
		t.Fatal("following a finished job never returned; kubectl logs -f would hang")
	}
}
