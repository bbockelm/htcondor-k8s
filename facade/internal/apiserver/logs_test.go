package apiserver

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

func TestPodLogsReadTheSpoolDirectory(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx := context.Background()
	if _, err := cs.CoreV1().Pods("default").Create(ctx, demoPod("chatty", "default"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.sync()

	// stream_output puts the job's stdout here as it is produced, which is
	// what makes logs answerable with no kubelet and no running starter.
	dir := filepath.Join(f.Server.SpoolRoot, "default", "chatty")
	if err := os.WriteFile(filepath.Join(dir, "stdout"), []byte("one\ntwo\nthree\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	body, err := cs.CoreV1().Pods("default").GetLogs("chatty", &corev1.PodLogOptions{}).DoRaw(ctx)
	if err != nil {
		t.Fatalf("kubectl logs: %v", err)
	}
	if string(body) != "one\ntwo\nthree\n" {
		t.Fatalf("got %q", body)
	}

	tail, err := cs.CoreV1().Pods("default").GetLogs("chatty", &corev1.PodLogOptions{TailLines: ptr(int64(2))}).DoRaw(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(tail) != "two\nthree\n" {
		t.Fatalf("tailLines=2 gave %q", tail)
	}
}

func TestPodLogsFallBackToPeekForARunningJob(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx := context.Background()
	if _, err := cs.CoreV1().Pods("default").Create(ctx, demoPod("remote", "default"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// No file in the spool directory: the deployment where the facade does
	// not share a filesystem with the schedd, where the starter is the
	// only thing that has the bytes.
	if err := os.RemoveAll(filepath.Join(f.Server.SpoolRoot, "default", "remote")); err != nil {
		t.Fatal(err)
	}
	f.Fake.SetStatus("1.0", translate.JobStatusRunning)
	f.Fake.Output["1.0"] = []byte("live output\n")
	f.sync()

	body, err := cs.CoreV1().Pods("default").GetLogs("remote", &corev1.PodLogOptions{}).DoRaw(ctx)
	if err != nil {
		t.Fatalf("kubectl logs: %v", err)
	}
	if string(body) != "live output\n" {
		t.Fatalf("got %q, want the starter's tail", body)
	}
}

func TestPodLogsForAnIdleJobAreEmptyNotAnError(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx := context.Background()
	if _, err := cs.CoreV1().Pods("default").Create(ctx, demoPod("waiting", "default"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(f.Server.SpoolRoot, "default", "waiting")); err != nil {
		t.Fatal(err)
	}
	f.sync()

	body, err := cs.CoreV1().Pods("default").GetLogs("waiting", &corev1.PodLogOptions{}).DoRaw(ctx)
	if err != nil {
		t.Fatalf("a job that has not started should produce no output, not an error: %v", err)
	}
	if len(body) != 0 {
		t.Fatalf("got %q", body)
	}
}

func ptr[T any](v T) *T { return &v }
