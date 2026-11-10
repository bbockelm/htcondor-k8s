package source

import (
	"context"
	"testing"
	"time"

	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
	"github.com/bbockelm/htcondor-k8s/facade/internal/store"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

const pollSubmit = `universe = container
container_image = docker://busybox
executable = /bin/true
request_cpus = 1
request_memory = 128
request_disk = 1024
+K8sPodName = "polled"
+K8sPodNamespace = "default"
queue
`

func TestPollConvergesOnTheQueue(t *testing.T) {
	fake := backend.NewFake()
	st := store.New(1)
	src := &Poll{Backend: fake, Interval: 10 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = src.Run(ctx, st) }()

	if _, err := fake.Submit(ctx, pollSubmit); err != nil {
		t.Fatalf("submitting: %v", err)
	}
	waitFor(t, "the new job to appear", func() bool {
		_, _, ok := st.Item("1.0")
		return ok
	})

	fake.SetStatus("1.0", translate.JobStatusRunning)
	waitFor(t, "the status change", func() bool {
		rec, _, ok := st.Item("1.0")
		return ok && rec.Ad.JobStatus == translate.JobStatusRunning
	})

	if err := fake.Remove(ctx, []string{"1.0"}, "test"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the deletion", func() bool {
		_, _, ok := st.Item("1.0")
		return !ok
	})
}
