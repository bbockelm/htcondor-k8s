package apiserver

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
	"github.com/bbockelm/htcondor-k8s/facade/internal/store"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// fixture is an API server over an in-memory job queue.
//
// sync is explicit rather than a background poller: a test that has to sleep
// for a source tick is a test that will be flaky on a loaded machine, and one
// that drives the pump itself can assert on exactly which snapshot produced
// which watch event.
type fixture struct {
	t      *testing.T
	Server *Server
	Fake   *backend.Fake
	HTTP   *httptest.Server
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	fake := backend.NewFake()
	caps := translate.DefaultCapabilities()
	srv := New(&Server{
		Backend:    fake,
		Store:      store.New(1),
		Caps:       caps,
		SpoolRoot:  t.TempDir(),
		ScheddName: "test-schedd",
	})
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return &fixture{t: t, Server: srv, Fake: fake, HTTP: ts}
}

// sync pumps the fake queue into the store, exactly as source.Poll would.
func (f *fixture) sync() {
	f.t.Helper()
	recs, err := f.Fake.Query(context.Background(), backend.Constraint(""))
	if err != nil {
		f.t.Fatalf("querying the fake queue: %v", err)
	}
	f.Server.Store.Replace(recs)
}

const podManifest = `{
  "apiVersion": "v1",
  "kind": "Pod",
  "metadata": {"name": "%s", "namespace": "%s", "labels": {"app": "demo"}},
  "spec": {
    "restartPolicy": "Never",
    "containers": [{
      "name": "main",
      "image": "docker://busybox:latest",
      "command": ["/bin/sh", "-c", "echo hello"],
      "env": [{"name": "GREETING", "value": "hi there"}],
      "resources": {"requests": {"cpu": "2", "memory": "512Mi", "ephemeral-storage": "2Gi"}}
    }]
  }
}`
