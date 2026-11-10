package apiserver

import (
	"log/slog"
	"net/http"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
	"github.com/bbockelm/htcondor-k8s/facade/internal/store"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// Server is the whole API surface.
//
// Reads are served from the Store, never from the schedd. That is not a cache
// for speed: a list and the watch that follows it must agree about what the
// world looked like at one resourceVersion, and the only way to promise that
// is for both to come from the same snapshot. Writes go to the schedd and then
// inject their own result into the store, so a create is visible to the get
// that follows it rather than one source tick later.
type Server struct {
	Backend backend.Backend
	Store   *store.Store
	Caps    translate.Capabilities

	// SpoolRoot is where the facade creates a per-object directory to be
	// the job's initialdir. stdout and stderr land there, which is what
	// makes `kubectl logs` answerable without a kubelet.
	SpoolRoot string

	// ScheddName is reported in an annotation. A Pod is only unique within
	// one job queue, and a user with two facades needs to be able to tell
	// which one they are looking at.
	ScheddName string

	Log *slog.Logger

	mux *http.ServeMux
}

// New wires the routes. The zero Logger is replaced so nothing has to nil-check.
func New(s *Server) *Server {
	if s.Log == nil {
		s.Log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	mux := http.NewServeMux()
	s.mux = mux

	mux.HandleFunc("GET /version", s.handleVersion)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleHealthz)
	mux.HandleFunc("GET /livez", s.handleHealthz)
	mux.HandleFunc("GET /api", s.handleAPIVersions)
	mux.HandleFunc("GET /api/v1", s.handleCoreV1Resources)
	mux.HandleFunc("GET /apis", s.handleAPIGroups)
	mux.HandleFunc("GET /apis/batch", s.handleBatchGroup)
	mux.HandleFunc("GET /apis/batch/v1", s.handleBatchV1Resources)
	mux.HandleFunc("GET /openapi/v2", s.handleOpenAPIV2)
	mux.HandleFunc("GET /swagger-2.0.0.pb-v1", s.handleOpenAPIV2)
	mux.HandleFunc("GET /openapi/v3", s.handleOpenAPIAbsent)
	mux.HandleFunc("GET /openapi/v3/", s.handleOpenAPIAbsent)
	mux.HandleFunc("GET /api/v1/events", s.handleEmptyEvents)
	mux.HandleFunc("GET /api/v1/namespaces/{ns}/events", s.handleEmptyEvents)

	// Namespaces are synthesized from the jobs in the queue. kubectl checks
	// that a namespace exists before some operations, and a 404 there turns
	// into an error about the namespace rather than about the Pod.
	mux.HandleFunc("GET /api/v1/namespaces", s.handleListNamespaces)
	mux.HandleFunc("GET /api/v1/namespaces/{name}", s.handleGetNamespace)

	mux.HandleFunc("GET /api/v1/pods", s.handleListPods)
	mux.HandleFunc("GET /api/v1/namespaces/{ns}/pods", s.handleListPods)
	mux.HandleFunc("POST /api/v1/namespaces/{ns}/pods", s.handleCreatePod)
	mux.HandleFunc("GET /api/v1/namespaces/{ns}/pods/{name}", s.handleGetPod)
	mux.HandleFunc("GET /api/v1/namespaces/{ns}/pods/{name}/status", s.handleGetPod)
	mux.HandleFunc("DELETE /api/v1/namespaces/{ns}/pods/{name}", s.handleDeletePod)
	mux.HandleFunc("GET /api/v1/namespaces/{ns}/pods/{name}/log", s.handlePodLog)
	mux.HandleFunc("PATCH /api/v1/namespaces/{ns}/pods/{name}", s.handleImmutable)
	mux.HandleFunc("PUT /api/v1/namespaces/{ns}/pods/{name}", s.handleImmutable)

	mux.HandleFunc("GET /apis/batch/v1/jobs", s.handleListJobs)
	mux.HandleFunc("GET /apis/batch/v1/namespaces/{ns}/jobs", s.handleListJobs)
	mux.HandleFunc("POST /apis/batch/v1/namespaces/{ns}/jobs", s.handleCreateJob)
	mux.HandleFunc("GET /apis/batch/v1/namespaces/{ns}/jobs/{name}", s.handleGetJob)
	mux.HandleFunc("GET /apis/batch/v1/namespaces/{ns}/jobs/{name}/status", s.handleGetJob)
	mux.HandleFunc("DELETE /apis/batch/v1/namespaces/{ns}/jobs/{name}", s.handleDeleteJob)
	mux.HandleFunc("PATCH /apis/batch/v1/namespaces/{ns}/jobs/{name}", s.handleImmutable)
	mux.HandleFunc("PUT /apis/batch/v1/namespaces/{ns}/jobs/{name}", s.handleImmutable)

	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// A verb or path this facade does not implement must say so as a
	// Status, not as Go's default plain-text 404: kubectl parses the body
	// and prints "the server could not find the requested resource", which
	// is the message a user can act on.
	rec := &notFoundSniffer{ResponseWriter: w, path: r.URL.Path, method: r.Method}
	s.mux.ServeHTTP(rec, r)
	rec.finish()
}

// notFoundSniffer turns ServeMux's bare 404 into a Kubernetes Status, without
// having to register a handler for every path that does not exist.
type notFoundSniffer struct {
	http.ResponseWriter
	path       string
	method     string
	wroteCode  int
	suppressed bool
}

func (n *notFoundSniffer) WriteHeader(code int) {
	n.wroteCode = code
	if code == http.StatusNotFound && n.Header().Get("Content-Type") != "application/json" {
		n.suppressed = true
		return
	}
	n.ResponseWriter.WriteHeader(code)
}

func (n *notFoundSniffer) Write(b []byte) (int, error) {
	if n.suppressed {
		return len(b), nil
	}
	return n.ResponseWriter.Write(b)
}

func (n *notFoundSniffer) Flush() {
	if f, ok := n.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (n *notFoundSniffer) finish() {
	if !n.suppressed {
		return
	}
	writeRawStatus(n.ResponseWriter, newStatus(http.StatusNotFound, "NotFound",
		"the server could not find the requested resource: "+n.method+" "+n.path))
}

// handleImmutable answers an update.
//
// A queued HTCondor job's image, command and resource request are fixed once
// it is submitted; HTCondor can edit some attributes of an idle job, but the
// ones a Kubernetes update would change are exactly the ones that are already
// baked into the match. So the facade says so, in the one place a user will
// see it -- `kubectl apply` on an object that already exists -- rather than
// accepting the patch and quietly running the old thing.
func (s *Server) handleImmutable(w http.ResponseWriter, r *http.Request) {
	writeStatus(w, http.StatusMethodNotAllowed, metav1.StatusReasonMethodNotAllowed,
		"%s is immutable here: an HTCondor job cannot be updated in place. Delete %q and create it again.",
		r.PathValue("name"), r.PathValue("name"))
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("ok"))
}
