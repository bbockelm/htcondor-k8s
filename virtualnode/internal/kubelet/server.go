// Package kubelet serves the handful of HTTP endpoints the Kubernetes API
// server expects to find on the node a Pod is bound to.
//
// This is the part of the Virtual Kubelet pattern that pays for the rest of
// it. `kubectl logs` is not an API-server feature: the API server looks at
// the Pod's .spec.nodeName, looks up that Node's status.addresses and
// status.daemonEndpoints.kubeletEndpoint.Port, and proxies the request to
// `https://<address>:<port>/containerLogs/<ns>/<pod>/<container>`. Anything
// answering on that address and port *is* the kubelet as far as the cluster
// is concerned. The HTCJob operator cannot do this at all -- it has no Pods,
// so there is no node to proxy to -- and recovering `kubectl logs` is one of
// the strongest arguments for this approach over that one.
//
// The bytes come from the job's spool directory, which the pod controller
// records on the Pod as api.AnnotationSpoolDir. Reading it off the Pod rather
// than re-deriving the path matters: a change to the pod controller's naming
// scheme would otherwise strand the output of every Pod submitted before the
// change, and the symptom would be an empty log rather than an error.
//
// # What is and is not implemented
//
//	GET /containerLogs/{ns}/{pod}/{container}   follow, tailLines, limitBytes
//	GET /healthz                                 200
//	exec, attach, portForward                    501, with a message saying why
//
// `timestamps`, `sinceSeconds` and `sinceTime` are accepted and ignored,
// because HTCondor's stdout stream carries no per-line timestamps and
// synthesizing them would be inventing data. The response says so in an
// X-HTCondor-Ignored-Parameters header; kubectl does not show it, but
// `kubectl get --raw` does and so does anything debugging this. `previous` is
// refused outright rather than ignored: a restarted HTCondor job truncates
// its output file, so the previous run's output does not exist, and silently
// serving the current run's output in its place would be a wrong answer to a
// question asked precisely because the current run is not the interesting one.
//
// # Security
//
// The API server authenticates itself to a real kubelet with a client
// certificate, and a real kubelet authorizes the request with a
// SubjectAccessReview against the API server. This serves neither check
// unless ClientCAFile is set, in which case a client certificate signed by
// that CA is required -- which authenticates the API server but still does
// not authorize the user behind the request. For a PoC that is the honest
// state: anything that can reach this port and present an acceptable client
// certificate can read any job's output.
package kubelet

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PodGetter is how the server finds the Pod behind a log request. It is an
// interface so the tests do not need an API server, and because the
// production implementation should be reading from a controller-runtime cache
// rather than issuing a GET per `kubectl logs`.
type PodGetter interface {
	GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error)
}

// PodGetterFunc adapts a function to PodGetter.
type PodGetterFunc func(ctx context.Context, namespace, name string) (*corev1.Pod, error)

// GetPod implements PodGetter.
func (f PodGetterFunc) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	return f(ctx, namespace, name)
}

// PodsFromClient reads Pods through a controller-runtime client, which in a
// manager is cache-backed and therefore free.
func PodsFromClient(c client.Client) PodGetter {
	return PodGetterFunc(func(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
		var pod corev1.Pod
		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &pod); err != nil {
			return nil, err
		}
		return &pod, nil
	})
}

// Peeker is the fallback for a job whose spool directory this process cannot
// read, which is every job on an access point that does not share a
// filesystem with us. It asks the running job's starter for the tail of its
// output over CEDAR.
//
// The signature matches golang-htcondor's Schedd.PeekJobOutput once the two
// streams are collapsed to a bool, which is the same shape the facade's
// backend settled on.
type Peeker interface {
	Peek(ctx context.Context, cluster, proc int, stderr bool, maxBytes int64) ([]byte, error)
}

// Config is what the server needs. Pods is the only required field.
type Config struct {
	Pods PodGetter

	// Peek is optional. Without it, a job whose spool directory is not
	// readable from here reports no output rather than a tail.
	Peek Peeker

	// Address and Port are what this listens on. They must be the same
	// address and port published in the Node's status, because that is
	// where the API server will dial; see the node package.
	Address string
	Port    int32

	// CertFile and KeyFile serve TLS. When both are empty a self-signed
	// certificate is generated at startup, which works because the API
	// server does not verify this certificate unless it was started with
	// --kubelet-certificate-authority. See SelfSignedCert.
	CertFile string
	KeyFile  string

	// ClientCAFile, when set, requires the client -- in practice the API
	// server -- to present a certificate signed by that CA.
	ClientCAFile string

	// StdoutName and StderrName are the file names the pod controller
	// gives the job's streams inside the spool directory. They are
	// configuration rather than constants because this package does not
	// own the submit file.
	StdoutName string
	StderrName string

	// TailBytes bounds one Peek. 64 KiB is golang-htcondor's own default.
	TailBytes int64

	// FollowInterval is how often a followed log is re-read. 500ms is
	// chosen so `kubectl logs -f` feels live without turning a watched
	// log into a busy loop on a shared filesystem.
	FollowInterval time.Duration

	Log logr.Logger
}

// Defaults.
const (
	DefaultStdoutName     = "stdout"
	DefaultStderrName     = "stderr"
	DefaultTailBytes      = 64 * 1024
	DefaultFollowInterval = 500 * time.Millisecond
)

// Server is the kubelet HTTP API.
type Server struct {
	cfg Config
	mux *http.ServeMux
	srv *http.Server
}

// New validates the configuration and wires the routes.
func New(cfg Config) (*Server, error) {
	if cfg.Pods == nil {
		return nil, fmt.Errorf("kubelet: Pods is required")
	}
	if cfg.StdoutName == "" {
		cfg.StdoutName = DefaultStdoutName
	}
	if cfg.StderrName == "" {
		cfg.StderrName = DefaultStderrName
	}
	if cfg.TailBytes <= 0 {
		cfg.TailBytes = DefaultTailBytes
	}
	if cfg.FollowInterval <= 0 {
		cfg.FollowInterval = DefaultFollowInterval
	}
	if (cfg.CertFile == "") != (cfg.KeyFile == "") {
		return nil, fmt.Errorf("kubelet: CertFile and KeyFile must be set together")
	}

	s := &Server{cfg: cfg, mux: http.NewServeMux()}

	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /containerLogs/{ns}/{pod}/{container}", s.handleContainerLogs)

	// The streaming verbs. The API server upgrades these connections to
	// SPDY or WebSocket; answering 501 before the upgrade is what makes
	// kubectl print our message instead of hanging.
	for _, p := range []string{
		"/exec/{ns}/{pod}/{container}",
		"/attach/{ns}/{pod}/{container}",
		"/run/{ns}/{pod}/{container}",
		"/portForward/{ns}/{pod}",
	} {
		s.mux.HandleFunc("GET "+p, s.handleNotImplemented)
		s.mux.HandleFunc("POST "+p, s.handleNotImplemented)
	}

	return s, nil
}

// ServeHTTP makes the Server a handler, which is what the tests use.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Listen opens the TLS listener. It is separate from Serve so that a caller
// -- or a test -- can learn the port when Port is 0.
func (s *Server) Listen() (net.Listener, error) {
	tlsCfg, err := s.tlsConfig()
	if err != nil {
		return nil, err
	}
	addr := net.JoinHostPort(s.cfg.Address, fmt.Sprint(s.cfg.Port))
	ln, err := tls.Listen("tcp", addr, tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("kubelet: listening on %s: %w", addr, err)
	}
	return ln, nil
}

// Serve runs until ctx is cancelled.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	s.srv = &http.Server{
		Handler: s,
		// No read timeout: `kubectl logs -f` is a request that is
		// supposed to last for hours. A write timeout would cut it off
		// for the same reason, so the lifetime of a followed log is
		// bounded by the client going away and by the job ending,
		// both of which the handler watches for.
		ReadHeaderTimeout: 10 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		err := s.srv.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errc <- err
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(shutdownCtx)
		return nil
	}
}

// Run is Listen plus Serve, which is what cmd wants.
func (s *Server) Run(ctx context.Context) error {
	ln, err := s.Listen()
	if err != nil {
		return err
	}
	s.log().Info("kubelet API listening", "addr", ln.Addr().String(),
		"clientCertRequired", s.cfg.ClientCAFile != "")
	return s.Serve(ctx, ln)
}

// tlsConfig builds the serving configuration.
//
// The API server does not verify this certificate unless it was started with
// --kubelet-certificate-authority, so in a default cluster a self-signed
// certificate generated at startup works and nothing complains. That is worth
// being explicit about rather than quietly relying on: it means the API
// server will proxy `kubectl logs` to whatever answers on the address in the
// Node's status, without checking that it is us. A real deployment should
// issue this certificate from the cluster CA -- or from a CA named in
// --kubelet-certificate-authority -- and set CertFile/KeyFile here.
func (s *Server) tlsConfig() (*tls.Config, error) {
	var cert tls.Certificate
	var err error
	if s.cfg.CertFile != "" {
		cert, err = tls.LoadX509KeyPair(s.cfg.CertFile, s.cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("kubelet: loading serving certificate: %w", err)
		}
	} else {
		cert, err = SelfSignedCert(s.cfg.Address)
		if err != nil {
			return nil, err
		}
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if s.cfg.ClientCAFile != "" {
		pem, err := os.ReadFile(s.cfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("kubelet: reading client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kubelet: client CA %s contained no certificates", s.cfg.ClientCAFile)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleNotImplemented answers exec, attach and port-forward.
//
// 501 with a sentence naming what is missing is the right answer for a PoC,
// and a better one than a half-working SPDY implementation. The path to
// implementing it for real is not speculative: golang-htcondor has
// schedd_ssh.go, which already does the GET_JOB_CONNECT_INFO handshake and
// opens an interactive channel to a running job's starter -- the same
// handshake the log fallback's Peek uses. What is left is bridging that
// channel to the SPDY or WebSocket stream the API server opens, plus deciding
// what `kubectl exec` into a *finished* job should even mean.
func (s *Server) handleNotImplemented(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w,
		"the HTCondor virtual kubelet does not implement %s.\n"+
			"Streaming to a running job needs the starter's interactive channel "+
			"(golang-htcondor's schedd_ssh.go) bridged to the API server's SPDY stream, "+
			"which this proof of concept does not do.\n"+
			"`kubectl logs` works.\n",
		verbOf(r.URL.Path))
}

// verbOf names the operation for the 501 message.
func verbOf(path string) string {
	switch {
	case hasPrefix(path, "/exec/"):
		return "kubectl exec"
	case hasPrefix(path, "/attach/"):
		return "kubectl attach"
	case hasPrefix(path, "/portForward/"):
		return "kubectl port-forward"
	default:
		return "this operation"
	}
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func (s *Server) log() logr.Logger {
	if s.cfg.Log.GetSink() == nil {
		return logr.Discard()
	}
	return s.cfg.Log
}
