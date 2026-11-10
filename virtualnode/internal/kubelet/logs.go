package kubelet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
)

// Serving a log comes down to one question -- which bytes -- and the answer
// was already worked out for the facade, which had the same problem with no
// kubelet to help it. The reasoning carries over unchanged:
//
// The submit file sets initialdir to a per-job spool directory, output to a
// file in it, and stream_output to True, so the starter writes the job's
// output back into that directory as it is produced. Reading the file is
// therefore the primary path: it works for a running job and a finished one,
// it needs no connection to the execute node, and it costs one open().
//
// Its precondition is that this process and the schedd share a filesystem.
// That holds when the virtual kubelet runs on the access point and fails for
// a remote AP, where the path either does not exist or -- worse -- is a
// different directory with the same name.
//
// The fallback is a Peek, which asks the running job's starter for the tail
// of its output over CEDAR. It needs no shared filesystem but only works
// while the job is running, and it returns a bounded tail rather than the
// whole file. Together they cover everything except "a finished job on a
// remote AP", which needs condor_transfer_data and is not done here.
//
// What is new in this approach is where the path comes from. The facade
// derived it, because the facade created it. Here the pod controller creates
// it and records it on the Pod as api.AnnotationSpoolDir, and we read it back
// off the Pod -- so the two halves can disagree about the naming scheme
// without stranding anybody's output.

// logOptions is the subset of the API server's query parameters this honors.
type logOptions struct {
	follow     bool
	tailLines  int
	limitBytes int64
	stream     string

	// ignored names the parameters that were asked for and cannot be
	// served, for the response header.
	ignored []string
}

func parseLogOptions(r *http.Request, cfg Config) (logOptions, error) {
	q := r.URL.Query()
	opts := logOptions{tailLines: -1, stream: cfg.StdoutName}

	opts.follow = q.Get("follow") == "true" || q.Get("follow") == "1"

	if raw := q.Get("tailLines"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return opts, fmt.Errorf("tailLines must be a non-negative integer, got %q", raw)
		}
		opts.tailLines = n
	}
	if raw := q.Get("limitBytes"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n <= 0 {
			return opts, fmt.Errorf("limitBytes must be a positive integer, got %q", raw)
		}
		opts.limitBytes = n
	}

	// `previous` is refused rather than ignored. HTCondor truncates the
	// output file when a job starts again, so the previous run's output is
	// gone -- and answering with the current run's output would be a wrong
	// answer to a question that was asked because the current run is not
	// the interesting one.
	if q.Get("previous") == "true" {
		return opts, fmt.Errorf(
			"previous terminated container logs are not available: an HTCondor job " +
				"that starts again truncates its output file, so the earlier run's " +
				"output no longer exists")
	}

	// Everything below needs per-line timestamps, which this stream does
	// not carry. Serving the whole log when five minutes were asked for is
	// the lesser evil against failing the request outright, but it is a
	// silent discrepancy, so it is reported in a header.
	for _, p := range []string{"timestamps", "sinceSeconds", "sinceTime"} {
		if v := q.Get(p); v != "" && v != "false" {
			opts.ignored = append(opts.ignored, p)
		}
	}

	// `stream` is ours, not the API server's. kubectl cannot send it --
	// it has no flag for it -- but `kubectl get --raw` can, and it is the
	// only way to reach stderr when the two streams were submitted to
	// separate files. See pickStream.
	if v := q.Get("stream"); v != "" {
		switch v {
		case "stdout", cfg.StdoutName:
			opts.stream = cfg.StdoutName
		case "stderr", cfg.StderrName:
			opts.stream = cfg.StderrName
		default:
			return opts, fmt.Errorf("stream must be stdout or stderr, got %q", v)
		}
	}
	return opts, nil
}

func (s *Server) handleContainerLogs(w http.ResponseWriter, r *http.Request) {
	ns, name, container := r.PathValue("ns"), r.PathValue("pod"), r.PathValue("container")

	pod, err := s.cfg.Pods.GetPod(r.Context(), ns, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			httpError(w, http.StatusNotFound, "pod %s/%s not found", ns, name)
			return
		}
		httpError(w, http.StatusInternalServerError, "reading pod %s/%s: %v", ns, name, err)
		return
	}

	// The API server passes the container name through verbatim. kubectl
	// has already checked it against the Pod spec, so a mismatch here is
	// either a hand-made request or a Pod whose spec changed -- both worth
	// an explicit error rather than silently serving the only container
	// we have.
	if !hasContainer(pod, container) {
		httpError(w, http.StatusBadRequest,
			"container %q is not in pod %s/%s (it has %s)",
			container, ns, name, strings.Join(containerNames(pod), ", "))
		return
	}

	opts, err := parseLogOptions(r, s.cfg)
	if err != nil {
		httpError(w, http.StatusBadRequest, "%v", err)
		return
	}

	spool := pod.Annotations[api.AnnotationSpoolDir]
	if spool == "" {
		// No spool directory means the pod controller has not submitted
		// this Pod yet. That is the Kubernetes equivalent of
		// ContainerCreating, and it is what kubectl will print.
		httpError(w, http.StatusBadRequest,
			"container %q in pod %s/%s is waiting to start: "+
				"the HTCondor job has not been submitted yet",
			container, ns, name)
		return
	}

	path := s.pickStream(spool, opts)
	if len(opts.ignored) > 0 {
		w.Header().Set("X-HTCondor-Ignored-Parameters", strings.Join(opts.ignored, ","))
	}
	w.Header().Set("Content-Type", "text/plain")

	if opts.follow {
		s.followLog(w, r, pod, path, opts)
		return
	}

	data, err := s.readLog(r.Context(), pod, path, opts)
	if err != nil {
		httpError(w, http.StatusInternalServerError,
			"reading output for pod %s/%s: %v", ns, name, err)
		return
	}
	data = applyLimits(data, opts)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// pickStream decides which file to serve.
//
// The default is stdout. The one piece of cleverness is the fallback: if the
// caller did not ask for a particular stream, stdout does not exist and
// stderr does, serve stderr -- because a job that wrote only to stderr
// otherwise reports as having produced nothing, which is the one wrong answer
// a user will not question.
//
// What this deliberately does not do is concatenate the two. They have no
// timestamps, so there is no honest way to interleave them, and a log whose
// lines are in the wrong order is worse than a log that is missing a half the
// `stream` parameter can fetch. The real fix is upstream of here: a submit
// file that points `error` at the same file as `output` merges the streams at
// the source, the way a container runtime does, and makes this whole question
// disappear. That is the pod controller's call, not ours.
func (s *Server) pickStream(spool string, opts logOptions) string {
	path := filepath.Join(spool, opts.stream)
	if opts.stream != s.cfg.StdoutName {
		return path
	}
	if _, err := os.Stat(path); err == nil {
		return path
	}
	alt := filepath.Join(spool, s.cfg.StderrName)
	if _, err := os.Stat(alt); err == nil {
		return alt
	}
	return path
}

// readLog returns the file if it is reachable, and otherwise whatever the
// starter will give us.
func (s *Server) readLog(ctx context.Context, pod *corev1.Pod, path string, opts logOptions) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		return data, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	if s.cfg.Peek == nil || pod.Status.Phase != corev1.PodRunning {
		// Nothing to read and nothing to ask: a job that has not
		// started has no output, and saying so beats an error that
		// looks like a malfunction.
		return nil, nil
	}
	cluster, proc, ok := jobID(pod)
	if !ok {
		return nil, nil
	}
	peeked, perr := s.cfg.Peek.Peek(ctx, cluster, proc,
		strings.HasSuffix(path, s.cfg.StderrName), s.cfg.TailBytes)
	if perr != nil {
		// A peek that fails is not an error the user can act on -- the
		// job may have just ended, or the starter may be behind a NAT
		// we cannot reach -- so it degrades to an empty log rather
		// than to a 500.
		s.log().V(1).Info("peek failed", "pod", pod.Name, "err", perr.Error())
		return nil, nil
	}
	return peeked, nil
}

// followLog streams the file as it grows, which is what `kubectl logs -f`
// needs. It ends when the Pod reaches a terminal phase or the client leaves.
func (s *Server) followLog(w http.ResponseWriter, r *http.Request, pod *corev1.Pod, path string, opts logOptions) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		httpError(w, http.StatusInternalServerError,
			"this server cannot stream over the current connection")
		return
	}
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	var offset int64
	ticker := time.NewTicker(s.cfg.FollowInterval)
	defer ticker.Stop()

	for {
		if n, err := copyFrom(w, path, offset); err == nil && n > 0 {
			offset += n
			flusher.Flush()
		}

		cur, err := s.cfg.Pods.GetPod(r.Context(), pod.Namespace, pod.Name)
		if err != nil || terminal(cur) {
			// One last pass, so the final lines are not lost to the
			// race between the job exiting and this loop noticing.
			if n, cerr := copyFrom(w, path, offset); cerr == nil && n > 0 {
				flusher.Flush()
			}
			return
		}

		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func copyFrom(w io.Writer, path string, offset int64) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}
	return io.Copy(w, f)
}

// applyLimits trims to tailLines and then to limitBytes.
//
// The order matters and matches the API server's own documentation of the
// two: tailLines selects which lines, limitBytes caps how much is sent.
func applyLimits(data []byte, opts logOptions) []byte {
	if opts.tailLines >= 0 {
		data = lastLines(data, opts.tailLines)
	}
	if opts.limitBytes > 0 && int64(len(data)) > opts.limitBytes {
		data = data[:opts.limitBytes]
	}
	return data
}

// lastLines returns the final n lines. n == 0 is a request for nothing, not a
// request for everything: `--tail=0` means what it says.
func lastLines(data []byte, n int) []byte {
	if n == 0 {
		return nil
	}
	trimmed := bytes.TrimRight(data, "\n")
	count := 0
	for i := len(trimmed) - 1; i >= 0; i-- {
		if trimmed[i] != '\n' {
			continue
		}
		count++
		if count == n {
			return data[i+1:]
		}
	}
	return data
}

// jobID reads the cluster and proc back off the Pod.
func jobID(pod *corev1.Pod) (cluster, proc int, ok bool) {
	raw := pod.Annotations[api.AnnotationJobID]
	c, p, found := strings.Cut(raw, ".")
	if !found {
		return 0, 0, false
	}
	cluster, err := strconv.Atoi(c)
	if err != nil {
		return 0, 0, false
	}
	proc, err = strconv.Atoi(p)
	if err != nil {
		return 0, 0, false
	}
	return cluster, proc, true
}

func terminal(pod *corev1.Pod) bool {
	if pod == nil {
		return true
	}
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

func hasContainer(pod *corev1.Pod, name string) bool {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == name {
			return true
		}
	}
	return false
}

func containerNames(pod *corev1.Pod) []string {
	out := make([]string, 0, len(pod.Spec.Containers))
	for i := range pod.Spec.Containers {
		out = append(out, pod.Spec.Containers[i].Name)
	}
	return out
}

// httpError writes a plain-text error. The API server passes the body
// through to kubectl, so the sentence here is what the user reads.
func httpError(w http.ResponseWriter, code int, format string, args ...any) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(code)
	fmt.Fprintf(w, format+"\n", args...)
}
