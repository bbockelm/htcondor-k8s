package apiserver

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// There is no kubelet here, so `kubectl logs` has to find the bytes itself.
//
// The submit file sets initialdir to a facade-created spool directory, output
// to "stdout" and stream_output to True, so the starter writes the job's
// output back into that directory as it is produced. Reading the file is
// therefore the primary path: it works for a running job and for a finished
// one, it needs no connection to the execute node, and it costs one open().
//
// Its precondition is that the facade and the schedd share a filesystem. That
// is true when the facade runs on the AP, which is the deployment this PoC
// assumes, and false for a facade talking to a remote schedd -- where the
// spool directory is on the other machine and the local path either does not
// exist or, worse, is a different directory with the same name.
//
// The fallback is Schedd.PeekJobOutput, which asks the running job's starter
// for the tail of its output over CEDAR. It needs no shared filesystem, but it
// only works while the job is running and it returns a bounded tail rather
// than the whole file. Together they cover everything except "a finished job
// on a remote AP", which is where condor_transfer_data would have to come in
// and does not yet.

// logTailBytes bounds a Peek. 64 KiB is golang-htcondor's own default and is
// enough to be useful without making one kubectl invocation expensive.
const logTailBytes = 64 * 1024

func (s *Server) handlePodLog(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	rec, _, ok := s.podByName(r.PathValue("ns"), name)
	if !ok {
		notFound(w, "pods", name)
		return
	}
	q := r.URL.Query()
	wantStderr := q.Get("container") == "stderr"
	stream := "stdout"
	if wantStderr {
		stream = "stderr"
	}

	if q.Get("follow") == "true" {
		s.followLog(w, r, rec, stream, wantStderr)
		return
	}

	data, err := s.readLog(r, rec, stream, wantStderr)
	if err != nil {
		writeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest,
			"reading output for job %s: %v", rec.Key(), err)
		return
	}
	if n := tailLines(q.Get("tailLines")); n > 0 {
		data = lastLines(data, n)
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// readLog returns the whole file if it is reachable, and otherwise the tail
// the starter will give us.
func (s *Server) readLog(r *http.Request, rec backend.JobRecord, stream string, stderr bool) ([]byte, error) {
	if path := logPath(rec, stream); path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			return data, nil
		}
		if !os.IsNotExist(err) {
			return nil, err
		}
	}
	if rec.Ad.JobStatus != translate.JobStatusRunning {
		// Nothing to read and nothing to ask: a job that has not
		// started has no output, and saying so beats an error that
		// looks like a malfunction.
		return nil, nil
	}
	return s.Backend.Peek(r.Context(), rec.Ad.ClusterID, rec.Ad.ProcID, stderr, logTailBytes)
}

// followLog streams the file as it grows, which is what `kubectl logs -f`
// needs. It ends when the job leaves the queue or the client goes away.
func (s *Server) followLog(w http.ResponseWriter, r *http.Request, rec backend.JobRecord, stream string, stderr bool) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError,
			"this server cannot stream over the current connection")
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	path := logPath(rec, stream)
	var offset int64
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		if path != "" {
			if n, err := copyFrom(w, path, offset); err == nil && n > 0 {
				offset += n
				flusher.Flush()
			}
		} else if data, err := s.Backend.Peek(r.Context(), rec.Ad.ClusterID, rec.Ad.ProcID, stderr, logTailBytes); err == nil {
			// Peek has no cursor here, so following over it would
			// repeat the same tail. Send it once and stop rather
			// than print the same lines forever.
			_, _ = w.Write(data)
			flusher.Flush()
			return
		}

		cur, _, ok := s.podByName(rec.Ad.Identity.Namespace, PodName(rec))
		if !ok || cur.Ad.Terminal() {
			// One last pass so the final lines are not lost to the
			// race between the job exiting and this loop noticing.
			if path != "" {
				if n, err := copyFrom(w, path, offset); err == nil && n > 0 {
					flusher.Flush()
				}
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

func logPath(rec backend.JobRecord, stream string) string {
	if rec.IWD == "" {
		return ""
	}
	return filepath.Join(rec.IWD, stream)
}

func tailLines(raw string) int {
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func lastLines(data []byte, n int) []byte {
	if n <= 0 {
		return data
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
