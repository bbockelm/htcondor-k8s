package apiserver

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
	"github.com/bbockelm/htcondor-k8s/facade/internal/store"
)

// A watch is the only verb where the facade has to make a promise it cannot
// take back. A list is a snapshot: if it is wrong, the next one corrects it.
// A watch stream is a client's entire view of reality from the moment it
// starts, and a single missing event is never noticed and never repaired. So
// everything here errs toward ending the stream: a client told to re-list
// recovers, a client that silently missed a deletion does not.

// defaultWatchTimeout bounds a stream the client forgot to close. Kubernetes
// uses a randomized hour; a few minutes is friendlier to a facade whose
// resourceVersion generation does not survive a restart anyway.
const defaultWatchTimeout = 30 * time.Minute

func (s *Server) watchPods(w http.ResponseWriter, r *http.Request, ns string, filter podFilter) {
	s.watch(w, r, watchSpec{
		gv:   corev1.SchemeGroupVersion,
		keep: inNamespace(ns),
		// Pods map one-to-one onto job records, so an event about a
		// record is an event about exactly one Pod.
		object: func(rec backend.JobRecord, v store.Version) (runtime.Object, bool) {
			pod := s.Pod(rec, v)
			return pod, filter.matchesPod(pod)
		},
		columns: podColumns,
		row:     func(o runtime.Object) metav1.TableRow { return podRow(o.(*corev1.Pod)) },
	})
}

func (s *Server) watchJobs(w http.ResponseWriter, r *http.Request, ns string, filter podFilter) {
	s.watch(w, r, watchSpec{
		gv:   batchv1.SchemeGroupVersion,
		keep: jobsIn(ns),
		// A Job is a whole cluster, so one proc changing means the Job
		// changed: re-derive it from the store rather than from the
		// single record the event carried.
		object: func(rec backend.JobRecord, _ store.Version) (runtime.Object, bool) {
			if rec.Ad.Identity.OwnerKind != "Job" {
				return nil, false
			}
			recs, rv, ok := s.jobByName(rec.Ad.Identity.Namespace, rec.Ad.Identity.OwnerName)
			if !ok {
				// Every proc is gone: the Job is deleted, and the
				// record in hand is the best description of it.
				job := s.Job([]backend.JobRecord{rec}, store.Version{})
				return job, job != nil && filter.matchesMeta(job.Name, job.Namespace, job.Labels)
			}
			job := s.Job(recs, rv)
			return job, job != nil && filter.matchesMeta(job.Name, job.Namespace, job.Labels)
		},
		// Several procs of one cluster changing in the same tick is one
		// Job change, not N.
		collapse: func(rec backend.JobRecord) string { return rec.Ad.Identity.OwnerName },
		columns:  jobColumns,
		row:      func(o runtime.Object) metav1.TableRow { return jobRow(o.(*batchv1.Job)) },
	})
}

type watchSpec struct {
	gv       schema.GroupVersion
	keep     func(backend.JobRecord) bool
	object   func(backend.JobRecord, store.Version) (runtime.Object, bool)
	collapse func(backend.JobRecord) string
	// columns and row render an event as a one-row Table. A client that
	// asked for server-side printing on the list asks for it on the watch
	// too, and sending it plain objects instead leaves `kubectl get -w`
	// printing the first screen and then nothing.
	columns []metav1.TableColumnDefinition
	row     func(runtime.Object) metav1.TableRow
}

func (s *Server) watch(w http.ResponseWriter, r *http.Request, spec watchSpec) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError,
			"this server cannot stream a watch over the current connection")
		return
	}

	from, sendInitial, err := parseWatchVersion(r.URL.Query().Get("resourceVersion"))
	if err != nil {
		writeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest, "%v", err)
		return
	}

	// Taking the snapshot and registering the watcher in this order is
	// what makes list-then-watch lossless: the snapshot is taken at a
	// version, and the watcher replays from that same version, so a change
	// that lands in between is reported rather than falling in the gap.
	var initial []backend.JobRecord
	var at store.Version
	if sendInitial {
		initial, at = s.Store.Snapshot(spec.keep)
		from = &at
	}

	watcher, err := s.Store.Watch(from)
	if err != nil {
		if errors.Is(err, store.ErrTooOld) {
			gone(w, "the requested resourceVersion is no longer available; list again and watch from the version the list reports")
			return
		}
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError, "%v", err)
		return
	}
	defer watcher.Stop()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	asTable := wantsTable(r) && spec.row != nil
	enc := json.NewEncoder(w)
	emit := func(evType store.EventType, obj runtime.Object) bool {
		var raw []byte
		var err error
		if asTable {
			table := newTable(resourceVersionOf(obj), spec.columns)
			table.Rows = []metav1.TableRow{spec.row(obj)}
			raw, err = json.Marshal(table)
		} else {
			raw, err = encode(obj, spec.gv)
		}
		if err != nil {
			s.Log.Error("encoding a watch event", "err", err)
			return true
		}
		if err := enc.Encode(metav1.WatchEvent{
			Type:   string(evType),
			Object: runtime.RawExtension{Raw: raw},
		}); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	// The initial state goes out as ADDED, which is what the API server
	// does for a watch with no resourceVersion and what every informer
	// expects before it considers itself synced.
	seen := map[string]bool{}
	for _, rec := range initial {
		if spec.collapse != nil {
			k := spec.collapse(rec)
			if seen[k] {
				continue
			}
			seen[k] = true
		}
		v, _ := s.Store.VersionOf(rec.Key())
		obj, keep := spec.object(rec, v)
		if !keep {
			continue
		}
		if !emit(store.Added, obj) {
			return
		}
	}

	timeout := defaultWatchTimeout
	if v := r.URL.Query().Get("timeoutSeconds"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			timeout = time.Duration(n) * time.Second
		}
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-deadline.C:
			// Ending a watch cleanly on a timeout is normal; the
			// client reconnects from the last version it saw.
			return
		case ev, open := <-watcher.Events():
			if !open {
				if watcher.Expired() {
					// The stream is already 200 and committed, so
					// the only way to say "re-list" is to stop.
					// A client that sees the stream end re-lists,
					// which is the recovery a 410 would have asked
					// for.
					s.Log.Warn("ending a watch that fell too far behind")
				}
				return
			}
			if spec.keep != nil && !spec.keep(ev.Record) && ev.Type != store.Deleted {
				continue
			}
			obj, keep := spec.object(ev.Record, ev.Version)
			if !keep || obj == nil {
				continue
			}
			// A deletion must carry the version the deletion
			// happened at, not the object's last update: a client
			// resuming from it has to land after the delete.
			if ev.Type == store.Deleted {
				setResourceVersion(obj, ev.Version)
			}
			if !emit(ev.Type, obj) {
				return
			}
		}
	}
}

func resourceVersionOf(obj runtime.Object) string {
	switch o := obj.(type) {
	case *corev1.Pod:
		return o.ResourceVersion
	case *batchv1.Job:
		return o.ResourceVersion
	}
	return ""
}

func setResourceVersion(obj runtime.Object, v store.Version) {
	switch o := obj.(type) {
	case *corev1.Pod:
		o.ResourceVersion = v.String()
	case *batchv1.Job:
		o.ResourceVersion = v.String()
	}
}

// parseWatchVersion reads the resourceVersion of a watch request.
//
// Empty and "0" both mean "I have no state": the server sends the current
// contents as ADDED and then streams. Anything else is a resume point and is
// honored exactly or refused with 410 -- never approximated, because a client
// that is told its resume succeeded will never go back for what it missed.
func parseWatchVersion(raw string) (*store.Version, bool, error) {
	if raw == "" || raw == "0" {
		return nil, true, nil
	}
	v, err := store.ParseVersion(raw)
	if err != nil {
		return nil, false, err
	}
	return &v, false, nil
}
