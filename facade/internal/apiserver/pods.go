package apiserver

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
	"github.com/bbockelm/htcondor-k8s/facade/internal/store"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// dnsName is the subset of RFC 1123 Kubernetes uses for object names. The
// facade checks it because the name becomes a directory under SpoolRoot and a
// ClassAd string in a constraint; neither is a place to find out that a client
// sent "../..".
var dnsName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

func (s *Server) handleCreatePod(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	var pod corev1.Pod
	if !s.decodeInto(w, r, &pod) {
		return
	}
	if pod.Namespace == "" {
		pod.Namespace = ns
	}
	if pod.Namespace != ns {
		writeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest,
			"the object's namespace %q does not match the URL's %q", pod.Namespace, ns)
		return
	}
	if pod.Name == "" && pod.GenerateName != "" {
		pod.Name = pod.GenerateName + randomSuffix()
	}
	if !dnsName.MatchString(pod.Name) || len(pod.Name) > 253 {
		writeStatus(w, http.StatusUnprocessableEntity, metav1.StatusReasonInvalid,
			"metadata.name %q is not a valid DNS-1123 label", pod.Name)
		return
	}

	// Admission is the one part of this that is not mechanical, and it is
	// the reason the facade can promise that a Pod it accepted will run the
	// way the Pod said. Every rejection goes into one message: a user
	// fixing a manifest one error per round trip is a user who gives up.
	if rejections := translate.Admit(&pod.Spec, &pod.ObjectMeta, s.Caps); !rejections.OK() {
		writeRawStatus(w, rejectionStatus("Pod", pod.Name, rejections))
		return
	}

	if _, _, found := s.podByName(pod.Namespace, pod.Name); found {
		st := newStatus(http.StatusConflict, metav1.StatusReasonAlreadyExists,
			fmt.Sprintf("pods %q already exists", pod.Name))
		st.Details = &metav1.StatusDetails{Name: pod.Name, Kind: "pods"}
		writeRawStatus(w, st)
		return
	}

	uid := string(pod.UID)
	if uid == "" {
		uid = uuid.NewString()
	}
	iwd, err := s.spoolDir(pod.Namespace, pod.Name)
	if err != nil {
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError,
			"creating the job's spool directory: %v", err)
		return
	}

	submitText, err := translate.PodToSubmit(&pod.Spec, translate.Identity{
		Name:        pod.Name,
		Namespace:   pod.Namespace,
		UID:         uid,
		Labels:      pod.Labels,
		Annotations: pod.Annotations,
	}, translate.SubmitOptions{
		Caps:            s.Caps,
		IWD:             iwd,
		Count:           1,
		ExtraAttributes: map[string]string{backend.AttrIWD: backend.AdString(iwd)},
	})
	if err != nil {
		writeStatus(w, http.StatusUnprocessableEntity, metav1.StatusReasonInvalid,
			"this pod cannot be rendered as a submit file: %v", err)
		return
	}

	recs, err := s.submitAndRefresh(r.Context(), submitText)
	if err != nil {
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError,
			"the access point refused the job: %v", err)
		return
	}
	if len(recs) == 0 {
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError,
			"the job was submitted but did not appear in the queue")
		return
	}
	rv, _ := s.Store.VersionOf(recs[0].Key())
	writeObject(w, http.StatusCreated, s.Pod(recs[0], rv), corev1.SchemeGroupVersion)
}

// submitAndRefresh queues a cluster and reads it straight back, so the object
// exists in the store before the response is written. Without this a `kubectl
// create` followed immediately by a `get` would report NotFound until the next
// source tick, which is the kind of thing that works in a demo and breaks
// every script.
func (s *Server) submitAndRefresh(ctx context.Context, submitText string) ([]backend.JobRecord, error) {
	cluster, err := s.Backend.Submit(ctx, submitText)
	if err != nil {
		return nil, err
	}
	recs, err := s.Backend.Query(ctx, backend.Constraint("", "ClusterId == "+strconv.Itoa(cluster)))
	if err != nil {
		return nil, fmt.Errorf("reading back cluster %d: %w", cluster, err)
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].Ad.ProcID < recs[j].Ad.ProcID })
	s.Store.Upsert(recs...)
	return recs, nil
}

func (s *Server) handleGetPod(w http.ResponseWriter, r *http.Request) {
	rec, rv, ok := s.podByName(r.PathValue("ns"), r.PathValue("name"))
	if !ok {
		notFound(w, "pods", r.PathValue("name"))
		return
	}
	pod := s.Pod(rec, rv)
	if wantsTable(r) {
		table := newTable(rv.String(), podColumns)
		table.Rows = append(table.Rows, podRow(pod))
		writeTable(w, table)
		return
	}
	writeObject(w, http.StatusOK, pod, corev1.SchemeGroupVersion)
}

func (s *Server) handleDeletePod(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	rec, rv, ok := s.podByName(r.PathValue("ns"), name)
	if !ok {
		notFound(w, "pods", name)
		return
	}
	if err := s.Backend.Remove(r.Context(), []string{rec.Key()}, "deleted through the Kubernetes API facade"); err != nil {
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError,
			"removing job %s: %v", rec.Key(), err)
		return
	}
	s.Store.Delete(rec.Key())
	// Kubernetes returns the object as it was, not a Status, so kubectl
	// can print `pod "x" deleted` with the right name.
	writeObject(w, http.StatusOK, s.Pod(rec, rv), corev1.SchemeGroupVersion)
}

func (s *Server) handleListPods(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	filter, err := parseFilter(r.URL.Query())
	if err != nil {
		writeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest, "%v", err)
		return
	}
	if r.URL.Query().Get("watch") == "true" || r.URL.Query().Get("watch") == "1" {
		s.watchPods(w, r, ns, filter)
		return
	}

	recs, version := s.Store.Snapshot(inNamespace(ns))
	var pods []*corev1.Pod
	for _, rec := range recs {
		rv, _ := s.Store.VersionOf(rec.Key())
		pod := s.Pod(rec, rv)
		if filter.matchesPod(pod) {
			pods = append(pods, pod)
		}
	}
	if wantsTable(r) {
		table := newTable(version.String(), podColumns)
		for _, pod := range pods {
			table.Rows = append(table.Rows, podRow(pod))
		}
		writeTable(w, table)
		return
	}
	list := &corev1.PodList{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"},
		ListMeta: metav1.ListMeta{ResourceVersion: version.String()},
	}
	for _, pod := range pods {
		list.Items = append(list.Items, *pod)
	}
	writeObject(w, http.StatusOK, list, corev1.SchemeGroupVersion)
}

// podByName resolves a Pod name to a job.
//
// It takes one pass over the snapshot rather than a schedd query, which also
// settles the "foo-0" ambiguity described on PodName: an exact match on a
// standalone Pod's own name wins over a Job proc that happens to render to the
// same string, because the exact match is checked first.
func (s *Server) podByName(ns, name string) (backend.JobRecord, store.Version, bool) {
	recs, _ := s.Store.Snapshot(inNamespace(ns))
	var fallback *backend.JobRecord
	for i := range recs {
		if PodName(recs[i]) != name {
			continue
		}
		if recs[i].Ad.Identity.OwnerKind == "" {
			rv, _ := s.Store.VersionOf(recs[i].Key())
			return recs[i], rv, true
		}
		if fallback == nil {
			fallback = &recs[i]
		}
	}
	if fallback != nil {
		rv, _ := s.Store.VersionOf(fallback.Key())
		return *fallback, rv, true
	}
	return backend.JobRecord{}, store.Version{}, false
}

func inNamespace(ns string) func(backend.JobRecord) bool {
	if ns == "" {
		return nil
	}
	return func(r backend.JobRecord) bool { return r.Ad.Identity.Namespace == ns }
}

// rejectionStatus renders an admission verdict as the 422 kubectl prints.
//
// Each rejection becomes a Cause with its field path, which is what makes
// `kubectl create` show the user the line of their manifest to change, and the
// joined message is what they see without -o json.
func rejectionStatus(kind, name string, rs translate.Rejections) *metav1.Status {
	causes := make([]metav1.StatusCause, 0, len(rs))
	msgs := make([]string, 0, len(rs))
	for _, rej := range rs {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseType(rej.Reason),
			Message: rej.Detail,
			Field:   rej.Field,
		})
		msgs = append(msgs, rej.Field+": "+rej.Detail)
	}
	st := newStatus(http.StatusUnprocessableEntity, metav1.StatusReasonInvalid,
		fmt.Sprintf("%s %q cannot be modeled as an HTCondor job: %s", kind, name, strings.Join(msgs, "; ")))
	st.Details = &metav1.StatusDetails{Name: name, Kind: kind, Causes: causes}
	return st
}

// spoolDir creates the per-object directory that becomes the job's initialdir.
func (s *Server) spoolDir(ns, name string) (string, error) {
	if s.SpoolRoot == "" {
		return "", nil
	}
	dir := filepath.Join(s.SpoolRoot, ns, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return dir, nil
}

func randomSuffix() string { return uuid.NewString()[:5] }

// namespaces are synthesized: there is no HTCondor object to back one, and a
// namespace with nothing in it is indistinguishable from one that was never
// created. The facade reports the namespaces its jobs claim to be in, plus
// "default", so `kubectl get pods -n x` on an empty namespace says "no
// resources found" rather than "namespace not found".
func (s *Server) namespaces() []string {
	recs, _ := s.Store.Snapshot(nil)
	seen := map[string]bool{"default": true}
	for _, r := range recs {
		if r.Ad.Identity.Namespace != "" {
			seen[r.Ad.Identity.Namespace] = true
		}
	}
	out := make([]string, 0, len(seen))
	for ns := range seen {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

func (s *Server) handleListNamespaces(w http.ResponseWriter, _ *http.Request) {
	version := s.Store.Version()
	list := &corev1.NamespaceList{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "NamespaceList"},
		ListMeta: metav1.ListMeta{ResourceVersion: version.String()},
	}
	for _, ns := range s.namespaces() {
		list.Items = append(list.Items, *namespaceObject(ns, version))
	}
	writeObject(w, http.StatusOK, list, corev1.SchemeGroupVersion)
}

func (s *Server) handleGetNamespace(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	for _, ns := range s.namespaces() {
		if ns == name {
			writeObject(w, http.StatusOK, namespaceObject(ns, s.Store.Version()), corev1.SchemeGroupVersion)
			return
		}
	}
	notFound(w, "namespaces", name)
}

func namespaceObject(name string, v store.Version) *corev1.Namespace {
	return &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			UID:             types.UID("htcondor-namespace-" + name),
			ResourceVersion: v.String(),
		},
		Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive},
	}
}
