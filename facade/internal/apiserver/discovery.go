package apiserver

import (
	"encoding/json"
	"net/http"
	"strings"

	openapiv2 "github.com/google/gnostic-models/openapiv2"
	"google.golang.org/protobuf/proto"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"
)

// Discovery is the first thing kubectl does and the thing that decides whether
// anything else works: it resolves "pods" to a group, version and kind here,
// and a document that is merely plausible produces errors much later and
// somewhere else. Everything below is what a 1.26 client actually reads.

func (s *Server) handleVersion(w http.ResponseWriter, _ *http.Request) {
	// kubectl compares Major/Minor to decide which features to use, so
	// claiming a version is claiming a contract. 1.26 is what the client
	// on this machine is, and it is the version the facade has been tested
	// against; GitVersion says plainly that this is not a real cluster.
	writeJSON(w, http.StatusOK, version.Info{
		Major:      "1",
		Minor:      "26",
		GitVersion: "v1.26.0-htcondor-facade",
		GitCommit:  "unknown",
		Platform:   "htcondor/schedd",
	})
}

func (s *Server) handleAPIVersions(w http.ResponseWriter, _ *http.Request) {
	writeObject(w, http.StatusOK, &metav1.APIVersions{
		TypeMeta:                   metav1.TypeMeta{APIVersion: "v1", Kind: "APIVersions"},
		Versions:                   []string{"v1"},
		ServerAddressByClientCIDRs: []metav1.ServerAddressByClientCIDR{},
	}, corev1.SchemeGroupVersion)
}

func (s *Server) handleAPIGroups(w http.ResponseWriter, _ *http.Request) {
	writeObject(w, http.StatusOK, &metav1.APIGroupList{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "APIGroupList"},
		Groups:   []metav1.APIGroup{batchGroup()},
	}, corev1.SchemeGroupVersion)
}

func (s *Server) handleBatchGroup(w http.ResponseWriter, _ *http.Request) {
	g := batchGroup()
	g.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "APIGroup"}
	writeObject(w, http.StatusOK, &g, corev1.SchemeGroupVersion)
}

func batchGroup() metav1.APIGroup {
	gv := metav1.GroupVersionForDiscovery{GroupVersion: "batch/v1", Version: "v1"}
	return metav1.APIGroup{
		Name:             "batch",
		Versions:         []metav1.GroupVersionForDiscovery{gv},
		PreferredVersion: gv,
	}
}

func (s *Server) handleCoreV1Resources(w http.ResponseWriter, _ *http.Request) {
	writeObject(w, http.StatusOK, &metav1.APIResourceList{
		TypeMeta:     metav1.TypeMeta{APIVersion: "v1", Kind: "APIResourceList"},
		GroupVersion: "v1",
		APIResources: []metav1.APIResource{
			{
				Name: "pods", SingularName: "pod", Namespaced: true, Kind: "Pod",
				Verbs:              metav1.Verbs{"create", "delete", "get", "list", "watch"},
				ShortNames:         []string{"po"},
				Categories:         []string{"all"},
				StorageVersionHash: "htcondor",
			},
			{Name: "pods/log", Namespaced: true, Kind: "Pod", Verbs: metav1.Verbs{"get"}},
			{Name: "pods/status", Namespaced: true, Kind: "Pod", Verbs: metav1.Verbs{"get"}},
			{
				Name: "events", SingularName: "event", Namespaced: true, Kind: "Event",
				Verbs: metav1.Verbs{"list"}, ShortNames: []string{"ev"},
			},
			{
				Name: "namespaces", SingularName: "namespace", Namespaced: false, Kind: "Namespace",
				Verbs:      metav1.Verbs{"get", "list"},
				ShortNames: []string{"ns"},
			},
		},
	}, corev1.SchemeGroupVersion)
}

func (s *Server) handleBatchV1Resources(w http.ResponseWriter, _ *http.Request) {
	writeObject(w, http.StatusOK, &metav1.APIResourceList{
		TypeMeta:     metav1.TypeMeta{APIVersion: "v1", Kind: "APIResourceList"},
		GroupVersion: "batch/v1",
		APIResources: []metav1.APIResource{
			{
				Name: "jobs", SingularName: "job", Namespaced: true, Kind: "Job",
				Group: "batch", Version: "v1",
				Verbs:              metav1.Verbs{"create", "delete", "get", "list", "watch"},
				Categories:         []string{"all"},
				StorageVersionHash: "htcondor",
			},
			{
				Name: "jobs/status", Namespaced: true, Kind: "Job",
				Group: "batch", Version: "v1", Verbs: metav1.Verbs{"get"},
			},
		},
	}, batchv1.SchemeGroupVersion)
}

// handleOpenAPIV2 serves an empty but structurally valid Swagger 2.0 document.
//
// A 404 here is not harmless: kubectl 1.26 fetches OpenAPI before `create` and
// `apply` and, finding none, stops with "the server could not find the
// requested resource" rather than sending the object. Nothing about the
// request was wrong, and the message points nowhere near the cause.
//
// The document is empty on purpose. kubectl's client-side validator skips any
// kind it cannot find a definition for, so an empty document means "validate
// nothing here" -- which is the right answer when the real validation is
// translate.Admit, running on the server, and a partial schema would reject
// Pods this server would have accepted.
//
// The cost is that `kubectl explain` has nothing to say, and that a typo in a
// manifest reaches the server instead of being caught locally. The server
// rejects it with a 422 that names the field, so it is caught either way.
// It is served as protobuf, not JSON. client-go's discovery client asks for
// OpenAPI v2 with Accept: ...+protobuf and has no JSON fallback: handing it
// JSON produces "proto: cannot parse invalid wire-format data", which is the
// same dead end as the 404 with a worse message.
func (s *Server) handleOpenAPIV2(w http.ResponseWriter, r *http.Request) {
	doc := &openapiv2.Document{
		Swagger: "2.0",
		Info: &openapiv2.Info{
			Title:   "HTCondor Kubernetes API facade",
			Version: "v1.26.0",
		},
		Paths:       &openapiv2.Paths{},
		Definitions: &openapiv2.Definitions{},
	}
	if strings.Contains(r.Header.Get("Accept"), "json") {
		writeJSON(w, http.StatusOK, map[string]any{
			"swagger":     "2.0",
			"info":        map[string]any{"title": doc.Info.Title, "version": doc.Info.Version},
			"paths":       map[string]any{},
			"definitions": map[string]any{},
		})
		return
	}
	body, err := proto.Marshal(doc)
	if err != nil {
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError,
			"encoding the OpenAPI document: %v", err)
		return
	}
	// Not the media type the client asked for: Go's own mime parser
	// rejects "application/com.github.proto-openapi.spec.v2@v1.0+protobuf"
	// ("unexpected content after media subtype") when client-go inspects
	// the response, so echoing it back turns a working document into an
	// InternalError. The client reads the body regardless of this header.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// handleOpenAPIAbsent answers the v3 schema requests with a 404. A 1.26
// client falls back to v2, which is served above.
func (s *Server) handleOpenAPIAbsent(w http.ResponseWriter, _ *http.Request) {
	writeStatus(w, http.StatusNotFound, metav1.StatusReasonNotFound,
		"this server publishes only an OpenAPI v2 document")
}

// handleEmptyEvents exists so `kubectl describe` does not end with an error.
//
// There are no Events here: the things Kubernetes would report as events --
// scheduled, pulled, failed -- are in the job ad and the hold reason, and are
// already surfaced as the Pod's status. An empty list says that plainly; a 404
// makes describe look broken.
func (s *Server) handleEmptyEvents(w http.ResponseWriter, _ *http.Request) {
	writeObject(w, http.StatusOK, &corev1.EventList{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "EventList"},
		ListMeta: metav1.ListMeta{ResourceVersion: s.Store.Version().String()},
	}, corev1.SchemeGroupVersion)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError, "%v", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}
