// Package apiserver serves the subset of the Kubernetes API that a kubectl
// client needs, backed by an HTCondor access point instead of etcd.
package apiserver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

var (
	scheme = runtime.NewScheme()
	codecs = serializer.NewCodecFactory(scheme)
)

func init() {
	// The API machinery's own scheme is what fills in apiVersion and kind
	// on the way out and strips them on the way in. Hand-rolling that is
	// where a facade starts disagreeing with kubectl in ways that only
	// show up for one verb on one resource.
	utilMust(corev1.AddToScheme(scheme))
	utilMust(batchv1.AddToScheme(scheme))
	metav1.AddToGroupVersion(scheme, corev1.SchemeGroupVersion)
	metav1.AddToGroupVersion(scheme, batchv1.SchemeGroupVersion)
	// metav1 types served outside any group version: the discovery
	// documents and Status are encoded against this.
	scheme.AddUnversionedTypes(schema.GroupVersion{Group: "", Version: "v1"},
		&metav1.Status{}, &metav1.APIVersions{}, &metav1.APIGroupList{},
		&metav1.APIGroup{}, &metav1.APIResourceList{})
}

func utilMust(err error) {
	if err != nil {
		panic(fmt.Sprintf("apiserver: building the scheme: %v", err))
	}
}

// encode renders an object with its apiVersion and kind filled in.
func encode(obj runtime.Object, gv schema.GroupVersion) ([]byte, error) {
	return runtime.Encode(codecs.LegacyCodec(gv), obj)
}

// writeObject sends one object as JSON.
func writeObject(w http.ResponseWriter, status int, obj runtime.Object, gv schema.GroupVersion) {
	body, err := encode(obj, gv)
	if err != nil {
		writeRawStatus(w, newStatus(http.StatusInternalServerError, metav1.StatusReasonInternalError,
			fmt.Sprintf("encoding the response: %v", err)))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// newStatus builds the error body kubectl parses and prints. Returning a bare
// HTTP code instead leaves the user with "error: the server rejected our
// request" and nothing else, which for an admission failure is exactly the
// information they needed.
func newStatus(code int, reason metav1.StatusReason, message string) *metav1.Status {
	return &metav1.Status{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
		Status:   metav1.StatusFailure,
		Code:     int32(code),
		Reason:   reason,
		Message:  message,
	}
}

func writeStatus(w http.ResponseWriter, code int, reason metav1.StatusReason, format string, args ...any) {
	writeRawStatus(w, newStatus(code, reason, fmt.Sprintf(format, args...)))
}

func writeRawStatus(w http.ResponseWriter, st *metav1.Status) {
	body, err := json.Marshal(st)
	if err != nil {
		http.Error(w, st.Message, int(st.Code))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(int(st.Code))
	_, _ = w.Write(body)
}

// notFound is the 404 kubectl turns into `Error from server (NotFound)`.
func notFound(w http.ResponseWriter, resource, name string) {
	st := newStatus(http.StatusNotFound, metav1.StatusReasonNotFound,
		fmt.Sprintf("%s %q not found", resource, name))
	st.Details = &metav1.StatusDetails{Name: name, Kind: resource}
	writeRawStatus(w, st)
}

// gone is the 410 that tells a client its watch cursor is unusable and it must
// re-list. Returning anything else here -- an empty stream, a 500 -- leaves
// the client believing it is still up to date.
func gone(w http.ResponseWriter, message string) {
	writeRawStatus(w, newStatus(http.StatusGone, metav1.StatusReasonExpired, message))
}

// decodeInto reads a request body into obj, reporting the failure as a Status.
//
// It accepts whatever the universal deserializer accepts, which is both JSON
// and YAML: kubectl converts manifests to JSON before sending them, but a
// `curl -H 'Content-Type: application/yaml'` is a reasonable thing to do
// against a server with no client library.
func (s *Server) decodeInto(w http.ResponseWriter, r *http.Request, obj runtime.Object) bool {
	defer func() { _ = r.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody))
	if err != nil {
		writeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest, "reading the request body: %v", err)
		return false
	}
	if len(body) == 0 {
		writeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest, "the request body is empty")
		return false
	}
	if _, _, err := codecs.UniversalDeserializer().Decode(body, nil, obj); err != nil {
		writeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest, "decoding the request body: %v", err)
		return false
	}
	return true
}

// maxRequestBody bounds what an unauthenticated client can make this process
// allocate. A Pod manifest that does not fit in a megabyte is not a Pod
// manifest.
const maxRequestBody = 1 << 20
