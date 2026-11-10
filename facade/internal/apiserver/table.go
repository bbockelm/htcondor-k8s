package apiserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/duration"
)

// Kubernetes moved column formatting from the client to the server: kubectl
// asks for `application/json;as=Table` and prints whatever columns come back.
// A server that ignores that gets the generic fallback printer, which knows
// only NAME and AGE -- so `kubectl get pods` against a facade without this
// cannot tell you whether anything is running. Serving a Table is what makes
// the output look like Kubernetes, and it is also the one place the facade
// gets to add a column Kubernetes does not have: the HTCondor job id.

// wantsTable reports whether the client asked for server-side printing.
func wantsTable(r *http.Request) bool {
	for _, clause := range strings.Split(r.Header.Get("Accept"), ",") {
		if strings.Contains(clause, "as=Table") {
			return true
		}
	}
	return false
}

func newTable(rv string, cols []metav1.TableColumnDefinition) *metav1.Table {
	return &metav1.Table{
		TypeMeta:          metav1.TypeMeta{APIVersion: "meta.k8s.io/v1", Kind: "Table"},
		ListMeta:          metav1.ListMeta{ResourceVersion: rv},
		ColumnDefinitions: cols,
	}
}

// Priority 1 columns are the ones kubectl shows only for -o wide.
var podColumns = []metav1.TableColumnDefinition{
	{Name: "Name", Type: "string", Format: "name", Description: "the pod's name"},
	{Name: "Ready", Type: "string", Description: "containers ready over containers total"},
	{Name: "Status", Type: "string", Description: "the pod's phase, or why it is not in one"},
	{Name: "Restarts", Type: "string", Description: "how many times HTCondor has started the job"},
	{Name: "Age", Type: "string", Description: "time since the job was queued"},
	{Name: "Node", Type: "string", Priority: 1, Description: "the execute node the job matched"},
	{Name: "Job", Type: "string", Priority: 1, Description: "the HTCondor cluster.proc behind this pod"},
	{Name: "Owner", Type: "string", Priority: 1, Description: "the HTCondor user the job runs as"},
}

func podRow(pod *corev1.Pod) metav1.TableRow {
	ready, total := 0, len(pod.Status.ContainerStatuses)
	restarts := int32(0)
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Ready {
			ready++
		}
		restarts += cs.RestartCount
	}
	// The reason is more useful than the phase when there is one: "Held"
	// and "Idle" are the two states a user of this system actually needs
	// to tell apart, and both read as Pending.
	status := string(pod.Status.Phase)
	if pod.Status.Reason != "" {
		status = pod.Status.Reason
	}
	raw, _ := encode(pod, corev1.SchemeGroupVersion)
	return metav1.TableRow{
		Cells: []any{
			pod.Name,
			fmt.Sprintf("%d/%d", ready, total),
			status,
			fmt.Sprint(restarts),
			age(pod.CreationTimestamp),
			orNone(pod.Spec.NodeName),
			orNone(pod.Annotations[AnnoJobID]),
			orNone(pod.Annotations[AnnoOwner]),
		},
		Object: runtime.RawExtension{Raw: raw},
	}
}

var jobColumns = []metav1.TableColumnDefinition{
	{Name: "Name", Type: "string", Format: "name", Description: "the job's name"},
	{Name: "Completions", Type: "string", Description: "succeeded procs over requested completions"},
	{Name: "Duration", Type: "string", Description: "how long the job has been running"},
	{Name: "Age", Type: "string", Description: "time since the cluster was queued"},
	{Name: "Active", Type: "string", Priority: 1, Description: "procs not yet in a terminal state"},
	{Name: "Failed", Type: "string", Priority: 1, Description: "procs that will not succeed"},
	{Name: "Cluster", Type: "string", Priority: 1, Description: "the HTCondor cluster behind this job"},
}

func jobRow(job *batchv1.Job) metav1.TableRow {
	completions := "<none>"
	if job.Spec.Completions != nil {
		completions = fmt.Sprintf("%d/%d", job.Status.Succeeded, *job.Spec.Completions)
	}
	dur := "<none>"
	if job.Status.StartTime != nil {
		end := time.Now()
		if job.Status.CompletionTime != nil {
			end = job.Status.CompletionTime.Time
		}
		dur = duration.HumanDuration(end.Sub(job.Status.StartTime.Time))
	}
	raw, _ := encode(job, batchv1.SchemeGroupVersion)
	return metav1.TableRow{
		Cells: []any{
			job.Name, completions, dur, age(job.CreationTimestamp),
			fmt.Sprint(job.Status.Active), fmt.Sprint(job.Status.Failed),
			orNone(job.Annotations[AnnoJobID]),
		},
		Object: runtime.RawExtension{Raw: raw},
	}
}

// writeTable sends a Table. It is encoded by hand rather than through the
// scheme because metav1.Table lives in meta.k8s.io/v1, which is not a group
// version the core scheme will encode into.
func writeTable(w http.ResponseWriter, table *metav1.Table) {
	body, err := json.Marshal(table)
	if err != nil {
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError,
			"encoding the table: %v", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func age(t metav1.Time) string {
	if t.IsZero() {
		return "<unknown>"
	}
	return duration.HumanDuration(time.Since(t.Time))
}

func orNone(s string) string {
	if s == "" {
		return "<none>"
	}
	return s
}

var _ = schema.GroupVersion{}
