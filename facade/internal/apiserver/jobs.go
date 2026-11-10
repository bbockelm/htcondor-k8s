package apiserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	"github.com/google/uuid"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
	"github.com/bbockelm/htcondor-k8s/facade/internal/store"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// A batch/v1 Job is one HTCondor cluster and its Pods are that cluster's
// procs. The mapping is close enough to be worth making: both are "run this N
// times and tell me when they are done", and HTCondor already has the retry,
// the completion accounting and the parallel execution that a Job controller
// would otherwise have to supply. What it does not have is a controller that
// owns the Job object, so everything below reads the cluster and derives the
// Job from it rather than storing a Job anywhere.

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	var job batchv1.Job
	if !s.decodeInto(w, r, &job) {
		return
	}
	if job.Namespace == "" {
		job.Namespace = ns
	}
	if job.Namespace != ns {
		writeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest,
			"the object's namespace %q does not match the URL's %q", job.Namespace, ns)
		return
	}
	if job.Name == "" && job.GenerateName != "" {
		job.Name = job.GenerateName + randomSuffix()
	}
	if !dnsName.MatchString(job.Name) || len(job.Name) > 253 {
		writeStatus(w, http.StatusUnprocessableEntity, metav1.StatusReasonInvalid,
			"metadata.name %q is not a valid DNS-1123 label", job.Name)
		return
	}

	if rejections := translate.AdmitJob(&job.Spec, &job.ObjectMeta, s.Caps); !rejections.OK() {
		writeRawStatus(w, rejectionStatus("Job", job.Name, rejections))
		return
	}
	if _, _, found := s.jobByName(job.Namespace, job.Name); found {
		st := newStatus(http.StatusConflict, metav1.StatusReasonAlreadyExists,
			fmt.Sprintf("jobs.batch %q already exists", job.Name))
		st.Details = &metav1.StatusDetails{Name: job.Name, Kind: "jobs", Group: "batch"}
		writeRawStatus(w, st)
		return
	}

	uid := string(job.UID)
	if uid == "" {
		uid = uuid.NewString()
	}
	iwd, err := s.spoolDir(job.Namespace, job.Name)
	if err != nil {
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError,
			"creating the job's spool directory: %v", err)
		return
	}

	opts := translate.JobSubmitOptions(&job.Spec, s.Caps)
	opts.IWD = iwd
	// The Job fields live nowhere else: translate stamps the PodSpec but
	// the Job's completions, backoffLimit and completion mode are what
	// make `kubectl get job -o yaml` round-trip, and the job queue is the
	// only store there is.
	specJSON, err := json.Marshal(job.Spec)
	if err != nil {
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError,
			"encoding the job spec: %v", err)
		return
	}
	opts.ExtraAttributes = map[string]string{
		backend.AttrJobSpec: backend.AdString(string(specJSON)),
		backend.AttrIWD:     backend.AdString(iwd),
	}

	// Labels on the Job's Pods are the Job's own labels plus the one a
	// real Job controller adds, so a selector written for Kubernetes finds
	// the same Pods here.
	podLabels := map[string]string{}
	for k, v := range job.Spec.Template.Labels {
		podLabels[k] = v
	}
	for k, v := range job.Labels {
		podLabels[k] = v
	}
	podLabels["job-name"] = job.Name
	podLabels["batch.kubernetes.io/job-name"] = job.Name

	submitText, err := translate.PodToSubmit(&job.Spec.Template.Spec, translate.Identity{
		Name:        job.Name,
		Namespace:   job.Namespace,
		UID:         uid,
		OwnerKind:   "Job",
		OwnerName:   job.Name,
		OwnerUID:    uid,
		Labels:      podLabels,
		Annotations: job.Spec.Template.Annotations,
	}, opts)
	if err != nil {
		writeStatus(w, http.StatusUnprocessableEntity, metav1.StatusReasonInvalid,
			"this job cannot be rendered as a submit file: %v", err)
		return
	}

	recs, err := s.submitAndRefresh(r.Context(), submitText)
	if err != nil {
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError,
			"the access point refused the job: %v", err)
		return
	}
	writeObject(w, http.StatusCreated, s.Job(recs, s.clusterVersion(recs)), batchv1.SchemeGroupVersion)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	recs, rv, ok := s.jobByName(r.PathValue("ns"), r.PathValue("name"))
	if !ok {
		notFound(w, "jobs.batch", r.PathValue("name"))
		return
	}
	job := s.Job(recs, rv)
	if wantsTable(r) {
		table := newTable(rv.String(), jobColumns)
		table.Rows = append(table.Rows, jobRow(job))
		writeTable(w, table)
		return
	}
	writeObject(w, http.StatusOK, job, batchv1.SchemeGroupVersion)
}

func (s *Server) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	recs, rv, ok := s.jobByName(r.PathValue("ns"), name)
	if !ok {
		notFound(w, "jobs.batch", name)
		return
	}
	ids := make([]string, 0, len(recs))
	for _, rec := range recs {
		ids = append(ids, rec.Key())
	}
	// Deleting a Job deletes its Pods, as in Kubernetes: here they are the
	// same cluster, so one condor_rm does both and there is no window in
	// which orphaned Pods exist.
	if err := s.Backend.Remove(r.Context(), ids, "deleted through the Kubernetes API facade"); err != nil {
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError,
			"removing cluster %d: %v", recs[0].Ad.ClusterID, err)
		return
	}
	s.Store.Delete(ids...)
	writeObject(w, http.StatusOK, s.Job(recs, rv), batchv1.SchemeGroupVersion)
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	filter, err := parseFilter(r.URL.Query())
	if err != nil {
		writeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest, "%v", err)
		return
	}
	if watchRequested(r) {
		s.watchJobs(w, r, ns, filter)
		return
	}

	recs, version := s.Store.Snapshot(jobsIn(ns))
	var jobs []*batchv1.Job
	for _, cluster := range sortedClusters(ClusterOf(recs)) {
		job := s.Job(cluster, s.clusterVersion(cluster))
		if job != nil && filter.matchesMeta(job.Name, job.Namespace, job.Labels) {
			jobs = append(jobs, job)
		}
	}
	if wantsTable(r) {
		table := newTable(version.String(), jobColumns)
		for _, job := range jobs {
			table.Rows = append(table.Rows, jobRow(job))
		}
		writeTable(w, table)
		return
	}
	list := &batchv1.JobList{
		TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "JobList"},
		ListMeta: metav1.ListMeta{ResourceVersion: version.String()},
	}
	for _, job := range jobs {
		list.Items = append(list.Items, *job)
	}
	writeObject(w, http.StatusOK, list, batchv1.SchemeGroupVersion)
}

// jobByName returns every proc of the cluster behind one Job.
func (s *Server) jobByName(ns, name string) ([]backend.JobRecord, store.Version, bool) {
	recs, _ := s.Store.Snapshot(func(r backend.JobRecord) bool {
		return (ns == "" || r.Ad.Identity.Namespace == ns) &&
			r.Ad.Identity.OwnerKind == "Job" && r.Ad.Identity.OwnerName == name
	})
	if len(recs) == 0 {
		return nil, store.Version{}, false
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].Ad.ProcID < recs[j].Ad.ProcID })
	return recs, s.clusterVersion(recs), true
}

// clusterVersion is the Job's resourceVersion: the newest version of any of
// its procs. A Job changes exactly when one of its Pods does, so this is both
// monotone and the right value to resume a watch from.
func (s *Server) clusterVersion(recs []backend.JobRecord) store.Version {
	var newest store.Version
	for _, r := range recs {
		if v, ok := s.Store.VersionOf(r.Key()); ok && newest.Before(v) {
			newest = v
		}
	}
	return newest
}

func jobsIn(ns string) func(backend.JobRecord) bool {
	return func(r backend.JobRecord) bool {
		if r.Ad.Identity.OwnerKind != "Job" {
			return false
		}
		return ns == "" || r.Ad.Identity.Namespace == ns
	}
}

func sortedClusters(m map[int][]backend.JobRecord) [][]backend.JobRecord {
	ids := make([]int, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	out := make([][]backend.JobRecord, 0, len(ids))
	for _, id := range ids {
		recs := m[id]
		sort.Slice(recs, func(i, j int) bool { return recs[i].Ad.ProcID < recs[j].Ad.ProcID })
		out = append(out, recs)
	}
	return out
}

func watchRequested(r *http.Request) bool {
	v := r.URL.Query().Get("watch")
	return v == "true" || v == "1"
}
