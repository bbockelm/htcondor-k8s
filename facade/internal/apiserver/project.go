package apiserver

import (
	"encoding/json"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
	"github.com/bbockelm/htcondor-k8s/facade/internal/store"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// Annotations the facade adds on the way out, so a user looking at a Pod can
// find the job behind it without being told the mapping.
const (
	AnnoJobID  = "htcondor.org/job-id"
	AnnoOwner  = "htcondor.org/owner"
	AnnoSchedd = "htcondor.org/schedd"
	AnnoIWD    = "htcondor.org/spool-dir"
)

// PodName is the Kubernetes name of the Pod a job ad stands for.
//
// The job ad is authoritative. A standalone Pod is one proc and carries its
// own name; a Job's procs each carry their own, stamped per proc at submit
// time via $(ProcId), in the same "<job>-<suffix>" shape a real Job's Pods
// have, with the suffix chosen to be meaningful instead of random.
//
// Deriving it here instead would mean the operator and the facade each
// inventing a name for the same Pod, which is why the derivation lives in
// translate.ProcPodName and the result is written into the queue.
//
// The cost is that a standalone Pod literally named "foo-0" is
// indistinguishable from proc 0 of Job "foo"; see the lookup in podByName for
// how that is resolved.
func PodName(rec backend.JobRecord) string {
	return rec.Ad.Identity.Name
}

// PodUID is the Pod's identity, likewise read from the ad rather than
// recomputed. Two Pods sharing a UID would break every client that caches by
// it, so the per-proc value is stamped at submit time.
func PodUID(rec backend.JobRecord) types.UID {
	return types.UID(rec.Ad.Identity.UID)
}

// Pod projects one job ad as a Pod.
func (s *Server) Pod(rec backend.JobRecord, rv store.Version) *corev1.Pod {
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:              PodName(rec),
			Namespace:         rec.Ad.Identity.Namespace,
			UID:               PodUID(rec),
			ResourceVersion:   rv.String(),
			Labels:            rec.Ad.Identity.Labels,
			Annotations:       map[string]string{},
			CreationTimestamp: metav1.NewTime(rec.Ad.QDate),
		},
	}
	for k, v := range rec.Ad.Identity.Annotations {
		pod.Annotations[k] = v
	}
	pod.Annotations[AnnoJobID] = rec.Key()
	if rec.Owner != "" {
		pod.Annotations[AnnoOwner] = rec.Owner
	}
	if s.ScheddName != "" {
		pod.Annotations[AnnoSchedd] = s.ScheddName
	}
	if rec.IWD != "" {
		pod.Annotations[AnnoIWD] = rec.IWD
	}
	if rec.Ad.Index >= 0 {
		pod.Annotations[batchv1.JobCompletionIndexAnnotation] = strconv.Itoa(rec.Ad.Index)
	}

	if rec.Ad.Identity.OwnerKind != "" {
		controller := true
		pod.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "batch/v1",
			Kind:       rec.Ad.Identity.OwnerKind,
			Name:       rec.Ad.Identity.OwnerName,
			UID:        types.UID(rec.Ad.Identity.OwnerUID),
			Controller: &controller,
		}}
	}

	containerName := "container"
	if rec.PodSpec != nil {
		pod.Spec = *rec.PodSpec
		if len(pod.Spec.Containers) > 0 {
			containerName = pod.Spec.Containers[0].Name
		}
	}
	// The slot the job matched, reported where kubectl -o wide looks for
	// it. Admission rejects nodeName on the way in -- a user may not pin a
	// job to a machine -- but reporting where it landed on the way out is
	// the only answer to "where is this running".
	pod.Spec.NodeName = hostOnly(rec.Ad.RemoteHost)

	pod.Status = translate.PodStatus(rec.Ad, containerName)
	if len(pod.Status.ContainerStatuses) > 0 && len(pod.Spec.Containers) > 0 {
		pod.Status.ContainerStatuses[0].Image = pod.Spec.Containers[0].Image
		pod.Status.ContainerStatuses[0].ImageID = pod.Spec.Containers[0].Image
		pod.Status.ContainerStatuses[0].ContainerID = "htcondor://" + rec.Key()
	}
	return pod
}

// Job projects every proc of one cluster as a batch/v1 Job.
func (s *Server) Job(recs []backend.JobRecord, rv store.Version) *batchv1.Job {
	if len(recs) == 0 {
		return nil
	}
	head := recs[0]
	job := &batchv1.Job{
		TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{
			Name:              head.Ad.Identity.OwnerName,
			Namespace:         head.Ad.Identity.Namespace,
			UID:               types.UID(head.Ad.Identity.OwnerUID),
			ResourceVersion:   rv.String(),
			Labels:            head.Ad.Identity.Labels,
			Annotations:       map[string]string{AnnoJobID: strconv.Itoa(head.Ad.ClusterID)},
			CreationTimestamp: metav1.NewTime(head.Ad.QDate),
		},
	}
	if s.ScheddName != "" {
		job.Annotations[AnnoSchedd] = s.ScheddName
	}
	if head.JobSpecJSON != "" {
		// A Job whose spec will not parse is still worth serving: its
		// status comes from the procs, and an empty spec is a visible
		// problem where a 500 is just an outage.
		_ = json.Unmarshal([]byte(head.JobSpecJSON), &job.Spec)
	}
	if head.PodSpec != nil {
		job.Spec.Template.Spec = *head.PodSpec
	}

	ads := make([]translate.JobAd, 0, len(recs))
	var earliest *metav1.Time
	for _, r := range recs {
		ads = append(ads, r.Ad)
		if !r.Ad.JobStartDate.IsZero() {
			t := metav1.NewTime(r.Ad.JobStartDate)
			if earliest == nil || t.Before(earliest) {
				earliest = &t
			}
		}
	}
	completions := int32(len(recs))
	if job.Spec.Completions != nil {
		completions = *job.Spec.Completions
	}
	job.Status = translate.JobStatus(ads, completions, earliest)
	return job
}

// hostOnly strips the slot prefix off a RemoteHost ("slot1@node.example" ->
// "node.example"), because a Kubernetes node name has no slot in it and a
// client that joins on nodeName would never match.
func hostOnly(h string) string {
	if i := strings.LastIndex(h, "@"); i >= 0 {
		return h[i+1:]
	}
	return h
}

// ClusterOf groups records into clusters, preserving proc order. Jobs are a
// whole cluster, so every job-level answer starts here.
func ClusterOf(recs []backend.JobRecord) map[int][]backend.JobRecord {
	out := map[int][]backend.JobRecord{}
	for _, r := range recs {
		out[r.Ad.ClusterID] = append(out[r.Ad.ClusterID], r)
	}
	return out
}
