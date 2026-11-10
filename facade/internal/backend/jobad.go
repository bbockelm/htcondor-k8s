package backend

import (
	"encoding/json"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	corev1 "k8s.io/api/core/v1"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// FromClassAd decodes a job ad into the shape the rest of the facade reads.
//
// Nothing here fails: a job ad that is missing an attribute is a job ad the
// schedd has not filled in yet (JobStartDate before the job starts) or one
// that is not ours. Returning an error for every such ad would make a single
// stray job break a whole list, so missing values become zero values and the
// caller decides whether the record is usable by looking at Ad.Identity.Name.
func FromClassAd(ad *classad.ClassAd) JobRecord {
	var r JobRecord
	r.Ad.ClusterID = int(attrInt(ad, "ClusterId"))
	r.Ad.ProcID = int(attrInt(ad, "ProcId"))
	r.Ad.JobStatus = int(attrInt(ad, "JobStatus"))
	r.Ad.ExitCode = int(attrInt(ad, "ExitCode"))
	r.Ad.ExitBySignal = attrBool(ad, "ExitBySignal")
	r.Ad.ExitSignal = int(attrInt(ad, "ExitSignal"))
	r.Ad.HoldReason, _ = ad.EvaluateAttrString("HoldReason")
	r.Ad.HoldReasonCode = int(attrInt(ad, "HoldReasonCode"))
	r.Ad.NumJobStarts = int(attrInt(ad, "NumJobStarts"))

	r.Ad.QDate = attrTime(ad, "QDate")
	r.Ad.JobStartDate = attrTime(ad, "JobStartDate")
	r.Ad.EnteredCurrentStatus = attrTime(ad, "EnteredCurrentStatus")
	r.Ad.CompletionDate = attrTime(ad, "CompletionDate")

	// RemoteHost is only present while the job runs; LastRemoteHost
	// survives it, and a finished Pod that can still name the machine it
	// ran on is strictly more useful than one that reports nothing.
	if h, ok := ad.EvaluateAttrString("RemoteHost"); ok && h != "" {
		r.Ad.RemoteHost = h
	} else if h, ok := ad.EvaluateAttrString("LastRemoteHost"); ok {
		r.Ad.RemoteHost = h
	}

	id := &r.Ad.Identity
	id.Name, _ = ad.EvaluateAttrString(translate.AttrPodName)
	id.Namespace, _ = ad.EvaluateAttrString(translate.AttrPodNamespace)
	id.UID, _ = ad.EvaluateAttrString(translate.AttrPodUID)
	id.OwnerKind, _ = ad.EvaluateAttrString(translate.AttrOwnerKind)
	id.OwnerName, _ = ad.EvaluateAttrString(translate.AttrOwnerName)
	id.OwnerUID, _ = ad.EvaluateAttrString(translate.AttrOwnerUID)
	id.Labels = attrStringMap(ad, translate.AttrLabels)
	id.Annotations = attrStringMap(ad, translate.AttrAnnotations)

	if idx, ok := ad.EvaluateAttrInt(translate.AttrIndex); ok {
		r.Ad.Index = int(idx)
	} else {
		r.Ad.Index = -1
	}

	if raw, ok := ad.EvaluateAttrString(translate.AttrPodSpec); ok && raw != "" {
		var spec corev1.PodSpec
		if json.Unmarshal([]byte(raw), &spec) == nil {
			r.PodSpec = &spec
		}
	}
	r.JobSpecJSON, _ = ad.EvaluateAttrString(AttrJobSpec)
	r.IWD, _ = ad.EvaluateAttrString(AttrIWD)
	if r.IWD == "" {
		// Submitted by something that did not stamp the attribute; the
		// schedd's own Iwd is the same directory.
		r.IWD, _ = ad.EvaluateAttrString("Iwd")
	}
	r.Owner, _ = ad.EvaluateAttrString("Owner")
	return r
}

// Usable reports whether the record names a Kubernetes object. A job with no
// K8sPodName is somebody else's work and must not appear as a Pod.
func (r JobRecord) Usable() bool {
	return r.Ad.Identity.Name != "" && r.Ad.ClusterID > 0
}

func attrInt(ad *classad.ClassAd, name string) int64 {
	v, _ := ad.EvaluateAttrInt(name)
	return v
}

func attrBool(ad *classad.ClassAd, name string) bool {
	v, _ := ad.EvaluateAttrBool(name)
	return v
}

// attrTime reads an HTCondor timestamp, which is always seconds since the
// epoch. Zero means "has not happened yet" and must stay the zero time, since
// shared/translate keys several decisions on IsZero.
func attrTime(ad *classad.ClassAd, name string) time.Time {
	v, ok := ad.EvaluateAttrInt(name)
	if !ok || v <= 0 {
		return time.Time{}
	}
	return time.Unix(v, 0).UTC()
}

// attrStringMap reads one of the JSON-encoded map attributes (labels,
// annotations). A map that will not parse is dropped rather than reported:
// losing a label is bad, but failing a whole list because one job carries a
// mangled attribute is worse.
func attrStringMap(ad *classad.ClassAd, name string) map[string]string {
	raw, ok := ad.EvaluateAttrString(name)
	if !ok || raw == "" {
		return nil
	}
	var m map[string]string
	if json.Unmarshal([]byte(raw), &m) != nil {
		return nil
	}
	return m
}

// Visible reports whether the record should appear as a Kubernetes object.
//
// A job in JobStatus 3 has been removed and is on its way out of the queue.
// condor_rm is the only way a Pod is deleted here, so a removed job *is* a
// deleted object: leaving it visible would make `kubectl delete pod` look like
// it silently failed for as long as the schedd took to drain it.
func (r JobRecord) Visible() bool {
	return r.Usable() && r.Ad.JobStatus != translate.JobStatusRemoved
}
