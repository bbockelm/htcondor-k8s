package podctl

import (
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
)

// Bookkeeping Kubernetes itself puts on a Job-created Pod. None of it is user
// intent, and carrying it into the job ad's +K8sLabels would make the Job
// controller's own join keys look like something a user could select on.
const (
	labelControllerUID       = "batch.kubernetes.io/controller-uid"
	labelJobName             = "batch.kubernetes.io/job-name"
	legacyLabelControllerUID = "controller-uid"
	legacyLabelJobName       = "job-name"

	// annotationCompletionIndex is what the Job controller stamps on each
	// Pod of an Indexed Job. We read it rather than deriving an index of
	// our own: the Job controller already assigned one, already injected
	// JOB_COMPLETION_INDEX into the container's environment, and two
	// sources for the same number is how they come to disagree.
	annotationCompletionIndex = "batch.kubernetes.io/job-completion-index"
)

// AttrVirtualNode is stamped on every job this controller submits, naming the
// virtual node the Pod was bound to.
//
// It exists for the reaper. Finding "every job this integration owns" needs a
// constraint, and there is no attribute in the shared schema that means that:
// K8sPodUID identifies one Pod and K8sOwnerUID identifies one Job. A job may
// also legitimately outlive the Pod object that named it, so the sweep cannot
// be driven from the Kubernetes side alone. SubmitOptions.ExtraAttributes is
// the sanctioned place for a deployment to add its own, which is what this is.
const AttrVirtualNode = "K8sVirtualNode"

// normalizeForAdmission returns the PodSpec as its author wrote it, with the
// fields this integration added taken back out.
//
// This exists because of a trap the HTCJob operator fell into first and wrote
// up in docs/OPERATOR.md: the API server defaults .spec.selector after
// mutating admission, so translate.AdmitJob run on the *stored* object
// rejected a Job the webhook had legitimately claimed, and the Job sat
// suspended forever. The shape of the bug is "admission is re-run on an object
// that something downstream has since written to", and this approach has two
// of its own instances of it:
//
//   - the webhook adds a toleration for the virtual node's taint, and
//     translate.Admit rejects tolerations outright ("there are no node taints
//     to tolerate");
//   - binding sets .spec.nodeName, and translate.Admit rejects nodeName ("a
//     job is matched to a slot by the negotiator and cannot be pinned").
//
// Both are fields we put there. Removing exactly those -- our toleration, by
// key and value, and our node name, by name -- and leaving everything else
// alone means a user's own toleration or a hand-set nodeName still gets
// refused, which is the point. The admitted spec is also what gets recorded as
// +K8sPodSpec, so what a reader gets back out of the job ad is the Pod as
// written rather than the Pod as we rewrote it.
func normalizeForAdmission(pod *corev1.Pod, nodeName string) *corev1.PodSpec {
	spec := pod.Spec.DeepCopy()

	if spec.NodeName == nodeName {
		spec.NodeName = ""
	}

	kept := spec.Tolerations[:0]
	for _, t := range spec.Tolerations {
		if t.Key == api.TaintKey && t.Value == api.TaintValue && t.Effect == corev1.TaintEffectNoSchedule {
			continue
		}
		kept = append(kept, t)
	}
	if len(kept) == 0 {
		spec.Tolerations = nil
	} else {
		spec.Tolerations = kept
	}

	return spec
}

// identityFor names the Kubernetes object a job stands for.
//
// Unlike the operator, which stamps the HTCJob's identity because that is the
// object it owns, this stamps the Pod's. The Pod is the unit of submission
// here -- one Pod, one cluster, one proc -- and its UID is what makes the
// submission idempotent, because the API server assigned it before this
// controller ever saw the object and will not reuse it for a replacement Pod
// of the same name.
//
// The owner is recorded too, so a human with condor_q can tell which Job a
// job belongs to, but nothing here queries on it: two Pods of one Job share an
// owner UID and would adopt each other's clusters.
func identityFor(pod *corev1.Pod) translate.Identity {
	id := translate.Identity{
		Name:        pod.Name,
		Namespace:   pod.Namespace,
		UID:         string(pod.UID),
		Labels:      userLabels(pod.Labels),
		Annotations: pod.Annotations,
	}
	if owner := metav1.GetControllerOf(pod); owner != nil {
		id.OwnerKind = owner.Kind
		id.OwnerName = owner.Name
		id.OwnerUID = string(owner.UID)
	}
	return id
}

func userLabels(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		switch k {
		case labelControllerUID, labelJobName, legacyLabelControllerUID, legacyLabelJobName:
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// completionIndex reads the index the Job controller assigned to this Pod.
//
// Indexed completion is one of the things this approach gets for free. The
// built-in Job controller stamps the index and injects JOB_COMPLETION_INDEX
// into the container environment itself, so by the time a Pod reaches here the
// work is done; all that is left is to copy the number onto the job ad so
// condor_q and the facade can see it.
func completionIndex(pod *corev1.Pod) (int, bool) {
	v, ok := pod.Annotations[annotationCompletionIndex]
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}
