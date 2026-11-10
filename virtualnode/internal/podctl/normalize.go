package podctl

import (
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/webhook"
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
//   - the webhook adds tolerations -- for the virtual node's taint and for the
//     not-ready/unreachable taints that would otherwise have the taint manager
//     delete live Pods five minutes after a restart -- and translate.Admit
//     rejects tolerations outright ("there are no node taints to tolerate");
//   - binding sets .spec.nodeName, and translate.Admit rejects nodeName ("a
//     job is matched to a slot by the negotiator and cannot be pinned").
//
// And a third that nothing we wrote put there at all. The API server's
// ServiceAccount admission plugin injects a projected `kube-api-access-<rand>`
// volume, and a matching read-only mount at the well-known service-account
// path, into *every* Pod in a real cluster. translate.Admit rejects volumes
// and volume mounts, so without stripping it this integration refuses every
// Pod anywhere outside a bare test API server -- which is exactly how it
// behaved until a k3s cluster was pointed at it. The envtest suite never saw
// it because the plugin is not enabled there.
//
// Stripping it is the honest reading rather than a convenience: the token
// grants access to an API server the job cannot reach, because the job runs on
// an HTCondor execute node outside the cluster. A deployment that wants to be
// explicit about this can set automountServiceAccountToken: false on the pod
// template, which stops the injection at the source; this handles the Pods
// that did not.
//
// The first two are fields we put there. Removing exactly those -- the tolerations from
// webhook.Tolerations, matched whole, and our node name, by name -- and
// leaving everything else alone means a user's own toleration or a hand-set
// nodeName still gets refused, which is the point. The list is imported rather
// than restated so that adding a fourth toleration in the webhook cannot
// quietly start failing admission here.
//
// The admitted spec is also what gets recorded as +K8sPodSpec, so what a
// reader gets back out of the job ad is the Pod as written rather than the Pod
// as we rewrote it.
func normalizeForAdmission(pod *corev1.Pod, nodeName string) *corev1.PodSpec {
	spec := pod.Spec.DeepCopy()

	if spec.NodeName == nodeName {
		spec.NodeName = ""
	}

	stripServiceAccountToken(spec)

	kept := spec.Tolerations[:0]
	for _, t := range spec.Tolerations {
		if isOurToleration(t) {
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

// serviceAccountTokenPath is where the ServiceAccount admission plugin mounts
// the projected token it injects.
const serviceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount"

// serviceAccountVolumePrefix is the generated name's fixed part. Matching on
// the name alone would be too loose -- a user may name a volume anything -- so
// the source is checked as well.
const serviceAccountVolumePrefix = "kube-api-access-"

// stripServiceAccountToken removes the volume and mounts the API server
// injected, and nothing else. A user's own projected volume, or their own
// mount at that path, is left in place and will be refused by admission, which
// is the correct answer: this integration cannot honor it.
func stripServiceAccountToken(spec *corev1.PodSpec) {
	injected := map[string]bool{}
	kept := spec.Volumes[:0]
	for _, v := range spec.Volumes {
		if isInjectedServiceAccountVolume(v) {
			injected[v.Name] = true
			continue
		}
		kept = append(kept, v)
	}
	if len(injected) == 0 {
		return
	}
	if len(kept) == 0 {
		spec.Volumes = nil
	} else {
		spec.Volumes = kept
	}

	for i := range spec.Containers {
		spec.Containers[i].VolumeMounts = stripMounts(spec.Containers[i].VolumeMounts, injected)
	}
	for i := range spec.InitContainers {
		spec.InitContainers[i].VolumeMounts = stripMounts(spec.InitContainers[i].VolumeMounts, injected)
	}
}

func isInjectedServiceAccountVolume(v corev1.Volume) bool {
	if !strings.HasPrefix(v.Name, serviceAccountVolumePrefix) || v.Projected == nil {
		return false
	}
	for _, src := range v.Projected.Sources {
		if src.ServiceAccountToken != nil {
			return true
		}
	}
	return false
}

func stripMounts(mounts []corev1.VolumeMount, injected map[string]bool) []corev1.VolumeMount {
	kept := mounts[:0]
	for _, m := range mounts {
		if injected[m.Name] && m.MountPath == serviceAccountTokenPath {
			continue
		}
		kept = append(kept, m)
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}

func isOurToleration(t corev1.Toleration) bool {
	for _, want := range webhook.Tolerations() {
		if t.Key == want.Key && t.Operator == want.Operator &&
			t.Value == want.Value && t.Effect == want.Effect &&
			t.TolerationSeconds == nil {
			return true
		}
	}
	return false
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
