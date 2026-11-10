package translate

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Reason is a machine-readable admission failure code. The facade turns these
// into the `reason` of a Kubernetes Status; the webhook turns them into the
// message of a denied AdmissionReview and into the HTCJob's Admitted=False
// condition. Keep them stable: users will grep for them.
type Reason string

const (
	ReasonMultipleContainers  Reason = "MultipleContainers"
	ReasonInitContainers      Reason = "InitContainersUnsupported"
	ReasonEphemeralContainers Reason = "EphemeralContainersUnsupported"
	ReasonRunsAsRoot          Reason = "RunsAsRoot"
	ReasonPrivileged          Reason = "PrivilegedUnsupported"
	ReasonCapabilities        Reason = "CapabilitiesUnsupported"
	ReasonHostNamespace       Reason = "HostNamespaceUnsupported"
	ReasonVolumeUnsupported   Reason = "VolumeTypeUnsupported"
	ReasonVolumeMount         Reason = "VolumeMountUnsupported"
	ReasonRestartPolicy       Reason = "RestartPolicyUnsupported"
	ReasonPortsUnsupported    Reason = "ContainerPortsUnsupported"
	ReasonProbesUnsupported   Reason = "ProbesUnsupported"
	ReasonLifecycleHooks      Reason = "LifecycleHooksUnsupported"
	ReasonEnvFrom             Reason = "EnvFromUnsupported"
	ReasonEnvValueFrom        Reason = "EnvValueFromUnsupported"
	ReasonNoImage             Reason = "ImageRequired"
	ReasonCommandRequired     Reason = "CommandRequired"
	ReasonResourcesRequired   Reason = "ResourceRequestRequired"
	ReasonResourceMismatch    Reason = "ResourceLimitBelowRequest"
	ReasonResourceUnsupported Reason = "ResourceTypeUnsupported"
	ReasonResourceTooLarge    Reason = "ResourceRequestTooLarge"
	ReasonSchedulingDirective Reason = "SchedulingDirectiveUnsupported"
	ReasonServiceAccount      Reason = "ServiceAccountUnsupported"
	ReasonDNSUnsupported      Reason = "DNSConfigUnsupported"
	ReasonSubresource         Reason = "PodSubresourceUnsupported"
	ReasonJobFieldUnsupported Reason = "JobFieldUnsupported"
	ReasonInvalidValue        Reason = "InvalidValue"
	ReasonMacroInjection      Reason = "SubmitMacroNotEscapable"
)

// Rejection is one reason a Pod cannot be modeled as an HTCondor job.
type Rejection struct {
	// Field is the offending path in the submitted object, in the same
	// style the API server uses ("spec.containers[1].ports").
	Field string
	// Reason is the stable code.
	Reason Reason
	// Detail is the human sentence shown to whoever ran kubectl.
	Detail string
}

func (r Rejection) Error() string {
	return fmt.Sprintf("%s: %s (%s)", r.Field, r.Detail, r.Reason)
}

// Rejections is the whole verdict. A Pod is admissible iff it is empty.
type Rejections []Rejection

func (rs Rejections) OK() bool { return len(rs) == 0 }

// Error renders every rejection in one message, which is what a user sees
// when kubectl refuses their Pod.
func (rs Rejections) Error() string {
	parts := make([]string, 0, len(rs))
	for _, r := range rs {
		parts = append(parts, r.Error())
	}
	return strings.Join(parts, "; ")
}

func (rs *Rejections) add(field string, reason Reason, format string, args ...any) {
	*rs = append(*rs, Rejection{Field: field, Reason: reason, Detail: fmt.Sprintf(format, args...)})
}

// Capabilities describes what the HTCondor pool behind this integration can
// actually honor. Admission is pool-dependent: a site whose EPs all run
// Apptainer under a per-job user can accept things a shared-Docker site cannot,
// and a site with no GPUs should reject a GPU request at admission rather than
// let the job sit idle forever.
type Capabilities struct {
	// MaxCPU, MaxMemoryBytes and MaxDiskBytes bound a single job. Zero means
	// "no local bound" — the negotiator will decide.
	MaxCPU         int64
	MaxMemoryBytes int64
	MaxDiskBytes   int64

	// GPUResourceNames are the extended resources that map onto request_gpus.
	// Empty means the pool has no GPUs and any GPU request is rejected.
	GPUResourceNames []string

	// AllowEmptyDir permits emptyDir volumes, which become subdirectories of
	// the job scratch directory. Medium: Memory is never allowed.
	AllowEmptyDir bool

	// NodeSelectorAttrs maps a Kubernetes nodeSelector key to the machine
	// ClassAd attribute that carries the same fact. A key that is not in this
	// map is rejected: silently dropping a scheduling constraint would land
	// the job somewhere the user explicitly excluded.
	NodeSelectorAttrs map[string]string
}

// DefaultCapabilities is a conservative pool profile: CPU/memory/disk only,
// emptyDir allowed, no GPUs, no nodeSelector keys.
func DefaultCapabilities() Capabilities {
	return Capabilities{
		AllowEmptyDir:     true,
		NodeSelectorAttrs: map[string]string{},
	}
}

// Admit reports every way the PodSpec departs from what an HTCondor job can
// express. It returns all of them rather than the first: a user fixing a Pod by
// trial and error through a webhook is a bad time.
//
// meta may be nil; it is consulted only for annotations that opt into
// otherwise-rejected behavior.
func Admit(spec *corev1.PodSpec, meta *metav1.ObjectMeta, caps Capabilities) Rejections {
	var rs Rejections
	if spec == nil {
		rs.add("spec", ReasonInvalidValue, "pod spec is empty")
		return rs
	}

	admitPodShape(spec, &rs)
	admitPodSecurity(spec, &rs)
	admitScheduling(spec, caps, &rs)
	volumes := admitVolumes(spec, caps, &rs)

	if len(spec.Containers) == 1 {
		admitContainer(&spec.Containers[0], "spec.containers[0]", volumes, caps, &rs)
	}
	admitMacroSafety(spec, meta, &rs)

	sort.SliceStable(rs, func(i, j int) bool { return rs[i].Field < rs[j].Field })
	return rs
}

// admitPodShape enforces the single-process model: one container, no init or
// ephemeral containers, and a restart policy a batch system can honor.
func admitPodShape(spec *corev1.PodSpec, rs *Rejections) {
	switch len(spec.Containers) {
	case 1:
	case 0:
		rs.add("spec.containers", ReasonInvalidValue, "a pod must declare exactly one container")
	default:
		rs.add("spec.containers", ReasonMultipleContainers,
			"an HTCondor job runs a single container; this pod declares %d, and there is no sidecar lifecycle to emulate",
			len(spec.Containers))
	}
	if len(spec.InitContainers) > 0 {
		rs.add("spec.initContainers", ReasonInitContainers,
			"init containers would need an ordered multi-step job; use a wrapper image instead")
	}
	if len(spec.EphemeralContainers) > 0 {
		rs.add("spec.ephemeralContainers", ReasonEphemeralContainers,
			"a running HTCondor job cannot be joined by a new container")
	}

	switch spec.RestartPolicy {
	case corev1.RestartPolicyNever, corev1.RestartPolicyOnFailure, "":
	case corev1.RestartPolicyAlways:
		rs.add("spec.restartPolicy", ReasonRestartPolicy,
			"restartPolicy Always describes a service, not a batch job; use Never or OnFailure")
	default:
		rs.add("spec.restartPolicy", ReasonRestartPolicy, "unknown restart policy %q", spec.RestartPolicy)
	}

	if spec.ActiveDeadlineSeconds != nil && *spec.ActiveDeadlineSeconds <= 0 {
		rs.add("spec.activeDeadlineSeconds", ReasonInvalidValue, "must be positive")
	}
}

// admitPodSecurity refuses anything that would run with more privilege than the
// submitting user has. An HTCondor EP runs the job as a mapped local user; a
// Pod that asks for uid 0 is asking for something the EP will not give it, and
// quietly running as somebody else would be a correctness bug, not a nicety.
func admitPodSecurity(spec *corev1.PodSpec, rs *Rejections) {
	if spec.HostNetwork || spec.HostPID || spec.HostIPC {
		rs.add("spec", ReasonHostNamespace,
			"hostNetwork/hostPID/hostIPC share the execute node with other jobs")
	}
	if sc := spec.SecurityContext; sc != nil {
		if sc.RunAsUser != nil && *sc.RunAsUser == 0 {
			rs.add("spec.securityContext.runAsUser", ReasonRunsAsRoot,
				"jobs run as the mapped pool user, never as uid 0")
		}
		if sc.RunAsNonRoot != nil && !*sc.RunAsNonRoot {
			rs.add("spec.securityContext.runAsNonRoot", ReasonRunsAsRoot,
				"runAsNonRoot=false asserts the job may run as root; it may not")
		}
		if len(sc.Sysctls) > 0 {
			rs.add("spec.securityContext.sysctls", ReasonPrivileged, "sysctls require a privileged runtime")
		}
	}
	if spec.ServiceAccountName != "" && spec.ServiceAccountName != "default" {
		rs.add("spec.serviceAccountName", ReasonServiceAccount,
			"there is no in-job Kubernetes API identity to project")
	}
	if spec.AutomountServiceAccountToken != nil && *spec.AutomountServiceAccountToken {
		rs.add("spec.automountServiceAccountToken", ReasonServiceAccount,
			"no service account token is projected into an HTCondor job")
	}
}

// admitScheduling rejects every placement directive that has no matchmaking
// equivalent. nodeSelector survives only for keys the pool has been told how to
// express as a machine ClassAd attribute.
func admitScheduling(spec *corev1.PodSpec, caps Capabilities, rs *Rejections) {
	if spec.NodeName != "" {
		rs.add("spec.nodeName", ReasonSchedulingDirective,
			"a job is matched to a slot by the negotiator and cannot be pinned to a node")
	}
	if spec.Affinity != nil {
		rs.add("spec.affinity", ReasonSchedulingDirective,
			"affinity has no matchmaking equivalent; express the requirement as a nodeSelector key the pool knows")
	}
	if len(spec.TopologySpreadConstraints) > 0 {
		rs.add("spec.topologySpreadConstraints", ReasonSchedulingDirective,
			"HTCondor does not spread a cluster of jobs across topology domains on request")
	}
	if len(spec.Tolerations) > 0 {
		rs.add("spec.tolerations", ReasonSchedulingDirective, "there are no node taints to tolerate")
	}
	if spec.PriorityClassName != "" {
		rs.add("spec.priorityClassName", ReasonSchedulingDirective,
			"job priority comes from the user's fair-share, not a priority class")
	}
	for _, key := range sortedKeys(spec.NodeSelector) {
		if _, ok := caps.NodeSelectorAttrs[key]; !ok {
			rs.add("spec.nodeSelector["+key+"]", ReasonSchedulingDirective,
				"this pool has no machine attribute for nodeSelector key %q", key)
		}
	}
	if spec.DNSPolicy != "" && spec.DNSPolicy != corev1.DNSClusterFirst && spec.DNSPolicy != corev1.DNSDefault {
		rs.add("spec.dnsPolicy", ReasonDNSUnsupported, "only the node's own resolver is available")
	}
	if spec.DNSConfig != nil {
		rs.add("spec.dnsConfig", ReasonDNSUnsupported, "the job inherits the execute node's resolver configuration")
	}
	if spec.Hostname != "" || spec.Subdomain != "" {
		rs.add("spec.hostname", ReasonDNSUnsupported, "a job has no stable network identity")
	}
	if len(spec.HostAliases) > 0 {
		rs.add("spec.hostAliases", ReasonDNSUnsupported, "/etc/hosts of the execute node is not writable per job")
	}
}

// admitVolumes returns the set of volume names that survived admission, so
// container mounts can be checked against it.
func admitVolumes(spec *corev1.PodSpec, caps Capabilities, rs *Rejections) map[string]bool {
	ok := map[string]bool{}
	for i := range spec.Volumes {
		v := &spec.Volumes[i]
		field := fmt.Sprintf("spec.volumes[%d]", i)
		switch {
		case v.EmptyDir != nil:
			if !caps.AllowEmptyDir {
				rs.add(field, ReasonVolumeUnsupported, "this pool does not allow emptyDir volumes")
				continue
			}
			if v.EmptyDir.Medium == corev1.StorageMediumMemory {
				rs.add(field, ReasonVolumeUnsupported,
					"emptyDir medium Memory would consume execute-node RAM outside the job's memory request")
				continue
			}
			ok[v.Name] = true
		case v.PersistentVolumeClaim != nil:
			rs.add(field, ReasonVolumeUnsupported,
				"there is no cluster storage to bind; stage data in with transfer_input_files instead")
		case v.Secret != nil, v.ConfigMap != nil, v.Projected != nil, v.DownwardAPI != nil:
			rs.add(field, ReasonVolumeUnsupported,
				"secret/configMap/projected/downwardAPI injection is not implemented yet")
		case v.HostPath != nil:
			rs.add(field, ReasonVolumeUnsupported,
				"hostPath exposes the execute node's filesystem to the job")
		default:
			rs.add(field, ReasonVolumeUnsupported, "volume %q uses an unsupported source type", v.Name)
		}
	}
	return ok
}

func admitContainer(c *corev1.Container, field string, volumes map[string]bool, caps Capabilities, rs *Rejections) {
	if strings.TrimSpace(c.Image) == "" {
		rs.add(field+".image", ReasonNoImage, "an image is required; there is nothing to run without one")
	}
	if len(c.Command) == 0 {
		// Real condor_submit falls back to the container image's own
		// entrypoint; golang-htcondor's submit parser does not yet, and
		// resolving the entrypoint would mean pulling the image manifest
		// at admission time. Rejecting is honest until one of those lands.
		rs.add(field+".command", ReasonCommandRequired,
			"command must be set explicitly; the image entrypoint is not resolved yet")
	}
	if len(c.Ports) > 0 {
		rs.add(field+".ports", ReasonPortsUnsupported,
			"a job gets no routable address and no Service can select it")
	}
	if c.LivenessProbe != nil || c.ReadinessProbe != nil || c.StartupProbe != nil {
		rs.add(field+".probes", ReasonProbesUnsupported, "nothing executes probes against a running job")
	}
	if c.Lifecycle != nil {
		rs.add(field+".lifecycle", ReasonLifecycleHooks, "postStart/preStop hooks have no equivalent")
	}
	if len(c.EnvFrom) > 0 {
		rs.add(field+".envFrom", ReasonEnvFrom, "ConfigMap/Secret environment injection is not implemented yet")
	}
	for i := range c.Env {
		if c.Env[i].ValueFrom != nil {
			rs.add(fmt.Sprintf("%s.env[%d].valueFrom", field, i), ReasonEnvValueFrom,
				"only literal environment values are supported")
		}
	}
	if sc := c.SecurityContext; sc != nil {
		if sc.Privileged != nil && *sc.Privileged {
			rs.add(field+".securityContext.privileged", ReasonPrivileged, "privileged containers are never allowed")
		}
		if sc.RunAsUser != nil && *sc.RunAsUser == 0 {
			rs.add(field+".securityContext.runAsUser", ReasonRunsAsRoot, "jobs run as the mapped pool user")
		}
		if sc.RunAsNonRoot != nil && !*sc.RunAsNonRoot {
			rs.add(field+".securityContext.runAsNonRoot", ReasonRunsAsRoot, "the job may not run as root")
		}
		if sc.Capabilities != nil && len(sc.Capabilities.Add) > 0 {
			rs.add(field+".securityContext.capabilities.add", ReasonCapabilities,
				"no Linux capabilities can be added to a job")
		}
		if sc.AllowPrivilegeEscalation != nil && *sc.AllowPrivilegeEscalation {
			rs.add(field+".securityContext.allowPrivilegeEscalation", ReasonPrivileged,
				"privilege escalation is not available inside a job")
		}
	}
	for i := range c.VolumeMounts {
		m := &c.VolumeMounts[i]
		if !volumes[m.Name] {
			// The volume itself was already rejected, or does not exist;
			// either way the mount cannot be honored. Report it on the
			// mount too, because that is where the user will look.
			rs.add(fmt.Sprintf("%s.volumeMounts[%d]", field, i), ReasonVolumeMount,
				"volume %q is not available to this job", m.Name)
			continue
		}
		if m.SubPath != "" || m.SubPathExpr != "" {
			rs.add(fmt.Sprintf("%s.volumeMounts[%d].subPath", field, i), ReasonVolumeMount,
				"subPath mounts are not supported")
		}
	}
	admitResources(c, field, caps, rs)
}

func admitResources(c *corev1.Container, field string, caps Capabilities, rs *Rejections) {
	req, err := Resources(c, caps)
	if err != nil {
		var rej Rejection
		if asRejection(err, &rej) {
			rej.Field = field + "." + rej.Field
			*rs = append(*rs, rej)
			return
		}
		rs.add(field+".resources", ReasonInvalidValue, "%v", err)
		return
	}
	if caps.MaxCPU > 0 && req.CPUs > caps.MaxCPU {
		rs.add(field+".resources.requests.cpu", ReasonResourceTooLarge,
			"request of %d CPUs exceeds the pool's largest slot (%d)", req.CPUs, caps.MaxCPU)
	}
	if caps.MaxMemoryBytes > 0 && req.MemoryBytes > caps.MaxMemoryBytes {
		rs.add(field+".resources.requests.memory", ReasonResourceTooLarge,
			"request of %d bytes of memory exceeds the pool's largest slot (%d)", req.MemoryBytes, caps.MaxMemoryBytes)
	}
	if caps.MaxDiskBytes > 0 && req.DiskBytes > caps.MaxDiskBytes {
		rs.add(field+".resources.requests.ephemeral-storage", ReasonResourceTooLarge,
			"request of %d bytes of disk exceeds the pool's largest slot (%d)", req.DiskBytes, caps.MaxDiskBytes)
	}
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// admitMacroSafety refuses the one piece of user text the submit file cannot
// carry safely.
//
// condor_submit expands macros in every command value, and $(DOLLAR) is the
// escape for a literal dollar sign. That escape covers every input but one:
// two adjacent dollar signs. Expansion is a single left-to-right pass, so the
// first $(DOLLAR) becomes a $ that then sits in front of the second one,
// forming $$( --- HTCondor's deferred match-time substitution --- which is
// left alone. The text would reach the job altered.
//
// This is narrow enough to reject outright. It is also a symptom: building
// submit-file *text* from untrusted input means living inside condor_submit's
// macro language. Constructing the job ClassAd directly would remove the whole
// class of problem.
func admitMacroSafety(spec *corev1.PodSpec, meta *metav1.ObjectMeta, rs *Rejections) {
	check := func(field, value string) {
		if strings.Contains(value, "$$") {
			rs.add(field, ReasonMacroInjection,
				"two adjacent dollar signs cannot be escaped in a submit file and would reach the job altered")
		}
	}

	for i := range spec.Containers {
		c := &spec.Containers[i]
		field := fmt.Sprintf("spec.containers[%d]", i)
		check(field+".image", c.Image)
		check(field+".workingDir", c.WorkingDir)
		for j, a := range c.Command {
			check(fmt.Sprintf("%s.command[%d]", field, j), a)
		}
		for j, a := range c.Args {
			check(fmt.Sprintf("%s.args[%d]", field, j), a)
		}
		for j := range c.Env {
			check(fmt.Sprintf("%s.env[%d]", field, j), c.Env[j].Name)
			check(fmt.Sprintf("%s.env[%d]", field, j), c.Env[j].Value)
		}
	}
	if meta == nil {
		return
	}
	for _, k := range sortedKeys(meta.Labels) {
		check("metadata.labels["+k+"]", k+"="+meta.Labels[k])
	}
	for _, k := range sortedKeys(meta.Annotations) {
		check("metadata.annotations["+k+"]", k+"="+meta.Annotations[k])
	}
}
