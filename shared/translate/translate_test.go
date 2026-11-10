package translate

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func goodPod() *corev1.PodSpec {
	return &corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers: []corev1.Container{{
			Name:    "work",
			Image:   "docker://busybox:latest",
			Command: []string{"/bin/sh", "-c", "echo hello"},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("512Mi"),
				},
			},
		}},
	}
}

func reasons(rs Rejections) []Reason {
	out := make([]Reason, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Reason)
	}
	return out
}

func TestAdmitAcceptsMinimalPod(t *testing.T) {
	rs := Admit(goodPod(), nil, DefaultCapabilities())
	require.True(t, rs.OK(), "expected admission, got: %s", rs.Error())
}

func TestAdmitRejections(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1.PodSpec)
		want   Reason
	}{
		{"second container", func(s *corev1.PodSpec) {
			s.Containers = append(s.Containers, s.Containers[0])
		}, ReasonMultipleContainers},
		{"init container", func(s *corev1.PodSpec) {
			s.InitContainers = []corev1.Container{{Name: "init", Image: "busybox"}}
		}, ReasonInitContainers},
		{"runs as root", func(s *corev1.PodSpec) {
			zero := int64(0)
			s.SecurityContext = &corev1.PodSecurityContext{RunAsUser: &zero}
		}, ReasonRunsAsRoot},
		{"runAsNonRoot false", func(s *corev1.PodSpec) {
			f := false
			s.SecurityContext = &corev1.PodSecurityContext{RunAsNonRoot: &f}
		}, ReasonRunsAsRoot},
		{"privileged container", func(s *corev1.PodSpec) {
			tr := true
			s.Containers[0].SecurityContext = &corev1.SecurityContext{Privileged: &tr}
		}, ReasonPrivileged},
		{"added capability", func(s *corev1.PodSpec) {
			s.Containers[0].SecurityContext = &corev1.SecurityContext{
				Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"NET_ADMIN"}},
			}
		}, ReasonCapabilities},
		{"host network", func(s *corev1.PodSpec) { s.HostNetwork = true }, ReasonHostNamespace},
		{"pvc volume", func(s *corev1.PodSpec) {
			s.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "c"},
			}}}
		}, ReasonVolumeUnsupported},
		{"secret volume", func(s *corev1.PodSpec) {
			s.Volumes = []corev1.Volume{{Name: "s", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: "s"},
			}}}
		}, ReasonVolumeUnsupported},
		{"hostPath volume", func(s *corev1.PodSpec) {
			s.Volumes = []corev1.Volume{{Name: "h", VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/etc"},
			}}}
		}, ReasonVolumeUnsupported},
		{"memory emptyDir", func(s *corev1.PodSpec) {
			s.Volumes = []corev1.Volume{{Name: "m", VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory},
			}}}
		}, ReasonVolumeUnsupported},
		{"restartPolicy Always", func(s *corev1.PodSpec) {
			s.RestartPolicy = corev1.RestartPolicyAlways
		}, ReasonRestartPolicy},
		{"container ports", func(s *corev1.PodSpec) {
			s.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 80}}
		}, ReasonPortsUnsupported},
		{"liveness probe", func(s *corev1.PodSpec) {
			s.Containers[0].LivenessProbe = &corev1.Probe{}
		}, ReasonProbesUnsupported},
		{"lifecycle hook", func(s *corev1.PodSpec) {
			s.Containers[0].Lifecycle = &corev1.Lifecycle{}
		}, ReasonLifecycleHooks},
		{"envFrom", func(s *corev1.PodSpec) {
			s.Containers[0].EnvFrom = []corev1.EnvFromSource{{}}
		}, ReasonEnvFrom},
		{"env valueFrom", func(s *corev1.PodSpec) {
			s.Containers[0].Env = []corev1.EnvVar{{Name: "X", ValueFrom: &corev1.EnvVarSource{}}}
		}, ReasonEnvValueFrom},
		{"no image", func(s *corev1.PodSpec) { s.Containers[0].Image = "" }, ReasonNoImage},
		{"no cpu request", func(s *corev1.PodSpec) {
			delete(s.Containers[0].Resources.Requests, corev1.ResourceCPU)
		}, ReasonResourcesRequired},
		{"no memory request", func(s *corev1.PodSpec) {
			delete(s.Containers[0].Resources.Requests, corev1.ResourceMemory)
		}, ReasonResourcesRequired},
		{"limit below request", func(s *corev1.PodSpec) {
			s.Containers[0].Resources.Limits = corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			}
		}, ReasonResourceMismatch},
		{"unknown extended resource", func(s *corev1.PodSpec) {
			s.Containers[0].Resources.Requests["example.com/fpga"] = resource.MustParse("1")
		}, ReasonResourceUnsupported},
		{"gpu with no gpu pool", func(s *corev1.PodSpec) {
			s.Containers[0].Resources.Requests["nvidia.com/gpu"] = resource.MustParse("1")
		}, ReasonResourceUnsupported},
		{"nodeName", func(s *corev1.PodSpec) { s.NodeName = "node-1" }, ReasonSchedulingDirective},
		{"affinity", func(s *corev1.PodSpec) { s.Affinity = &corev1.Affinity{} }, ReasonSchedulingDirective},
		{"toleration", func(s *corev1.PodSpec) {
			s.Tolerations = []corev1.Toleration{{Key: "x"}}
		}, ReasonSchedulingDirective},
		{"unknown nodeSelector key", func(s *corev1.PodSpec) {
			s.NodeSelector = map[string]string{"disktype": "ssd"}
		}, ReasonSchedulingDirective},
		{"service account", func(s *corev1.PodSpec) {
			s.ServiceAccountName = "builder"
		}, ReasonServiceAccount},
		{"host alias", func(s *corev1.PodSpec) {
			s.HostAliases = []corev1.HostAlias{{IP: "1.2.3.4"}}
		}, ReasonDNSUnsupported},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := goodPod()
			tc.mutate(spec)
			rs := Admit(spec, nil, DefaultCapabilities())
			require.False(t, rs.OK(), "expected a rejection")
			assert.Contains(t, reasons(rs), tc.want, "got: %s", rs.Error())
		})
	}
}

func TestAdmitReportsEveryProblemAtOnce(t *testing.T) {
	spec := goodPod()
	spec.HostNetwork = true
	spec.NodeName = "node-1"
	spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 80}}

	rs := Admit(spec, nil, DefaultCapabilities())
	assert.Len(t, rs, 3, "a user should see all of their problems at once: %s", rs.Error())
}

func TestAdmitGPUWhenPoolHasThem(t *testing.T) {
	caps := DefaultCapabilities()
	caps.GPUResourceNames = []string{"nvidia.com/gpu"}

	spec := goodPod()
	spec.Containers[0].Resources.Requests["nvidia.com/gpu"] = resource.MustParse("2")

	rs := Admit(spec, nil, caps)
	require.True(t, rs.OK(), rs.Error())

	out, err := PodToSubmit(spec, Identity{Name: "p", Namespace: "n"}, SubmitOptions{Caps: caps})
	require.NoError(t, err)
	assert.Contains(t, out, "request_gpus = 2")
}

func TestAdmitNodeSelectorKeyTheePoolKnows(t *testing.T) {
	caps := DefaultCapabilities()
	caps.NodeSelectorAttrs = map[string]string{"htcondor.org/os": "OpSysAndVer"}

	spec := goodPod()
	spec.NodeSelector = map[string]string{"htcondor.org/os": "AlmaLinux9"}

	require.True(t, Admit(spec, nil, caps).OK())
	out, err := PodToSubmit(spec, Identity{Name: "p", Namespace: "n"}, SubmitOptions{Caps: caps})
	require.NoError(t, err)
	assert.Contains(t, out, `requirements = (TARGET.OpSysAndVer == "AlmaLinux9")`)
}

func TestAdmitSizeCaps(t *testing.T) {
	caps := DefaultCapabilities()
	caps.MaxCPU = 4
	caps.MaxMemoryBytes = 8 << 30

	spec := goodPod()
	spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("16")
	rs := Admit(spec, nil, caps)
	require.False(t, rs.OK())
	assert.Contains(t, reasons(rs), ReasonResourceTooLarge)
}

func TestFractionalCPURoundsUp(t *testing.T) {
	spec := goodPod()
	spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("500m")

	req, err := Resources(&spec.Containers[0], DefaultCapabilities())
	require.NoError(t, err)
	assert.Equal(t, int64(1), req.CPUs, "a job must never get less CPU than the pod asked for")

	spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("2500m")
	req, err = Resources(&spec.Containers[0], DefaultCapabilities())
	require.NoError(t, err)
	assert.Equal(t, int64(3), req.CPUs)
}

func TestLimitWinsOverRequest(t *testing.T) {
	spec := goodPod()
	spec.Containers[0].Resources.Limits = corev1.ResourceList{
		corev1.ResourceMemory: resource.MustParse("2Gi"),
		corev1.ResourceCPU:    resource.MustParse("4"),
	}
	req, err := Resources(&spec.Containers[0], DefaultCapabilities())
	require.NoError(t, err)
	assert.Equal(t, int64(4), req.CPUs)
	assert.Equal(t, int64(2<<30), req.MemoryBytes)
}

func TestPodToSubmitShape(t *testing.T) {
	spec := goodPod()
	spec.Containers[0].Env = []corev1.EnvVar{
		{Name: "GREETING", Value: "hello world"},
		{Name: "PLAIN", Value: "x"},
	}
	id := Identity{
		Name: "my-pod", Namespace: "team-a", UID: "abc-123",
		Labels: map[string]string{"app": "demo"},
	}
	out, err := PodToSubmit(spec, id, SubmitOptions{Caps: DefaultCapabilities(), IWD: "/spool/team-a/my-pod"})
	require.NoError(t, err)

	for _, want := range []string{
		"universe = container",
		"container_image = docker://busybox:latest",
		"executable = /bin/sh",
		`arguments = "-c 'echo hello'"`,
		`environment = "GREETING='hello world' PLAIN=x"`,
		"request_cpus = 1",
		"request_memory = 512",
		"initialdir = /spool/team-a/my-pod",
		`+K8sPodName = "my-pod"`,
		`+K8sPodNamespace = "team-a"`,
		`+K8sPodUID = "abc-123"`,
		"queue",
	} {
		assert.Contains(t, out, want)
	}
	assert.NotContains(t, out, "request_gpus")
}

func TestPodSpecRoundTripsThroughTheJobAd(t *testing.T) {
	spec := goodPod()
	out, err := PodToSubmit(spec, Identity{Name: "p", Namespace: "n"}, SubmitOptions{Caps: DefaultCapabilities()})
	require.NoError(t, err)

	// The +K8sPodSpec attribute is how `kubectl get pod -o yaml` works
	// without an object store: pull the literal back out and re-decode it.
	line := findLine(t, out, "+"+AttrPodSpec+" = ")
	literal := strings.TrimPrefix(line, "+"+AttrPodSpec+" = ")
	var unquoted string
	require.NoError(t, json.Unmarshal([]byte(literal), &unquoted),
		"a ClassAd string literal must also be valid JSON so the round trip is lossless")

	var back corev1.PodSpec
	require.NoError(t, json.Unmarshal([]byte(unquoted), &back))
	assert.Equal(t, spec.Containers[0].Image, back.Containers[0].Image)
	assert.Equal(t, spec.Containers[0].Command, back.Containers[0].Command)
}

func TestIndexedJobQueuesWithIndex(t *testing.T) {
	spec := goodPod()
	opts := SubmitOptions{Caps: DefaultCapabilities(), Count: 5, Indexed: true}
	out, err := PodToSubmit(spec, Identity{Name: "p", Namespace: "n"}, opts)
	require.NoError(t, err)
	assert.Contains(t, out, "queue 5")
	assert.Contains(t, out, "+"+AttrIndex+" = $(ProcId)")
	assert.Contains(t, out, "JOB_COMPLETION_INDEX=$(ProcId)")
}

func TestActiveDeadlineBecomesPeriodicRemove(t *testing.T) {
	out, err := PodToSubmit(goodPod(), Identity{Name: "p", Namespace: "n"},
		SubmitOptions{Caps: DefaultCapabilities(), ActiveDeadlineSeconds: 600})
	require.NoError(t, err)
	assert.Contains(t, out, "periodic_remove = (time() - QDate) > 600")
	assert.NotContains(t, out, "EnteredCurrentStatus",
		"a deadline anchored on the current status restarts on every transition")
}

func TestClassAdStringEscaping(t *testing.T) {
	id := Identity{Name: `we"ird\name`, Namespace: "n"}
	out, err := PodToSubmit(goodPod(), id, SubmitOptions{Caps: DefaultCapabilities()})
	require.NoError(t, err)
	assert.Contains(t, out, `+K8sPodName = "we\"ird\\name"`)
}

func TestPodStatusMapping(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	tests := []struct {
		name  string
		ad    JobAd
		phase corev1.PodPhase
	}{
		{"idle", JobAd{JobStatus: JobStatusIdle}, corev1.PodPending},
		{"running", JobAd{JobStatus: JobStatusRunning, JobStartDate: start}, corev1.PodRunning},
		{"transferring output is still running", JobAd{JobStatus: JobStatusXferOut}, corev1.PodRunning},
		{"completed ok", JobAd{JobStatus: JobStatusCompleted, ExitCode: 0}, corev1.PodSucceeded},
		{"completed nonzero", JobAd{JobStatus: JobStatusCompleted, ExitCode: 3}, corev1.PodFailed},
		{"killed by signal", JobAd{JobStatus: JobStatusCompleted, ExitBySignal: true, ExitSignal: 9}, corev1.PodFailed},
		{"removed", JobAd{JobStatus: JobStatusRemoved}, corev1.PodFailed},
		{"user hold is pending", JobAd{JobStatus: JobStatusHeld, HoldReasonCode: 1}, corev1.PodPending},
		{"image pull hold is failed", JobAd{JobStatus: JobStatusHeld, HoldReasonCode: 35}, corev1.PodFailed},
		{"input transfer hold is failed", JobAd{JobStatus: JobStatusHeld, HoldReasonCode: 13}, corev1.PodFailed},
		{"spooling input is pending", JobAd{JobStatus: JobStatusHeld, HoldReasonCode: 16}, corev1.PodPending},
		{"startd hold is pending", JobAd{JobStatus: JobStatusHeld, HoldReasonCode: 21}, corev1.PodPending},
		{"unknown hold is failed", JobAd{JobStatus: JobStatusHeld, HoldReasonCode: 99}, corev1.PodFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := PodStatus(tc.ad, "work")
			assert.Equal(t, tc.phase, got.Phase)
			require.Len(t, got.ContainerStatuses, 1)
		})
	}
}

func TestSignalledExitUses128PlusSignal(t *testing.T) {
	st := PodStatus(JobAd{JobStatus: JobStatusCompleted, ExitBySignal: true, ExitSignal: 9}, "work")
	require.NotNil(t, st.ContainerStatuses[0].State.Terminated)
	assert.Equal(t, int32(137), st.ContainerStatuses[0].State.Terminated.ExitCode)
}

func TestRestartCountIsStartsMinusOne(t *testing.T) {
	st := PodStatus(JobAd{JobStatus: JobStatusRunning, NumJobStarts: 3}, "work")
	assert.Equal(t, int32(2), st.ContainerStatuses[0].RestartCount)

	st = PodStatus(JobAd{JobStatus: JobStatusIdle, NumJobStarts: 0}, "work")
	assert.Equal(t, int32(0), st.ContainerStatuses[0].RestartCount)
}

func TestAdmitJob(t *testing.T) {
	two := int32(2)
	one := int32(1)
	spec := &batchv1.JobSpec{
		Completions: &two,
		Template:    corev1.PodTemplateSpec{Spec: *goodPod()},
	}
	require.True(t, AdmitJob(spec, &metav1.ObjectMeta{}, DefaultCapabilities()).OK())

	spec.Parallelism = &one
	rs := AdmitJob(spec, &metav1.ObjectMeta{}, DefaultCapabilities())
	require.False(t, rs.OK())
	assert.Contains(t, reasons(rs), ReasonJobFieldUnsupported)
}

func TestJobStatusAggregation(t *testing.T) {
	ads := []JobAd{
		{JobStatus: JobStatusCompleted, ExitCode: 0},
		{JobStatus: JobStatusCompleted, ExitCode: 0},
		{JobStatus: JobStatusRunning},
	}
	st := JobStatus(ads, 3, nil)
	assert.Equal(t, int32(2), st.Succeeded)
	assert.Equal(t, int32(1), st.Active)
	assert.Empty(t, st.Conditions)

	ads[2] = JobAd{JobStatus: JobStatusCompleted, ExitCode: 0}
	st = JobStatus(ads, 3, nil)
	assert.Equal(t, int32(3), st.Succeeded)
	require.Len(t, st.Conditions, 2)
	assert.Equal(t, batchv1.JobComplete, st.Conditions[1].Type)
}

func TestJobStatusFailsWhenNothingIsLeftRunning(t *testing.T) {
	st := JobStatus([]JobAd{
		{JobStatus: JobStatusCompleted, ExitCode: 1},
		{JobStatus: JobStatusHeld, HoldReasonCode: 35},
	}, 2, nil)
	assert.Equal(t, int32(2), st.Failed)
	require.Len(t, st.Conditions, 2)
	assert.Equal(t, batchv1.JobFailed, st.Conditions[1].Type)
}

func findLine(t *testing.T, text, prefix string) string {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, prefix) {
			return line
		}
	}
	t.Fatalf("no line with prefix %q in:\n%s", prefix, text)
	return ""
}

func TestAdmitRejectsAdjacentDollars(t *testing.T) {
	spec := goodPod()
	spec.Containers[0].Args = []string{"cost is $$(Memory)"}
	rs := Admit(spec, nil, DefaultCapabilities())
	require.False(t, rs.OK())
	assert.Contains(t, reasons(rs), ReasonMacroInjection)

	// A single dollar sign is escapable and must stay allowed: shell
	// snippets passed to sh -c are full of them.
	spec = goodPod()
	spec.Containers[0].Args = []string{"echo $HOME and $(date)"}
	assert.True(t, Admit(spec, nil, DefaultCapabilities()).OK())
}

func TestAdmitRejectsAdjacentDollarsInAnnotations(t *testing.T) {
	spec := goodPod()
	meta := &metav1.ObjectMeta{Annotations: map[string]string{"note": "$$(Memory)"}}
	rs := Admit(spec, meta, DefaultCapabilities())
	require.False(t, rs.OK())
	assert.Contains(t, reasons(rs), ReasonMacroInjection)
}

// The Job registry fills in .spec.selector after mutating admission, so a
// webhook that claimed a Job must not find it inadmissible the next time
// anyone looks at the stored object.
func TestAdmitJobAcceptsTheAPIServersOwnSelector(t *testing.T) {
	two := int32(2)
	spec := &batchv1.JobSpec{
		Completions: &two,
		Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"batch.kubernetes.io/controller-uid": "uid"}},
		Template:    corev1.PodTemplateSpec{Spec: *goodPod()},
	}
	assert.True(t, AdmitJob(spec, &metav1.ObjectMeta{}, DefaultCapabilities()).OK(),
		"a defaulted selector is the API server's, not the user's")

	manual := true
	spec.ManualSelector = &manual
	rs := AdmitJob(spec, &metav1.ObjectMeta{}, DefaultCapabilities())
	require.False(t, rs.OK(), "a hand-written selector means adopting pods that do not exist")
	assert.Contains(t, reasons(rs), ReasonJobFieldUnsupported)
}

func TestJobStatusEmitsTheInterimTerminalConditions(t *testing.T) {
	st := JobStatus([]JobAd{{JobStatus: JobStatusCompleted, ExitCode: 0}}, 1, nil)
	types := []batchv1.JobConditionType{}
	for _, c := range st.Conditions {
		types = append(types, c.Type)
	}
	assert.Equal(t, []batchv1.JobConditionType{batchv1.JobSuccessCriteriaMet, batchv1.JobComplete}, types,
		"the API server refuses Complete without SuccessCriteriaMet")

	st = JobStatus([]JobAd{{JobStatus: JobStatusCompleted, ExitCode: 1}}, 1, nil)
	types = types[:0]
	for _, c := range st.Conditions {
		types = append(types, c.Type)
	}
	assert.Equal(t, []batchv1.JobConditionType{batchv1.JobFailureTarget, batchv1.JobFailed}, types)
}
