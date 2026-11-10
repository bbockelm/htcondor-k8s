package webhook

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
)

func translatableJob() *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "work", Namespace: "default"},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:    "work",
						Image:   "docker://busybox:latest",
						Command: []string{"/bin/sh", "-c", "echo hi"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1"),
								corev1.ResourceMemory: resource.MustParse("512Mi"),
							},
						},
					}},
				},
			},
		},
	}
}

func i32(v int32) *int32 { return &v }

func TestDecideTable(t *testing.T) {
	caps := translate.DefaultCapabilities()

	tests := []struct {
		name   string
		op     admissionv1.Operation
		mutate func(*batchv1.Job)
		want   bool
	}{
		{"plain translatable job", admissionv1.Create, nil, true},

		// The whole reason this approach exists: the built-in Job
		// controller does the throttling, so parallelism below
		// completions is not a reason to refuse. shared/translate
		// refuses it for the two front ends that reimplement Job
		// semantics themselves; see admitForPodInterception.
		{"parallelism below completions", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Completions = i32(10)
			j.Spec.Parallelism = i32(2)
		}, true},

		// ...but a parallelism problem is the only rejection on that
		// field that gets filtered. Everything else still counts.
		{"parallelism below completions and a PVC", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Completions = i32(10)
			j.Spec.Parallelism = i32(2)
			j.Spec.Template.Spec.Volumes = []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "c"},
				},
			}}
		}, false},

		// Refused here although shared/translate admits it. There is no
		// kubelet to restart a container in place.
		{"restartPolicy OnFailure", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
		}, false},

		{"second container", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Template.Spec.Containers = append(j.Spec.Template.Spec.Containers,
				j.Spec.Template.Spec.Containers[0])
		}, false},
		{"a toleration the user wrote", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Template.Spec.Tolerations = []corev1.Toleration{{Key: "spot"}}
		}, false},
		{"no memory request", admissionv1.Create, func(j *batchv1.Job) {
			delete(j.Spec.Template.Spec.Containers[0].Resources.Requests, corev1.ResourceMemory)
		}, false},

		// podReplacementPolicy is the one field this webhook writes
		// that is not part of .spec.template, so an explicit value is
		// refused rather than overridden.
		// Neither value is a reason to decline: the API server has
		// already defaulted the field by the time a webhook sees it, so
		// the two are indistinguishable. See Stamp.
		{"explicit podReplacementPolicy Failed", admissionv1.Create, func(j *batchv1.Job) {
			p := batchv1.Failed
			j.Spec.PodReplacementPolicy = &p
		}, true},
		{"defaulted podReplacementPolicy TerminatingOrFailed", admissionv1.Create, func(j *batchv1.Job) {
			p := batchv1.TerminatingOrFailed
			j.Spec.PodReplacementPolicy = &p
		}, true},

		{"somebody else's scheduler", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Template.Spec.SchedulerName = "volcano"
		}, false},
		{"unstamped job on update", admissionv1.Update, nil, false},
		{"already stamped on update", admissionv1.Update, func(j *batchv1.Job) {
			j.Spec.Template.Spec.SchedulerName = api.SchedulerName
		}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := translatableJob()
			if tc.mutate != nil {
				tc.mutate(job)
			}
			got := Decide(job, tc.op, caps)
			require.Equal(t, tc.want, got.Stamp, "why=%q rejections=%v", got.Why, got.Rejections)
		})
	}
}

// TestStampWritesOnlyTheThreeFields is the discipline test for this whole
// approach. The HTCJob operator's injuries all came from writing to fields
// somebody else owns, so the exact set of fields this webhook touches is a
// thing to pin down rather than a thing to be careful about.
func TestStampWritesOnlyTheThreeFields(t *testing.T) {
	before := translatableJob()
	job := before.DeepCopy()
	Stamp(job)

	require.Equal(t, api.SchedulerName, job.Spec.Template.Spec.SchedulerName)
	require.Equal(t, Tolerations(), job.Spec.Template.Spec.Tolerations)
	require.NotNil(t, job.Spec.PodReplacementPolicy)
	require.Equal(t, batchv1.Failed, *job.Spec.PodReplacementPolicy)

	// Everything else is the user's. In particular suspend, which the
	// HTCJob operator had to commandeer and could never hand back.
	require.Nil(t, job.Spec.Suspend, "suspend is the user's field; this approach never takes it")
	require.Empty(t, job.Annotations, "no claim annotation: the mechanism is the schedulerName itself")
	require.Equal(t, batchv1.JobStatus{}, job.Status, "the webhook never writes Job status")

	// And nothing else in the spec moved.
	want := before.DeepCopy()
	want.Spec.Template.Spec.SchedulerName = api.SchedulerName
	want.Spec.Template.Spec.Tolerations = Tolerations()
	p := batchv1.Failed
	want.Spec.PodReplacementPolicy = &p
	require.Equal(t, want, job)
}

func TestStampIsIdempotent(t *testing.T) {
	job := translatableJob()
	Stamp(job)
	once := job.DeepCopy()
	Stamp(job)
	require.Equal(t, once, job, "a second stamp must not append a second toleration")
}

func TestStampKeepsUserTolerations(t *testing.T) {
	// Decide refuses such a Job, but Stamp must still be total: a Job that
	// was stamped and then hand-edited to add a toleration gets re-stamped
	// on update, and the user's toleration is not ours to delete.
	job := translatableJob()
	job.Spec.Template.Spec.Tolerations = []corev1.Toleration{{Key: "spot"}}
	Stamp(job)
	require.Len(t, job.Spec.Template.Spec.Tolerations, 1+len(Tolerations()))
	require.Equal(t, "spot", job.Spec.Template.Spec.Tolerations[0].Key)
}

// TestStampTicksTheNotReadyTolerationsForever guards a failure that happens
// every time this controller is restarted, not an edge case.
//
// A stopped virtual kubelet makes its Node Ready=False; the node lifecycle
// controller taints it node.kubernetes.io/not-ready:NoExecute; the
// DefaultTolerationSeconds admission plugin would otherwise have given every
// Pod a 300-second toleration for that taint; and five minutes later the taint
// manager deletes every bound Pod. Each deletion runs the pod controller's
// finalizer, which condor_rm's a job that is running perfectly well.
//
// nil tolerationSeconds means forever. Zero would mean evict immediately,
// which is the exact opposite, so the test asserts on nil rather than on
// presence.
func TestStampTolerateNotReadyForever(t *testing.T) {
	job := translatableJob()
	Stamp(job)

	for _, key := range []string{corev1.TaintNodeNotReady, corev1.TaintNodeUnreachable} {
		var found *corev1.Toleration
		for i := range job.Spec.Template.Spec.Tolerations {
			if job.Spec.Template.Spec.Tolerations[i].Key == key {
				found = &job.Spec.Template.Spec.Tolerations[i]
			}
		}
		require.NotNilf(t, found, "no toleration for %s; a restart would delete live Pods", key)
		require.Equal(t, corev1.TaintEffectNoExecute, found.Effect)
		require.Equal(t, corev1.TolerationOpExists, found.Operator)
		require.Nilf(t, found.TolerationSeconds,
			"%s must be tolerated forever; a number here is a deadline on the controller's downtime", key)
	}
}

func newStamper() *JobStamper {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return &JobStamper{
		Caps:    translate.DefaultCapabilities(),
		Decoder: admission.NewDecoder(scheme),
		Log:     logr.Discard(),
	}
}

func handle(t *testing.T, js *JobStamper, job *batchv1.Job, op admissionv1.Operation) admission.Response {
	t.Helper()
	raw, err := json.Marshal(job)
	require.NoError(t, err)
	return js.Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: op,
			Object:    runtime.RawExtension{Raw: raw},
		},
	})
}

// TestHandleLeavesUnstampedJobsByteIdentical is the asymmetry argument made
// executable: declining to take a Job must cost that Job nothing at all.
func TestHandleLeavesUnstampedJobsByteIdentical(t *testing.T) {
	job := translatableJob()
	job.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 80}}

	resp := handle(t, newStamper(), job, admissionv1.Create)
	require.True(t, resp.Allowed)
	require.Empty(t, resp.Patches, "a Job we do not stamp must be returned unpatched")
}

func TestHandlePatchesStampedJob(t *testing.T) {
	resp := handle(t, newStamper(), translatableJob(), admissionv1.Create)
	require.True(t, resp.Allowed)
	require.NotEmpty(t, resp.Patches)

	paths := map[string]bool{}
	for _, p := range resp.Patches {
		paths[p.Path] = true
	}
	require.True(t, paths["/spec/template/spec/schedulerName"], "got %v", paths)
	require.True(t, paths["/spec/template/spec/tolerations"], "got %v", paths)
	require.True(t, paths["/spec/podReplacementPolicy"], "got %v", paths)
	require.False(t, paths["/spec/suspend"], "this approach never suspends the Job: %v", paths)
}

// TestHandleAllowsUndecodableInput: a decoding bug of ours must not take down
// Job creation for everything this webhook matches.
func TestHandleAllowsUndecodableInput(t *testing.T) {
	resp := newStamper().Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: []byte("{not json")},
		},
	})
	require.True(t, resp.Allowed)
	require.Empty(t, resp.Patches)
}
