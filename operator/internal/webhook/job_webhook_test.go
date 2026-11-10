package webhook

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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

	"github.com/bbockelm/htcondor-k8s/operator/api/v1alpha1"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
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

func TestDecideClaimTable(t *testing.T) {
	caps := translate.DefaultCapabilities()

	tests := []struct {
		name   string
		op     admissionv1.Operation
		mutate func(*batchv1.Job)
		want   bool
	}{
		{"plain translatable job", admissionv1.Create, nil, true},
		{"second container", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Template.Spec.Containers = append(j.Spec.Template.Spec.Containers,
				j.Spec.Template.Spec.Containers[0])
		}, false},
		{"init container", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Template.Spec.InitContainers = []corev1.Container{{Name: "i", Image: "busybox"}}
		}, false},
		{"pvc volume", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Template.Spec.Volumes = []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "c"},
				},
			}}
		}, false},
		{"no command", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Template.Spec.Containers[0].Command = nil
		}, false},
		{"no memory request", admissionv1.Create, func(j *batchv1.Job) {
			delete(j.Spec.Template.Spec.Containers[0].Resources.Requests, corev1.ResourceMemory)
		}, false},
		{"restartPolicy Always", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyAlways
		}, false},
		{"service account", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Template.Spec.ServiceAccountName = "builder"
		}, false},
		{"node affinity", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Template.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}}
		}, false},
		{"unknown nodeSelector key", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Template.Spec.NodeSelector = map[string]string{"kubernetes.io/arch": "amd64"}
		}, false},
		{"pod failure policy", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.PodFailurePolicy = &batchv1.PodFailurePolicy{}
		}, false},
		{"parallelism below completions", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Completions = ptr(int32(4))
			j.Spec.Parallelism = ptr(int32(2))
		}, false},
		{"indexed completions", admissionv1.Create, func(j *batchv1.Job) {
			mode := batchv1.IndexedCompletion
			j.Spec.CompletionMode = &mode
			j.Spec.Completions = ptr(int32(4))
			j.Spec.Parallelism = ptr(int32(4))
		}, true},
		{"opted out", admissionv1.Create, func(j *batchv1.Job) {
			j.Annotations = map[string]string{v1alpha1.AnnotationSkip: ""}
		}, false},
		{"another scheduler asked first", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Template.Spec.SchedulerName = "volcano"
		}, false},
		{"default scheduler named explicitly", admissionv1.Create, func(j *batchv1.Job) {
			j.Spec.Template.Spec.SchedulerName = corev1.DefaultSchedulerName
		}, true},
		{"translatable but an update of an unclaimed job", admissionv1.Update, nil, false},
		{"update of a claimed job", admissionv1.Update, func(j *batchv1.Job) {
			j.Annotations = map[string]string{v1alpha1.AnnotationClaimed: "true"}
		}, true},
		{"claimed job that no longer translates is still ours", admissionv1.Update, func(j *batchv1.Job) {
			j.Annotations = map[string]string{v1alpha1.AnnotationClaimed: "true"}
			j.Spec.Template.Spec.InitContainers = []corev1.Container{{Name: "i", Image: "busybox"}}
		}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := translatableJob()
			if tc.mutate != nil {
				tc.mutate(job)
			}
			got := Decide(job, tc.op, caps)
			require.Equal(t, tc.want, got.Claim,
				"why=%q rejections=%v", got.Why, got.Rejections)
		})
	}
}

func TestDecideRespectsPoolCapabilities(t *testing.T) {
	// The same Job is claimed or not depending on the pool profile, which
	// is why admission cannot live in the CRD schema.
	job := translatableJob()
	job.Spec.Template.Spec.NodeSelector = map[string]string{"gpu-model": "a100"}

	require.False(t, Decide(job, admissionv1.Create, translate.DefaultCapabilities()).Claim)

	caps := translate.DefaultCapabilities()
	caps.NodeSelectorAttrs["gpu-model"] = "GPUsDeviceName"
	require.True(t, Decide(job, admissionv1.Create, caps).Claim)
}

func TestClaimIsIdempotentAndRecordsSuspendIntent(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

	job := translatableJob()
	Claim(job, now)
	require.Equal(t, v1alpha1.SchedulerName, job.Spec.Template.Spec.SchedulerName)
	require.NotNil(t, job.Spec.Suspend)
	require.True(t, *job.Spec.Suspend, "a claimed Job is suspended so no Pods are created")
	require.Equal(t, "false", job.Annotations[v1alpha1.AnnotationUserSuspend])
	require.Equal(t, now.Format(time.RFC3339), job.Annotations[v1alpha1.AnnotationClaimedAt])
	require.True(t, Claimed(job))
	require.False(t, UserWantsSuspend(job))

	// Claiming again changes nothing, including the timestamp.
	before := job.DeepCopy()
	Claim(job, now.Add(time.Hour))
	require.Equal(t, before.Annotations, job.Annotations)

	// A user writing .spec.suspend=false is asking to resume.
	f := false
	job.Spec.Suspend = &f
	Claim(job, now.Add(2*time.Hour))
	require.Equal(t, "false", job.Annotations[v1alpha1.AnnotationUserSuspend])
	require.True(t, *job.Spec.Suspend, "the field is held at true regardless")
}

func TestClaimPreservesExplicitSuspend(t *testing.T) {
	job := translatableJob()
	tr := true
	job.Spec.Suspend = &tr
	Claim(job, time.Now())
	require.Equal(t, "true", job.Annotations[v1alpha1.AnnotationUserSuspend])
	require.True(t, UserWantsSuspend(job))
}

func TestClaimedNeedsBothHalves(t *testing.T) {
	job := translatableJob()
	job.Annotations = map[string]string{v1alpha1.AnnotationClaimed: "true"}
	require.False(t, Claimed(job), "an annotation alone is not a claim")

	job.Annotations = nil
	job.Spec.Template.Spec.SchedulerName = v1alpha1.SchedulerName
	require.False(t, Claimed(job), "a scheduler name alone is not a claim")
}

// TestHandleLeavesUnclaimedJobsByteIdentical is the invariant that keeps this
// webhook from ever breaking a cluster's own Jobs.
func TestHandleLeavesUnclaimedJobsByteIdentical(t *testing.T) {
	jc := newClaimer()

	job := translatableJob()
	job.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 80}}
	raw, err := json.Marshal(job)
	require.NoError(t, err)

	resp := jc.Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: raw},
		},
	})
	require.True(t, resp.Allowed)
	require.Empty(t, resp.Patches, "a Job we do not claim must be returned unpatched")
}

func TestHandlePatchesClaimedJob(t *testing.T) {
	jc := newClaimer()

	raw, err := json.Marshal(translatableJob())
	require.NoError(t, err)

	resp := jc.Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: raw},
		},
	})
	require.True(t, resp.Allowed)
	require.NotEmpty(t, resp.Patches)

	paths := map[string]bool{}
	for _, p := range resp.Patches {
		paths[p.Path] = true
	}
	require.True(t, paths["/spec/suspend"], "expected a suspend patch, got %v", paths)
	require.True(t, paths["/spec/template/spec/schedulerName"], "expected a schedulerName patch, got %v", paths)
}

// TestHandleAllowsUndecodableInput: failing open is the whole point of
// failurePolicy Ignore, and the handler must not undo it by erroring.
func TestHandleAllowsUndecodableInput(t *testing.T) {
	jc := newClaimer()
	resp := jc.Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: []byte(`{"spec":{"completions":"not a number"}}`)},
		},
	})
	require.True(t, resp.Allowed)
	require.Empty(t, resp.Patches)
}

func newClaimer() *JobClaimer {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		panic(err)
	}
	return &JobClaimer{
		Caps:    translate.DefaultCapabilities(),
		Decoder: admission.NewDecoder(s),
		Log:     logr.Discard(),
		Now:     func() time.Time { return time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC) },
	}
}

func ptr[T any](v T) *T { return &v }
