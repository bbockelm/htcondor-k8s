package adapter

import (
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

func sourceJob() *batchv1.JobSpec {
	mode := batchv1.IndexedCompletion
	return &batchv1.JobSpec{
		Completions:    ptr(int32(4)),
		Parallelism:    ptr(int32(4)),
		CompletionMode: &mode,
		BackoffLimit:   ptr(int32(3)),
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "sim"}},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyOnFailure,
				Volumes: []corev1.Volume{{
					Name:         "scratch",
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				}},
				Containers: []corev1.Container{{
					Name:       "sim",
					Image:      "docker://rockylinux:9",
					Command:    []string{"/usr/bin/run"},
					Args:       []string{"--seed", "$(JOB_COMPLETION_INDEX)"},
					WorkingDir: "/work",
					Env:        []corev1.EnvVar{{Name: "MODE", Value: "fast"}},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "scratch", MountPath: "/scratch"},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:              resource.MustParse("4"),
							corev1.ResourceMemory:           resource.MustParse("8Gi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
						},
					},
				}},
			},
		},
	}
}

// TestRoundTripPreservesEverythingAdmissionKept is the property the CRD rests
// on: the subset the type can express is exactly the subset admission accepts,
// so projecting an admitted Job into an HTCJob and back loses nothing that
// reaches the submit file.
func TestRoundTripPreservesEverythingAdmissionKept(t *testing.T) {
	caps := translate.DefaultCapabilities()
	src := sourceJob()
	require.True(t, translate.AdmitJob(src, nil, caps).OK())

	hs := JobToHTCJobSpec(src)
	back := HTCJobToJobSpec(&hs)
	require.True(t, translate.AdmitJob(back, nil, caps).OK(),
		"the projection must not produce something admission refuses")

	id := translate.Identity{Name: "sim", Namespace: "default"}
	wantSubmit, err := translate.PodToSubmit(&src.Template.Spec, id, translate.JobSubmitOptions(src, caps))
	require.NoError(t, err)
	gotSubmit, err := translate.PodToSubmit(&back.Template.Spec, id, translate.JobSubmitOptions(back, caps))
	require.NoError(t, err)
	require.Equal(t, wantSubmit, gotSubmit,
		"the same Job must produce the same submit file through the CRD as directly")
}

func TestRoundTripKeepsJobLevelKnobs(t *testing.T) {
	hs := JobToHTCJobSpec(sourceJob())
	require.Equal(t, int32(4), *hs.Completions)
	require.Equal(t, "Indexed", hs.CompletionMode)
	require.Equal(t, int32(3), *hs.BackoffLimit)
	require.Equal(t, "OnFailure", hs.Template.Spec.RestartPolicy)
	require.Equal(t, map[string]string{"app": "sim"}, hs.Template.Labels)
	require.Len(t, hs.Template.Spec.Volumes, 1)

	back := HTCJobToJobSpec(&hs)
	require.Equal(t, batchv1.IndexedCompletion, *back.CompletionMode)
	// Parallelism is restored equal to completions rather than left nil,
	// which is what AdmitJob requires and what HTCondor actually does.
	require.Equal(t, int32(4), *back.Parallelism)
}

func TestPodTemplateDeadlineIsCarried(t *testing.T) {
	src := sourceJob()
	src.Template.Spec.ActiveDeadlineSeconds = ptr(int64(600))
	hs := JobToHTCJobSpec(src)
	require.Equal(t, int64(600), *hs.ActiveDeadlineSeconds)

	// A deadline on the Job itself wins, matching Kubernetes, where the
	// Job-level one terminates the whole Job.
	src.ActiveDeadlineSeconds = ptr(int64(300))
	hs = JobToHTCJobSpec(src)
	require.Equal(t, int64(300), *hs.ActiveDeadlineSeconds)
}

func TestCompletionsDefaultsToOne(t *testing.T) {
	hs := JobToHTCJobSpec(&batchv1.JobSpec{})
	require.Equal(t, int32(1), Completions(&hs))
	require.False(t, Suspended(&hs))
}

func TestProjectionIsDeepEnoughToNotAlias(t *testing.T) {
	src := sourceJob()
	hs := JobToHTCJobSpec(src)
	hs.Template.Spec.Container.Command[0] = "/bin/mutated"
	hs.Template.Labels["app"] = "mutated"
	require.Equal(t, "/usr/bin/run", src.Template.Spec.Containers[0].Command[0])
	require.Equal(t, "sim", src.Template.Labels["app"])
}

// TestNormalizeStoredJobUndoesTheAPIServersOwnDefaulting guards a trap that
// cost a full envtest run to find: the Job registry sets .spec.selector and
// the controller-uid labels *after* mutating admission, so a Job the webhook
// legitimately claimed is refused by admission ever after.
func TestNormalizeStoredJobUndoesTheAPIServersOwnDefaulting(t *testing.T) {
	caps := translate.DefaultCapabilities()
	src := sourceJob()
	require.True(t, translate.AdmitJob(src, nil, caps).OK(), "the incoming Job is admissible")

	stored := src.DeepCopy()
	stored.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		"batch.kubernetes.io/controller-uid": "6f1b",
	}}
	stored.Template.Labels["batch.kubernetes.io/controller-uid"] = "6f1b"
	stored.Template.Labels["batch.kubernetes.io/job-name"] = "sim"
	require.False(t, translate.AdmitJob(stored, nil, caps).OK(),
		"the stored Job is refused, which is the trap")

	norm := NormalizeStoredJob(stored)
	require.True(t, translate.AdmitJob(norm, nil, caps).OK(),
		"normalizing must restore the webhook's verdict")
	require.Nil(t, norm.Selector)
	require.Equal(t, map[string]string{"app": "sim"}, norm.Template.Labels)

	// The stored object itself must not be touched.
	require.NotNil(t, stored.Selector)
}

// A selector the user really did write is not ours to drop: manualSelector is
// the API's own statement of intent, and such a Job must stay refused.
func TestNormalizeStoredJobKeepsAManualSelector(t *testing.T) {
	stored := sourceJob()
	stored.ManualSelector = ptr(true)
	stored.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"mine": "yes"}}

	norm := NormalizeStoredJob(stored)
	require.NotNil(t, norm.Selector)
	require.False(t, translate.AdmitJob(norm, nil, translate.DefaultCapabilities()).OK())
}
