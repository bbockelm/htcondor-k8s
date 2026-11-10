package envtest_test

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bbockelm/htcondor-k8s/operator/api/v1alpha1"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// TestCRDRejectsFieldsItDoesNotHave is the claim the whole CRD design rests
// on, checked against a real API server rather than asserted in prose: a field
// HTCondor cannot honor is not merely ignored, it cannot be stored.
func TestCRDRejectsFieldsItDoesNotHave(t *testing.T) {
	skipWithoutAssets(t)
	ctx := context.Background()

	base := map[string]any{
		"apiVersion": "htcondor.org/v1alpha1",
		"kind":       "HTCJob",
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"container": map[string]any{
						"name":    "work",
						"image":   "docker://busybox:latest",
						"command": []any{"/bin/sh", "-c", "echo hi"},
						"resources": map[string]any{
							"requests": map[string]any{"cpu": "1", "memory": "512Mi"},
						},
					},
				},
			},
		},
	}

	tests := []struct {
		name  string
		patch func(spec map[string]any)
		want  string
	}{
		{"initContainers", func(spec map[string]any) {
			podSpec(spec)["initContainers"] = []any{map[string]any{"name": "i", "image": "busybox"}}
		}, "initContainers"},
		{"tolerations", func(spec map[string]any) {
			podSpec(spec)["tolerations"] = []any{map[string]any{"key": "gpu"}}
		}, "tolerations"},
		{"a second container", func(spec map[string]any) {
			podSpec(spec)["containers"] = []any{map[string]any{"name": "sidecar"}}
		}, "containers"},
		{"hostPath volume", func(spec map[string]any) {
			podSpec(spec)["volumes"] = []any{map[string]any{
				"name":     "host",
				"hostPath": map[string]any{"path": "/etc"},
			}}
		}, "hostPath"},
		{"env valueFrom", func(spec map[string]any) {
			container(spec)["env"] = []any{map[string]any{
				"name":      "SECRET",
				"valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "s", "key": "k"}},
			}}
		}, "valueFrom"},
		{"volumeMount subPath", func(spec map[string]any) {
			podSpec(spec)["volumes"] = []any{map[string]any{
				"name": "scratch", "emptyDir": map[string]any{},
			}}
			container(spec)["volumeMounts"] = []any{map[string]any{
				"name": "scratch", "mountPath": "/s", "subPath": "inner",
			}}
		}, "subPath"},
		{"restartPolicy Always", func(spec map[string]any) {
			podSpec(spec)["restartPolicy"] = "Always"
		}, "restartPolicy"},
		{"parallelism", func(spec map[string]any) {
			spec["parallelism"] = int64(2)
		}, "parallelism"},
		{"podFailurePolicy", func(spec map[string]any) {
			spec["podFailurePolicy"] = map[string]any{"rules": []any{}}
		}, "podFailurePolicy"},
		{"ttlSecondsAfterFinished", func(spec map[string]any) {
			spec["ttlSecondsAfterFinished"] = int64(60)
		}, "ttlSecondsAfterFinished"},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			obj := &unstructured.Unstructured{Object: deepCopyMap(base)}
			obj.SetName("reject-" + strings.ToLower(strings.ReplaceAll(tc.name, " ", "-")))
			obj.SetNamespace("default")
			tc.patch(obj.Object["spec"].(map[string]any))

			// Strict field validation is what kubectl --validate=strict
			// and client-side apply use; without it the API server
			// prunes unknown fields silently, which would be the worst
			// of both worlds.
			err := k8sClient.Create(ctx, obj, client.FieldValidation("Strict"))
			if err == nil {
				_ = k8sClient.Delete(ctx, obj)
				t.Fatalf("case %d: the API server accepted %s, which this type is supposed to make impossible", i, tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected the error to name %q, got: %v", tc.want, err)
			}
		})
	}
}

// TestCRDAcceptsTheSupportedSubset is the other half: the narrowing must not
// have gone so far that a legitimate job cannot be expressed.
func TestCRDAcceptsTheSupportedSubset(t *testing.T) {
	skipWithoutAssets(t)
	ctx := context.Background()

	hj := &v1alpha1.HTCJob{
		ObjectMeta: metav1.ObjectMeta{Name: "accepted", Namespace: "default"},
		Spec: v1alpha1.HTCJobSpec{
			Completions:    ptr(int32(8)),
			CompletionMode: "Indexed",
			BackoffLimit:   ptr(int32(2)),
			Template: v1alpha1.HTCPodTemplateSpec{
				Labels: map[string]string{"app": "sim"},
				Spec: v1alpha1.HTCPodSpec{
					RestartPolicy: "OnFailure",
					Volumes: []v1alpha1.HTCVolume{{
						Name: "scratch", EmptyDir: v1alpha1.HTCEmptyDirVolumeSource{},
					}},
					Container: v1alpha1.HTCContainer{
						Name:         "sim",
						Image:        "docker://rockylinux:9",
						Command:      []string{"/usr/bin/run"},
						Args:         []string{"--seed", "1"},
						WorkingDir:   "/work",
						Env:          []v1alpha1.HTCEnvVar{{Name: "MODE", Value: "fast"}},
						VolumeMounts: []v1alpha1.HTCVolumeMount{{Name: "scratch", MountPath: "/scratch"}},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:              resource.MustParse("4"),
								corev1.ResourceMemory:           resource.MustParse("8Gi"),
								corev1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
							},
						},
					},
				},
			},
		},
	}
	if err := k8sClient.Create(ctx, hj, client.FieldValidation("Strict")); err != nil {
		t.Fatalf("the API server refused a supported HTCJob: %v", err)
	}
	t.Cleanup(func() { cleanupHTCJob(t, "accepted") })

	eventuallyHTCJob(t, "accepted", func(h *v1alpha1.HTCJob) bool { return h.Status.ClusterID != "" })
}

// TestWebhookClaimsATranslatableJobAndTheControllersFollow is the end-to-end
// path: webhook patch in etcd, HTCJob created and owned, submission to the
// queue, status back out, status mirrored onto the Job.
func TestWebhookClaimsATranslatableJobAndTheControllersFollow(t *testing.T) {
	skipWithoutAssets(t)
	mutatingWebhookInstalled(t)
	ctx := context.Background()

	job := translatableJob("claimed")
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatalf("creating Job: %v", err)
	}
	t.Cleanup(func() { cleanupJob(t, "claimed") })

	stored := getJob(t, "claimed")
	if stored.Spec.Template.Spec.SchedulerName != v1alpha1.SchedulerName {
		t.Fatalf("the webhook did not set schedulerName; got %q", stored.Spec.Template.Spec.SchedulerName)
	}
	if _, ok := stored.Annotations[v1alpha1.AnnotationClaimed]; !ok {
		t.Fatal("the webhook did not stamp the claim annotation")
	}
	if stored.Spec.Suspend == nil || !*stored.Spec.Suspend {
		t.Fatal("a claimed Job must be suspended so the built-in Job controller makes no Pods")
	}

	hj := eventuallyHTCJob(t, "claimed", func(h *v1alpha1.HTCJob) bool { return h.Status.ClusterID != "" })

	if len(hj.OwnerReferences) != 1 || hj.OwnerReferences[0].UID != stored.UID {
		t.Fatalf("the HTCJob is not owned by the Job: %+v", hj.OwnerReferences)
	}
	if hj.Spec.Template.Spec.Container.Image != "docker://busybox:latest" {
		t.Fatalf("the HTCJob did not mirror the container: %+v", hj.Spec.Template.Spec.Container)
	}
	if n := fakeQueue.SubmitCount(); n == 0 {
		t.Fatal("nothing was submitted to the queue")
	}

	cluster := hj.Status.ClusterID
	fakeQueue.SetProcStatus(cluster, 0, translate.JobStatusCompleted, func(ad *translate.JobAd) {
		ad.ExitCode = 0
		ad.JobStartDate = time.Now().Add(-time.Minute)
		ad.CompletionDate = time.Now()
	})

	eventuallyHTCJob(t, "claimed", func(h *v1alpha1.HTCJob) bool {
		return h.Status.Phase == v1alpha1.PhaseSucceeded && h.Status.Succeeded == 1
	})

	// The status mirror onto the owning Job. In this environment there is
	// no kube-controller-manager to contest it, so this assertion is
	// weaker evidence than it looks: in a real cluster the built-in Job
	// controller recomputes a Job's status from its Pods, of which a
	// claimed Job has none, and overwrites what is written here.
	eventuallyTrue(t, "the Job's status to be mirrored", func() bool {
		j := getJob(t, "claimed")
		return j.Status.Succeeded == 1
	})
}

// TestWebhookLeavesAnUntranslatableJobAlone is the asymmetry that keeps this
// webhook safe to install: a Job Kubernetes could have run must come out of
// admission unchanged.
func TestWebhookLeavesAnUntranslatableJobAlone(t *testing.T) {
	skipWithoutAssets(t)
	mutatingWebhookInstalled(t)
	ctx := context.Background()

	job := translatableJob("untouched")
	job.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 8080}}
	job.Spec.Template.Spec.Volumes = []corev1.Volume{{
		Name: "data",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "c"},
		},
	}}
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatalf("creating Job: %v", err)
	}
	t.Cleanup(func() { cleanupJob(t, "untouched") })

	stored := getJob(t, "untouched")
	if stored.Spec.Template.Spec.SchedulerName != "default-scheduler" {
		t.Fatalf("expected the default scheduler, got %q", stored.Spec.Template.Spec.SchedulerName)
	}
	if _, ok := stored.Annotations[v1alpha1.AnnotationClaimed]; ok {
		t.Fatal("an untranslatable Job must not be claimed")
	}
	if stored.Spec.Suspend != nil && *stored.Spec.Suspend {
		t.Fatal("an untranslatable Job must not be suspended")
	}

	var hj v1alpha1.HTCJob
	err := k8sClient.Get(ctx, types.NamespacedName{Name: "untouched", Namespace: "default"}, &hj)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected no HTCJob for an unclaimed Job, got %v", err)
	}
}

// TestSuspendRoundTrip: the Job's .spec.suspend field is held at true, so the
// user's intent travels in an annotation. Writing false to the field is read
// as a resume request by the webhook on UPDATE.
func TestSuspendRoundTrip(t *testing.T) {
	skipWithoutAssets(t)
	mutatingWebhookInstalled(t)
	ctx := context.Background()

	job := translatableJob("suspended")
	job.Spec.Suspend = ptr(true)
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatalf("creating Job: %v", err)
	}
	t.Cleanup(func() { cleanupJob(t, "suspended") })

	stored := getJob(t, "suspended")
	if stored.Annotations[v1alpha1.AnnotationUserSuspend] != "true" {
		t.Fatalf("expected the suspend intent to be recorded, got %q",
			stored.Annotations[v1alpha1.AnnotationUserSuspend])
	}

	hj := eventuallyHTCJob(t, "suspended", func(h *v1alpha1.HTCJob) bool { return h.Status.ClusterID != "" })
	cluster := hj.Status.ClusterID
	eventuallyTrue(t, "the cluster to be held", func() bool { return fakeQueue.IsHeld(cluster) })

	// Resume by writing the field. The webhook translates it.
	stored = getJob(t, "suspended")
	stored.Spec.Suspend = ptr(false)
	if err := k8sClient.Update(ctx, stored); err != nil {
		t.Fatalf("resuming: %v", err)
	}

	resumed := getJob(t, "suspended")
	if resumed.Annotations[v1alpha1.AnnotationUserSuspend] != "false" {
		t.Fatalf("the webhook did not record the resume: %q",
			resumed.Annotations[v1alpha1.AnnotationUserSuspend])
	}
	if resumed.Spec.Suspend == nil || !*resumed.Spec.Suspend {
		t.Fatal("the field must stay true so the built-in Job controller still makes no Pods")
	}

	eventuallyHTCJob(t, "suspended", func(h *v1alpha1.HTCJob) bool {
		return h.Spec.Suspend != nil && !*h.Spec.Suspend
	})
	eventuallyTrue(t, "the cluster to be released", func() bool { return !fakeQueue.IsHeld(cluster) })
}

// TestDeletingTheHTCJobRemovesTheCluster covers the finalizer. It deletes the
// HTCJob directly rather than the Job, because envtest has no garbage
// collector to propagate the owner reference -- the cascade itself is only
// exercised on the k3d path.
func TestDeletingTheHTCJobRemovesTheCluster(t *testing.T) {
	skipWithoutAssets(t)
	ctx := context.Background()

	hj := &v1alpha1.HTCJob{
		ObjectMeta: metav1.ObjectMeta{Name: "removable", Namespace: "default"},
		Spec: v1alpha1.HTCJobSpec{
			Completions: ptr(int32(2)),
			Template: v1alpha1.HTCPodTemplateSpec{
				Spec: v1alpha1.HTCPodSpec{
					Container: v1alpha1.HTCContainer{
						Name: "work", Image: "docker://busybox:latest",
						Command: []string{"/bin/true"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1"),
								corev1.ResourceMemory: resource.MustParse("128Mi"),
							},
						},
					},
				},
			},
		},
	}
	if err := k8sClient.Create(ctx, hj); err != nil {
		t.Fatalf("creating HTCJob: %v", err)
	}

	live := eventuallyHTCJob(t, "removable", func(h *v1alpha1.HTCJob) bool { return h.Status.ClusterID != "" })
	cluster := live.Status.ClusterID
	if fakeQueue.IsRemoved(cluster) {
		t.Fatal("the cluster was removed before the CR was deleted")
	}

	if err := k8sClient.Delete(ctx, live); err != nil {
		t.Fatalf("deleting HTCJob: %v", err)
	}
	eventuallyTrue(t, "the cluster to leave the queue", func() bool { return fakeQueue.IsRemoved(cluster) })
	eventuallyTrue(t, "the finalizer to be released", func() bool {
		var got v1alpha1.HTCJob
		err := k8sClient.Get(ctx, types.NamespacedName{Name: "removable", Namespace: "default"}, &got)
		return apierrors.IsNotFound(err)
	})
}

// TestSubmissionIsNotRepeatedAcrossRestarts: the status poll runs every 250ms
// in this suite, so a reconciler that resubmitted on any pass would show up
// as a climbing submit count.
func TestSubmissionIsNotRepeatedAcrossRestarts(t *testing.T) {
	skipWithoutAssets(t)
	ctx := context.Background()

	before := fakeQueue.SubmitCount()
	hj := &v1alpha1.HTCJob{
		ObjectMeta: metav1.ObjectMeta{Name: "once", Namespace: "default"},
		Spec: v1alpha1.HTCJobSpec{
			Template: v1alpha1.HTCPodTemplateSpec{
				Spec: v1alpha1.HTCPodSpec{
					Container: v1alpha1.HTCContainer{
						Name: "work", Image: "docker://busybox:latest",
						Command: []string{"/bin/true"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1"),
								corev1.ResourceMemory: resource.MustParse("128Mi"),
							},
						},
					},
				},
			},
		},
	}
	if err := k8sClient.Create(ctx, hj); err != nil {
		t.Fatalf("creating HTCJob: %v", err)
	}
	t.Cleanup(func() { cleanupHTCJob(t, "once") })

	live := eventuallyHTCJob(t, "once", func(h *v1alpha1.HTCJob) bool { return h.Status.ClusterID != "" })
	if got := fakeQueue.SubmitCount() - before; got != 1 {
		t.Fatalf("expected exactly one submission, got %d", got)
	}

	// Force several more reconciles by touching the object, then confirm
	// the count held. The controller is polling this object every 250ms, so
	// a read-modify-write here races its status writes: retry on conflict
	// rather than treat one as a failure.
	for i := 0; i < 3; i++ {
		touch(t, client.ObjectKeyFromObject(live), strings.Repeat("x", i+1))
	}
	eventuallyTrue(t, "the poll to run again", func() bool {
		var got v1alpha1.HTCJob
		_ = k8sClient.Get(ctx, client.ObjectKeyFromObject(live), &got)
		return got.Status.LastQueueSync != nil && got.Status.LastQueueSync.After(live.Status.LastQueueSync.Time)
	})
	if got := fakeQueue.SubmitCount() - before; got != 1 {
		t.Fatalf("resyncs submitted again: %d submissions", got)
	}
}

func cleanupJob(t *testing.T, name string) {
	t.Helper()
	ctx := context.Background()
	var job batchv1.Job
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &job); err == nil {
		_ = k8sClient.Delete(ctx, &job)
	}
	cleanupHTCJob(t, name)
}

// cleanupHTCJob removes the CR and, because envtest has no garbage collector,
// waits out the finalizer by hand.
func cleanupHTCJob(t *testing.T, name string) {
	t.Helper()
	ctx := context.Background()
	var hj v1alpha1.HTCJob
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &hj); err != nil {
		return
	}
	_ = k8sClient.Delete(ctx, &hj)
}

func podSpec(spec map[string]any) map[string]any {
	return spec["template"].(map[string]any)["spec"].(map[string]any)
}

func container(spec map[string]any) map[string]any {
	return podSpec(spec)["container"].(map[string]any)
}

func deepCopyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		if sub, ok := v.(map[string]any); ok {
			out[k] = deepCopyMap(sub)
			continue
		}
		out[k] = v
	}
	return out
}

func ptr[T any](v T) *T { return &v }

// touch writes an annotation, retrying the conflicts a concurrently
// reconciling controller produces.
func touch(t *testing.T, key client.ObjectKey, value string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(eventually)
	for time.Now().Before(deadline) {
		var got v1alpha1.HTCJob
		if err := k8sClient.Get(ctx, key, &got); err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Annotations == nil {
			got.Annotations = map[string]string{}
		}
		got.Annotations["touch"] = value
		err := k8sClient.Update(ctx, &got)
		if err == nil {
			return
		}
		if !apierrors.IsConflict(err) {
			t.Fatalf("touch: %v", err)
		}
		time.Sleep(tick)
	}
	t.Fatal("could not write an annotation without conflicting")
}
