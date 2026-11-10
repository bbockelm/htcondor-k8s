package podctl_test

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
	htcwebhook "github.com/bbockelm/htcondor-k8s/virtualnode/internal/webhook"
)

func barePod(name string, admissible bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1.PodSpec{
			SchedulerName: api.SchedulerName,
			RestartPolicy: corev1.RestartPolicyNever,
			Tolerations:   htcwebhook.Tolerations(),
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
	}
	if !admissible {
		// Two containers and a hostPath: the whitelist refuses both, and
		// nothing admitted this object on its way in.
		pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
			Name: "sidecar", Image: "docker://busybox:latest",
		})
		pod.Spec.Volumes = []corev1.Volume{{
			Name:         "host",
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/etc"}},
		}}
	}
	return pod
}

// TestBarePodIsSubmittedWithoutAWebhook: a Pod created directly with
// schedulerName: htcondor is a first-class path. kube-scheduler ignores it,
// nothing stamped it, and the pod controller has to take it on its own.
func TestBarePodIsSubmittedWithoutAWebhook(t *testing.T) {
	skipWithoutAssets(t)
	ctx := context.Background()

	pod := barePod("bare-ok", true)
	if err := k8sClient.Create(ctx, pod); err != nil {
		t.Fatalf("creating the Pod: %v", err)
	}
	eventuallyTrue(t, "the bare Pod to be bound", func() bool {
		return getPod(t, "bare-ok").Spec.NodeName == api.NodeName
	})
	got := getPod(t, "bare-ok")
	if got.Annotations[api.AnnotationJobID] == "" {
		t.Fatal("no HTCondor job id was recorded")
	}
	if got.Status.Phase != corev1.PodPending {
		t.Fatalf("phase is %s, want Pending while the job is idle", got.Status.Phase)
	}
}

// TestInadmissiblePodIsRefusedAndNeverBound also proves something the unit
// tests cannot: that a real API server accepts a status update moving an
// *unbound* Pod to Failed. If it did not, an inadmissible bare Pod would have
// nowhere to put its explanation and would sit Pending forever.
func TestInadmissiblePodIsRefusedAndNeverBound(t *testing.T) {
	skipWithoutAssets(t)
	ctx := context.Background()

	if err := k8sClient.Create(ctx, barePod("bare-bad", false)); err != nil {
		t.Fatalf("creating the Pod: %v", err)
	}
	eventuallyTrue(t, "the bare Pod to be refused", func() bool {
		return getPod(t, "bare-bad").Status.Phase == corev1.PodFailed
	})

	got := getPod(t, "bare-bad")
	if got.Status.Reason != "NotAdmitted" {
		t.Fatalf("reason is %q, want NotAdmitted", got.Status.Reason)
	}
	if got.Spec.NodeName != "" {
		t.Fatalf("an inadmissible Pod was bound to %q", got.Spec.NodeName)
	}
	for _, want := range []string{"spec.containers", "spec.volumes[0]"} {
		if !contains(got.Status.Message, want) {
			t.Fatalf("message does not name %s: %q", want, got.Status.Message)
		}
	}
}

// TestDeletingAPodRemovesTheHTCondorJob: the finalizer has to come off, and
// the job has to be gone before it does.
func TestDeletingAPodRemovesTheHTCondorJob(t *testing.T) {
	skipWithoutAssets(t)
	ctx := context.Background()

	if err := k8sClient.Create(ctx, barePod("bare-delete", true)); err != nil {
		t.Fatalf("creating the Pod: %v", err)
	}
	eventuallyTrue(t, "the Pod to be bound", func() bool {
		return getPod(t, "bare-delete").Annotations[api.AnnotationJobID] != ""
	})
	jobID := getPod(t, "bare-delete").Annotations[api.AnnotationJobID]

	if err := k8sClient.Delete(ctx, getPod(t, "bare-delete")); err != nil {
		t.Fatalf("deleting the Pod: %v", err)
	}
	eventuallyTrue(t, "the Pod object to go away", func() bool {
		var p corev1.Pod
		err := k8sClient.Get(ctx, client.ObjectKey{Namespace: "default", Name: "bare-delete"}, &p)
		return err != nil
	})
	if !fakeQueue.IsRemoved(jobID) {
		t.Fatalf("HTCondor job %s is still in the queue after its Pod was deleted", jobID)
	}
}

// TestNodeNameCannotBeSetByAnOrdinaryUpdate is the executable form of the
// comment on PodReconciler.bind.
//
// It is not a style preference that binding goes through pods/binding: the
// Pod strategy's ValidatePodUpdate allows a Pod's spec to change in a short
// allowlist of ways and nodeName is not one of them. This asserts both halves
// -- that the plain update is refused and that the subresource succeeds --
// because a comment claiming an API server behaves a certain way is worth
// exactly as much as the test under it.
func TestNodeNameCannotBeSetByAnOrdinaryUpdate(t *testing.T) {
	skipWithoutAssets(t)
	ctx := context.Background()

	// Addressed to another scheduler so that the pod controller leaves it
	// alone and this test has the Pod to itself.
	pod := barePod("binding-probe", true)
	pod.Spec.SchedulerName = "some-other-scheduler"
	if err := k8sClient.Create(ctx, pod); err != nil {
		t.Fatalf("creating the Pod: %v", err)
	}

	live := getPod(t, "binding-probe")
	live.Spec.NodeName = api.NodeName
	if err := k8sClient.Update(ctx, live); err == nil {
		t.Fatal("the API server accepted a plain update setting spec.nodeName; " +
			"the binding subresource would then be optional, and it is not")
	}

	live = getPod(t, "binding-probe")
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{Namespace: live.Namespace, Name: live.Name, UID: live.UID},
		Target:     corev1.ObjectReference{Kind: "Node", Name: api.NodeName},
	}
	if err := k8sClient.SubResource("binding").Create(ctx, live, binding); err != nil {
		t.Fatalf("binding through pods/binding: %v", err)
	}
	if got := getPod(t, "binding-probe").Spec.NodeName; got != api.NodeName {
		t.Fatalf("after binding, nodeName is %q", got)
	}

	// And a second binding is a conflict rather than a silent overwrite,
	// which is what stops two controllers quietly disagreeing about where
	// a Pod ran.
	live = getPod(t, "binding-probe")
	if err := k8sClient.SubResource("binding").Create(ctx, live, binding); err == nil {
		t.Fatal("re-binding an already-bound Pod was accepted")
	}
}

// --- the webhook, through a real admission round trip -------------------

// TestAPIServerDefaultsPodReplacementPolicyBeforeAdmission pins the finding
// that shaped the webhook, and it is the same shape as the trap
// docs/OPERATOR.md records for .spec.selector: a field the API server fills in
// while decoding, before mutating admission runs, cannot carry user intent to
// a webhook.
//
// An earlier version of this webhook declined to stamp any Job whose
// podReplacementPolicy was explicitly TerminatingOrFailed, reasoning that
// overriding a user's field was the operator's .spec.suspend injury again. It
// declined every Job in the cluster, because there is no such thing as a Job
// whose podReplacementPolicy is unset by the time a webhook sees it. This test
// is what says so.
func TestAPIServerDefaultsPodReplacementPolicyBeforeAdmission(t *testing.T) {
	skipWithoutAssets(t)
	ctx := context.Background()

	// No opt-in label, so this Job never reaches our webhook: what comes
	// back is the API server's own defaulting and nothing else.
	job := translatableJob("defaulting-probe")
	job.Labels = nil
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatalf("creating the Job: %v", err)
	}

	stored := getJob(t, "defaulting-probe")
	if stored.Spec.Template.Spec.SchedulerName == api.SchedulerName {
		t.Fatal("a Job without the opt-in label was stamped; the objectSelector is not working")
	}
	if stored.Spec.PodReplacementPolicy == nil {
		t.Skip("this API server does not default podReplacementPolicy; " +
			"the webhook may set it only when unset, and should be simplified to do so")
	}
	if *stored.Spec.PodReplacementPolicy != batchv1.TerminatingOrFailed {
		t.Fatalf("defaulted podReplacementPolicy is %q, want TerminatingOrFailed",
			*stored.Spec.PodReplacementPolicy)
	}
}

// TestUntranslatableJobReachesEtcdByteIdentical is the asymmetry argument,
// through a real API server rather than against the handler: declining to
// take a Job costs that Job nothing at all.
func TestUntranslatableJobReachesEtcdByteIdentical(t *testing.T) {
	skipWithoutAssets(t)
	mutatingWebhookInstalled(t)
	ctx := context.Background()

	job := translatableJob("untranslatable")
	job.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 80}}
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatalf("creating the Job: %v", err)
	}

	stored := getJob(t, "untranslatable")
	if stored.Spec.Template.Spec.SchedulerName == api.SchedulerName {
		t.Fatal("a Job with a container port was stamped")
	}
	if len(stored.Spec.Template.Spec.Tolerations) != 0 {
		t.Fatalf("an unstamped Job picked up tolerations: %v", stored.Spec.Template.Spec.Tolerations)
	}
}

// TestOnFailureJobIsNotStamped: there is no kubelet to restart a container in
// place, so this approach refuses OnFailure although shared/translate admits
// it. The Job then runs on Kubernetes, where OnFailure works properly, which
// is the right outcome for the user.
func TestOnFailureJobIsNotStamped(t *testing.T) {
	skipWithoutAssets(t)
	ctx := context.Background()

	job := translatableJob("onfailure")
	job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatalf("creating the Job: %v", err)
	}
	if getJob(t, "onfailure").Spec.Template.Spec.SchedulerName == api.SchedulerName {
		t.Fatal("an OnFailure Job was stamped")
	}
}

// TestStampedTemplateTolerationsSurviveAdmission checks that the tolerations
// the webhook writes are the ones that reach etcd -- in particular that
// tolerationSeconds stays nil rather than being defaulted to 300 by the
// DefaultTolerationSeconds admission plugin, which is the whole point of
// writing them.
func TestStampedTemplateTolerationsSurviveAdmission(t *testing.T) {
	skipWithoutAssets(t)
	ctx := context.Background()

	if err := k8sClient.Create(ctx, translatableJob("tolerations")); err != nil {
		t.Fatalf("creating the Job: %v", err)
	}
	stored := getJob(t, "tolerations")
	for _, key := range []string{corev1.TaintNodeNotReady, corev1.TaintNodeUnreachable} {
		var found *corev1.Toleration
		for i := range stored.Spec.Template.Spec.Tolerations {
			if stored.Spec.Template.Spec.Tolerations[i].Key == key {
				found = &stored.Spec.Template.Spec.Tolerations[i]
			}
		}
		if found == nil {
			t.Fatalf("no toleration for %s survived admission", key)
		}
		if found.TolerationSeconds != nil {
			t.Fatalf("%s has tolerationSeconds=%d; it must be tolerated forever, "+
				"or a restart longer than that deletes live Pods and condor_rm's their jobs",
				key, *found.TolerationSeconds)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
