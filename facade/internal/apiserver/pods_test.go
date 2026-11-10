package apiserver

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// The Pod tests go through client-go rather than net/http. A facade is only
// useful if the standard client can drive it, and the standard client is
// fussier than a hand-written request about content types, status bodies and
// the shape of a List.
func clientFor(t *testing.T, f *fixture) kubernetes.Interface {
	t.Helper()
	cs, err := kubernetes.NewForConfig(&rest.Config{Host: f.HTTP.URL})
	if err != nil {
		t.Fatalf("building a clientset: %v", err)
	}
	return cs
}

func demoPod(name, ns string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{"app": "demo"}},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "main",
				Image:   "docker://busybox:latest",
				Command: []string{"/bin/sh", "-c", "echo hello"},
				Env:     []corev1.EnvVar{{Name: "GREETING", Value: "hi there"}},
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("2"),
					corev1.ResourceMemory:           resource.MustParse("512Mi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
				}},
			}},
		},
	}
}

func TestCreatePodSubmitsAJobWithTheRightResources(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)

	created, err := cs.CoreV1().Pods("research").Create(context.Background(), demoPod("hello", "research"), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating a pod: %v", err)
	}
	if created.Name != "hello" || created.Namespace != "research" {
		t.Fatalf("got %s/%s", created.Namespace, created.Name)
	}
	if created.ResourceVersion == "" || created.ResourceVersion == "0" {
		t.Errorf("a created object must carry a usable resourceVersion, got %q", created.ResourceVersion)
	}
	if created.UID == "" {
		t.Error("a created object must carry a UID")
	}

	// Assert on the job ad the submit file actually produced, not on the
	// facade's reading of it: the interesting failure is submit text that
	// parses but says something other than what the Pod said.
	ad := f.Fake.Ad("1.0")
	if ad == nil {
		t.Fatal("no job 1.0 in the queue")
	}
	if cpus, _ := ad.EvaluateAttrInt("RequestCpus"); cpus != 2 {
		t.Errorf("RequestCpus = %d, want 2", cpus)
	}
	if mem, _ := ad.EvaluateAttrInt("RequestMemory"); mem != 512 {
		t.Errorf("RequestMemory = %d MB, want 512", mem)
	}
	if disk, _ := ad.EvaluateAttrInt("RequestDisk"); disk != 2*1024*1024 {
		t.Errorf("RequestDisk = %d KB, want %d", disk, 2*1024*1024)
	}
	for attr, want := range map[string]string{
		translate.AttrPodName:      "hello",
		translate.AttrPodNamespace: "research",
	} {
		if got, _ := ad.EvaluateAttrString(attr); got != want {
			t.Errorf("%s = %q, want %q", attr, got, want)
		}
	}
	// The whole PodSpec round-trips through the job ad, which is what lets
	// a system with no etcd answer `kubectl get pod -o yaml`.
	raw, _ := ad.EvaluateAttrString(translate.AttrPodSpec)
	var spec corev1.PodSpec
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		t.Fatalf("K8sPodSpec does not round-trip: %v", err)
	}
	if len(spec.Containers) != 1 || spec.Containers[0].Image != "docker://busybox:latest" {
		t.Errorf("K8sPodSpec lost the container: %+v", spec.Containers)
	}
}

func TestCreatePodRejectionIsA422WithEveryReason(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)

	bad := demoPod("bad", "default")
	bad.Spec.RestartPolicy = corev1.RestartPolicyAlways
	bad.Spec.HostNetwork = true
	bad.Spec.NodeName = "node1"
	bad.Spec.Containers = append(bad.Spec.Containers, corev1.Container{Name: "sidecar", Image: "x"})

	_, err := cs.CoreV1().Pods("default").Create(context.Background(), bad, metav1.CreateOptions{})
	if err == nil {
		t.Fatal("a pod that cannot be modeled as an HTCondor job must be rejected")
	}
	status, ok := err.(apierrors.APIStatus)
	if !ok {
		t.Fatalf("the error is not a Kubernetes Status: %T %v", err, err)
	}
	st := status.Status()
	if st.Code != 422 {
		t.Errorf("code = %d, want 422", st.Code)
	}
	if st.Reason != metav1.StatusReasonInvalid {
		t.Errorf("reason = %q, want Invalid", st.Reason)
	}
	// Every problem at once: a user fixing a manifest one round trip per
	// mistake is the failure mode this is here to avoid.
	fields := map[string]bool{}
	for _, c := range st.Details.Causes {
		fields[c.Field] = true
	}
	for _, want := range []string{"spec", "spec.nodeName", "spec.restartPolicy", "spec.containers"} {
		if !fields[want] {
			t.Errorf("no cause for %s; causes were %v", want, fields)
		}
	}
	for _, want := range []string{"hostNetwork", "restartPolicy Always", "cannot be pinned"} {
		if !strings.Contains(st.Message, want) {
			t.Errorf("the message a user sees does not mention %q: %s", want, st.Message)
		}
	}
	if f.Fake.Ad("1.0") != nil {
		t.Error("a rejected pod must not reach the queue")
	}
}

func TestCreatePodRejectsADuplicateName(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	if _, err := cs.CoreV1().Pods("default").Create(context.Background(), demoPod("dup", "default"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.sync()
	_, err := cs.CoreV1().Pods("default").Create(context.Background(), demoPod("dup", "default"), metav1.CreateOptions{})
	if !apierrors.IsAlreadyExists(err) {
		t.Fatalf("a second pod of the same name must be a 409 AlreadyExists, got %v", err)
	}
}

func TestGetAndListPodsWithSelectors(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx := context.Background()

	for _, name := range []string{"alpha", "beta"} {
		if _, err := cs.CoreV1().Pods("research").Create(ctx, demoPod(name, "research"), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	other := demoPod("gamma", "other")
	other.Labels = map[string]string{"app": "other"}
	if _, err := cs.CoreV1().Pods("other").Create(ctx, other, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.sync()

	got, err := cs.CoreV1().Pods("research").Get(ctx, "alpha", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.Phase != corev1.PodPending {
		t.Errorf("an idle job should read as Pending, got %s", got.Status.Phase)
	}
	if len(got.Spec.Containers) != 1 || got.Spec.Containers[0].Image != "docker://busybox:latest" {
		t.Error("the PodSpec did not survive the round trip through the job ad")
	}

	if _, err := cs.CoreV1().Pods("research").Get(ctx, "nope", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("a missing pod must be a 404 NotFound, got %v", err)
	}

	for _, tc := range []struct {
		name string
		ns   string
		opts metav1.ListOptions
		want []string
	}{
		{"namespace scope", "research", metav1.ListOptions{}, []string{"alpha", "beta"}},
		{"all namespaces", "", metav1.ListOptions{}, []string{"alpha", "beta", "gamma"}},
		{"label selector", "", metav1.ListOptions{LabelSelector: "app=demo"}, []string{"alpha", "beta"}},
		{"field selector phase", "", metav1.ListOptions{FieldSelector: "status.phase=Pending"}, []string{"alpha", "beta", "gamma"}},
		{"field selector no match", "", metav1.ListOptions{FieldSelector: "status.phase=Running"}, nil},
		{"name selector", "", metav1.ListOptions{FieldSelector: "metadata.name=beta"}, []string{"beta"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			list, err := cs.CoreV1().Pods(tc.ns).List(ctx, tc.opts)
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			var names []string
			for _, p := range list.Items {
				names = append(names, p.Name)
			}
			if strings.Join(names, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("got %v, want %v", names, tc.want)
			}
			if list.ResourceVersion == "" {
				t.Error("a list must carry the version a watch can resume from")
			}
		})
	}

	// An unsupported field selector is an error, not a silently dropped
	// clause: returning everything would be telling the user something
	// untrue about their pool.
	if _, err := cs.CoreV1().Pods("").List(ctx, metav1.ListOptions{FieldSelector: "spec.schedulerName=default"}); err == nil {
		t.Error("an unindexed field selector must be refused")
	}
}

func TestDeletePodRemovesTheJob(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx := context.Background()
	if _, err := cs.CoreV1().Pods("default").Create(ctx, demoPod("doomed", "default"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.sync()
	if err := cs.CoreV1().Pods("default").Delete(ctx, "doomed", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := cs.CoreV1().Pods("default").Get(ctx, "doomed", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("the pod should be gone immediately, not after the next poll; got %v", err)
	}
	recs, _ := f.Fake.Query(ctx, "true")
	if len(recs) != 0 {
		t.Errorf("the job is still in the queue: %+v", recs)
	}
}

func TestPodStatusFollowsJobStatus(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx := context.Background()
	if _, err := cs.CoreV1().Pods("default").Create(ctx, demoPod("lifecycle", "default"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		status int
		want   corev1.PodPhase
	}{
		{translate.JobStatusIdle, corev1.PodPending},
		{translate.JobStatusRunning, corev1.PodRunning},
		{translate.JobStatusCompleted, corev1.PodSucceeded},
	} {
		f.Fake.SetStatus("1.0", tc.status)
		f.sync()
		pod, err := cs.CoreV1().Pods("default").Get(ctx, "lifecycle", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("JobStatus %d: %v", tc.status, err)
		}
		if pod.Status.Phase != tc.want {
			t.Errorf("JobStatus %d read as %s, want %s", tc.status, pod.Status.Phase, tc.want)
		}
	}
}
