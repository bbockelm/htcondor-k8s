// Package adapter converts between the HTCJob API and the batch/v1 shapes
// that shared/translate speaks.
//
// translate is the contract both integration approaches share and is
// deliberately written against the Kubernetes types, so the CRD has to meet it
// there rather than the other way around. Keeping the conversion in one place
// means there is exactly one answer to "what does this HTCJob actually submit",
// and the webhook's verdict on a Job and the controller's verdict on the HTCJob
// derived from it cannot drift: the round trip is covered by a test.
package adapter

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bbockelm/htcondor-k8s/operator/api/v1alpha1"
)

// JobToHTCJobSpec projects a batch/v1 JobSpec onto the CRD's narrower spec.
//
// It is lossy by construction: every field it drops is one translate.AdmitJob
// has already refused, so a Job that passed admission loses nothing. Calling it
// on an unadmitted Job silently discards meaning, which is why the Job
// controller re-runs admission before it ever gets here.
func JobToHTCJobSpec(js *batchv1.JobSpec) v1alpha1.HTCJobSpec {
	out := v1alpha1.HTCJobSpec{}
	if js == nil {
		return out
	}

	out.Completions = copyInt32(js.Completions)
	if out.Completions == nil {
		out.Completions = ptr(int32(1))
	}
	out.CompletionMode = "NonIndexed"
	if js.CompletionMode != nil && *js.CompletionMode == batchv1.IndexedCompletion {
		out.CompletionMode = "Indexed"
	}
	out.BackoffLimit = copyInt32(js.BackoffLimit)
	if js.ActiveDeadlineSeconds != nil {
		out.ActiveDeadlineSeconds = ptr(*js.ActiveDeadlineSeconds)
	} else if js.Template.Spec.ActiveDeadlineSeconds != nil {
		// A deadline may be set on either the Job or the pod template;
		// Kubernetes enforces whichever fires first, and for a
		// single-attempt job they mean the same thing.
		out.ActiveDeadlineSeconds = ptr(*js.Template.Spec.ActiveDeadlineSeconds)
	}
	out.Suspend = copyBool(js.Suspend)

	out.Template = v1alpha1.HTCPodTemplateSpec{
		Labels:      copyMap(js.Template.Labels),
		Annotations: copyMap(js.Template.Annotations),
		Spec:        podToHTCPodSpec(&js.Template.Spec),
	}
	return out
}

func podToHTCPodSpec(ps *corev1.PodSpec) v1alpha1.HTCPodSpec {
	out := v1alpha1.HTCPodSpec{
		RestartPolicy: string(corev1.RestartPolicyNever),
		NodeSelector:  copyMap(ps.NodeSelector),
	}
	if ps.RestartPolicy == corev1.RestartPolicyOnFailure {
		out.RestartPolicy = string(corev1.RestartPolicyOnFailure)
	}
	for _, v := range ps.Volumes {
		if v.EmptyDir == nil {
			continue
		}
		hv := v1alpha1.HTCVolume{Name: v.Name}
		if sl := v.EmptyDir.SizeLimit; sl != nil {
			hv.EmptyDir.SizeLimit = ptr(sl.Value())
		}
		out.Volumes = append(out.Volumes, hv)
	}
	if len(ps.Containers) > 0 {
		out.Container = containerToHTC(&ps.Containers[0])
	}
	return out
}

func containerToHTC(c *corev1.Container) v1alpha1.HTCContainer {
	out := v1alpha1.HTCContainer{
		Name:       c.Name,
		Image:      c.Image,
		Command:    append([]string(nil), c.Command...),
		Args:       append([]string(nil), c.Args...),
		WorkingDir: c.WorkingDir,
		Resources:  *c.Resources.DeepCopy(),
	}
	for _, e := range c.Env {
		// ValueFrom was rejected at admission, so dropping it here can
		// only happen for a caller that skipped admission.
		out.Env = append(out.Env, v1alpha1.HTCEnvVar{Name: e.Name, Value: e.Value})
	}
	for _, m := range c.VolumeMounts {
		out.VolumeMounts = append(out.VolumeMounts, v1alpha1.HTCVolumeMount{
			Name: m.Name, MountPath: m.MountPath, ReadOnly: m.ReadOnly,
		})
	}
	return out
}

// HTCJobToJobSpec is the inverse projection: the batch/v1 JobSpec an HTCJob
// stands for. The controller feeds this to translate.AdmitJob and
// translate.JobSubmitOptions, so the CRD gets exactly the same admission
// verdict the webhook gave the original Job, and a hand-written HTCJob that
// never passed through a Job gets that verdict too.
func HTCJobToJobSpec(hs *v1alpha1.HTCJobSpec) *batchv1.JobSpec {
	js := &batchv1.JobSpec{
		Completions:           copyInt32(hs.Completions),
		BackoffLimit:          copyInt32(hs.BackoffLimit),
		ActiveDeadlineSeconds: copyInt64(hs.ActiveDeadlineSeconds),
		Suspend:               copyBool(hs.Suspend),
	}
	// AdmitJob refuses parallelism below completions. Setting Parallelism
	// equal to Completions, rather than leaving it nil, states the fact
	// that every proc runs as soon as it matches.
	js.Parallelism = copyInt32(js.Completions)
	if hs.CompletionMode == "Indexed" {
		mode := batchv1.IndexedCompletion
		js.CompletionMode = &mode
	}
	js.Template = corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      copyMap(hs.Template.Labels),
			Annotations: copyMap(hs.Template.Annotations),
		},
		Spec: htcPodSpecToPod(&hs.Template.Spec),
	}
	return js
}

func htcPodSpecToPod(hp *v1alpha1.HTCPodSpec) corev1.PodSpec {
	ps := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		NodeSelector:  copyMap(hp.NodeSelector),
	}
	if hp.RestartPolicy == string(corev1.RestartPolicyOnFailure) {
		ps.RestartPolicy = corev1.RestartPolicyOnFailure
	}
	for _, v := range hp.Volumes {
		src := corev1.EmptyDirVolumeSource{}
		if v.EmptyDir.SizeLimit != nil {
			q := resource.NewQuantity(*v.EmptyDir.SizeLimit, resource.BinarySI)
			src.SizeLimit = q
		}
		ps.Volumes = append(ps.Volumes, corev1.Volume{
			Name:         v.Name,
			VolumeSource: corev1.VolumeSource{EmptyDir: &src},
		})
	}
	ps.Containers = []corev1.Container{htcContainerToPod(&hp.Container)}
	return ps
}

func htcContainerToPod(hc *v1alpha1.HTCContainer) corev1.Container {
	c := corev1.Container{
		Name:       hc.Name,
		Image:      hc.Image,
		Command:    append([]string(nil), hc.Command...),
		Args:       append([]string(nil), hc.Args...),
		WorkingDir: hc.WorkingDir,
		Resources:  *hc.Resources.DeepCopy(),
	}
	for _, e := range hc.Env {
		c.Env = append(c.Env, corev1.EnvVar{Name: e.Name, Value: e.Value})
	}
	for _, m := range hc.VolumeMounts {
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
			Name: m.Name, MountPath: m.MountPath, ReadOnly: m.ReadOnly,
		})
	}
	return c
}

// Completions is .spec.completions with the default applied, which several
// callers need as a plain int32.
func Completions(hs *v1alpha1.HTCJobSpec) int32 {
	if hs.Completions != nil && *hs.Completions > 0 {
		return *hs.Completions
	}
	return 1
}

// Suspended reports whether the procs should be held.
func Suspended(hs *v1alpha1.HTCJobSpec) bool {
	return hs.Suspend != nil && *hs.Suspend
}

func ptr[T any](v T) *T { return &v }

func copyInt32(v *int32) *int32 {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

func copyInt64(v *int64) *int64 {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

func copyBool(v *bool) *bool {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

func copyMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Labels and the selector key the API server generates for every Job.
const (
	labelControllerUID       = "batch.kubernetes.io/controller-uid"
	labelJobName             = "batch.kubernetes.io/job-name"
	legacyLabelControllerUID = "controller-uid"
	legacyLabelJobName       = "job-name"
)

// NormalizeStoredJob strips the bookkeeping the API server adds to a Job on
// its way into etcd, so that what we record as the user's labels is what the
// user actually wrote.
//
// The batch registry strategy stamps controller-uid and job-name onto the pod
// template after admission has run. Those are the Job controller's internal
// join keys, not user intent, and carrying them into the job ad's +K8sLabels
// would make them look like something a user could select on.
//
// It deliberately leaves .spec.selector alone. The registry generates one too,
// and admission used to refuse it --- claiming a Job and then refusing it ever
// after, which left the Job suspended forever --- but translate.AdmitJob now
// keys on .spec.manualSelector, the API's own flag for "the user set this", so
// a generated selector is no longer anyone's problem.
func NormalizeStoredJob(js *batchv1.JobSpec) *batchv1.JobSpec {
	out := js.DeepCopy()
	for _, k := range []string{labelControllerUID, labelJobName, legacyLabelControllerUID, legacyLabelJobName} {
		delete(out.Template.Labels, k)
	}
	if len(out.Template.Labels) == 0 {
		out.Template.Labels = nil
	}
	return out
}
