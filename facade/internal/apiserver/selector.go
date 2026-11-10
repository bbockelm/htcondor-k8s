package apiserver

import (
	"fmt"
	"net/url"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// podFilter is the combined label and field selector of one list or watch.
type podFilter struct {
	labels labels.Selector
	// fields holds the field selector clauses the facade understands.
	name      string
	namespace string
	phase     string
	nodeName  string
}

// parseFilter reads the selectors off a request.
//
// An unsupported field selector is an error rather than a silently ignored
// clause. kubectl shows the user the list it got back; a facade that dropped
// `--field-selector spec.nodeName=x` and returned everything would be telling
// them something untrue about their pool.
func parseFilter(q url.Values) (podFilter, error) {
	var f podFilter
	sel, err := labels.Parse(q.Get("labelSelector"))
	if err != nil {
		return f, fmt.Errorf("labelSelector: %w", err)
	}
	f.labels = sel

	for _, clause := range strings.Split(q.Get("fieldSelector"), ",") {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			continue
		}
		key, value, ok := strings.Cut(clause, "=")
		if !ok || strings.HasSuffix(key, "!") {
			return f, fmt.Errorf("fieldSelector %q: only equality clauses are supported", clause)
		}
		switch strings.TrimSpace(key) {
		case "metadata.name":
			f.name = value
		case "metadata.namespace":
			f.namespace = value
		case "status.phase":
			f.phase = value
		case "spec.nodeName":
			f.nodeName = value
		default:
			return f, fmt.Errorf("fieldSelector %q: this server indexes only metadata.name, metadata.namespace, status.phase and spec.nodeName", clause)
		}
	}
	return f, nil
}

func (f podFilter) matchesPod(pod *corev1.Pod) bool {
	if f.labels != nil && !f.labels.Matches(labels.Set(pod.Labels)) {
		return false
	}
	if f.name != "" && pod.Name != f.name {
		return false
	}
	if f.namespace != "" && pod.Namespace != f.namespace {
		return false
	}
	if f.phase != "" && string(pod.Status.Phase) != f.phase {
		return false
	}
	if f.nodeName != "" && pod.Spec.NodeName != f.nodeName {
		return false
	}
	return true
}

// matchesMeta is the Job-side filter: Jobs have no phase, so only the label
// selector and the name clauses apply.
func (f podFilter) matchesMeta(name, namespace string, l map[string]string) bool {
	if f.labels != nil && !f.labels.Matches(labels.Set(l)) {
		return false
	}
	if f.name != "" && name != f.name {
		return false
	}
	if f.namespace != "" && namespace != f.namespace {
		return false
	}
	// A field selector that only a Pod can satisfy excludes every Job,
	// which is what the API server does too.
	return f.phase == "" && f.nodeName == ""
}
