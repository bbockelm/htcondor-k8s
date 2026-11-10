// Package v1alpha1 holds the HTCJob API.
//
// The group is deliberately not batch.k8s.io-shaped: an HTCJob is not a
// drop-in Job, and pretending otherwise would invite users to reach for fields
// that do not exist here.
//
// +kubebuilder:object:generate=true
// +groupName=htcondor.org
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the group/version for this API.
	GroupVersion = schema.GroupVersion{Group: "htcondor.org", Version: "v1alpha1"}

	// SchemeBuilder registers the types with a runtime.Scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to a Scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
