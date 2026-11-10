package apiserver

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

// Discovery is asserted through the same client library kubectl uses rather
// than by reading the JSON back. A document that decodes but does not let a
// RESTMapper resolve "pods" is a document that passes a hand-written test and
// fails the only client that matters.
func discoveryClient(t *testing.T, f *fixture) discovery.DiscoveryInterface {
	t.Helper()
	c, err := discovery.NewDiscoveryClientForConfig(&rest.Config{Host: f.HTTP.URL})
	if err != nil {
		t.Fatalf("building a discovery client: %v", err)
	}
	return c
}

func TestDiscoveryResolvesPodsAndJobs(t *testing.T) {
	f := newFixture(t)
	dc := discoveryClient(t, f)

	groupResources, err := restmapper.GetAPIGroupResources(dc)
	if err != nil {
		t.Fatalf("a real Kubernetes discovery client could not read this server: %v", err)
	}
	// The shortcut expander is what turns "po" into "pods"; building the
	// mapper the way kubectl does is the only way to find out that the
	// shortNames in the APIResourceList are actually usable.
	mapper := restmapper.NewShortcutExpander(
		restmapper.NewDiscoveryRESTMapper(groupResources), dc, func(string) {})

	for _, tc := range []struct {
		arg  string
		want schema.GroupVersionKind
	}{
		{"pods", schema.GroupVersionKind{Version: "v1", Kind: "Pod"}},
		{"po", schema.GroupVersionKind{Version: "v1", Kind: "Pod"}},
		{"pod", schema.GroupVersionKind{Version: "v1", Kind: "Pod"}},
		{"jobs", schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"}},
		{"job", schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"}},
		{"jobs.batch", schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"}},
	} {
		gvr, gk := schema.ParseResourceArg(tc.arg)
		if gvr == nil {
			gvr = &schema.GroupVersionResource{Group: gk.Group, Resource: gk.Resource}
		}
		full, err := mapper.ResourceFor(*gvr)
		if err != nil {
			t.Errorf("kubectl could not resolve %q: %v", tc.arg, err)
			continue
		}
		kind, err := mapper.KindFor(full)
		if err != nil {
			t.Errorf("kubectl could not find a kind for %q: %v", tc.arg, err)
			continue
		}
		if kind != tc.want {
			t.Errorf("%q resolved to %v, want %v", tc.arg, kind, tc.want)
			continue
		}
		mapping, err := mapper.RESTMapping(kind.GroupKind(), kind.Version)
		if err != nil {
			t.Errorf("kubectl could not map %q: %v", tc.arg, err)
			continue
		}
		if mapping.Scope.Name() != meta.RESTScopeNameNamespace {
			t.Errorf("%q should be namespaced", tc.arg)
		}
	}
}

func TestDiscoveryAdvertisesTheVerbsWeImplement(t *testing.T) {
	f := newFixture(t)
	dc := discoveryClient(t, f)

	for _, gv := range []string{"v1", "batch/v1"} {
		list, err := dc.ServerResourcesForGroupVersion(gv)
		if err != nil {
			t.Fatalf("reading %s: %v", gv, err)
		}
		for _, r := range list.APIResources {
			if r.Name != "pods" && r.Name != "jobs" {
				continue
			}
			// Advertising a verb the server does not implement is
			// worse than not advertising it: kubectl offers it to
			// the user and then fails somewhere unhelpful.
			for _, verb := range []string{"create", "delete", "get", "list", "watch"} {
				if !containsString(r.Verbs, verb) {
					t.Errorf("%s/%s does not advertise %q", gv, r.Name, verb)
				}
			}
			for _, verb := range []string{"update", "patch", "deletecollection"} {
				if containsString(r.Verbs, verb) {
					t.Errorf("%s/%s advertises %q, which is not implemented", gv, r.Name, verb)
				}
			}
		}
	}
}

func TestVersionEndpoint(t *testing.T) {
	f := newFixture(t)
	v, err := discoveryClient(t, f).ServerVersion()
	if err != nil {
		t.Fatalf("/version: %v", err)
	}
	if v.Major != "1" || v.Minor != "26" {
		t.Fatalf("got version %s.%s, want 1.26", v.Major, v.Minor)
	}
}

func containsString(v metav1.Verbs, want string) bool {
	for _, s := range v {
		if s == want {
			return true
		}
	}
	return false
}
