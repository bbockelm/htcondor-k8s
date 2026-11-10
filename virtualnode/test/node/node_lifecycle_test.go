// Package node_test exercises the virtual node's lifecycle against a real
// kube-apiserver and etcd.
//
// A fake client would pass every one of these tests while the real thing was
// broken, which is the whole reason this suite exists. Only a real API server
// will tell you that a Lease with an ownerReference to a cluster-scoped Node
// is accepted, that a Node's status survives being set on create, that
// UpdateStatus does not silently drop daemonEndpoints, or that our capacity
// quantities round-trip through the API's own decoding. Each of those is a
// place where a wrong guess produces a node that looks right in a unit test
// and is NotReady in a cluster.
//
// What this suite still cannot see: envtest runs only the API server and
// etcd. There is no node lifecycle controller, so nothing here marks the node
// NotReady when the lease goes stale, and no garbage collector, so nothing
// here deletes the Lease when the Node is deleted. Both are asserted
// indirectly -- the lease's renewTime advancing, the ownerReference naming
// the live UID -- which is the closest this can get.
package node_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/node"
)

var (
	nodeTestEnv   *envtest.Environment
	nodeClient    kubernetes.Interface
	nodeRestCfg   *rest.Config
	assetsMissing bool
)

const (
	// eventually is the ceiling for something that normally takes one API
	// round trip. Generous, because a loaded machine starting etcd is slow
	// and a flaky timeout reads as a product bug.
	eventually = 30 * time.Second
	tick       = 50 * time.Millisecond
)

func TestMain(m *testing.M) {
	assets, ok := findAssets()
	if !ok {
		// Exit 0 with every test announcing its own skip: a machine
		// without the binaries must not fail the build, but an empty
		// run must never be mistaken for a passing one.
		fmt.Fprintln(os.Stderr,
			"envtest: no kube-apiserver/etcd binaries found; skipping.\n"+
				"  get them with: go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use -p path")
		assetsMissing = true
		os.Exit(m.Run())
	}
	os.Setenv("KUBEBUILDER_ASSETS", assets)

	code, err := run(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "envtest setup:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run(m *testing.M) (int, error) {
	nodeTestEnv = &envtest.Environment{}
	cfg, err := nodeTestEnv.Start()
	if err != nil {
		return 0, fmt.Errorf("starting the control plane: %w", err)
	}
	defer func() { _ = nodeTestEnv.Stop() }()
	nodeRestCfg = cfg

	nodeClient, err = kubernetes.NewForConfig(cfg)
	if err != nil {
		return 0, err
	}
	if err := ensureLeaseNamespace(context.Background()); err != nil {
		return 0, err
	}
	return m.Run(), nil
}

// ensureLeaseNamespace makes sure kube-node-lease is there. The API server
// creates it itself, but it does so from a bootstrap controller that races
// the first test.
func ensureLeaseNamespace(ctx context.Context) error {
	deadline := time.Now().Add(eventually)
	for time.Now().Before(deadline) {
		_, err := nodeClient.CoreV1().Namespaces().Get(ctx, corev1.NamespaceNodeLease, metav1.GetOptions{})
		if err == nil {
			return nil
		}
		if !apierrors.IsNotFound(err) {
			return err
		}
		_, err = nodeClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: corev1.NamespaceNodeLease},
		}, metav1.CreateOptions{})
		if err == nil || apierrors.IsAlreadyExists(err) {
			return nil
		}
		time.Sleep(tick)
	}
	return fmt.Errorf("kube-node-lease namespace never appeared")
}

func skipWithoutAssets(t *testing.T) {
	t.Helper()
	if assetsMissing {
		t.Skip("envtest binaries are not installed")
	}
}

func findAssets() (string, bool) {
	if p := os.Getenv("KUBEBUILDER_ASSETS"); p != "" {
		if _, err := os.Stat(filepath.Join(p, "kube-apiserver")); err == nil {
			return p, true
		}
		return "", false
	}
	out, err := exec.Command("go", "run",
		"sigs.k8s.io/controller-runtime/tools/setup-envtest@latest", "use", "-i", "-p", "path").Output()
	if err == nil {
		p := strings.TrimSpace(string(out))
		if _, err := os.Stat(filepath.Join(p, "kube-apiserver")); err == nil {
			return p, true
		}
	}
	return "", false
}

// --- helpers -----------------------------------------------------------

// config returns a controller configuration with cadences compressed so a
// test can watch several heartbeats go by. The relationship between them is
// the same as in production: the term is several renewals long.
func config(t *testing.T, name string) node.Config {
	t.Helper()
	return node.Config{
		Client:          nodeClient,
		NodeName:        name,
		Address:         "10.42.0.9",
		Port:            10250,
		OS:              "linux",
		Arch:            "amd64",
		HeartbeatPeriod: 100 * time.Millisecond,
		LeaseDuration:   2 * time.Second,
		StatusPeriod:    150 * time.Millisecond,
	}
}

// startNode runs the controller until the test ends, and returns a function
// that stops it and waits for the shutdown path to finish.
func startNode(t *testing.T, cfg node.Config) (*node.Controller, func()) {
	t.Helper()
	c, err := node.New(cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	// Registering synchronously before Run means the test body never
	// races the first heartbeat. Run registers again, which is the
	// idempotency this relies on in production anyway.
	require.NoError(t, c.Register(ctx))

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(eventually):
			t.Fatal("the node controller did not stop")
		}
	}
	t.Cleanup(func() {
		stop()
		_ = nodeClient.CoreV1().Nodes().Delete(context.Background(), cfg.NodeName, metav1.DeleteOptions{})
	})

	return c, stop
}

func eventuallyTrue(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventually)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(tick)
	}
	t.Fatalf("timed out after %s waiting for %s", eventually, what)
}

func getNode(t *testing.T, name string) *corev1.Node {
	t.Helper()
	n, err := nodeClient.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)
	return n
}

func getLease(t *testing.T, name string) *metav1.ObjectMeta {
	t.Helper()
	l, err := nodeClient.CoordinationV1().Leases(corev1.NamespaceNodeLease).
		Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)
	return &l.ObjectMeta
}

func taint(n *corev1.Node, key string) *corev1.Taint {
	for i := range n.Spec.Taints {
		if n.Spec.Taints[i].Key == key {
			return &n.Spec.Taints[i]
		}
	}
	return nil
}

func condition(n *corev1.Node, typ corev1.NodeConditionType) *corev1.NodeCondition {
	for i := range n.Status.Conditions {
		if n.Status.Conditions[i].Type == typ {
			return &n.Status.Conditions[i]
		}
	}
	return nil
}

// --- the tests ---------------------------------------------------------

func TestNodeRegistersWithTheTaintLabelsAndKubeletEndpoint(t *testing.T) {
	skipWithoutAssets(t)
	name := "vk-register"
	_, _ = startNode(t, config(t, name))

	n := getNode(t, name)

	// The taint is what keeps everybody else's Pods off a node that
	// cannot run them, and NoExecute would evict ours.
	//
	// It is asserted by lookup rather than by index because a freshly
	// created Node is not alone in its taints: the API server's
	// TaintNodesByCondition admission plugin stamps
	// node.kubernetes.io/not-ready:NoSchedule onto every new Node, and it
	// is the node lifecycle controller -- which envtest does not run --
	// that removes it once Ready goes True. So on a real cluster our
	// heartbeat is also what un-taints the node, and here it stays.
	ours := taint(n, api.TaintKey)
	require.NotNil(t, ours, "our taint is missing from %v", n.Spec.Taints)
	require.Equal(t, api.TaintValue, ours.Value)
	require.Equal(t, corev1.TaintEffectNoSchedule, ours.Effect,
		"NoExecute would hand our Pods to the taint manager")

	require.Equal(t, "htcondor://"+name, n.Spec.ProviderID)
	require.Equal(t, "linux", n.Labels[corev1.LabelOSStable])
	require.Equal(t, "amd64", n.Labels[corev1.LabelArchStable])
	require.Equal(t, name, n.Labels[corev1.LabelHostname])
	require.Equal(t, "virtual-kubelet", n.Labels["type"])
	require.Contains(t, n.Labels, "node-role.kubernetes.io/agent")

	// This pair is the entire `kubectl logs` route. The API server reads
	// them and dials; nothing discovers us.
	require.Equal(t, int32(10250), n.Status.DaemonEndpoints.KubeletEndpoint.Port)
	require.Contains(t, n.Status.Addresses,
		corev1.NodeAddress{Type: corev1.NodeInternalIP, Address: "10.42.0.9"})

	ready := condition(n, corev1.NodeReady)
	require.NotNil(t, ready)
	require.Equal(t, corev1.ConditionTrue, ready.Status)
	require.Equal(t, node.ReasonReady, ready.Reason)

	for _, typ := range []corev1.NodeConditionType{
		corev1.NodeMemoryPressure, corev1.NodeDiskPressure, corev1.NodePIDPressure,
	} {
		c := condition(n, typ)
		require.NotNil(t, c, "%s condition is missing", typ)
		require.Equal(t, corev1.ConditionFalse, c.Status)
	}

	require.NotEmpty(t, n.Status.Capacity, "capacity survived the API server's decoding")
	require.Equal(t, n.Status.Capacity, n.Status.Allocatable)
	require.Equal(t, "placeholder", n.Annotations[node.AnnotationCapacitySource])
}

func TestTheLeaseIsCreatedOwnedByTheNodeAndRenewed(t *testing.T) {
	skipWithoutAssets(t)
	name := "vk-lease"
	_, _ = startNode(t, config(t, name))

	n := getNode(t, name)
	meta := getLease(t, name)

	require.Len(t, meta.OwnerReferences, 1)
	ref := meta.OwnerReferences[0]
	require.Equal(t, "Node", ref.Kind)
	require.Equal(t, "v1", ref.APIVersion)
	require.Equal(t, name, ref.Name)
	// A wrong UID here is silent: the garbage collector deletes the Lease
	// immediately and it looks exactly like a heartbeat that never runs.
	require.Equal(t, n.UID, ref.UID)

	lease, err := nodeClient.CoordinationV1().Leases(corev1.NamespaceNodeLease).
		Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, lease.Spec.HolderIdentity)
	require.Equal(t, name, *lease.Spec.HolderIdentity)
	require.NotNil(t, lease.Spec.LeaseDurationSeconds)
	require.Equal(t, int32(2), *lease.Spec.LeaseDurationSeconds)
	require.NotNil(t, lease.Spec.AcquireTime)

	first := lease.Spec.RenewTime
	require.NotNil(t, first)

	// The renewal is the only liveness signal the node lifecycle
	// controller reads. If it stops advancing, the node is NotReady
	// within the monitor grace period no matter what the conditions say.
	eventuallyTrue(t, "the lease's renewTime to advance", func() bool {
		cur, err := nodeClient.CoordinationV1().Leases(corev1.NamespaceNodeLease).
			Get(context.Background(), name, metav1.GetOptions{})
		return err == nil && cur.Spec.RenewTime.After(first.Time)
	})
}

func TestADeletedLeaseIsRecreatedByTheNextHeartbeat(t *testing.T) {
	skipWithoutAssets(t)
	name := "vk-lease-gone"
	_, _ = startNode(t, config(t, name))

	leases := nodeClient.CoordinationV1().Leases(corev1.NamespaceNodeLease)
	require.NoError(t, leases.Delete(context.Background(), name, metav1.DeleteOptions{}))

	// This is the real-world case of an administrator (or the GC, after a
	// Node was recreated) removing the Lease. A kubelet that cached it and
	// only ever issued updates would heartbeat into the void forever.
	eventuallyTrue(t, "the lease to come back", func() bool {
		_, err := leases.Get(context.Background(), name, metav1.GetOptions{})
		return err == nil
	})
}

func TestTheReadyConditionFollowsTheHealthCheck(t *testing.T) {
	skipWithoutAssets(t)
	name := "vk-ready"

	healthy := make(chan bool, 1)
	healthy <- true
	state := true
	cfg := config(t, name)
	cfg.ReadyCheck = func(context.Context) error {
		select {
		case state = <-healthy:
		default:
		}
		if state {
			return nil
		}
		return fmt.Errorf("the schedd is not answering")
	}
	_, _ = startNode(t, cfg)

	eventuallyTrue(t, "Ready=True", func() bool {
		c := condition(getNode(t, name), corev1.NodeReady)
		return c != nil && c.Status == corev1.ConditionTrue
	})
	readyAt := condition(getNode(t, name), corev1.NodeReady).LastTransitionTime

	healthy <- false
	eventuallyTrue(t, "Ready=False with the check's own message", func() bool {
		c := condition(getNode(t, name), corev1.NodeReady)
		return c != nil && c.Status == corev1.ConditionFalse &&
			c.Reason == node.ReasonNotReady &&
			strings.Contains(c.Message, "schedd is not answering")
	})
	notReadyAt := condition(getNode(t, name), corev1.NodeReady).LastTransitionTime
	require.True(t, notReadyAt.After(readyAt.Time) || notReadyAt.Equal(&readyAt),
		"a flip must move LastTransitionTime")

	healthy <- true
	eventuallyTrue(t, "Ready=True again", func() bool {
		c := condition(getNode(t, name), corev1.NodeReady)
		return c != nil && c.Status == corev1.ConditionTrue
	})
}

func TestTheHeartbeatDoesNotRewriteTheTransitionTime(t *testing.T) {
	skipWithoutAssets(t)
	name := "vk-transition"
	_, _ = startNode(t, config(t, name))

	first := condition(getNode(t, name), corev1.NodeReady)
	require.NotNil(t, first)
	firstTransition := first.LastTransitionTime

	// Several status writes must go by without the transition time
	// moving; otherwise nothing can tell a node that has been Ready for a
	// week from one that just recovered.
	eventuallyTrue(t, "a later heartbeat", func() bool {
		c := condition(getNode(t, name), corev1.NodeReady)
		return c != nil && c.LastHeartbeatTime.After(first.LastHeartbeatTime.Time)
	})
	later := condition(getNode(t, name), corev1.NodeReady)
	require.Equal(t, firstTransition, later.LastTransitionTime)
}

func TestShutdownMarksTheNodeNotReadyAndLeavesItInPlace(t *testing.T) {
	skipWithoutAssets(t)
	name := "vk-shutdown"
	_, stop := startNode(t, config(t, name))

	stop()

	// Deleting the Node would have the Pod GC controller delete every Pod
	// bound to it, and each of those deletions removes a running HTCondor
	// job. The node stays; it just stops claiming to be ready.
	n := getNode(t, name)
	require.Equal(t, name, n.Name)

	ready := condition(n, corev1.NodeReady)
	require.NotNil(t, ready)
	require.Equal(t, corev1.ConditionFalse, ready.Status)
	require.Equal(t, node.ReasonShutdown, ready.Reason)
	require.Contains(t, ready.Message, "keep running in the pool")

	// The Lease is left to expire on its own, so a restart that finishes
	// inside the term is never noticed by anything.
	_, err := nodeClient.CoordinationV1().Leases(corev1.NamespaceNodeLease).
		Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err, "shutdown must not delete the lease")
}

func TestRestartAdoptsTheExistingNodeKeepingItsUIDAndForeignTaints(t *testing.T) {
	skipWithoutAssets(t)
	name := "vk-adopt"
	_, stop := startNode(t, config(t, name))
	first := getNode(t, name)
	stop()

	// An administrator cordons the node by hand while we are down. The
	// re-read is because the shutdown path has just written status.
	drained := getNode(t, name).DeepCopy()
	drained.Spec.Taints = append(drained.Spec.Taints, corev1.Taint{
		Key: "example.com/drain", Value: "yes", Effect: corev1.TaintEffectNoSchedule,
	})
	drained.Labels["example.com/rack"] = "b12"
	_, err := nodeClient.CoreV1().Nodes().Update(context.Background(), drained, metav1.UpdateOptions{})
	require.NoError(t, err)

	_, _ = startNode(t, config(t, name))
	second := getNode(t, name)

	// The UID is the part that matters. A recreated Node is a new object,
	// and the Pod GC controller deletes Pods bound to a Node that no
	// longer exists -- which here means removing live jobs because a
	// controller restarted.
	require.Equal(t, first.UID, second.UID)
	require.Equal(t, "b12", second.Labels["example.com/rack"])
	require.NotNil(t, taint(second, "example.com/drain"),
		"a taint an administrator added must survive our restart")
	require.NotNil(t, taint(second, api.TaintKey))

	// And the Lease still points at the node that exists.
	meta := getLease(t, name)
	require.Equal(t, second.UID, meta.OwnerReferences[0].UID)
}
