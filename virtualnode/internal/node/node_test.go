package node

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
)

func TestNewRejectsALeaseShorterThanItsHeartbeat(t *testing.T) {
	// A term that expires before the next renewal makes a healthy node
	// flap NotReady, and the symptom -- Pods evicted on a cycle -- looks
	// nothing like the cause.
	_, err := New(Config{
		Client:          fake.NewSimpleClientset(),
		HeartbeatPeriod: 30 * time.Second,
		LeaseDuration:   10 * time.Second,
	})
	require.ErrorContains(t, err, "must exceed")
}

func TestNewRequiresAClient(t *testing.T) {
	_, err := New(Config{})
	require.ErrorContains(t, err, "Client is required")
}

func TestNewDefaultsToTheSharedNodeName(t *testing.T) {
	c, err := New(Config{Client: fake.NewSimpleClientset()})
	require.NoError(t, err)
	require.Equal(t, api.NodeName, c.NodeName())
	require.Equal(t, "htcondor://"+api.NodeName, c.cfg.ProviderID)
	require.Equal(t, "placeholder", c.capacitySource())
}

func TestConfiguredCapacityIsReportedAsConfigured(t *testing.T) {
	c, err := New(Config{
		Client:   fake.NewSimpleClientset(),
		Capacity: corev1.ResourceList{corev1.ResourcePods: quantity(7, "DecimalSI")},
	})
	require.NoError(t, err)
	require.Equal(t, "configured", c.capacitySource())
}

func TestEnsureTaintLeavesOtherTaintsAlone(t *testing.T) {
	// An administrator drains this node by hand with their own taint;
	// our re-registration must not throw it away.
	existing := []corev1.Taint{{
		Key: "example.com/drain", Value: "yes", Effect: corev1.TaintEffectNoSchedule,
	}}
	out := ensureTaint(existing, corev1.Taint{
		Key: api.TaintKey, Value: api.TaintValue, Effect: corev1.TaintEffectNoSchedule,
	})
	require.Len(t, out, 2)
	require.Equal(t, "example.com/drain", out[0].Key)
	require.Equal(t, api.TaintKey, out[1].Key)

	// Applying it twice is not two taints.
	out = ensureTaint(out, corev1.Taint{
		Key: api.TaintKey, Value: api.TaintValue, Effect: corev1.TaintEffectNoSchedule,
	})
	require.Len(t, out, 2)
}

func TestTheTaintIsNoScheduleAndNeverNoExecute(t *testing.T) {
	// NoExecute would hand our Pods to the taint manager, which would
	// evict them -- and every eviction removes a running HTCondor job.
	// This is pinned because it is a one-word change that would do real
	// damage and break nothing visible.
	c, err := New(Config{Client: fake.NewSimpleClientset()})
	require.NoError(t, err)
	node := c.newNode(t.Context(), nil)

	require.Len(t, node.Spec.Taints, 1)
	require.Equal(t, corev1.TaintEffectNoSchedule, node.Spec.Taints[0].Effect)
	require.Equal(t, api.TaintKey, node.Spec.Taints[0].Key)
	require.Equal(t, api.TaintValue, node.Spec.Taints[0].Value)
}

func TestMergeConditionKeepsTheTransitionTimeUntilTheStatusChanges(t *testing.T) {
	then := metav1.NewTime(time.Now().Add(-time.Hour))
	prev := []corev1.NodeCondition{{
		Type: corev1.NodeReady, Status: corev1.ConditionTrue,
		LastTransitionTime: then, LastHeartbeatTime: then,
	}}

	same := mergeCondition(prev, corev1.NodeCondition{
		Type: corev1.NodeReady, Status: corev1.ConditionTrue,
		LastHeartbeatTime: metav1.NewTime(time.Now()),
	})
	require.Equal(t, then, same.LastTransitionTime, "a heartbeat is not a transition")

	flipped := mergeCondition(prev, corev1.NodeCondition{
		Type: corev1.NodeReady, Status: corev1.ConditionFalse,
		LastHeartbeatTime: metav1.NewTime(time.Now()),
	})
	require.NotEqual(t, then, flipped.LastTransitionTime)
	require.Equal(t, flipped.LastHeartbeatTime, flipped.LastTransitionTime)
}

func TestNodeStatusCarriesTheKubeletEndpointAndAddress(t *testing.T) {
	// status.daemonEndpoints and status.addresses are the only things
	// telling the API server where to proxy `kubectl logs`.
	c, err := New(Config{
		Client:  fake.NewSimpleClientset(),
		Address: "10.1.2.3",
		Port:    10250,
	})
	require.NoError(t, err)

	st := c.nodeStatus(nil, DefaultCapacity(), readyCondition{
		Status: corev1.ConditionTrue, Reason: ReasonReady,
	})
	require.Equal(t, int32(10250), st.DaemonEndpoints.KubeletEndpoint.Port)
	require.Contains(t, st.Addresses, corev1.NodeAddress{
		Type: corev1.NodeInternalIP, Address: "10.1.2.3",
	})
	require.Len(t, st.Conditions, 4)
	require.Equal(t, st.Capacity, st.Allocatable)
}

// This pins the client-go trap described on retryOnConflict: an update that
// fails because the context was cancelled makes retry.RetryOnConflict return
// nil, so the caller believes a write happened that did not. Reverting
// retryOnConflict to the bare RetryOnConflict makes this test fail with a nil
// dereference on the object the "successful" update never returned.
func TestACancelledUpdateIsNotReportedAsSuccess(t *testing.T) {
	cs := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n"}})
	cs.PrependReactor("update", "nodes",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, context.Canceled
		})

	c, err := New(Config{Client: cs, NodeName: "n"})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, c.Register(ctx), "a cancelled write must not look like a successful one")
}
