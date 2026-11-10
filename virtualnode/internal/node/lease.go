package node

import (
	"context"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The Lease in kube-node-lease is the node's liveness signal, and it is the
// only one anything reads. The node lifecycle controller watches leases, not
// node conditions: `--node-monitor-grace-period` (50s by default) is measured
// against the Lease's renewTime, and a node whose lease has gone stale is
// marked NotReady no matter how cheerful its status conditions are.
//
// So the invariant this file maintains is narrow and absolute: renewTime must
// advance at least every HeartbeatPeriod, and nothing in the slower status
// path is allowed to block it.

// ensureLease creates the Lease if it is missing and adopts it otherwise.
func (c *Controller) ensureLease(ctx context.Context) error {
	leases := c.cfg.Client.CoordinationV1().Leases(corev1.NamespaceNodeLease)

	existing, err := leases.Get(ctx, c.cfg.NodeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		created, cerr := leases.Create(ctx, c.newLease(nil), metav1.CreateOptions{})
		if cerr != nil {
			return cerr
		}
		c.lease = created
		return nil
	}
	if err != nil {
		return err
	}

	// A lease left behind by a previous run of this process carries an
	// ownerReference to the Node's old UID if the Node was ever recreated.
	// Rewriting it here is what keeps garbage collection correct, and it is
	// cheap because we are already doing one PUT.
	updated, err := leases.Update(ctx, c.newLease(existing), metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	c.lease = updated
	return nil
}

// renewLease advances renewTime. This is the heartbeat.
func (c *Controller) renewLease(ctx context.Context) error {
	leases := c.cfg.Client.CoordinationV1().Leases(corev1.NamespaceNodeLease)

	if c.lease != nil {
		candidate := c.lease.DeepCopy()
		candidate.Spec.RenewTime = ref(metav1.NewMicroTime(time.Now()))
		updated, err := leases.Update(ctx, candidate, metav1.UpdateOptions{})
		if err == nil {
			c.lease = updated
			return nil
		}
		// Anything other than a clean update invalidates the cached
		// copy: a conflict means somebody else wrote it, and a
		// NotFound means it was deleted out from under us (which an
		// administrator does by hand, and which the GC does if the
		// Node goes away).
		c.lease = nil
		if !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return c.ensureLease(ctx)
}

// newLease produces the Lease we want to exist.
//
// The ownerReference is the whole reason this is not just another object we
// have to remember to delete. A Lease owned by the Node is deleted by the
// garbage collector when the Node is, which means a cluster that has finished
// with this integration -- `kubectl delete node htcondor-virtual-node` -- is
// left clean without this process having to be alive to clean up. Getting it
// wrong is silent: a wrong UID makes the GC delete the Lease *immediately*,
// which looks exactly like a heartbeat that never happens.
//
// Note the cross-scope reference: a namespaced Lease owned by a cluster-scoped
// Node. That is legal, it is what the real kubelet does, and it is the one
// direction the GC supports (the reverse -- cluster-scoped owned by
// namespaced -- is not).
func (c *Controller) newLease(existing *coordinationv1.Lease) *coordinationv1.Lease {
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.cfg.NodeName,
			Namespace: corev1.NamespaceNodeLease,
		},
	}
	if existing != nil {
		lease = existing.DeepCopy()
	}

	lease.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "v1",
		Kind:       "Node",
		Name:       c.cfg.NodeName,
		UID:        c.nodeUID,
	}}

	lease.Spec.HolderIdentity = ref(c.cfg.NodeName)
	lease.Spec.LeaseDurationSeconds = ref(int32(c.cfg.LeaseDuration / time.Second))
	now := metav1.NewMicroTime(time.Now())
	if lease.Spec.AcquireTime == nil {
		lease.Spec.AcquireTime = ref(now)
	}
	lease.Spec.RenewTime = ref(now)
	return lease
}

// ref is k8s.io/utils/ptr.To. It is three lines here rather than a dependency
// because the Lease API is all optional fields and this is the only place in
// the module that needs it.
func ref[T any](v T) *T { return &v }
