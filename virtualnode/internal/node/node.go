// Package node registers the virtual node this integration binds Pods to, and
// then keeps it alive.
//
// Registering is the easy half and it is three API calls. Keeping the Node
// alive is the half that decides whether the approach works at all, because a
// virtual node that misses its heartbeats is not merely cosmetic: the node
// lifecycle controller marks it NotReady, the taint manager adds
// `node.kubernetes.io/not-ready:NoExecute`, and five minutes later Kubernetes
// starts deleting Pods that have live HTCondor jobs behind them. Our pod
// controller holds a finalizer on each of those Pods and removes the job when
// the Pod goes away, so a heartbeat bug does not degrade the integration --
// it kills users' work.
//
// So the shape here is the one the real kubelet settled on, for the same
// reasons:
//
//   - A Lease in kube-node-lease renewed every ~10s is the liveness signal.
//     It is a tiny object and renewing it is one PUT of a few dozen bytes,
//     which is what makes a 10s cadence affordable at cluster scale.
//   - The Node's own status is written on a much slower cadence, because it
//     is a large object and writing it is what used to melt etcd before
//     leases existed. Nothing reads node.status.conditions for liveness.
//   - The two are independent failure domains on purpose. If the status write
//     fails we are stale but alive; if the lease renewal fails we are dead.
//
// What this package deliberately does not do is schedule. The Node exists so
// that Pods have somewhere legal to be bound, so that `kubectl get pods -o
// wide` names something, and so that the API server knows where to proxy
// `kubectl logs` to. Capacity is informational -- see capacity.go.
package node

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/go-logr/logr"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"

	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
)

// Defaults for the two cadences and the lease term.
//
// 10s/40s is what kubelet ships (`--node-lease-duration-seconds` defaults to
// 40, renewal is a quarter of it). The numbers matter together rather than
// individually: the node lifecycle controller marks a node NotReady when the
// lease has not been renewed within `--node-monitor-grace-period` (50s by
// default), so the term has to leave room for several missed renewals before
// anybody concludes we are gone. A term shorter than the grace period buys
// nothing, and a renewal period that is not a small fraction of the term
// means one slow API call is an outage.
const (
	DefaultHeartbeatPeriod = 10 * time.Second
	DefaultLeaseDuration   = 40 * time.Second

	// DefaultStatusPeriod is how often the Node object itself is rewritten
	// when nothing has changed. Kubelet uses 5m; we use 1m because a
	// virtual node's status is small, there is exactly one of them, and the
	// capacity numbers behind it can move when the pool does.
	DefaultStatusPeriod = time.Minute
)

// Reasons that appear in node conditions. They are exported because they are
// what an operator greps for in `kubectl describe node`, which makes them
// part of the interface rather than an implementation detail.
const (
	ReasonReady            = "KubeletReady"
	ReasonNotReady         = "HTCondorUnreachable"
	ReasonShutdown         = "KubeletShutdown"
	ReasonNoPressure       = "HTCondorNoPressure"
	kubeletVersionFallback = "htcondor-virtual-kubelet"
)

// AnnotationCapacitySource records where the numbers in status.capacity came
// from, because they mean three different things and nothing else on the
// object distinguishes them: "collector" is a measurement of the pool,
// "configured" is a site administrator's statement, and "placeholder" is this
// package's default, which is not a fact about anything.
//
// It lives here rather than in api/names.go because it is written onto a Node
// rather than onto a Pod, and nothing outside this package reads it.
const AnnotationCapacitySource = "htcondor.org/capacity-source"

// Config is everything the node lifecycle needs. Only Client is mandatory;
// every other field has a defensible default, because a PoC that cannot be
// started without a config file does not get started.
type Config struct {
	// Client talks to the API server. The typed client-go interface is
	// used here rather than controller-runtime's, because none of this is
	// reconciliation: there is no watch, no cache and no desired state
	// held anywhere but in this process.
	Client kubernetes.Interface

	// NodeName defaults to api.NodeName. It is a field only so that two
	// tests can run against one API server without fighting.
	NodeName string

	// Address and Port are what the API server will dial when a user runs
	// `kubectl logs`. They are published in status.addresses and
	// status.daemonEndpoints respectively, and getting them wrong is the
	// single most common way for logs to break: the API server does not
	// discover the kubelet, it reads these fields.
	Address string
	Port    int32

	// ProviderID defaults to htcondor://<NodeName>. Nothing in this PoC
	// consumes it, but a cloud-provider-shaped identifier is what tooling
	// expects to find on a node that is not a machine.
	ProviderID string

	// OS and Arch default to this process's. They are a claim about where
	// the *jobs* run, not about where this binary runs, so a site whose EPs
	// are linux/amd64 and whose control plane is darwin/arm64 must set them.
	OS   string
	Arch string

	// KubeletVersion is reported in status.nodeInfo. It is a lie whatever
	// we put there -- there is no kubelet -- so it says what we are
	// instead of inventing a Kubernetes version.
	KubeletVersion string

	// Capacity and CapacitySource decide what `kubectl describe node`
	// prints. See capacity.go for why this is informational.
	Capacity       corev1.ResourceList
	CapacitySource CapacitySource

	// Labels are merged onto the node on top of the ones every virtual
	// node is expected to carry.
	Labels map[string]string

	// ReadyCheck, when set, decides the Ready condition. Returning an
	// error makes the node NotReady with that error as the message.
	//
	// Wiring the schedd's reachability in here is tempting and is probably
	// wrong: see the comment on setReady.
	ReadyCheck func(context.Context) error

	HeartbeatPeriod time.Duration
	LeaseDuration   time.Duration
	StatusPeriod    time.Duration

	Log logr.Logger
}

// Controller owns the Node object and its Lease for the lifetime of the
// process.
type Controller struct {
	cfg Config

	// nodeUID is captured at registration and is what the Lease's
	// ownerReference points at. A Lease whose ownerReference names a UID
	// that no longer exists is garbage-collected immediately, so this must
	// be the UID of the Node we actually registered rather than one
	// remembered from a previous run.
	nodeUID types.UID

	// lease is the object as we last wrote it, kept so the common path is
	// one PUT rather than a GET and a PUT.
	lease *coordinationv1.Lease

	// usingDefaultCapacity remembers that nobody configured capacity, so
	// the node can say so rather than presenting the default as a fact.
	usingDefaultCapacity bool
}

// New validates the configuration and fills in defaults.
func New(cfg Config) (*Controller, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("node: Client is required")
	}
	if cfg.NodeName == "" {
		cfg.NodeName = api.NodeName
	}
	if cfg.ProviderID == "" {
		cfg.ProviderID = "htcondor://" + cfg.NodeName
	}
	if cfg.OS == "" {
		cfg.OS = runtime.GOOS
	}
	if cfg.Arch == "" {
		cfg.Arch = runtime.GOARCH
	}
	if cfg.KubeletVersion == "" {
		cfg.KubeletVersion = kubeletVersionFallback
	}
	if cfg.HeartbeatPeriod <= 0 {
		cfg.HeartbeatPeriod = DefaultHeartbeatPeriod
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = DefaultLeaseDuration
	}
	if cfg.StatusPeriod <= 0 {
		cfg.StatusPeriod = DefaultStatusPeriod
	}
	if cfg.LeaseDuration <= cfg.HeartbeatPeriod {
		// Not a style preference: a term that expires before the next
		// renewal makes the node flap NotReady on a perfectly healthy
		// cluster, and the symptom (Pods evicted every minute) looks
		// nothing like the cause.
		return nil, fmt.Errorf("node: LeaseDuration (%s) must exceed HeartbeatPeriod (%s)",
			cfg.LeaseDuration, cfg.HeartbeatPeriod)
	}
	placeholder := false
	if cfg.Capacity == nil && cfg.CapacitySource == nil {
		cfg.Capacity = DefaultCapacity()
		placeholder = true
	}
	return &Controller{cfg: cfg, usingDefaultCapacity: placeholder}, nil
}

// NodeName is the name of the Node object this controller owns.
func (c *Controller) NodeName() string { return c.cfg.NodeName }

// Run registers the node and keeps it alive until ctx is cancelled.
//
// It returns nil on a clean shutdown: a cancelled context is how this is
// asked to stop, not a failure. It returns an error only when registration
// itself fails, which is a startup problem and should take the process down
// rather than leave a virtual kubelet running with no node to bind to.
func (c *Controller) Run(ctx context.Context) error {
	if err := c.Register(ctx); err != nil {
		if ctx.Err() == nil {
			return err
		}
		// Told to stop before we finished starting. That is a
		// shutdown, not a failure, and reporting it as one makes a
		// clean rollout look like a crash loop. If we got far enough
		// to have a Node, it still gets the NotReady write that every
		// other shutdown gets.
		if c.nodeUID != "" {
			c.Shutdown()
		}
		return nil
	}

	heartbeat := time.NewTicker(c.cfg.HeartbeatPeriod)
	defer heartbeat.Stop()
	status := time.NewTicker(c.cfg.StatusPeriod)
	defer status.Stop()

	for {
		select {
		case <-ctx.Done():
			c.Shutdown()
			return nil
		case <-heartbeat.C:
			if err := c.renewLease(ctx); err != nil {
				// Logged rather than returned. One failed
				// renewal out of four within the lease term is
				// survivable, and taking the process down on a
				// transient API error would turn a blip into
				// the eviction we are trying to avoid.
				c.log().Error(err, "renewing node lease")
			}
		case <-status.C:
			if err := c.updateStatus(ctx, c.readyCondition(ctx)); err != nil {
				c.log().Error(err, "updating node status")
			}
		}
	}
}

// Register creates or adopts the Node object and its Lease.
//
// Adopting rather than recreating is deliberate and load-bearing. Deleting
// and recreating the Node would give it a new UID, and every Pod already
// bound to the old one would be deleted by the Pod GC controller as an
// orphan -- which on this integration means every running HTCondor job is
// removed because a controller restarted.
func (c *Controller) Register(ctx context.Context) error {
	node, err := c.ensureNode(ctx)
	if err != nil {
		return fmt.Errorf("registering node %s: %w", c.cfg.NodeName, err)
	}
	c.nodeUID = node.UID

	if err := c.updateStatus(ctx, c.readyCondition(ctx)); err != nil {
		return fmt.Errorf("writing initial status for node %s: %w", c.cfg.NodeName, err)
	}
	if err := c.ensureLease(ctx); err != nil {
		return fmt.Errorf("creating lease for node %s: %w", c.cfg.NodeName, err)
	}
	c.log().Info("virtual node registered",
		"node", c.cfg.NodeName, "uid", c.nodeUID,
		"kubeletEndpoint", fmt.Sprintf("%s:%d", c.cfg.Address, c.cfg.Port))
	return nil
}

// Shutdown marks the node NotReady without deleting it.
//
// This is the decision in this package most worth arguing about, so the
// reasoning is written down rather than implied.
//
// Deleting the Node on SIGTERM is what a cloud virtual kubelet usually does,
// and it is wrong here. The Pod GC controller deletes Pods bound to a Node
// that no longer exists; each of those deletions runs our finalizer; our
// finalizer removes the HTCondor job. A rolling restart of this Deployment
// would therefore remove every running job in the pool. The jobs are the
// durable thing in this system -- they outlive the controller, which is the
// best property the HTCondor side gives us -- and a shutdown path that throws
// them away gives it up for nothing.
//
// Going NotReady is honest (nothing is binding Pods while we are gone) and
// costs one thing that has to be said out loud: the taint manager adds
// `node.kubernetes.io/not-ready:NoExecute`, and the DefaultTolerationSeconds
// admission plugin has given every Pod a 300s toleration for it, so Pods
// bound here are deleted five minutes later -- with the same consequence as
// above, only slower. The mitigation is for the webhook to stamp an explicit
// toleration for that taint with no tolerationSeconds onto the Pod template,
// which is safe precisely because nothing about our Pods depends on the node
// being up: we bind them by hand and HTCondor runs them.
//
// We also deliberately leave the Lease alone instead of deleting it. Letting
// it expire on its own means a restart that completes inside the lease term
// is never noticed by anything, which is the common case for a Deployment
// rollout.
func (c *Controller) Shutdown() {
	// The context that stopped us is already cancelled, so this needs its
	// own with a short deadline: a shutdown that hangs on an unreachable
	// API server is worse than one that gives up.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cond := readyCondition{
		Status: corev1.ConditionFalse,
		Reason: ReasonShutdown,
		Message: "the HTCondor virtual kubelet is shutting down; jobs already " +
			"submitted keep running in the pool",
	}
	if err := c.updateStatus(ctx, cond); err != nil {
		c.log().Error(err, "marking node NotReady on shutdown")
		return
	}
	c.log().Info("virtual node marked NotReady", "node", c.cfg.NodeName)
}

// ensureNode creates the Node if it is absent and otherwise reconciles the
// fields we own onto the existing one.
func (c *Controller) ensureNode(ctx context.Context) (*corev1.Node, error) {
	nodes := c.cfg.Client.CoreV1().Nodes()

	existing, err := nodes.Get(ctx, c.cfg.NodeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		// Node is one of the few resources whose registry keeps
		// .status on create, which is why capacity is set here and not
		// only in the status update that follows.
		created, cerr := nodes.Create(ctx, c.newNode(ctx, nil), metav1.CreateOptions{})
		if cerr == nil {
			return created, nil
		}
		if !apierrors.IsAlreadyExists(cerr) {
			return nil, cerr
		}
		existing, err = nodes.Get(ctx, c.cfg.NodeName, metav1.GetOptions{})
	}
	if err != nil {
		return nil, err
	}

	var out *corev1.Node
	err = retryOnConflict(ctx, func() error {
		updated, uerr := nodes.Update(ctx, c.newNode(ctx, existing), metav1.UpdateOptions{})
		if uerr == nil {
			out = updated
			return nil
		}
		if apierrors.IsConflict(uerr) {
			fresh, gerr := nodes.Get(ctx, c.cfg.NodeName, metav1.GetOptions{})
			if gerr != nil {
				return gerr
			}
			existing = fresh
		}
		return uerr
	})
	if err != nil {
		return nil, err
	}
	if out == nil {
		// Belt and braces for the trap retryOnConflict exists to
		// cover: reaching here with no error and no object would
		// otherwise be a nil dereference one line later.
		return nil, fmt.Errorf("updating node %s: the update neither failed nor returned an object", c.cfg.NodeName)
	}
	return out, nil
}

// newNode produces the object we want to exist. When existing is non-nil the
// result is that object with our fields reconciled onto it, which is how
// labels and taints somebody else added survive our restart.
func (c *Controller) newNode(ctx context.Context, existing *corev1.Node) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: c.cfg.NodeName},
	}
	if existing != nil {
		node = existing.DeepCopy()
	}

	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	for k, v := range c.labels() {
		node.Labels[k] = v
	}

	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	node.Annotations[AnnotationCapacitySource] = c.capacitySource()

	node.Spec.ProviderID = c.cfg.ProviderID
	node.Spec.Taints = ensureTaint(node.Spec.Taints, corev1.Taint{
		Key:    api.TaintKey,
		Value:  api.TaintValue,
		Effect: corev1.TaintEffectNoSchedule,
	})

	if existing == nil {
		node.Status = c.nodeStatus(nil, c.capacity(ctx), readyCondition{
			Status:  corev1.ConditionTrue,
			Reason:  ReasonReady,
			Message: "the HTCondor virtual kubelet has just registered",
		})
	}
	return node
}

// labels are the ones a virtual node is expected to carry.
//
// `type=virtual-kubelet` is the de facto marker other tooling keys on, and
// `node-role.kubernetes.io/agent` is what makes `kubectl get nodes` print a
// ROLE instead of "<none>". kubernetes.io/hostname matters more than it
// looks: it is the default topology key, and a Pod with no topology
// constraints still has it defaulted onto it.
func (c *Controller) labels() map[string]string {
	out := map[string]string{
		corev1.LabelOSStable:            c.cfg.OS,
		corev1.LabelArchStable:          c.cfg.Arch,
		corev1.LabelHostname:            c.cfg.NodeName,
		"type":                          "virtual-kubelet",
		"node-role.kubernetes.io/agent": "",
		"htcondor.org/virtual-node":     "true",
	}
	for k, v := range c.cfg.Labels {
		out[k] = v
	}
	return out
}

// ensureTaint adds t if an equivalent taint is not already present, and
// leaves every other taint alone. Clobbering the list would discard a taint
// an administrator added by hand, which is a thing administrators do to a
// node they want to stop using.
func ensureTaint(taints []corev1.Taint, t corev1.Taint) []corev1.Taint {
	for i := range taints {
		if taints[i].Key == t.Key && taints[i].Effect == t.Effect {
			taints[i].Value = t.Value
			return taints
		}
	}
	return append(taints, t)
}

// readyCondition is the Ready condition this process currently believes in.
type readyCondition struct {
	Status  corev1.ConditionStatus
	Reason  string
	Message string
}

// readyCondition asks the configured health check, if there is one.
//
// Wiring the schedd's reachability into this is the obvious thing to do and
// it is probably a mistake, which is why it is a hook rather than the
// default. Ready=False has teeth -- the taint manager starts deleting Pods
// bound here -- and a schedd that is briefly unreachable does not mean the
// jobs it already accepted have stopped running. A site that wants the
// coupling can have it; the default is a node that is Ready as long as this
// process is alive to say so, which is the only thing it can honestly claim.
func (c *Controller) readyCondition(ctx context.Context) readyCondition {
	if c.cfg.ReadyCheck != nil {
		if err := c.cfg.ReadyCheck(ctx); err != nil {
			return readyCondition{
				Status:  corev1.ConditionFalse,
				Reason:  ReasonNotReady,
				Message: err.Error(),
			}
		}
	}
	return readyCondition{
		Status:  corev1.ConditionTrue,
		Reason:  ReasonReady,
		Message: "the HTCondor virtual kubelet is posting status",
	}
}

// updateStatus writes capacity, addresses, endpoints and conditions.
func (c *Controller) updateStatus(ctx context.Context, ready readyCondition) error {
	capacity := c.capacity(ctx)

	nodes := c.cfg.Client.CoreV1().Nodes()
	return retryOnConflict(ctx, func() error {
		current, err := nodes.Get(ctx, c.cfg.NodeName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		current.Status = c.nodeStatus(&current.Status, capacity, ready)
		_, err = nodes.UpdateStatus(ctx, current, metav1.UpdateOptions{})
		return err
	})
}

// nodeStatus builds the status we want, carrying forward the transition
// times from prev so that `kubectl describe node` can say how long the node
// has been Ready rather than resetting the clock every minute.
func (c *Controller) nodeStatus(prev *corev1.NodeStatus, capacity corev1.ResourceList, ready readyCondition) corev1.NodeStatus {
	status := corev1.NodeStatus{}
	if prev != nil {
		status = *prev.DeepCopy()
	}

	// Allocatable equals Capacity. A real kubelet subtracts what the OS and
	// the kubelet itself reserve; we reserve nothing because we run none of
	// the Pod's work on this machine.
	status.Capacity = capacity
	status.Allocatable = capacity.DeepCopy()

	status.Phase = corev1.NodeRunning
	status.NodeInfo = corev1.NodeSystemInfo{
		OperatingSystem:         c.cfg.OS,
		Architecture:            c.cfg.Arch,
		KubeletVersion:          c.cfg.KubeletVersion,
		KubeProxyVersion:        c.cfg.KubeletVersion,
		ContainerRuntimeVersion: "htcondor://container-universe",
		OSImage:                 "HTCondor",
		MachineID:               c.cfg.NodeName,
		SystemUUID:              c.cfg.NodeName,
		BootID:                  c.cfg.NodeName,
		KernelVersion:           "n/a",
	}
	status.Addresses = []corev1.NodeAddress{
		{Type: corev1.NodeInternalIP, Address: c.cfg.Address},
		{Type: corev1.NodeHostName, Address: c.cfg.NodeName},
	}
	// This is the field `kubectl logs` travels through. The API server
	// dials InternalIP:this port; there is no discovery.
	status.DaemonEndpoints = corev1.NodeDaemonEndpoints{
		KubeletEndpoint: corev1.DaemonEndpoint{Port: c.cfg.Port},
	}

	now := metav1.NewTime(time.Now())
	status.Conditions = []corev1.NodeCondition{
		mergeCondition(status.Conditions, corev1.NodeCondition{
			Type: corev1.NodeReady, Status: ready.Status,
			Reason: ready.Reason, Message: ready.Message,
			LastHeartbeatTime: now,
		}),
		// The three pressure conditions are reported False and that is
		// the truth, not a stub: they describe this machine's disk,
		// memory and process table, and no Pod's work runs on this
		// machine. What could run out is the pool, and HTCondor
		// expresses that by leaving a job idle rather than by evicting
		// it, so there is nothing here to map them onto.
		mergeCondition(status.Conditions, corev1.NodeCondition{
			Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse,
			Reason: ReasonNoPressure, Message: "no Pod memory is allocated on this node",
			LastHeartbeatTime: now,
		}),
		mergeCondition(status.Conditions, corev1.NodeCondition{
			Type: corev1.NodeDiskPressure, Status: corev1.ConditionFalse,
			Reason: ReasonNoPressure, Message: "no Pod storage is allocated on this node",
			LastHeartbeatTime: now,
		}),
		mergeCondition(status.Conditions, corev1.NodeCondition{
			Type: corev1.NodePIDPressure, Status: corev1.ConditionFalse,
			Reason: ReasonNoPressure, Message: "no Pod processes run on this node",
			LastHeartbeatTime: now,
		}),
	}
	return status
}

// mergeCondition keeps LastTransitionTime stable across heartbeats. A
// condition whose transition time moves every minute makes it impossible to
// tell a node that has been Ready for a week from one that just recovered,
// and that is exactly the question being asked when somebody looks.
func mergeCondition(prev []corev1.NodeCondition, next corev1.NodeCondition) corev1.NodeCondition {
	for _, p := range prev {
		if p.Type != next.Type {
			continue
		}
		if p.Status == next.Status {
			next.LastTransitionTime = p.LastTransitionTime
			return next
		}
		break
	}
	next.LastTransitionTime = next.LastHeartbeatTime
	return next
}

// capacity asks the configured source, falling back to the static value when
// the pool cannot be reached. A collector that is down must not make the node
// report zero capacity: that is a much more alarming signal than it deserves,
// and nothing schedules against these numbers anyway.
func (c *Controller) capacity(ctx context.Context) corev1.ResourceList {
	if c.cfg.CapacitySource == nil {
		return c.cfg.Capacity.DeepCopy()
	}
	got, err := c.cfg.CapacitySource.Capacity(ctx)
	if err != nil || len(got) == 0 {
		if err != nil {
			c.log().V(1).Info("falling back to configured capacity", "err", err.Error())
		}
		if c.cfg.Capacity != nil {
			return c.cfg.Capacity.DeepCopy()
		}
		return DefaultCapacity()
	}
	return got
}

// retryOnConflict is retry.RetryOnConflict with its one sharp edge filed off.
//
// client-go's retry.OnError ends with `if wait.Interrupted(err) { err = lastErr }`,
// and wait.Interrupted is true for context.Canceled as well as for a
// backoff that ran out of steps. lastErr only ever holds a *retriable*
// error -- a conflict -- so a call that failed because the context was
// cancelled sets lastErr to nil and RetryOnConflict returns **nil**. The
// caller is told the write succeeded when nothing was written.
//
// That is not hypothetical: it is how this package first failed, as a nil
// dereference on the object a successful update was supposed to have
// returned. Any RetryOnConflict in this module whose context can be
// cancelled -- which is every one of them, since a controller's context is
// cancelled on SIGTERM -- has the same hole.
func retryOnConflict(ctx context.Context, fn func() error) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, fn)
	if err == nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// capacitySource names the provenance of status.capacity for the annotation.
func (c *Controller) capacitySource() string {
	switch {
	case c.cfg.CapacitySource != nil:
		return "collector"
	case c.cfg.Capacity != nil && !c.usingDefaultCapacity:
		return "configured"
	default:
		return "placeholder"
	}
}

func (c *Controller) log() logr.Logger {
	if c.cfg.Log.GetSink() == nil {
		return logr.Discard()
	}
	return c.cfg.Log
}

// quantity is a small helper so the capacity tables read as numbers.
func quantity(v int64, format resource.Format) resource.Quantity {
	return *resource.NewQuantity(v, format)
}
