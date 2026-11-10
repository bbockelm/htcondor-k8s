package podctl

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/condor"
)

// Reaper removes jobs whose Pod is gone.
//
// The finalizer handles the ordinary case: a Pod is deleted, we condor_rm, the
// finalizer comes off. This is for the cases the finalizer cannot reach, and
// they are not hypothetical:
//
//   - a Pod deleted with --force --grace-period=0, which drops finalizers;
//   - a Pod this controller submitted for in the instant before it crashed,
//     where the finalizer write had not landed;
//   - a namespace deleted out from under us;
//   - an etcd restore to a point before the Pod existed.
//
// Every one leaves a job running and paid for with nothing in Kubernetes
// pointing at it. HTCondor's queue is durable and will happily run it to
// completion, which is the good property that makes this approach survive a
// controller outage and the bad property that makes an orphan permanent.
//
// The sweep is the reverse of the reconcile: it starts from the queue, not
// from Kubernetes. That direction is the only one that can see an orphan at
// all, because an orphan is defined by the absence of the object a
// Kubernetes-side walk would have to start from.
type Reaper struct {
	client.Client

	Queue condor.Queue

	// NodeName scopes the sweep to jobs this virtual node submitted. Two
	// virtual nodes against one schedd must not reap each other's work.
	NodeName string

	// Interval is how often to sweep.
	Interval time.Duration

	// Grace is how long a job must have been in the queue before it is
	// eligible.
	//
	// This is the whole safety margin, and it guards a real race rather
	// than a theoretical one: between Submit returning and the annotation
	// write landing, a job exists whose Pod the controller's own cache may
	// not have caught up with -- and a sweep that read a stale cache would
	// reap a job that is thirty milliseconds old and perfectly healthy.
	// Minutes, not seconds, because the cost of reaping late is a job that
	// runs a few minutes too long and the cost of reaping early is the
	// user's work deleted underneath them.
	Grace time.Duration
}

const (
	defaultReapInterval = 5 * time.Minute
	defaultReapGrace    = 10 * time.Minute
)

// Start runs the sweep until the context is cancelled. It satisfies
// manager.Runnable, so wiring it is mgr.Add(reaper).
func (r *Reaper) Start(ctx context.Context) error {
	t := time.NewTicker(r.interval())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := r.Sweep(ctx); err != nil {
				log.FromContext(ctx).Error(err, "sweeping for orphaned HTCondor jobs")
			}
		}
	}
}

// NeedLeaderElection keeps the sweep on one replica. Two reapers would not
// corrupt anything -- removing an already-removed job is not an error -- but
// they would double the query load on the schedd for no benefit.
func (r *Reaper) NeedLeaderElection() bool { return true }

// Sweep removes every job of this virtual node whose Pod no longer exists.
// It returns the number of jobs it removed.
func (r *Reaper) Sweep(ctx context.Context) error {
	lg := log.FromContext(ctx)

	ads, err := r.Queue.Query(ctx, NodeConstraint(r.nodeName()))
	if err != nil {
		return err
	}

	cutoff := time.Now().Add(-r.grace())
	var orphans []string
	for _, ad := range ads {
		if ad.Identity.UID == "" || ad.Identity.Name == "" {
			// A job of ours with no Pod identity is a job we could
			// not match to anything even if we wanted to. Leave it;
			// removing on the strength of a missing attribute is how
			// a projection bug turns into deleted work.
			continue
		}
		if ad.QDate.IsZero() || ad.QDate.After(cutoff) {
			continue
		}
		gone, err := r.podIsGone(ctx, ad)
		if err != nil {
			// An API error is not evidence of absence.
			lg.Error(err, "checking whether a Pod still exists", "pod", ad.Identity.Name)
			continue
		}
		if gone {
			orphans = append(orphans, ad.JobID())
		}
	}

	if len(orphans) == 0 {
		return nil
	}
	lg.Info("removing HTCondor jobs whose Pod is gone", "count", len(orphans), "ids", orphans)
	return r.Queue.Remove(ctx, orphans, "the Kubernetes Pod no longer exists")
}

// podIsGone answers the question the sweep turns on, and the UID comparison is
// the part that matters.
//
// A Pod of the same namespace and name may well exist: restartPolicy Never
// means the Job controller replaces a failed Pod, and for a non-indexed Job
// the replacement can reuse the name. Matching on name alone would see that
// replacement, conclude the original is fine, and leave the original's job
// running forever beside the replacement's. The UID is what distinguishes the
// object from its successor.
func (r *Reaper) podIsGone(ctx context.Context, ad translate.JobAd) (bool, error) {
	var pod corev1.Pod
	key := types.NamespacedName{Namespace: ad.Identity.Namespace, Name: ad.Identity.Name}
	err := r.Get(ctx, key, &pod)
	switch {
	case apierrors.IsNotFound(err):
		return true, nil
	case err != nil:
		return false, err
	}
	return string(pod.UID) != ad.Identity.UID, nil
}

func (r *Reaper) nodeName() string {
	if r.NodeName != "" {
		return r.NodeName
	}
	return api.NodeName
}

func (r *Reaper) interval() time.Duration {
	if r.Interval > 0 {
		return r.Interval
	}
	return defaultReapInterval
}

func (r *Reaper) grace() time.Duration {
	if r.Grace > 0 {
		return r.Grace
	}
	return defaultReapGrace
}

// SetupWithManager adds the sweep to a manager's runnables.
func (r *Reaper) SetupWithManager(mgr ctrl.Manager) error { return mgr.Add(r) }
