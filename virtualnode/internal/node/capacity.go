package node

import (
	"context"
	"fmt"

	"github.com/PelicanPlatform/classad/classad"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Capacity on this node is informational, and that needs saying plainly
// because every other node in a cluster means something different by it.
//
// On a real node, capacity is a promise: the scheduler subtracts every bound
// Pod's requests from it and refuses to place one more than fits. Here,
// nothing consults it. Our Pods arrive with .spec.schedulerName set to
// something kube-scheduler ignores, and our pod controller binds them to this
// node by hand, so the numbers below never gate anything. Whether a job
// actually runs is decided by the negotiator against the pool's real slots,
// which is a decision this object cannot represent -- a pool is not a machine
// with a cpu count, it is a set of slots with shapes.
//
// What capacity is for, then, is `kubectl describe node` and `kubectl top`:
// it is how a user asks "how big is the thing behind this node". So the
// honest answer is the pool's own totals when we can get them cheaply, and a
// configured number when we cannot. What we must not do is invent a number
// that reads as a guarantee -- which is why DefaultCapacity is a stated
// placeholder rather than a plausible-looking machine size, and why it is
// annotated as such on the node.

// CapacitySource supplies the numbers reported as capacity and allocatable.
type CapacitySource interface {
	Capacity(ctx context.Context) (corev1.ResourceList, error)
}

// DefaultMaxPods bounds how many Pods this node will admit to holding.
//
// Unlike cpu and memory this one is not cosmetic -- it is the only number in
// the status that corresponds to a real limit on our side, since every bound
// Pod is an object this process watches and a job it tracks. It is set high
// because the limit it approximates is the schedd's, not ours, and a schedd
// holds far more than this.
const DefaultMaxPods = 1000

// DefaultCapacity is the placeholder used when no capacity is configured and
// no source is wired.
//
// The cpu and memory values are deliberately round and deliberately large:
// they are a statement that the pool is bigger than any one Pod, not a
// measurement. A site that wants `kubectl describe node` to say something
// true sets Config.Capacity or wires a CapacitySource.
func DefaultCapacity() corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:              quantity(1000, resource.DecimalSI),
		corev1.ResourceMemory:           quantity(4<<40, resource.BinarySI), // 4Ti
		corev1.ResourceEphemeralStorage: quantity(100<<40, resource.BinarySI),
		corev1.ResourcePods:             quantity(DefaultMaxPods, resource.DecimalSI),
	}
}

// StaticCapacity is a CapacitySource that returns a fixed list, which is what
// a site with a hand-written config file wants.
type StaticCapacity corev1.ResourceList

// Capacity implements CapacitySource.
func (s StaticCapacity) Capacity(context.Context) (corev1.ResourceList, error) {
	return corev1.ResourceList(s).DeepCopy(), nil
}

// AdQuery is the shape of (*htcondor.Collector).QueryAds.
//
// It is a function type rather than an interface so that wiring the real
// collector in is `PoolCapacity{Query: collector.QueryAds}` and testing it is
// a closure. The collector client itself is deliberately not imported here:
// this package has no business holding a CEDAR connection, and the cost of
// that choice is one line in cmd/.
type AdQuery func(ctx context.Context, adType, constraint string) ([]*classad.ClassAd, error)

// PoolCapacity reports the pool's own totals, read from the collector.
type PoolCapacity struct {
	Query AdQuery

	// MaxPods is reported as the pods capacity. Zero means DefaultMaxPods.
	MaxPods int64

	// Constraint narrows which slots count. A site that dedicates part of
	// its pool to Kubernetes work sets it; empty means the whole pool.
	Constraint string
}

// Capacity queries the collector for slot ads and sums them.
func (p PoolCapacity) Capacity(ctx context.Context) (corev1.ResourceList, error) {
	if p.Query == nil {
		return nil, fmt.Errorf("node: PoolCapacity has no collector query")
	}
	ads, err := p.Query(ctx, "Startd", p.Constraint)
	if err != nil {
		return nil, fmt.Errorf("querying the collector for slot ads: %w", err)
	}
	maxPods := p.MaxPods
	if maxPods <= 0 {
		maxPods = DefaultMaxPods
	}
	return SumSlotAds(ads, maxPods), nil
}

// SumSlotAds adds up what a set of slot ads says the pool holds.
//
// Summing `Cpus` and `Memory` over *every* slot ad is correct and is what
// `condor_status -total` does, which is worth spelling out because it looks
// like double counting. A partitionable slot's `Cpus` is what remains
// unclaimed on that machine; each dynamic slot carved out of it carries the
// rest. Adding them all up recovers the machine's total exactly once. Using
// `TotalCpus` instead would count the whole machine once per slot ad on it.
//
// The one real inaccuracy left is static slots on a machine that also has a
// partitionable slot, which is a configuration almost nobody runs.
func SumSlotAds(ads []*classad.ClassAd, maxPods int64) corev1.ResourceList {
	var cpus, memoryMB, diskKB int64
	for _, ad := range ads {
		if ad == nil {
			continue
		}
		cpus += adInt(ad, "Cpus")
		memoryMB += adInt(ad, "Memory")
		diskKB += adInt(ad, "Disk")
	}
	return corev1.ResourceList{
		corev1.ResourceCPU:    quantity(cpus, resource.DecimalSI),
		corev1.ResourceMemory: quantity(memoryMB<<20, resource.BinarySI),
		// HTCondor's Disk is in KiB; ephemeral-storage is in bytes.
		corev1.ResourceEphemeralStorage: quantity(diskKB<<10, resource.BinarySI),
		corev1.ResourcePods:             quantity(maxPods, resource.DecimalSI),
	}
}

// adInt reads an integer attribute, treating absent and unevaluable the same
// as zero. A slot ad missing `Memory` is a broken startd, not a reason to
// fail the whole capacity read.
func adInt(ad *classad.ClassAd, name string) int64 {
	if v, ok := ad.EvaluateAttrInt(name); ok {
		return v
	}
	// Memory and Disk are integers in every startd ad we have seen, but
	// Cpus is a real on a machine with fractional CPU slots.
	if v, ok := ad.EvaluateAttrNumber(name); ok {
		return int64(v)
	}
	return 0
}
