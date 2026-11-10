package node

import (
	"context"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func parseAd(t *testing.T, text string) *classad.ClassAd {
	t.Helper()
	ad, err := classad.Parse(text)
	require.NoError(t, err, "parsing %q", text)
	return ad
}

// A partitionable slot reports what is *left* on the machine and its dynamic
// children carry the rest, so summing every ad recovers the machine total
// exactly once. This is the thing about SumSlotAds that looks like a bug and
// is not, so it gets the explicit test.
func TestSumSlotAdsCountsAPartitionableMachineOnce(t *testing.T) {
	ads := []*classad.ClassAd{
		// A 16-core machine with two dynamic slots carved out of it.
		parseAd(t, `[ Name = "slot1@ep1"; SlotType = "Partitionable"; Cpus = 10; Memory = 20480; Disk = 1000000; TotalCpus = 16 ]`),
		parseAd(t, `[ Name = "slot1_1@ep1"; SlotType = "Dynamic"; Cpus = 4; Memory = 8192; Disk = 500000; TotalCpus = 16 ]`),
		parseAd(t, `[ Name = "slot1_2@ep1"; SlotType = "Dynamic"; Cpus = 2; Memory = 4096; Disk = 200000; TotalCpus = 16 ]`),
	}

	got := SumSlotAds(ads, 64)

	cpu := got[corev1.ResourceCPU]
	require.Equal(t, int64(16), cpu.Value(), "cpus should add up to the machine, not to 16 per ad")
	mem := got[corev1.ResourceMemory]
	require.Equal(t, int64(32768)<<20, mem.Value(), "memory is reported in MiB by the startd")
	disk := got[corev1.ResourceEphemeralStorage]
	require.Equal(t, int64(1700000)<<10, disk.Value(), "Disk is reported in KiB by the startd")
	pods := got[corev1.ResourcePods]
	require.Equal(t, int64(64), pods.Value())
}

func TestSumSlotAdsToleratesBrokenAds(t *testing.T) {
	ads := []*classad.ClassAd{
		nil,
		parseAd(t, `[ Name = "slot1@ep1" ]`), // no resources at all
		parseAd(t, `[ Name = "slot2@ep1"; Cpus = 2.5 ]`), // fractional cpus
		parseAd(t, `[ Name = "slot3@ep1"; Cpus = 1; Memory = 1024 ]`),
	}

	got := SumSlotAds(ads, 0)

	cpu := got[corev1.ResourceCPU]
	require.Equal(t, int64(3), cpu.Value(), "2.5 truncates to 2, plus 1")
	mem := got[corev1.ResourceMemory]
	require.Equal(t, int64(1024)<<20, mem.Value())
}

func TestPoolCapacityReportsWhatTheCollectorSaid(t *testing.T) {
	var sawType, sawConstraint string
	p := PoolCapacity{
		MaxPods:    5,
		Constraint: `HasKubernetes == true`,
		Query: func(_ context.Context, adType, constraint string) ([]*classad.ClassAd, error) {
			sawType, sawConstraint = adType, constraint
			return []*classad.ClassAd{
				parseAd(t, `[ Cpus = 8; Memory = 16384; Disk = 100 ]`),
			}, nil
		},
	}

	got, err := p.Capacity(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Startd", sawType)
	require.Equal(t, `HasKubernetes == true`, sawConstraint)

	cpu := got[corev1.ResourceCPU]
	require.Equal(t, int64(8), cpu.Value())
	pods := got[corev1.ResourcePods]
	require.Equal(t, int64(5), pods.Value())
}

func TestPoolCapacityWithoutAQueryIsAnError(t *testing.T) {
	_, err := PoolCapacity{}.Capacity(context.Background())
	require.Error(t, err)
}

func TestStaticCapacityDoesNotHandOutItsOwnMap(t *testing.T) {
	// The caller gets this list and hands it to the API server, which is
	// free to mutate it. A source that returned its own map would see the
	// mutation on the next poll.
	src := StaticCapacity(DefaultCapacity())
	first, err := src.Capacity(context.Background())
	require.NoError(t, err)
	delete(first, corev1.ResourceCPU)

	second, err := src.Capacity(context.Background())
	require.NoError(t, err)
	require.Contains(t, second, corev1.ResourceCPU)
}
