package translate

import (
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Request is the resource ask of a job, in the units HTCondor's submit
// language uses: whole CPUs, bytes of memory and bytes of disk.
type Request struct {
	CPUs        int64
	MemoryBytes int64
	DiskBytes   int64
	GPUs        int64
}

// DefaultDiskBytes is what a container with no ephemeral-storage request gets.
// HTCondor requires an explicit request_disk or it inherits a pool default that
// is often far too small for a container image, so we always emit one.
const DefaultDiskBytes = 1 << 30 // 1 GiB

// Resources converts a container's requests and limits into a Request.
//
// HTCondor has no burst: a job gets exactly what it provisioned, and exceeding
// it means eviction, not throttling. So the *limit* is the honest translation
// of what the job will be allowed to use, and a limit below the request is a
// contradiction we refuse rather than silently resolve.
func Resources(c *corev1.Container, caps Capabilities) (Request, error) {
	var out Request

	cpu, err := pickQuantity(c, corev1.ResourceCPU)
	if err != nil {
		return out, err
	}
	if cpu == nil {
		return out, rejection{"resources.requests.cpu", ReasonResourcesRequired,
			"an HTCondor job must provision whole CPUs, so a cpu request or limit is required"}
	}
	// HTCondor allocates whole cores. Round a fractional request up: giving
	// 500m a full core over-provisions, which is safe; rounding down would
	// hand the job less CPU than Kubernetes promised it.
	milli := cpu.MilliValue()
	out.CPUs = (milli + 999) / 1000
	if out.CPUs < 1 {
		out.CPUs = 1
	}

	mem, err := pickQuantity(c, corev1.ResourceMemory)
	if err != nil {
		return out, err
	}
	if mem == nil {
		return out, rejection{"resources.requests.memory", ReasonResourcesRequired,
			"a memory request or limit is required; HTCondor evicts a job that exceeds its provisioned memory"}
	}
	out.MemoryBytes = mem.Value()
	if out.MemoryBytes <= 0 {
		return out, rejection{"resources.requests.memory", ReasonInvalidValue, "memory must be positive"}
	}

	disk, err := pickQuantity(c, corev1.ResourceEphemeralStorage)
	if err != nil {
		return out, err
	}
	if disk != nil {
		out.DiskBytes = disk.Value()
	} else {
		out.DiskBytes = DefaultDiskBytes
	}

	gpus, err := pickGPU(c, caps)
	if err != nil {
		return out, err
	}
	out.GPUs = gpus

	if err := rejectUnknownResources(c, caps); err != nil {
		return out, err
	}
	return out, nil
}

// pickQuantity returns the limit if set, else the request, and refuses a limit
// that is below its own request.
func pickQuantity(c *corev1.Container, name corev1.ResourceName) (*resource.Quantity, error) {
	req, hasReq := c.Resources.Requests[name]
	lim, hasLim := c.Resources.Limits[name]
	switch {
	case hasLim && hasReq:
		if lim.Cmp(req) < 0 {
			return nil, rejection{
				fmt.Sprintf("resources.limits.%s", name), ReasonResourceMismatch,
				fmt.Sprintf("limit %s is below request %s", lim.String(), req.String()),
			}
		}
		return &lim, nil
	case hasLim:
		return &lim, nil
	case hasReq:
		return &req, nil
	}
	return nil, nil
}

func pickGPU(c *corev1.Container, caps Capabilities) (int64, error) {
	for _, name := range caps.GPUResourceNames {
		q, err := pickQuantity(c, corev1.ResourceName(name))
		if err != nil {
			return 0, err
		}
		if q != nil {
			return q.Value(), nil
		}
	}
	// No configured GPU resource was requested. If the container asks for
	// some *other* vendor's GPU, rejectUnknownResources will catch it.
	return 0, nil
}

// rejectUnknownResources refuses any resource name the pool has not been told
// how to provision. Dropping an extended resource would run the job on hardware
// the user excluded.
func rejectUnknownResources(c *corev1.Container, caps Capabilities) error {
	known := map[corev1.ResourceName]bool{
		corev1.ResourceCPU:              true,
		corev1.ResourceMemory:           true,
		corev1.ResourceEphemeralStorage: true,
	}
	for _, name := range caps.GPUResourceNames {
		known[corev1.ResourceName(name)] = true
	}
	for _, list := range []corev1.ResourceList{c.Resources.Requests, c.Resources.Limits} {
		for name := range list {
			if known[name] {
				continue
			}
			return rejection{
				"resources", ReasonResourceUnsupported,
				fmt.Sprintf("this pool cannot provision resource %q", name),
			}
		}
	}
	return nil
}

// rejection lets the resource helpers return a typed failure that Admit can
// fold into its list with the right field and code.
type rejection struct {
	field  string
	reason Reason
	detail string
}

func (r rejection) Error() string { return fmt.Sprintf("%s: %s (%s)", r.field, r.detail, r.reason) }

func asRejection(err error, out *Rejection) bool {
	var r rejection
	if errors.As(err, &r) {
		*out = Rejection{Field: r.field, Reason: r.reason, Detail: r.detail}
		return true
	}
	return false
}
