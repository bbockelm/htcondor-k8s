// Package translate is the shared contract between the two HTCondor/Kubernetes
// integration experiments in this repo.
//
// Both the kubectl-compatible API facade and the HTCJob CRD operator have to
// answer the same two questions, and they must answer them identically:
//
//  1. Admission — can this PodSpec be modeled faithfully by a single HTCondor
//     job? A Pod that HTCondor would run *differently* than Kubernetes would is
//     worse than one it refuses to run at all, so [Admit] is deliberately a
//     whitelist: anything it has not been taught to preserve is rejected.
//
//  2. Translation — if it can, what submit file says so? [PodToSubmit] is a
//     pure function from an admitted Pod to HTCondor submit-file text, and
//     [JobStatus] is the inverse direction: an HTCondor job ad read back as
//     Kubernetes status.
//
// The package deliberately depends only on k8s.io/api and k8s.io/apimachinery.
// It holds no HTCondor client code, so it can be imported by a webhook running
// inside a cluster and by a facade running next to a schedd without dragging
// either one's dependency tree into the other.
package translate
