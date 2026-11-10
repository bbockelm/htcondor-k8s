# htcondor-k8s

Two experiments in making Kubernetes workloads run on an HTCondor pool, sharing
one translation layer so they can be compared on their merits rather than on
whose Pod-to-job mapping happened to be better.

| Directory | What it is |
|---|---|
| [`shared/`](shared/) | The contract: admission rules and Pod ⇄ HTCondor-job translation, with no HTCondor or Kubernetes client code in it |
| [`facade/`](facade/) | A kubectl-compatible API server. There is no etcd and no kubelet; the schedd's job queue is the object store |
| [`operator/`](operator/) | An `HTCJob` CRD, a mutating webhook that claims ordinary `batch/v1` Jobs, and a controller that keeps the two in sync |

Both front ends sit in front of an HTCondor access point and submit through
[golang-htcondor](https://github.com/bbockelm/golang-htcondor).

See [docs/DESIGN.md](docs/DESIGN.md) for the shared design, and the per-approach
notes in each directory.

## Build

```sh
make build
make test
make test-integration   # skips itself without HTCondor / envtest binaries
```
