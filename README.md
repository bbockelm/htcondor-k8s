# htcondor-k8s

Three experiments in making Kubernetes workloads run on an HTCondor pool,
sharing one translation layer so they can be compared on their merits rather
than on whose Pod-to-job mapping happened to be better.

| Directory | What it is |
|---|---|
| [`shared/`](shared/) | The contract: admission rules and Pod ⇄ HTCondor-job translation, with no HTCondor or Kubernetes client code in it |
| [`facade/`](facade/) | A kubectl-compatible API server. There is no etcd and no kubelet; the schedd's job queue is the object store |
| [`operator/`](operator/) | An `HTCJob` CRD, a mutating webhook that claims ordinary `batch/v1` Jobs, and a controller that keeps the two in sync |
| [`virtualnode/`](virtualnode/) | A virtual node. The built-in Job controller runs untouched and makes real Pods; this takes those Pods, runs them on HTCondor, and lets the Job controller derive Job status from the Pod status it writes |

All three sit in front of an HTCondor access point and submit through
[golang-htcondor](https://github.com/bbockelm/golang-htcondor).

**Start with [docs/COMPARISON.md](docs/COMPARISON.md)** — which approach, and
why. Then [docs/DESIGN.md](docs/DESIGN.md) for the shared layer (including a
submit-file injection finding that applies well beyond this repo), and
[docs/FACADE.md](docs/FACADE.md), [docs/OPERATOR.md](docs/OPERATOR.md) and
[docs/VIRTUALNODE.md](docs/VIRTUALNODE.md) for each approach in detail.

## Build

```sh
make build
make test
make test-integration   # skips itself without HTCondor / envtest binaries
```

The integration tests need real HTCondor on `PATH` and envtest assets
(`setup-envtest use -p path`); they skip themselves loudly without them, so CI
greps for named passing tests rather than for the absence of `FAIL`.

One gap is worth knowing before reading the results: **no test runs against a
real `kube-controller-manager`.** envtest ships only `kube-apiserver`, `etcd`
and `kubectl`. That matters most to `virtualnode/`, whose central premise is
that the built-in Job controller derives Job status from the Pod status it
writes — tested there by a simulation transcribed from Kubernetes v1.37.0.

The `shared` module's oracle tests run the generated submit files through a
real `condor_submit` and assert on the job ad that comes back, and two fuzzers
do the same over random argument and environment text. They have found four
real bugs so far.
