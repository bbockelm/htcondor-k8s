# The virtual node and its kubelet API

Notes on `internal/node` and `internal/kubelet`, the two halves of step 4 of the Pod-interception approach: somewhere legal to bind a Pod to, and something for the API server to proxy `kubectl logs` to. The webhook and the pod controller are written up elsewhere; this file is the part that belongs to neither.

## Keeping the node alive is the work

Registering a `Node` is three API calls. The rest of `internal/node` exists because a virtual node that misses its heartbeats is not cosmetically wrong — it is destructive. The node lifecycle controller marks a stale node `NotReady`, the taint manager adds `node.kubernetes.io/not-ready:NoExecute`, and five minutes later Kubernetes starts deleting Pods. Each of those deletions runs the pod controller's finalizer, and that finalizer removes a live HTCondor job. A heartbeat bug does not degrade this integration; it kills users' work.

So the shape is the one the real kubelet settled on, for the same reasons:

| | cadence | why |
|---|---|---|
| Lease in `kube-node-lease` | renew every 10s, 40s term | the only liveness signal anything reads — the node lifecycle controller measures `--node-monitor-grace-period` (50s) against `renewTime`, not against node conditions |
| Node status | every 60s | a large object; writing it at heartbeat frequency is what made leases necessary in the first place |

They are independent failure domains on purpose: a failed status write means stale, a failed lease renewal means dead. A single transient API error is logged and the loop continues, because taking the process down on a blip would cause exactly the eviction the heartbeat exists to prevent.

The Lease carries an `ownerReference` to the Node, so `kubectl delete node htcondor-virtual-node` cleans up after itself with nobody running. That reference is the easiest thing in the package to get silently wrong: a wrong UID makes the garbage collector delete the Lease *immediately*, which is indistinguishable from a heartbeat that never happens. The envtest suite asserts the UID against the live Node.

## Capacity is informational, and says so on the object

Every other node in a cluster means something by `status.capacity`: the scheduler subtracts bound Pods' requests from it. Nothing consults ours. Our Pods carry a `schedulerName` kube-scheduler ignores and the pod controller binds them by hand, so these numbers gate nothing. Whether a job runs is decided by the negotiator against real slots, which is a fact this object cannot represent — a pool is not a machine with a CPU count, it is a set of slots with shapes.

What capacity is for here is `kubectl describe node`. So there are three sources and the node says which one it used, in a `htcondor.org/capacity-source` annotation:

- **`collector`** — `PoolCapacity` sums `Cpus`, `Memory` and `Disk` over every slot ad. This looks like double counting and is not: a partitionable slot reports what is *unclaimed* and its dynamic children carry the rest, so summing every ad recovers each machine exactly once. It is what `condor_status -total` does. (`TotalCpus` would be the wrong attribute — it counts the whole machine once per slot ad on it.) One collector query per status period.
- **`configured`** — a site administrator's `Config.Capacity`.
- **`placeholder`** — nobody configured anything. The default is deliberately round and large (1000 cpu, 4Ti) rather than a plausible machine size, because a plausible number would read as a measurement.

`allocatable` equals `capacity`: a real kubelet subtracts what the OS and the kubelet reserve, and we run none of the Pod's work on this machine.

`pods` is the one number that is not cosmetic — every bound Pod is an object this process watches — and it is configuration, defaulting to 1000.

## Shutdown goes NotReady and does not delete the Node

This is the decision most worth second-guessing.

Deleting the Node on SIGTERM is what a cloud virtual kubelet usually does, and it is wrong here. The Pod GC controller deletes Pods bound to a Node that no longer exists; those deletions run the finalizer; the finalizer removes the job. A rolling restart of this Deployment would remove every running job in the pool. HTCondor's queue outliving the controller is the best property this integration has — it is the thing the operator write-up singles out as the good case — and a shutdown path that throws it away gives it up for nothing.

So `Shutdown` writes `Ready=False` with reason `KubeletShutdown` and leaves the object alone. It also leaves the Lease alone rather than deleting it, so a restart that finishes inside the 40s term is never noticed by anything, which is the common case for a rollout.

**The cost, stated plainly:** `Ready=False` starts the taint manager's clock. The `DefaultTolerationSeconds` admission plugin has given every Pod a 300s toleration for `node.kubernetes.io/not-ready:NoExecute`, so Pods bound here are deleted five minutes later — the same damage as deleting the Node, only slower. Any exit ends in `NotReady` eventually (the lease expires after 40s whatever we do), so this is a question of how soon, not whether.

**The mitigation belongs to the webhook:** stamping an explicit toleration for `node.kubernetes.io/not-ready` with no `tolerationSeconds` onto the Pod template makes the Pods immune, and it is safe precisely because nothing about these Pods depends on the node being up — we bind them by hand and HTCondor runs them. That is a change in the webhook's half, not this one, and until it is made the five-minute window is real.

Related: `api.TaintKey` is `NoSchedule` and never `NoExecute`, for the same reason, and a unit test pins the effect because it is a one-word change that would do real damage and break nothing visible.

The `Ready` condition is otherwise true as long as this process is alive to say so. There is a `ReadyCheck` hook for wiring the schedd's reachability in, and it is a hook rather than the default on purpose: `Ready=False` has teeth, and a schedd that is briefly unreachable does not mean the jobs it already accepted have stopped running.

## Two things a real API server told us that a fake client would not

**The API server taints new Nodes itself.** `TaintNodesByCondition` admission stamps `node.kubernetes.io/not-ready:NoSchedule` onto every newly created Node, and it is the node lifecycle controller — which envtest does not run — that removes it once `Ready` goes True. So our heartbeat is also what un-taints the node, and any code that assumes it owns `spec.taints` is wrong. `ensureTaint` merges instead of assigning, which also preserves a taint an administrator added by hand.

**`retry.RetryOnConflict` reports success when the context is cancelled.** `retry.OnError` ends with `if wait.Interrupted(err) { err = lastErr }`, and `wait.Interrupted` is true for `context.Canceled` as well as for an exhausted backoff. `lastErr` only ever holds a *retriable* error — a conflict — so a call that failed because the context was cancelled sets `lastErr` to nil and the function returns **nil**. The caller is told a write succeeded when nothing was written. This surfaced here as a nil dereference on the object the "successful" update was supposed to return. `retryOnConflict` in `node.go` wraps it; `TestACancelledUpdateIsNotReportedAsSuccess` pins it, and removing the guards reproduces the original panic. **Every `RetryOnConflict` in this module has the same hole**, since every controller context is cancelled on SIGTERM.

## The kubelet API

`kubectl logs` is not an API-server feature. The API server reads the Pod's `.spec.nodeName`, looks up that Node's `status.addresses` and `status.daemonEndpoints.kubeletEndpoint.Port`, and proxies to `https://<address>:<port>/containerLogs/<ns>/<pod>/<container>`. Whatever answers there *is* the kubelet as far as the cluster is concerned. Those two status fields are the entire route; there is no discovery, and getting them wrong is the most likely way for this to break.

The bytes come from the job's spool directory, read off the Pod as `api.AnnotationSpoolDir` rather than re-derived, so a change to the pod controller's naming scheme cannot strand the output of Pods submitted before it. The reading strategy is the facade's, unchanged: the submit file sets `stream_output`, so the file is the primary path and works for running and finished jobs at the cost of one `open()`; its precondition is a filesystem shared with the schedd. The fallback is a `Peek` of the running job's starter over CEDAR, which needs no shared filesystem but only works while the job runs and returns a bounded tail. Together they cover everything except a finished job on a remote AP, which needs `condor_transfer_data`.

### Query parameters: what is honored and what is not

| parameter | |
|---|---|
| `follow` | **honored** — the file is streamed as it grows and the stream ends when the Pod reaches a terminal phase, with a final pass so the last lines are not lost to the race |
| `tailLines` | **honored**, including `0`, which asks for no lines |
| `limitBytes` | **honored**, applied after `tailLines` |
| `container` | **validated** against the Pod spec; a mismatch is a 400 naming the containers that exist |
| `previous` | **refused** with a 400. HTCondor truncates the output file when a job starts again, so the earlier run's output does not exist, and serving the current run's in its place is a wrong answer to a question asked precisely because the current run is not the interesting one |
| `timestamps` | **ignored** — the stream carries no per-line timestamps and synthesizing them would be inventing data |
| `sinceSeconds`, `sinceTime` | **ignored** — they need the timestamps we do not have |

Anything ignored is named in an `X-HTCondor-Ignored-Parameters` response header. kubectl does not show it; `kubectl get --raw` and anything debugging this does.

### stdout, stderr, and a recommendation for the submit file

A container runtime interleaves the two streams and a Pod has one log. HTCondor writes two files and there is no honest way to interleave them without timestamps, so this serves `stdout`, falls back to `stderr` when `stdout` does not exist (a job that wrote only to stderr must not report as silent — that is the empty log a user will believe), and exposes `?stream=stderr` for the rest. kubectl cannot send that parameter; `kubectl get --raw` can.

**The real fix is upstream of here:** a submit file that points `error` at the same file as `output` merges the streams at the source, the way a container runtime does, and makes the whole question disappear. That is the pod controller's call.

### exec, attach, port-forward

`501`, with a message naming what is missing. The path to implementing it is not speculative: golang-htcondor's `schedd_ssh.go` already does the `GET_JOB_CONNECT_INFO` handshake and opens an interactive channel to a running job's starter — the same handshake the log fallback uses. What is left is bridging that channel to the SPDY or WebSocket stream the API server opens, plus deciding what `kubectl exec` into a finished job should mean. Answering 501 before the upgrade is what makes kubectl print the message instead of hanging.

### TLS, and what is not authenticated

The server requires TLS, and generates a self-signed certificate at startup when none is configured. That works because **the API server does not verify the kubelet's serving certificate unless it was started with `--kubelet-certificate-authority`** — which means in a default cluster it will proxy `kubectl logs` to whatever answers at the address in the Node's status, without checking that it is us. A real deployment should issue this certificate from the cluster CA (a CertificateSigningRequest with the `kubelet-serving` signer) and the cluster should set that flag.

In the other direction, a real kubelet authenticates the API server's client certificate and then authorizes the request with a SubjectAccessReview. This does the first only if `ClientCAFile` is set, and never the second. For a PoC that is the honest state: **anything that can reach this port, with an acceptable client certificate if one is required at all, can read any job's output.**

`/metrics/resource` and `/stats/summary` are not served, so metrics-server gets a 404 and `kubectl top pod` does not work.

## Running the tests

```bash
cd virtualnode
export GOWORK=off GOFLAGS=-mod=mod

go test ./internal/node/... ./internal/kubelet/... -count=1      # 24 unit tests
go test ./test/kubelet/... -count=1 -v                           # 17, real TLS server + real files

export KUBEBUILDER_ASSETS="$(go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use -p path)"
go test ./test/node/... -count=1 -v                              # 7, real kube-apiserver + etcd
```

`test/node` skips every test loudly when the envtest binaries are absent rather than passing an empty run.
