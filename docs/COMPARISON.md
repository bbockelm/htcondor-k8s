# Which approach, and why

Read [DESIGN.md](DESIGN.md) first for the shared layer; [FACADE.md](FACADE.md)
and [OPERATOR.md](OPERATOR.md) argue each side in detail. This is the
cross-reading, written after both were built and run.

## Both were built, both work

| | Facade | Operator |
|---|---|---|
| Unit tests | 47, green under `-race` | 75, 0 skips |
| Against real HTCondor | 4 integration tests, real `condor_master` + real `kubectl` 1.26 | 1 test, real schedd |
| Against a real API server | n/a — it *is* the API server | 7 envtest cases, real `kube-apiserver` + etcd |
| Verified by hand | `kubectl apply/get/get -o wide/describe/logs/delete`; a job ran on a startd and `kubectl logs` returned its stdout | CRD structurally rejects 10 unsupported fields; webhook patches over TLS; suspend/resume; finalizer |

Neither is a sketch. The interesting result is what each one's failure modes
turned out to be, because they are not symmetrical.

## The facade's wall is `resourceVersion`

Kubernetes clients require an opaque, monotonic, **persistent and shared**
version token, with a working "resume from here" and a 410 Gone when the
server can no longer prove it has every event since. The facade's answer is
`"<gen>.<seq>"`: `seq` bumps once per observed change — hashed on the decoded
record, so job-ad churn does not spam informers — and `gen` bumps when the
sequence space restarts.

That works, and its limits are structural rather than unfinished:

- `gen` is the wall clock at process start, so **a facade restart 410s every
  watcher**.
- **One replica.** There is no shared counter, so a second replica invents its
  own sequence space.

The htcondordb watch cursor cannot stand in for it, and the facade agent
checked this in htcondordb's source rather than assuming: the cursor is a
16-wide vector clock with no total order, its epoch is **per-process random for
mutable tables** (so every htcondordb restart would 410-storm every client),
there is no "too old" error, and the per-watcher buffer is 1024 — which a
`job_queue.log` compaction overruns. The cursor stays private to the source,
which is the right call.

So: making the facade's version persistent and shared means building etcd. At
that point the operator has already won, because it gets exactly that from the
API server for free.

## The operator's wall is co-ownership

Every injury the operator took comes from writing to an object somebody else
owns — the `batch/v1` Job — and all three were found by running the code, not
by reasoning about it:

1. **`.spec.suspend` had to be commandeered**, producing an asymmetric resume.
2. **`job.status` has a second writer**, the built-in Job controller, which
   wins. The mirror ships **off**.
3. **The API server fills in `.spec.selector` *after* mutating admission**, so
   admission re-run on the stored object refused a Job the webhook had
   legitimately claimed — leaving it suspended forever with a `NotAdmitted`
   event. Silent, and indistinguishable from the pool rejecting the work. Now
   fixed in `shared/translate` by keying on `.spec.manualSelector`.

That list is an argument about the *claiming* half, not about the CRD. Which
leads to the finding I think matters most here:

> **The `HTCJob` CRD and the Job-claiming webhook are nearly independent
> products, and only one of them is paying for itself.**

The CRD alone — applied directly, no Job, no webhook, nothing of the user's
mutated — keeps every advantage: the type system does the refusing (ten
unsupported fields rejected by the API server with field paths and no code
running), state is durable across operator restarts, and owner references,
finalizers, conditions, printer columns, events and per-resource RBAC all come
free. It carries none of the co-ownership problems. `config/samples/htcjob.yaml`
runs with no Job and no webhook anywhere.

## My read

**Take the HTCJob CRD plus its controller forward. Drop the Job-claiming
webhook, or demote it to opt-in behind a namespace label.** That is also where
the operator agent landed independently, and the three concrete injuries above
are the evidence.

**Keep the facade for the case it is actually good at**: a site with HTCondor
and no Kubernetes cluster, where the point is that `kubectl` works at all and
there is nothing to install. It genuinely does work, the job queue genuinely is
a usable object store, and deletion is atomic in a way a reconcile loop cannot
match. Its honest trigger to abandon is **multi-replica HA** — if that comes up,
switch rather than chase it.

One asymmetry worth weighing separately: **`kubectl logs` works in the facade
and is a dead end in the operator.** There are no Pods for the kubelet to serve
logs from and the bytes are on an AP the cluster knows nothing about. If
"people can debug their jobs with the tools they already use" is a goal, that
is not a detail.

## What neither one solved

- **Namespaces are a naming convention, not isolation.** Both map a namespace
  onto `+K8sPodNamespace` and filter on it. There is one HTCondor identity for
  all namespaces, and anyone who can `condor_submit` to the same schedd can set
  that attribute by hand and read or inject objects in any namespace. Nothing
  maps a namespace onto an accounting group or a Unix user. This is the biggest
  gap in the whole prototype and it is the same gap in both.
- **No authn/authz/TLS in the facade at all.**
- **Secrets and ConfigMaps**, as planned. Worth noting the operator has a real
  structural advantage here: it runs in-cluster with a service account that can
  read the Secret, while the facade would need its own privileged path to one.
- **`parallelism` below `completions`** — rejected rather than throttled in both.
- **The multi-schedd case** is unaddressed in both.
- **An `args`-only container is untested at runtime** (no container runtime on
  this machine); see DESIGN.md.

## The thing I would fix before either goes further

Not an architecture question, and it applies to both: **stop building
submit-file text out of untrusted input.** `condor_submit` expands macros in
every command value, including inside `+Attr = "…"` literals, and `$ENV(X)`
reads the submitting process's environment. That is closed by escaping now, and
the escape has a known hole that admission rejects — but the whole class
disappears if the Pod is translated into a job ClassAd and handed to
`SendClusterAttributes`/`SendProcAttributes` directly. DESIGN.md has the detail.
