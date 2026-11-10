# Which approach, and why

Read [DESIGN.md](DESIGN.md) first for the shared layer; [FACADE.md](FACADE.md),
[OPERATOR.md](OPERATOR.md) and [VIRTUALNODE.md](VIRTUALNODE.md) argue each side
in detail. This is the cross-reading, written after all three were built and
run.

## All three were built, all three work

| | Facade | Operator | Pod interception |
|---|---|---|---|
| What it is | a kubectl-compatible API server over a schedd | an `HTCJob` CRD plus a webhook that claims Jobs | a virtual node that takes the Pods the Job controller makes |
| Tests passing | 47 unit (green under `-race`) + 4 integration | 75, 0 skips | 105, 0 skips |
| Against real HTCondor | 4 tests, real `condor_master` + real `kubectl` 1.26 | 1 test, real schedd | 2 tests, real schedd |
| Against a real API server | n/a — it *is* the API server | 7 envtest cases | 17 envtest cases |
| Verified by hand | `kubectl apply/get/get -o wide/describe/logs/delete`; a job ran on a startd and `kubectl logs` returned its stdout | CRD structurally rejects 10 unsupported fields; webhook patches over TLS; suspend/resume; finalizer | binding subresource accepted and a raw `nodeName` write refused; deletion removes the job and the object |

None of them is a sketch. The interesting result is what each one's failure
modes turned out to be, because they are not symmetrical — and in two cases the
failure mode was only visible by running the thing against a real API server.

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

That list is an argument about the *claiming* half, not about the CRD. But it
is an argument about *that implementation* of claiming, and a later reading of
the Kubernetes source showed the implementation was avoidable.

## `spec.managedBy` — the claiming half could have been done correctly

`batch/v1` has a field whose entire purpose is handing a Job to a third party:

> ManagedBy field indicates the controller that manages a Job. The k8s Job
> controller reconciles jobs which don't have this field at all or the field
> value is the reserved string `kubernetes.io/job-controller`, but skips
> reconciling Jobs with a custom value for this field.

The `JobManagedBy` gate is **GA and locked-on since 1.35**, and beta-on-by-default
since **1.32**. The controller checks it in three places and leaves the sync
loop entirely. Validation: a domain-prefixed path, 63 characters, and
**immutable after create** — so a mutating webhook at CREATE is both the right
place and the only place.

Setting it would have dissolved two of the three injuries outright:

1. **`.spec.suspend`** was commandeered only to stop Pod creation. With
   `managedBy` set, the Job controller is not reconciling at all, so nothing
   creates Pods and nothing needs suspending.
2. **`job.status`** stops being contested. We become the only writer,
   legitimately, and the status mirror could ship on.
3. **`.spec.selector`** is still defaulted after admission — that is the
   registry, not the controller — but it becomes inert, because nothing adopts
   Pods.

This is the extension point Kueue and MultiKueue use. **The operator was not
wrong to claim Jobs; it was wrong about how.**

> **Three ways to own a `batch/v1` Job, not two.** `managedBy` keeps the
> one-object-per-Job scaling that makes HTCondor worth using and gives up every
> Pod-level affordance. Pod interception gives up the scaling and gets all of
> them. The HTCJob CRD declines the question.

## A third approach: intercept the Pod

[VIRTUALNODE.md](VIRTUALNODE.md) builds the other answer. Rather than taking
the Job away from its controller, it lets the Job controller run untouched and
takes the *Pods* it creates: the webhook stamps `schedulerName`, kube-scheduler
declines them, a virtual node binds them, and this controller writes
`pod.status` while the Job controller derives `job.status` from it.

It is the strongest of the three on fidelity. `parallelism`, `backoffLimit` and
the Job-level `activeDeadlineSeconds` are implemented by the real Job
controller rather than approximated — `shared/translate` rejects
`parallelism < completions` and that rejection is simply unnecessary here. And
`kubectl logs` works, which is a dead end in the operator.

It pays for that with one etcd object per task, and with two findings that only
appeared when it was run against a real API server:

- **A deleted Pod counts as failed.** `isPodFailed` treats any Pod with a
  `deletionTimestamp` that is not `Succeeded` as a failure, deliberately, to
  catch orphans. A finalizer that holds the Pod open during `condor_rm`
  therefore burns a `backoffLimit` retry and spawns a replacement — a duplicate
  HTCondor submission. Mitigated by writing the terminal phase before dropping
  the finalizer, and by `podReplacementPolicy: Failed`.
- **`podReplacementPolicy` cannot be refused, only set.** `SetDefaults_Job`
  fills it in during decode, before mutating admission runs, so refusing an
  explicit `TerminatingOrFailed` refuses every Job in the cluster. This is the
  `.spec.selector` trap for the third time, and the pattern is now clear enough
  to state as a rule: **never write an admission check against the absence of a
  defaulted field.**

Its own co-ownership cost is real but far milder than the operator's: one
field, one safe value, and nothing ever needs the user's intent read back.

## My read

**For the Job-shaped case, try `managedBy` before anything else.** It is the
supported answer, it is one immutable field set at admission, and it keeps the
scaling that makes HTCondor the right backend. Nothing in this repo implements
it yet — that is the clearest next piece of work, and it is small.

**Take the HTCJob CRD plus its controller forward** as the durable core. The
type system does the refusing, state survives restarts, and the Kubernetes
affordances come free. It is also the only one of the three that does not
depend on a Kubernetes behavior that could change under it.

**Take Pod interception when fidelity matters more than width** — a site whose
users think in Pods, run modest fan-out, and expect `kubectl logs` and
`kubectl describe` to work. Not for a 10,000-way parameter sweep.

**Keep the facade for the case it is actually good at**: a site with HTCondor
and no Kubernetes cluster, where the point is that `kubectl` works at all and
there is nothing to install. Its honest trigger to abandon is **multi-replica
HA** — if that comes up, switch rather than chase it.

## What none of them solved

- **Namespaces are a naming convention, not isolation.** All three map a
  namespace onto `+K8sPodNamespace` and filter on it. There is one HTCondor
  identity for every namespace, and anyone who can `condor_submit` to the same
  schedd can set that attribute by hand and read or inject objects in any
  namespace. Nothing maps a namespace onto an accounting group or a Unix user.
  This is the biggest gap in the whole prototype and it is identical in all
  three.
- **No authn/authz/TLS in the facade at all**, and the virtual node's kubelet
  API has a serving certificate the API server does not verify and no
  SubjectAccessReview — so without a client CA, anyone who can reach the port
  reads any job's output.
- **Secrets and ConfigMaps**, as planned. The two in-cluster approaches have a
  real structural advantage here: they run with a service account that can read
  the Secret, while the facade would need its own privileged path to one.
- **`parallelism` below `completions`** — rejected in the facade and the
  operator, and genuinely solved only by Pod interception, where the built-in
  Job controller does the throttling.
- **The multi-schedd case** is unaddressed everywhere.
- **An `args`-only container is untested at runtime** (no container runtime on
  this machine); see DESIGN.md.
- **No end-to-end run against a real `kube-controller-manager`.** envtest ships
  only `kube-apiserver`, `etcd` and `kubectl`. This matters most to Pod
  interception, whose central premise is simulated rather than observed.

## The thing I would fix before any of them goes further

Not an architecture question, and it applies to all three: **stop building
submit-file text out of untrusted input.** `condor_submit` expands macros in
every command value, including inside `+Attr = "…"` literals, and `$ENV(X)`
reads the submitting process's environment. That is closed by escaping now, and
the escape has a known hole that admission rejects — but the whole class
disappears if the Pod is translated into a job ClassAd and handed to
`SendClusterAttributes`/`SendProcAttributes` directly. DESIGN.md has the detail.

## The pattern worth carrying out of this repo

Three separate bugs, in two different approaches, had one shape: **an admission
check written against the absence of a field the API server defaults.**
`.spec.selector` suspended claimed Jobs forever; `.spec.manualSelector` was the
fix; `podReplacementPolicy` would have refused every Job in the cluster.
Defaulting happens during decode, before mutating admission runs, so a webhook
never sees an unset defaulted field — but code that re-runs admission against a
stored object does see the default, and disagrees with the webhook that let it
in. Check the value, never the presence.
