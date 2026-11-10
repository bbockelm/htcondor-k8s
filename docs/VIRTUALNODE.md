# Approach 3: intercept the Pod, not the Job

Read [DESIGN.md](DESIGN.md) first for the shared translation layer, and
[COMPARISON.md](COMPARISON.md) for where this sits against the other two.

## The idea

The operator ([OPERATOR.md](OPERATOR.md)) tried to own a `batch/v1` Job it did
not create, and took three injuries for it. The worst was that `job.status` has
a legitimate writer already — the built-in Job controller — which wins every
contest. The status mirror had to ship off.

This approach does not compete for that write. It gives the Job controller what
it wants:

1. A mutating webhook stamps `schedulerName: htcondor` on a translatable Job's
   pod template. It does **not** suspend the Job.
2. The Job controller therefore behaves completely normally, and creates real
   Pod objects — honoring `parallelism`, `completions`, `backoffLimit` and
   `activeDeadlineSeconds` itself.
3. kube-scheduler sees a `schedulerName` that is not its own and ignores them,
   so they sit `Pending`.
4. The pod controller takes each Pod, submits it to HTCondor, and binds it to a
   virtual node that this process registers and keeps alive.
5. The pod controller writes `pod.status`. **The Job controller derives
   `job.status` from those Pods.** We never write Job status at all.

Step 5 is the whole point. The co-ownership problem is not worked around, it is
dissolved: there is exactly one writer for each field, and it is the writer
Kubernetes expects.

## What it buys, and what it costs

Three things the operator could not do come free, because the real Job
controller is doing them:

- **`parallelism` below `completions`** is throttled properly. `shared/translate`
  rejects it, and that rejection is unnecessary here.
- **`backoffLimit` retries** are real: a new Pod per attempt, real backoff, each
  attempt its own object and its own HTCondor job.
- **`kubectl logs`** works, which is a dead end in the operator. The API server
  proxies to the kubelet of the node a Pod is bound to, so registering a virtual
  node and serving that HTTP API is all it takes.

The cost is one etcd object per task. A 10,000-completion Job becomes 10,000
Pods — precisely the scale Kubernetes is bad at and HTCondor is built for. The
operator's cluster-per-Job mapping is one object regardless of width. **This
approach optimizes for fidelity; the HTCJob CRD optimizes for width.** A site
that needs both should run both.

## Status

Built and tested: 110 top-level tests pass across the module, 0 skipped where
a container runtime is present. The node lifecycle and the pod controller are
tested against a real `kube-apiserver` via envtest, submission against a real
schedd, and **the central premise against a real `kube-controller-manager`**
running in k3s (`test/k3s`).

That last suite is the one that matters, because the approach depends on a
component nobody here wrote. It proves, end to end: the real Job controller
creates Pods from a stamped template and honors `parallelism`; kube-scheduler
declines a Pod carrying a foreign `schedulerName` and leaves it unbound; this
controller binds it through the binding subresource and writes `pod.status`;
and **the real Job controller counts those statuses**, marks the Job
`Complete`, and creates the next Pod. One HTCondor submission per completion.

It skips itself, loudly, without a container runtime — which is right on a
laptop and would be a vacuous pass on CI, so the workflow greps for the named
tests and fails if anything skipped.

It earned its keep on the first run. See [the service-account
token](#the-injected-service-account-token) below: a real API server injects a
projected volume into every Pod, `translate.Admit` rejects volumes, and this
integration therefore refused **every Pod on any real cluster**. envtest does
not enable that admission plugin, so no amount of simulation would have found
it.

---

## The webhook and the pod controller

This is one half of approach 3. The other half — the Node object, its
lifecycle and the kubelet HTTP API that serves `kubectl logs` — is in
[NODE-AND-KUBELET.md](NODE-AND-KUBELET.md).

Read the first two sections if you only read two: the premise is the whole
argument for this approach, and the evidence for it is weaker than the rest of
the suite.

---

### The shape, and why it dissolves the operator's problem

```
user applies a batch/v1 Job
   -> mutating webhook stamps .spec.template: schedulerName + tolerations
   -> built-in Job controller makes real Pods, honoring parallelism,
      completions, backoffLimit, activeDeadlineSeconds
   -> kube-scheduler sees a foreign schedulerName and ignores them
   -> pod controller submits each Pod to HTCondor, binds it to the virtual node
   -> pod controller writes pod.status
   -> built-in Job controller derives job.status from those Pod statuses
```

Nothing here ever writes a Job. That is the point.

`docs/OPERATOR.md` records three injuries from the HTCJob approach, and all
three come from writing to objects somebody else owns:

| Injury | What happens here |
|---|---|
| `.spec.suspend` had to be commandeered, producing a resume that cannot be made symmetric | never touched; the Job is never suspended and makes its Pods normally |
| `job.status` has a second writer in kube-controller-manager that always wins, so the mirror ships off | never written; the only writer is the one Kubernetes believes |
| `.spec.selector` is defaulted *after* mutating admission, so the webhook and the controller judged the same object differently | the same trap exists here, once, and is documented below |

The cost moves rather than disappearing. It lands on Pod status, which is now
load-bearing in a way HTCJob status never was: the Job controller **counts**
these phases. A wrong `Succeeded` does not merely mislead a human, it silently
credits a completion that never happened and stops a Job that should have
retried. Most of the care in `internal/podctl` is about that one sentence.

The upside is not only tidiness. `parallelism` below `completions` works here,
and it works for free. Both other approaches refuse it outright, because
HTCondor starts every idle proc of a cluster as soon as a slot matches and
nothing throttles it. Here the Job controller never creates the Pod in the
first place, so there is nothing to throttle.

---

### The premise, and how it is now tested

**envtest cannot run kube-controller-manager.** Its `ControlPlane` type has
exactly two fields, `APIServer` and `Etcd`, and the asset bundle
`setup-envtest` downloads contains `kube-apiserver`, `etcd` and `kubectl` —
there is no `kube-controller-manager` binary to run. No configuration of
envtest makes the Job controller appear.

So `test/podctl/pod_premise_test.go` contains a `jobControllerSim` that plays
its part: it creates Pods from the stamped template, counts phases, honors
`parallelism` and `backoffLimit`, and writes `job.status`. Two rules in it were
read out of kubernetes v1.37.0 `pkg/controller/job/job_controller.go` rather
than recalled, and are written out in full in the test so a reader can check
them — `isPodFailed` and `onlyReplaceFailedPods`.

What that buys and what it does not:

- **Proved against a real API server**: the webhook's mutation survives a real
  admission round trip; a bare Pod is admitted, submitted and bound; binding
  works through `pods/binding` and a plain update of `spec.nodeName` is
  refused; an inadmissible Pod can be moved to `Failed` while unbound; a
  deleted Pod's job is removed and the Pod object actually goes away.
- **Proved against a real `kube-controller-manager`**, in `test/k3s`: that the
  Job controller creates the Pods, honors `parallelism`, reads back the
  statuses this controller writes, and arrives at the right `job.status`.

The simulation is kept because it is fast, can construct states the real
controller would not produce, and drives the webhook over TLS — but `test/k3s`
is the evidence, and where the two disagree the real one wins. That ordering is
not theoretical: the real cluster failed on the first run, for a reason the
simulation could not represent.


#### The injected service-account token

A real API server's ServiceAccount admission plugin injects a projected
`kube-api-access-<rand>` volume — and a read-only mount of it at the
well-known service-account path — into **every** Pod. `translate.Admit`
rejects volumes and volume mounts, so before this was handled, this
integration refused every Pod on any real cluster. envtest does not enable
that plugin, which is why 105 passing tests and a careful simulation all
agreed it worked.

`normalizeForAdmission` now strips exactly that volume and its mounts, matching
on both the generated name prefix and the projected `serviceAccountToken`
source, so a user's own projected volume is still refused. Stripping is the
honest reading rather than a convenience: the token grants access to an API
server the job cannot reach, because the job runs on an HTCondor execute node
outside the cluster. A deployment that wants to be explicit can set
`automountServiceAccountToken: false` on the pod template and stop the
injection at its source.

This is the third instance in this repo of the same shape — admission re-run
on an object that something downstream has since written to — after
`.spec.selector` and `podReplacementPolicy`, and the first where the writer was
neither the user nor us.

---

### The webhook writes four things

`internal/webhook` is deliberately the smallest webhook that works. Compare
`operator/internal/webhook/job_webhook.go`, which is much larger and worse for
it.

| Written | Why |
|---|---|
| `.spec.template.spec.schedulerName` | the fence: kube-scheduler skips a name that is not its own, and the pod controller selects on it |
| toleration for `htcondor.org/virtual-node:NoSchedule` | the node's taint keeps other people's Pods off; ours have to say they can live there |
| tolerations for `node.kubernetes.io/{not-ready,unreachable}:NoExecute`, `tolerationSeconds` **unset** | see below — without it, restarting this controller deletes live work |
| `.spec.podReplacementPolicy: Failed` | see below — without it, our own finalizer window is counted as a failure |

It does **not** write `suspend`, status, or any annotation. There is no claim
annotation at all: the mechanism *is* the `schedulerName`, which is a fact
about the object the Job controller actually copies, not a note we left
ourselves.

#### The not-ready tolerations are not defensive programming

When the virtual kubelet exits, the Node goes `Ready=False` — and any exit
reaches that state once the lease expires. The node lifecycle controller then
applies `node.kubernetes.io/not-ready:NoExecute`, and the
`DefaultTolerationSeconds` admission plugin has already given every Pod a
300-second toleration for it. Five minutes later the taint manager **deletes
every Pod bound to the node**. Each deletion runs the pod controller's
finalizer, and that finalizer `condor_rm`s a job that is running perfectly
well on an execute node that never heard of Kubernetes.

A controller restart that takes longer than five minutes therefore kills the
user's work. `tolerationSeconds` is left nil, which means forever; zero would
mean *evict immediately*, the exact opposite, and is the one way to write this
field wrong.

Tolerating a down node forever is wrong for a real node and right here, for the
reason that is the whole pattern: nothing about these Pods depends on the node
being up. There is no kubelet, no container on that machine, no local state.
The Pod object is bookkeeping for work running somewhere else, and bookkeeping
should not be deleted because the bookkeeper went to lunch.

#### `podReplacementPolicy`, and the operator's `.spec.selector` trap again

The built-in Job controller's `isPodFailed` counts a Pod carrying a
`deletionTimestamp` as failed unless its phase is already `Succeeded`,
deliberately, to catch orphans that will never reach a phase. Every Pod here
spends a window in exactly that state, because the pod controller holds a
finalizer while it reaches a schedd. Under the default `TerminatingOrFailed`
that window burns a `backoffLimit` retry and manufactures a replacement Pod — a
second HTCondor submission for work that already ran. `Failed` makes the
controller wait for a real terminal phase.

The first version of this webhook *refused* a Job that explicitly set
`TerminatingOrFailed`, on the theory that overriding a user-meaningful field
was the `.spec.suspend` injury all over again. **It refused every Job in the
cluster.** The API server applies scheme defaulting while decoding the request
body, *before* mutating admission runs, and `SetDefaults_Job` fills the field
in with `TerminatingOrFailed` — so there is no such thing as a Job whose
`podReplacementPolicy` is unset by the time a webhook sees it, and no
observable difference between a user who asked for it and a user who said
nothing.

That is precisely the shape of the `.spec.selector` trap `docs/OPERATOR.md`
describes, found the same way, by envtest. It is now pinned by
`TestAPIServerDefaultsPodReplacementPolicyBeforeAdmission`, which skips with a
message if a future API server stops defaulting the field — at which point the
webhook should go back to writing it only when unset.

So this approach does pay a little of the co-ownership cost, in one field. It
is much milder than `.spec.suspend` in the two ways that matter: there is only
one safe value for an asynchronous provider, so nothing is lost by holding it;
and nothing ever needs to read the user's original intent back, so there is no
asymmetric resume to be stuck with.

#### `failurePolicy: Fail`, against the operator's `Ignore`

The operator's webhook matched every Job in the cluster, so failing open was
right: a Job it did not claim ran on Kubernetes the way it always had.

This one is opt-in — `objectSelector` matches `htcondor.org/scheduler:
htcondor` — so every Job it sees is one whose author asked for HTCondor by
name. Failing open sends that work to kube-scheduler quietly, onto nodes that
may not have the image, the resources or the data. Failing closed refuses the
create with an error naming this webhook. Visible beats silent, the blast
radius is bounded to the Jobs that opted in, and there is a workaround even
then: a bare Pod with `schedulerName: htcondor` needs no webhook at all.

---

### Two judgement calls worth reviewing

#### Pod-level `activeDeadlineSeconds`: HTCondor enforces it

The *Job*-level deadline is the Job controller's. It enforces it by deleting
active Pods, which arrives here as a `deletionTimestamp` and comes out as a
`condor_rm`. Nothing here sets a `periodic_remove` for it; two enforcers with
two clocks for one deadline is a bug waiting to happen, and the Job's deadline
never appears in a Pod spec anyway, so there is not even an ambiguity to
resolve.

The *Pod*-level deadline is normally the kubelet's, and there is no kubelet. Of
the two places it could go — a timer in this controller, or an expression on
the job — the job wins on every count. It survives a controller outage. It is
evaluated by the schedd against the job's own clock rather than against ours.
And it leaves exactly one enforcer, so there is no pair of clocks to disagree.
`translate.PodToSubmit` already renders it as `periodic_remove = (time() -
QDate) > N`, and when such a job is removed the Pod reports
`reason: DeadlineExceeded` rather than the generic `Removed`, which is the word
the rest of Kubernetes uses.

One honest divergence: a kubelet measures from `.status.startTime`, which is
after scheduling, while `QDate` is when the job entered the queue. In this
architecture those are the same instant — a Pod is submitted the moment it is
taken, and `startTime` is reported *as* `QDate` for exactly that reason — but
on a pool where jobs sit idle for a long time, queued time counts against the
deadline here and would not on a real node.

`TestPodDeadlineBecomesAnExpressionTheScheddParsed` checks the thing that would
otherwise fail silently: an expression that did not parse produces a submit
that succeeds with no `periodic_remove` at all and a deadline that never fires.

#### `restartPolicy: OnFailure`: refused, and `Never` is better anyway

**Decision: this approach refuses `OnFailure`.** The webhook does not stamp
such a Job — it runs on Kubernetes, where `OnFailure` works properly — and the
pod controller refuses such a bare Pod with a message naming the field.

The reasoning. `OnFailure` means the kubelet restarts the container in place:
the Pod stays Running, `containerStatuses[].restartCount` increments, and the
Job controller does **not** make a new Pod. There is no kubelet here. The
nearest HTCondor equivalent, `max_retries`, is a different thing — a fresh
execution of the whole job, possibly on another execute node — and using it
would mean synthesizing `restartCount` from `NumJobStarts` to make the Job
controller's `OnFailure` backoff accounting work. That accounting keys on
`restartCount`, so getting the synthesis wrong is not a cosmetic error, it is
an unbounded retry loop. And the bound to retry *to* is the Job's
`backoffLimit`, which is not visible from a Pod at all; reading it would mean
fetching the owning Job, re-coupling this controller to the object it exists
not to touch.

The trade-off, stated plainly: this is **narrower than `shared/translate`**,
which admits `OnFailure`, and narrower than the operator, which maps it onto
`max_retries`. A user who wants retries writes `restartPolicy: Never` with
`backoffLimit: N`.

What makes the refusal easy rather than merely defensible is that `Never` here
is *better* than `OnFailure` anywhere else in this project. Under `Never` the
real Job controller implements the retry: a genuinely new Pod per attempt, with
real backoff, counted against a real limit, with each attempt visible as its
own object and its own HTCondor job. Neither of the other two approaches offers
that. Pretending to support `OnFailure` would trade a working retry story for a
broken one.

---

### Crash safety

The hazard: a reconcile that submits and dies before recording the job id
leaves a job running that nothing points at, and a next pass that trusted its
own annotations would submit it again — running the user's work twice.

The fix is to stop trusting the annotation. Every submit stamps `+K8sPodUID`
with the Pod's UID, which the API server assigned before this controller ever
saw the object and which a replacement Pod of the same name does not inherit.
So the order is always **look, then submit**:

1. `Query(K8sPodUID == "<uid>")`. Non-empty → adopt it, whatever we thought.
2. Otherwise submit, and record the id immediately as its own write.

A submit whose *outcome* is unknown — the connection dropped during the commit
— resolves on the next pass through step 1, because a committed qmgmt
transaction is visible to the very next query and an aborted one left nothing
behind. There is no window in which both answers are possible.
`TestCrashBetweenSubmitAndRecordDoesNotResubmit` simulates exactly that (the
fake queues the job and *then* reports an error); deleting the adoption branch
makes it fail with 2 submissions rather than with a timeout.

The whole argument rests on that query working against a real schedd, which the
unit tests cannot see — their fake resolves the constraint by construction, so
a typo in the attribute name would leave every one of them green.
`test/condor/pod_pool_test.go` is there for that one assertion, and mutating
the attribute name makes it fail with `matched 0 jobs`.

**The window this does not close**, and it is the same gap
`docs/OPERATOR.md` calls the most likely source of wrong answers in production:
a submit whose result was lost *and* whose job then left the queue before the
next reconcile is indistinguishable from a Pod that was never submitted, and
would be submitted again. Closing it needs a history query (`condor_history`,
or the job-epoch ads) on the Pod UID, and the `Queue` interface has no method
for one.

---

### Deletion, and the two things a kubelet does that nothing else would

The finalizer goes on in a pass of its own, before anything is submitted. Added
in the same pass as the submit, a crash in between would leave a job nothing is
obliged to clean up — deleting the Pod would succeed immediately and the job
would run on.

On the way out, `finalize` does three things in this order, and the order is
the content:

1. **Read the queue once more and write the terminal phase**, if the job
   reached one and the Pod has not reported it. This is the `isPodFailed` race
   described above: a job that succeeded between two polls, on a Pod the user
   then deleted, would be counted as a failure. `podReplacementPolicy: Failed`
   makes the Job controller wait for a real phase; this is what puts the phase
   there to be waited for.
2. **`condor_rm`**, and on failure block the delete rather than dropping the
   finalizer. A stuck delete is visible; an orphaned job is not.
3. **Remove our finalizer, then complete the deletion.** Deleting a *bound* Pod
   is a two-step conversation: the API server marks it and waits for the node
   to confirm, which a kubelet does with its own `DELETE` at grace period zero.
   There is no kubelet, so without that step the Pod sits in `Terminating`
   forever — finalizers all removed, HTCondor job long gone, object still
   there — and the Job controller, which tracks terminating Pods separately and
   holds its own `batch.kubernetes.io/job-tracking` finalizer until each is
   accounted for, never finishes either. envtest found this immediately; a fake
   client cannot see it, because it deletes as soon as the last finalizer comes
   off. The delete carries a UID precondition, so it can never destroy a
   replacement Pod that reused the name.

A Job-created Pod carries **two** finalizers — ours and the Job controller's —
so nothing here assumes removing ours makes the object disappear.

#### The reverse case

`Reaper` sweeps from the queue rather than from Kubernetes, which is the only
direction that can see an orphan at all: an orphan is defined by the absence of
the object a Kubernetes-side walk would start from. It queries
`K8sVirtualNode == "<node>"` (an attribute this controller adds through
`SubmitOptions.ExtraAttributes`, since nothing in the shared schema means "every
job this integration owns"), and removes any job older than a grace period
whose Pod is absent **or has a different UID**. The UID comparison is the part
that matters: `restartPolicy: Never` means the Job controller replaces a failed
Pod, and for a non-indexed Job the replacement can reuse the name.

---

### Status accuracy

`translate.PodStatus` does the mapping, shared with the facade and the operator
so one job ad cannot mean two things. What this controller adds is the handful
of fields that are facts about the architecture rather than the job — the image
(the status projection deliberately does not fetch `K8sPodSpec`), a
`htcondor://cluster.proc` container id, a `startTime` that never moves, the
`qosClass` the API server computed at create — plus two refusals to guess.

**A job ad that is not settled is not believed.** `translate.JobAd`'s zero
`ExitCode` is `0`, which reads as "succeeded". An ad that says `Completed` and
carries neither a completion time nor an exit status is one the schedd has not
finished writing, and the Pod is left where it was for another poll. A Pod that
stalls `Running` is visible; a Pod wrongly reported `Succeeded` is not.

**A job that left the queue fails.** Completed jobs are reaped into history, so
a poll that misses the terminal transition sees nothing. There is no honest way
to tell success from failure from there, and the two wrong answers are not
equally bad: a wrong `Failed` makes a replacement Pod — visible, and reported
as a bug — while a wrong `Succeeded` corrupts the user's Job status silently.
Its cost is real, though: under `backoffLimit` it runs the work again.

Statuses are compared before being written, and every timestamp is truncated to
the second first, because `metav1.Time` serializes with second precision and a
status computed with nanoseconds still on it never equals the stored one. Without
that, every poll of every running Pod is an etcd write and a watch event that
wakes the Job controller for nothing.

---

### Changes needed in `shared/translate`

Neither was made here; `shared/translate` exists so the three approaches cannot
drift into three different Pod/job mappings, and a local filter in
`webhook.admitForPodInterception` is visible in a way a fork is not.

#### 1. `AdmitJob` should be split

`AdmitJob` currently mixes two kinds of check:

- checks about **the translation** — the pod template, and whether indexed
  completion has a completions count;
- checks that only apply to a front end that is **reimplementing Job semantics
  itself** — today `parallelism != completions`, `podFailurePolicy`,
  `successPolicy` and `ttlSecondsAfterFinished`.

The facade and the operator need both. This approach needs only the first,
because the built-in Job controller is still running and owns the second group
outright. `parallelism` is the one that bites today and is filtered locally;
`podFailurePolicy` and `ttlSecondsAfterFinished` would both work here too and
are left refused only to keep the local override to a single rule.

Suggested shape — additive, non-breaking:

```go
// AdmitJobTemplate checks everything about a Job that is a property of the
// translation. AdmitJob is AdmitJobTemplate plus the Job-level fields a front
// end must refuse when it is the one implementing Job semantics.
func AdmitJobTemplate(spec *batchv1.JobSpec, meta *metav1.ObjectMeta, caps Capabilities) Rejections
```

**A `Capabilities` flag would be the wrong fix.** `Capabilities` describes *the
pool* — slot sizes, GPU resource names, which `nodeSelector` keys the machines
advertise. Whether `parallelism` is honored is a property of the *front end's
architecture*, identical at every site. Putting it in `Capabilities` would make
a site-configuration object carry a fact about which binary is running, which
is exactly the kind of skew `docs/OPERATOR.md` calls an operational trap.

Note that the rejection is also stricter than its comment: it fires on
`*Parallelism != *Completions`, so `parallelism` *above* `completions` — which
is meaningless but harmless in Kubernetes — is refused too.

#### 2. Possibly: a shared `K8sVirtualNode`, or a documented extension point

`AttrVirtualNode` is invented in `internal/podctl` and travels through
`SubmitOptions.ExtraAttributes`, which is the sanctioned extension point and
works. If a pool is ever expected to run more than one of these integrations at
once — which `docs/DESIGN.md` gives as the reason all three share one attribute
schema — then "which front end owns this job" is a question the shared schema
should be able to answer, and it currently cannot.

---

### Known gaps

- **`kubectl logs` is unproven end to end.** `test/k3s` runs the controller
  outside the cluster, so the API server cannot dial back to the kubelet API
  to prove the log proxy. The server itself is tested directly over TLS.
- **The lost-submit-then-reaped window**, which needs a history query.
- **`restartPolicy: OnFailure` is refused**, narrowing the admitted subset
  below what `shared/translate` allows.
- **`internal/podctl` ships its own `FakeQueue`.** `internal/condor.FakeQueue`
  cannot serve this controller: its resolver understands only `K8sOwnerUID ==`
  and `ClusterId ==`, and its `Submit` never reads `+K8sPodUID` back, so its
  ads carry no `Identity.UID` — the exact field adoption and the orphan sweep
  compare on. A fake that cannot express the thing under test would make these
  tests pass vacuously. The two should be folded together once the `Queue`
  interface settles.
- **The spool directory is created locally.** It only works when this process
  and the access point share a filesystem, the same limitation the operator
  has. The kubelet API's `Peek` fallback is the answer for the remote case; see
  [NODE-AND-KUBELET.md](NODE-AND-KUBELET.md).

---

### Running it

```bash
cd virtualnode
export GOWORK=off GOFLAGS=-mod=mod

go build ./... && go vet ./...
go test ./internal/podctl/... ./internal/webhook/... -count=1      # unit

export KUBEBUILDER_ASSETS="$(go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use -p path)"
go test ./test/podctl/... -count=1 -v                              # real kube-apiserver + etcd

PATH=/path/to/release_dir/bin:/path/to/release_dir/sbin:$PATH \
  go test ./test/condor/... -count=1 -v                            # real schedd
```

Both integration suites skip cleanly when their dependency is absent, and every
test announces the skip, so an empty run is never mistaken for a passing one.

---

## The virtual node and its kubelet API

Notes on `internal/node` and `internal/kubelet`, the two halves of step 4 of the Pod-interception approach: somewhere legal to bind a Pod to, and something for the API server to proxy `kubectl logs` to. The webhook and the pod controller are written up elsewhere; this file is the part that belongs to neither.

### Keeping the node alive is the work

Registering a `Node` is three API calls. The rest of `internal/node` exists because a virtual node that misses its heartbeats is not cosmetically wrong — it is destructive. The node lifecycle controller marks a stale node `NotReady`, the taint manager adds `node.kubernetes.io/not-ready:NoExecute`, and five minutes later Kubernetes starts deleting Pods. Each of those deletions runs the pod controller's finalizer, and that finalizer removes a live HTCondor job. A heartbeat bug does not degrade this integration; it kills users' work.

So the shape is the one the real kubelet settled on, for the same reasons:

| | cadence | why |
|---|---|---|
| Lease in `kube-node-lease` | renew every 10s, 40s term | the only liveness signal anything reads — the node lifecycle controller measures `--node-monitor-grace-period` (50s) against `renewTime`, not against node conditions |
| Node status | every 60s | a large object; writing it at heartbeat frequency is what made leases necessary in the first place |

They are independent failure domains on purpose: a failed status write means stale, a failed lease renewal means dead. A single transient API error is logged and the loop continues, because taking the process down on a blip would cause exactly the eviction the heartbeat exists to prevent.

The Lease carries an `ownerReference` to the Node, so `kubectl delete node htcondor-virtual-node` cleans up after itself with nobody running. That reference is the easiest thing in the package to get silently wrong: a wrong UID makes the garbage collector delete the Lease *immediately*, which is indistinguishable from a heartbeat that never happens. The envtest suite asserts the UID against the live Node.

### Capacity is informational, and says so on the object

Every other node in a cluster means something by `status.capacity`: the scheduler subtracts bound Pods' requests from it. Nothing consults ours. Our Pods carry a `schedulerName` kube-scheduler ignores and the pod controller binds them by hand, so these numbers gate nothing. Whether a job runs is decided by the negotiator against real slots, which is a fact this object cannot represent — a pool is not a machine with a CPU count, it is a set of slots with shapes.

What capacity is for here is `kubectl describe node`. So there are three sources and the node says which one it used, in a `htcondor.org/capacity-source` annotation:

- **`collector`** — `PoolCapacity` sums `Cpus`, `Memory` and `Disk` over every slot ad. This looks like double counting and is not: a partitionable slot reports what is *unclaimed* and its dynamic children carry the rest, so summing every ad recovers each machine exactly once. It is what `condor_status -total` does. (`TotalCpus` would be the wrong attribute — it counts the whole machine once per slot ad on it.) One collector query per status period.
- **`configured`** — a site administrator's `Config.Capacity`.
- **`placeholder`** — nobody configured anything. The default is deliberately round and large (1000 cpu, 4Ti) rather than a plausible machine size, because a plausible number would read as a measurement.

`allocatable` equals `capacity`: a real kubelet subtracts what the OS and the kubelet reserve, and we run none of the Pod's work on this machine.

`pods` is the one number that is not cosmetic — every bound Pod is an object this process watches — and it is configuration, defaulting to 1000.

### Shutdown goes NotReady and does not delete the Node

This is the decision most worth second-guessing.

Deleting the Node on SIGTERM is what a cloud virtual kubelet usually does, and it is wrong here. The Pod GC controller deletes Pods bound to a Node that no longer exists; those deletions run the finalizer; the finalizer removes the job. A rolling restart of this Deployment would remove every running job in the pool. HTCondor's queue outliving the controller is the best property this integration has — it is the thing the operator write-up singles out as the good case — and a shutdown path that throws it away gives it up for nothing.

So `Shutdown` writes `Ready=False` with reason `KubeletShutdown` and leaves the object alone. It also leaves the Lease alone rather than deleting it, so a restart that finishes inside the 40s term is never noticed by anything, which is the common case for a rollout.

**The cost, stated plainly:** `Ready=False` starts the taint manager's clock. The `DefaultTolerationSeconds` admission plugin has given every Pod a 300s toleration for `node.kubernetes.io/not-ready:NoExecute`, so Pods bound here are deleted five minutes later — the same damage as deleting the Node, only slower. Any exit ends in `NotReady` eventually (the lease expires after 40s whatever we do), so this is a question of how soon, not whether.

**The mitigation belongs to the webhook:** stamping an explicit toleration for `node.kubernetes.io/not-ready` with no `tolerationSeconds` onto the Pod template makes the Pods immune, and it is safe precisely because nothing about these Pods depends on the node being up — we bind them by hand and HTCondor runs them. That is a change in the webhook's half, not this one, and until it is made the five-minute window is real.

Related: `api.TaintKey` is `NoSchedule` and never `NoExecute`, for the same reason, and a unit test pins the effect because it is a one-word change that would do real damage and break nothing visible.

The `Ready` condition is otherwise true as long as this process is alive to say so. There is a `ReadyCheck` hook for wiring the schedd's reachability in, and it is a hook rather than the default on purpose: `Ready=False` has teeth, and a schedd that is briefly unreachable does not mean the jobs it already accepted have stopped running.

### Two things a real API server told us that a fake client would not

**The API server taints new Nodes itself.** `TaintNodesByCondition` admission stamps `node.kubernetes.io/not-ready:NoSchedule` onto every newly created Node, and it is the node lifecycle controller — which envtest does not run — that removes it once `Ready` goes True. So our heartbeat is also what un-taints the node, and any code that assumes it owns `spec.taints` is wrong. `ensureTaint` merges instead of assigning, which also preserves a taint an administrator added by hand.

**`retry.RetryOnConflict` reports success when the context is cancelled.** `retry.OnError` ends with `if wait.Interrupted(err) { err = lastErr }`, and `wait.Interrupted` is true for `context.Canceled` as well as for an exhausted backoff. `lastErr` only ever holds a *retriable* error — a conflict — so a call that failed because the context was cancelled sets `lastErr` to nil and the function returns **nil**. The caller is told a write succeeded when nothing was written. This surfaced here as a nil dereference on the object the "successful" update was supposed to return. `retryOnConflict` in `node.go` wraps it; `TestACancelledUpdateIsNotReportedAsSuccess` pins it, and removing the guards reproduces the original panic. **Every `RetryOnConflict` in this module has the same hole**, since every controller context is cancelled on SIGTERM.

### The kubelet API

`kubectl logs` is not an API-server feature. The API server reads the Pod's `.spec.nodeName`, looks up that Node's `status.addresses` and `status.daemonEndpoints.kubeletEndpoint.Port`, and proxies to `https://<address>:<port>/containerLogs/<ns>/<pod>/<container>`. Whatever answers there *is* the kubelet as far as the cluster is concerned. Those two status fields are the entire route; there is no discovery, and getting them wrong is the most likely way for this to break.

The bytes come from the job's spool directory, read off the Pod as `api.AnnotationSpoolDir` rather than re-derived, so a change to the pod controller's naming scheme cannot strand the output of Pods submitted before it. The reading strategy is the facade's, unchanged: the submit file sets `stream_output`, so the file is the primary path and works for running and finished jobs at the cost of one `open()`; its precondition is a filesystem shared with the schedd. The fallback is a `Peek` of the running job's starter over CEDAR, which needs no shared filesystem but only works while the job runs and returns a bounded tail. Together they cover everything except a finished job on a remote AP, which needs `condor_transfer_data`.

#### Query parameters: what is honored and what is not

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

#### stdout, stderr, and a recommendation for the submit file

A container runtime interleaves the two streams and a Pod has one log. HTCondor writes two files and there is no honest way to interleave them without timestamps, so this serves `stdout`, falls back to `stderr` when `stdout` does not exist (a job that wrote only to stderr must not report as silent — that is the empty log a user will believe), and exposes `?stream=stderr` for the rest. kubectl cannot send that parameter; `kubectl get --raw` can.

**The real fix is upstream of here:** a submit file that points `error` at the same file as `output` merges the streams at the source, the way a container runtime does, and makes the whole question disappear. That is the pod controller's call.

#### exec, attach, port-forward

`501`, with a message naming what is missing. The path to implementing it is not speculative: golang-htcondor's `schedd_ssh.go` already does the `GET_JOB_CONNECT_INFO` handshake and opens an interactive channel to a running job's starter — the same handshake the log fallback uses. What is left is bridging that channel to the SPDY or WebSocket stream the API server opens, plus deciding what `kubectl exec` into a finished job should mean. Answering 501 before the upgrade is what makes kubectl print the message instead of hanging.

#### TLS, and what is not authenticated

The server requires TLS, and generates a self-signed certificate at startup when none is configured. That works because **the API server does not verify the kubelet's serving certificate unless it was started with `--kubelet-certificate-authority`** — which means in a default cluster it will proxy `kubectl logs` to whatever answers at the address in the Node's status, without checking that it is us. A real deployment should issue this certificate from the cluster CA (a CertificateSigningRequest with the `kubelet-serving` signer) and the cluster should set that flag.

In the other direction, a real kubelet authenticates the API server's client certificate and then authorizes the request with a SubjectAccessReview. This does the first only if `ClientCAFile` is set, and never the second. For a PoC that is the honest state: **anything that can reach this port, with an acceptable client certificate if one is required at all, can read any job's output.**

`/metrics/resource` and `/stats/summary` are not served, so metrics-server gets a 404 and `kubectl top pod` does not work.

### Running the tests

```bash
cd virtualnode
export GOWORK=off GOFLAGS=-mod=mod

go test ./internal/node/... ./internal/kubelet/... -count=1      # 24 unit tests
go test ./test/kubelet/... -count=1 -v                           # 17, real TLS server + real files

export KUBEBUILDER_ASSETS="$(go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use -p path)"
go test ./test/node/... -count=1 -v                              # 7, real kube-apiserver + etcd
```

`test/node` skips every test loudly when the envtest binaries are absent rather than passing an empty run.
