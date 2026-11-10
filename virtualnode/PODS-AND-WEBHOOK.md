# The webhook and the pod controller

This is one half of approach 3. The other half — the Node object, its
lifecycle and the kubelet HTTP API that serves `kubectl logs` — is in
[NODE-AND-KUBELET.md](NODE-AND-KUBELET.md).

Read the first two sections if you only read two: the premise is the whole
argument for this approach, and the evidence for it is weaker than the rest of
the suite.

---

## The shape, and why it dissolves the operator's problem

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

## The premise is tested by simulation, not by the real Job controller

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
- **Proved only by simulation**: that the real Job controller reads the
  statuses we write the way this simulation does, and that it ends up with the
  right `job.status`.

That gap is the load-bearing assumption of the entire approach and it is not
closed. Closing it needs a cluster with a `kube-controller-manager` in it — a
k3d or kind script in the shape of `operator/hack/k3d-e2e.sh` — which could not
be run here because Docker is not running.

---

## The webhook writes four things

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

### The not-ready tolerations are not defensive programming

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

### `podReplacementPolicy`, and the operator's `.spec.selector` trap again

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

### `failurePolicy: Fail`, against the operator's `Ignore`

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

## The two decisions you asked for

### Pod-level `activeDeadlineSeconds`: HTCondor enforces it

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

### `restartPolicy: OnFailure`: refused, and `Never` is better anyway

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

## Crash safety

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

## Deletion, and the two things a kubelet does that nothing else would

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

### The reverse case

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

## Status accuracy

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

## Changes needed in `shared/translate`

Neither was made here; `shared/translate` exists so the three approaches cannot
drift into three different Pod/job mappings, and a local filter in
`webhook.admitForPodInterception` is visible in a way a fork is not.

### 1. `AdmitJob` should be split

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

### 2. Possibly: a shared `K8sVirtualNode`, or a documented extension point

`AttrVirtualNode` is invented in `internal/podctl` and travels through
`SubmitOptions.ExtraAttributes`, which is the sanctioned extension point and
works. If a pool is ever expected to run more than one of these integrations at
once — which `docs/DESIGN.md` gives as the reason all three share one attribute
schema — then "which front end owns this job" is a question the shared schema
should be able to answer, and it currently cannot.

---

## Known gaps

- **The premise is simulated.** Named above; it is the first thing to fix.
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

## Running it

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
