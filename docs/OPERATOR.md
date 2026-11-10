# HTCJob operator — design note

Brian: this is approach 2 of the two-approach experiment. The code is in `operator/`, it builds and vets clean, and 46 tests pass including an envtest suite against a real `kube-apiserver` and one test against a real schedd from the golang-htcondor harness. What follows is what I built, what I learned, and where I think this approach is weaker than the facade.

Read the "What this told me about the two approaches" section first if you only read one.

---

## The object model

Three objects, three owners.

| Object | Written by | Authoritative for |
|---|---|---|
| `batch/v1 Job` | the user; webhook adds a claim | the user's intent |
| `HTCJob` (`htcondor.org/v1alpha1`) | `JobReconciler`, or a user directly | what will be submitted, and the submission's status |
| the job queue | `HTCJobReconciler` via `Submit`/`Query`/`Hold`/`Release`/`Remove` | what is actually running |

`HTCJob` is a first-class submission path, not only a mirror. `config/samples/htcjob.yaml` is applied with no Job anywhere, and runs.

### Why HTCJobSpec is not a wrapped JobSpec

The obvious thing is `spec: {jobSpec: batchv1.JobSpec}` plus a validating webhook that runs `translate.AdmitJob`. I deliberately did not do that, and the envtest suite is what makes the argument concrete rather than aesthetic.

With an embedded JobSpec, `tolerations`, `initContainers`, `podFailurePolicy`, `env.valueFrom` and `volumeMounts.subPath` are all *valid fields of a valid object*. They reach etcd. Something downstream has to notice and refuse, at a time of its choosing, with a message of its choosing. The user's feedback is a controller's opinion, delivered asynchronously.

With the fields absent from the type, the API server's own structural schema refuses them, synchronously, at `kubectl apply` time, with the field path in the error. `TestCRDRejectsFieldsItDoesNotHave` checks ten of them against a real API server:

```
spec.template.spec.initContainers        unknown field
spec.template.spec.tolerations           unknown field
spec.template.spec.containers            unknown field   (the field is `container`, singular)
spec.template.spec.volumes[0].hostPath   unknown field
spec.template.spec.container.env[0].valueFrom              unknown field
spec.template.spec.container.volumeMounts[0].subPath       unknown field
spec.template.spec.restartPolicy: Unsupported value "Always"
spec.parallelism / spec.podFailurePolicy / spec.ttlSecondsAfterFinished   unknown field
```

The specific choices worth arguing about:

- **`container`, not `containers`.** A list that may only ever hold one element is a lie told in the type system. This is the single clearest signal in the whole API that an HTCJob is not a Pod.
- **No `parallelism`.** `AdmitJob` already refuses parallelism below completions, because nothing throttles proc starts. Making it a field would mean accepting `parallelism: 1, completions: 100` and then starting 100 procs.
- **`command` is required, `env.valueFrom` does not exist.** Both are places where a Pod's optionality has no meaning on an EP: there is no image-manifest fetch at admission time, and no API server to read a Secret from inside a job.
- **`emptyDir` is a struct with no `medium`.** tmpfs would consume execute-node RAM outside the memory request, which HTCondor accounts for and evicts over.

The cost is real: every schema evolution in `batch/v1` that we want to support is now a field we add by hand, and `adapter.JobToHTCJobSpec`/`HTCJobToJobSpec` is a projection pair that can drift. `TestRoundTripPreservesEverythingAdmissionKept` pins it: an admitted JobSpec projected into an HTCJob and back must render a **byte-identical submit file**.

### Status

`HTCJobStatus` carries `clusterID`, per-proc rows (`procID`, raw `jobStatus`, mapped `phase`, `exitCode`, `holdReason`, `remoteHost`, `index`), aggregate counts, and conditions (`Admitted`, `Submitted`, `QueueReachable`, `Complete`, `Failed`).

Two deliberate choices. The raw HTCondor `jobStatus` integer is reported *alongside* the Kubernetes phase, because the mapping is lossy — a held job is neither Pending nor Failed — and anyone debugging a pool wants the number `condor_q` showed them. And `procs` is capped at 256 by the CRD, so a 100k-completion cluster cannot turn one CR into an object etcd refuses; past the cap only the counts are maintained, and the delete path falls back to a constraint-based `RemoveJobs` because the proc list is known to be short.

---

## Webhook claims, controller creates

A mutating webhook can only return a patch for the object under admission. It cannot create the HTCJob. That constraint turns out to be the right architecture anyway:

- The webhook is on the synchronous CREATE path of **every Job in the cluster**. Everything that can block — a schedd round trip, an API write, a retry — must not be there. `timeoutSeconds: 5`, `sideEffects: None`, and a handler that is a pure function of the incoming object.
- `failurePolicy: Ignore`. An operator that is down, slow or mis-certificated must not stop a cluster from running the Jobs it has always run.

**The asymmetry is the whole safety argument.** Claiming a Job is an opinion; declining to claim one is not. A Job we do not understand comes out of admission *byte-identical* — `TestHandleLeavesUnclaimedJobsByteIdentical` asserts `resp.Patches` is empty, and a mutation that forces the claim fails it. So a bug in admission costs HTCondor a job it could have run, never Kubernetes a job it could have run.

### What the claim actually writes

```
spec.template.spec.schedulerName = "htcondor"
spec.suspend                     = true
metadata.annotations["htcondor.org/claimed"]    = "true"
metadata.annotations["htcondor.org/claimed-at"] = <RFC3339>
metadata.annotations["htcondor.org/suspend"]    = <the user's own suspend intent>
```

`schedulerName` is a **fence, not a promise**. Nothing in this operator schedules Pods. The name guarantees that if a Pod is ever made from this template, kube-scheduler leaves it alone rather than placing it on a node while HTCondor is also running the work.

`suspend: true` is the actual mechanism, and this is the part of the design I like least. Without it, the built-in Job controller manufactures N Pod objects that nothing will ever schedule: they count against `ResourceQuota`, they show up Pending in `kubectl get pods` forever, and `job.status.active` counts them. Suspending the Job makes zero Pods, which is honest. But `.spec.suspend` is a user-meaningful field and we have taken it over.

The consequence is an **asymmetric resume**: writing `spec.suspend: false` is read by the webhook on UPDATE as a resume request (it records `htcondor.org/suspend: "false"` and forces the field back to true, which propagates to the HTCJob and releases the procs). Re-suspending has to go through the annotation, because once we hold the field at `true` we cannot distinguish "the user set true" from "we set true". `TestSuspendRoundTrip` covers the resume direction end to end against a real API server.

I do not think this is fixable within the claim model. It is a direct cost of writing to an object somebody else owns.

### UPDATE handling

`Decide` refuses to claim an **unclaimed** Job on UPDATE, no matter how well it translates. By then the Job may already have running Pods under the default scheduler, and claiming it would run the same work twice. An already-claimed Job is re-claimed on every update, which is how a hand-edit that tried to clear `schedulerName` or unsuspend gets reverted.

`rules.resources` is `["jobs"]` and never `["jobs/status"]` — otherwise the operator's own status writes would re-enter its webhook on every poll.

### Scoping

The shipped `namespaceSelector` only excludes `kube-system`, `kube-node-lease` and `htcondor-system`. There is a commented opt-in selector (`htcondor.org/claim-jobs: "true"`), and **I think that should be the default on any shared cluster.** A Job silently moving to a different compute pool is a surprise, and the blast radius of a wrong `Capabilities` profile is otherwise cluster-wide.

---

## Submission idempotency

The hazard: a reconcile that submits and dies before writing `.status.clusterID` leaves jobs running that nothing points at, and the next reconcile — seeing an empty `clusterID` — submits them again.

The fix is to make the queue itself the record. Every submit stamps `+K8sOwnerUID` with the CR's UID, assigned by the API server before the controller ever saw the object, never changed, never reused for a recreated object of the same name. So `ensureSubmitted` is:

1. `Query(K8sOwnerUID == "<uid>")`. Non-empty → adopt that cluster, record the id, done.
2. Otherwise `Submit`.
3. Record `clusterID` as its own status write before anything else.

A submit whose outcome is *unknown* — the connection dropped during the commit — resolves the same way on the next pass: a committed qmgmt transaction is visible to the very next query and an aborted one left nothing behind. There is no window in which both answers are possible.

A failed submit returns `RequeueAfter`, deliberately **not** an error. An error return retries with controller-runtime's backoff and re-enters `Reconcile`, which is fine — but the point is that the retry must go through the query, not straight back to `Submit`. `TestHTCJobReconcilerDoesNotResubmitAfterALostSubmitResult` simulates exactly this (the fake queues the cluster and *then* reports an error), and a mutation disabling the adoption branch makes it fail with 2 submissions.

The finalizer goes on in a pass of its own, before anything is submitted. Added in the same pass as the submit, a crash in between would leave a cluster that nothing is obliged to clean up: deleting the CR would succeed immediately and the jobs would run on, orphaned.

**The remaining hole is leader election.** Two managers without it would each run the owner-UID query first, so the duplicate window is only between one controller's query and its commit — narrow, but real. `config/manager/manager.yaml` passes `-leader-elect`.

---

## Status flow

```
schedd  --Query(ClusterId == N)-->  []classad.ClassAd
        --condor.ParseJobAd-->      []translate.JobAd
        --translate.JobStatus-->    counts + Complete/Failed
        --translate.PodStatus-->    per-proc phase (incl. the hold cases)
        -->                         HTCJob.status
        -->                         Job.status   (contested; see below)
```

There is no watch on a job queue, so the whole freshness story is a poll (`-poll-interval`, default 15s) that stops once every proc is terminal. `.status.lastQueueSync` is there so that stale is distinguishable from wrong.

`condor.Projection` deliberately omits `K8sPodSpec`. A job ad carries the entire admitted PodSpec as JSON, and pulling it on every status poll of every job would make the poll cost scale with the size of pod templates rather than the number of jobs.

### Mirroring onto the Job's status does not really work

This is the biggest thing envtest taught me, and it is a point **against** this approach.

In a real cluster, `kube-controller-manager`'s Job controller owns `job.status` and recomputes it from Pods — of which a claimed Job has none. Two writers, and ours is not the one Kubernetes believes. `config/manager/manager.yaml` therefore ships `-mirror-job-status=false`, with the HTCJob as the authoritative status. **envtest cannot show this fight**, because envtest runs only the API server and etcd; the mirroring test there passes for a reason that will not hold in production, and I have said so in the suite's package comment.

Worse, even writing a *valid* Job status is a moving target. The first attempt was rejected:

```
Job.batch "claimed" is invalid: [status.completionTime: Required value: completionTime is
required for Complete jobs, status.conditions: Invalid value: cannot set Complete=True
condition without the SuccessCriteriaMet=true condition]
```

`SuccessCriteriaMet` and `FailureTarget` are *interim* conditions the built-in Job controller sets on its way through its own state machine. Anything writing a Job status has to reproduce the whole sequence, and that sequence is version-coupled to upstream Kubernetes. That is a standing maintenance cost with no upside, paid only to make `kubectl get job` print something.

---

## Failure modes

**Operator down.** Webhook fails open (`Ignore`), so new Jobs are simply not claimed and go to the default scheduler — correct behavior. Already-claimed Jobs sit suspended with no HTCJob, doing nothing, visibly. Existing HTCJobs stop being polled; their status goes stale but the jobs keep running in the pool, because HTCondor does not need us. On restart, the owner-UID query reattaches to every cluster. **This is the good case, and it is good because HTCondor's queue is durable state that outlives the controller.**

**Schedd unreachable.** `QueueReachable=False` with the error in the message; previous counts are retained rather than zeroed (`TestHTCJobReconcilerKeepsStaleStatusWhenTheScheddIsUnreachable`), and `lastQueueSync` stops advancing. A delete in this state **blocks on the finalizer** rather than dropping it: a stuck delete is visible, an orphaned cluster is not.

**Capabilities skew between webhook and controller.** The webhook claims with profile A, the controller refuses with profile B, and the Job sits suspended forever with a `NotAdmitted` event. Nothing detects this. It is a real operational trap — both must be configured from the same flags, which today means "the same process", but would not survive splitting the webhook out.

**A hand-forged claim annotation.** The Job controller re-runs admission on every claimed Job rather than trusting the annotation, so a restored-from-backup or hand-edited Job that was never really admitted gets an event, not a submission.

---

## The three-way reconciliation hazard

Job, HTCJob and job queue can each change independently. What I did:

- **Job → HTCJob** is one-directional and total. The whole spec is reassigned on every pass (not patched), so a change to the projection is picked up on the next reconcile rather than only for new objects. `.spec.template` of a Job is immutable anyway, so in practice only suspend moves.
- **HTCJob → queue** converges from the *queue's own state*, not from a flag in status. `reconcileSuspend` holds only procs that are idle/running and releases only procs held with `HoldReasonCode == 1`. A hold the *system* placed — failed image pull, memory overrun — is a fact about the job, and releasing it would just put it back on hold. A `condor_hold` placed by hand is converged back rather than fought over blindly.
- **Queue → HTCJob → Job** is recomputed from scratch every poll, never applied as a delta. That is why a status-update conflict is swallowed and retried rather than treated as an error.
- **Deletion** is one chain: `kubectl delete job` → GC deletes the owned HTCJob → its finalizer removes the cluster. One owner reference, one finalizer, one direction.

### What is genuinely unhandled

**A cluster that leaves the queue between polls.** Completed jobs are reaped into history. If the poll misses the terminal transition, `applyQueueStatus` sees zero ads and marks the HTCJob `Failed` with `reason: LeftQueue` — which is a guess, and sometimes a wrong one. The fix is a history query (`condor_history` / job-epoch ads) on the owner UID; the `Queue` interface has no method for it yet. **This is the most likely source of wrong answers in production.**

**`kubectl logs` is impossible.** `-spool-root` sets a per-HTCJob `initialdir` so stdout/stderr/`job.log` land somewhere, but only if the operator Pod and the AP share a filesystem, and there is no API serving those bytes. The facade's `kubectl logs` story is strictly better and this one is a dead end without a side-channel.

**Garbage collection is untested here.** envtest has no GC controller, so `TestDeletingTheHTCJobRemovesTheCluster` deletes the HTCJob directly. The Job → HTCJob cascade is only exercised on the k3d path (`operator/hack/k3d-e2e.sh`), which could not run on this machine because Docker is not running.

---

## What this told me about the two approaches

Concretely, from building it:

**Where the CRD wins**

1. **The type system does the refusing.** Ten unsupported fields are rejected by the API server with field paths, no code running. In a facade, every one of those is a runtime check that has to be written, tested and kept in sync — and a facade that *accepts* `batch/v1` has to accept the fields structurally and reject them in logic, which is exactly the thing this avoids.
2. **Durable state, not a translation layer.** The HTCJob survives the operator; the queue survives both. An operator restart reattaches by owner UID. A facade that is down is an API that is down.
3. **Native Kubernetes affordances come free.** Owner references, finalizers, status conditions, `kubectl get htcj` printer columns, events, RBAC per resource. None of that had to be built.
4. **`HTCJob` is honest about being different.** Nobody applies a `HTCJob` expecting a Pod to appear.

**Where the facade wins, and I think it wins more than I expected**

1. **Nothing is co-owned.** This is the deepest problem with the claim model. Approach 2 writes to a `batch/v1 Job` — an object the API server defaults, the Job controller reconciles, and the user edits. Three concrete injuries from that, all found by running the code:
   - `.spec.suspend` had to be commandeered, producing an asymmetric resume that cannot be fixed.
   - `job.status` has a second writer that will win; the mirror ships **off**.
   - The API server fills in `.spec.selector` *after* mutating admission, so `translate.AdmitJob` run on the stored object rejects a Job the webhook legitimately claimed — a silent "suspended forever" failure, and the reason `adapter.NormalizeStoredJob` exists.

   A facade owns its objects end to end and has none of these.
2. **`kubectl logs`, `kubectl exec`, `kubectl describe pod`.** The facade can serve them from the job's spool directory. The operator fundamentally cannot: there are no Pods, and the log bytes are on an AP the kubelet knows nothing about.
3. **The failure surface is smaller.** A facade is one process answering requests. This is a webhook on every Job create in the cluster, plus two controllers, plus serving certificates, plus CRD versioning, plus the capabilities-skew trap.
4. **No etcd scaling question.** One CR per Job with capped per-proc status is fine at thousands; I would not want to find out at 100k-proc scale what an operator polling every cluster every 15s does to the API server. A facade reads from the queue, which is already the right store.

**A thing that surprised me.** The `HTCJob` CRD and the webhook are nearly independent products. The CRD alone — applied directly, no Job, no webhook, no mutation of anything the user owns — has essentially all of the upside and none of the co-ownership problems. Every single injury in the list above comes from the *claiming* half. If I were taking one piece of this forward, it would be the CRD plus a controller, and I would drop the Job-claiming webhook entirely, or demote it to an opt-in convenience behind a namespace label.

**Where I would not bet on either yet.** Neither approach has an answer for `activeDeadlineSeconds` semantics matching Kubernetes exactly, for secrets/configmaps reaching a job, or for the multi-schedd case. Those are shared problems and they are not small.

---

## Changes this drove in `shared/translate`

1. **`AdmitJob` keys the selector check on `.spec.manualSelector`.** The Job
   registry strategy sets `.spec.selector` and the
   `batch.kubernetes.io/controller-uid` / `job-name` template labels in
   `PrepareForCreate`, which runs *after* mutating admission. Keying on the
   presence of the selector meant the webhook saw none and claimed the Job,
   while every later look at the stored object refused it — leaving the Job
   suspended forever with a `NotAdmitted` event, which is the worst available
   failure mode: silent, and it looks like the pool rejected the work.
   `manualSelector` is the API's own flag for "the user wrote this".

   `adapter.NormalizeStoredJob` no longer touches the selector. It still strips
   the registry's bookkeeping labels, so they do not reach the job ad's
   `+K8sLabels` looking like something a user could select on.

2. **`translate.JobStatus` emits `SuccessCriteriaMet` and `FailureTarget`**
   alongside `Complete` and `Failed`. Kubernetes ≥1.33 refuses a Job status
   that sets a terminal condition without the interim one that precedes it.

Still open: a `translate.Capabilities` fingerprint the webhook could stamp on
the claim, so the controller can tell it is judging against a different pool
profile than the webhook did. Harmless while the two are one process; it would
not survive splitting the webhook out.

---

## Running it

```bash
cd operator
export GOWORK=off GOFLAGS=-mod=mod GOPRIVATE=github.com/bbockelm,github.com/PelicanPlatform

go build ./... && go vet ./...
go test ./internal/... -count=1                       # unit

export KUBEBUILDER_ASSETS="$(go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use -p path)"
go test ./test/envtest/... -count=1 -v                # real kube-apiserver + etcd

PATH=/path/to/release_dir/bin:/path/to/release_dir/sbin:$PATH \
  go test ./test/condor/... -count=1 -v               # real schedd

./hack/k3d-e2e.sh                                     # skips without Docker
```

Both integration suites skip cleanly when their dependency is absent. The CI workflow is at `operator/hack/ci/operator.yml` — **it has to be moved or symlinked to `.github/workflows/operator.yml`**, since GitHub only reads workflows from the repo root and I did not own the root. It greps for named passing tests rather than for the absence of `FAIL`.

Regenerate after changing `api/`:

```bash
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.19.0 object paths=./api/...
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.19.0 crd paths=./api/... output:crd:dir=config/crd
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.19.0 rbac:roleName=htcondor-operator paths=./internal/... output:rbac:dir=config/rbac
```
