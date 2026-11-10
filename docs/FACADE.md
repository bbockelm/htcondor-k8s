# Approach 1: a kubectl-compatible API server over a schedd

Brian — this is the write-up for the facade half of the experiment. It assumes you know
HTCondor cold and Kubernetes well, so it skips the tutorial and goes straight at the parts
that are either load-bearing or wrong. The short version: **the job queue really can serve as
the object store, and kubectl really does work against it — but `resourceVersion` is the thing
that decides whether this is a product or a demo, and right now it is a demo.**

Everything claimed below as "verified" was run against a real `condor_master` from
`htcondor-build/release_dir` and the real kubectl 1.26 at `~/.local/bin/kubectl`. Everything
else is marked.

---

## 1. Architecture

```
kubectl ──HTTP──▶ internal/apiserver   REST, discovery, Table printing, admission
                        │   ▲
                 reads  │   │ writes (submit / condor_rm)
                        ▼   │
                  internal/store       resourceVersion, watch fan-out, replay buffer
                        ▲
                        │ snapshots / deltas
                  internal/source      JobEventSource: Poll | HTCondorDB
                        │
                  internal/backend     Backend: Schedd | Fake
                        │
                 golang-htcondor ──CEDAR──▶ schedd
```

Four decisions are worth defending.

**Reads come from the store, not from the schedd.** This is not a cache for speed. A `list`
and the `watch` that follows it have to agree about what the world looked like at one
`resourceVersion`; if they come from two different queries, an object that changes in between
is either reported twice or lost forever, and the lost case is invisible until an informer has
been running for a week. `Store.Snapshot` returns records *and* the version they were taken
at, and `Store.Watch(&that)` replays from exactly there. The price is that a list is as fresh
as the last source tick.

**Writes inject their own result synchronously.** `submitAndRefresh` submits, immediately
queries the new cluster, and upserts it into the store before writing the response. Without
that, `kubectl create && kubectl get` races the poll interval — the kind of thing that works
in a demo and breaks every script.

**The event source is an interface with two implementations.** `source.Poll` re-queries the
whole queue on a timer and hands the store a snapshot; the store diffs it. There is
deliberately no incremental path in the poller, because a delta stream derived from polling is
a delta stream with gaps. `source.HTCondorDB` follows htcondordb's change feed. Both are
tested; neither is a stub (§6).

**`translate.Admit` runs before anything touches the schedd.** The facade's value over "just
run condor_submit" is almost entirely that the rejection is synchronous, in the user's
terminal, with field paths. Verified output for a Pod with `restartPolicy: Always` and
`hostNetwork: true`:

```
The Pod "live" is invalid:
* spec: hostNetwork/hostPID/hostIPC share the execute node with other jobs
* spec.restartPolicy: restartPolicy Always describes a service, not a batch job; use Never or OnFailure
```

### Object model

| Kubernetes | HTCondor |
|---|---|
| Pod | one proc (`cluster.proc`) |
| Job | one cluster; its Pods are the procs |
| Pod name | `K8sPodName`, or `K8sPodName-<ProcId>` when `K8sOwnerKind` is set |
| Pod UID | `K8sPodUID`, or `<K8sOwnerUID>-<ProcId>` for a Job's procs |
| Pod spec | `K8sPodSpec`, the whole PodSpec as JSON — **verified to round-trip through a real schedd** |
| Job spec | `K8sJobSpec` (facade-local; see §8) |
| delete | `condor_rm`; `JobStatus == 3` is filtered out of the store, so a removal reads as a deletion |
| logs | `K8sSpoolDir` is the job's `initialdir`; `stdout`/`stderr` land there via `stream_output` |

The `foo-0` ambiguity is real and left in: a standalone Pod literally named `sweep-0` is
indistinguishable from proc 0 of Job `sweep`. `podByName` resolves it by preferring an exact
match on a standalone Pod, which is the less surprising of the two answers. A real fix needs
either a facade-minted name attribute or a per-proc `K8sPodName`.

---

## 2. resourceVersion — the hard part

### What it is

`store.Version{Gen, Seq}`, rendered `"<gen>.<seq>"`. Never `""` and never `"0"`, both of which
`ListOptions.ResourceVersion` reserves to mean something else.

- **`Seq`** is a facade-assigned counter, bumped once per *observed* change. "Observed" means
  the FNV-1a hash of the decoded `JobRecord` changed. That hash is over exactly the fields the
  facade surfaces, which is the property that matters: a job ad churns constantly
  (`ImageSize`, `DiskUsage`, schedd bookkeeping) and emitting a MODIFIED for each would turn
  every informer into a hot loop, while missing a `JobStatus` transition would lose a Pod
  phase change permanently.
- **`Gen`** changes whenever the sequence space restarts. Ordering compares `Gen` first, so a
  version from an older generation is always older — sound only because a new generation is
  always *larger*, which is the one invariant every `JobEventSource` must uphold.

A watch resuming across a generation boundary gets **410 Gone**, which is exactly what the
Kubernetes contract wants when a server can no longer honor a cursor. The client re-lists.
Nothing silently misses an event. The same 410 fires when the version predates the replay
buffer's floor (4096 events) or when the backlog alone would overrun a watcher.

### Why not the schedd's job-queue log sequence

It was the first candidate and it is the wrong one for this layer. The byte offset into
`job_queue.log` *is* monotonic within a generation, and `classadlog.Prober` can even detect
rotation. But it requires the facade to read SPOOL directly (so, co-location plus file
permissions), it resets on every schedd log rotation, and — the real objection — it indexes
*transactions*, not *facade-visible changes*. Most transactions change nothing the facade
surfaces, so every rotation of a busy queue would hand clients thousands of versions that mean
nothing. It would also tie the RV to one schedd forever.

### Why not htcondordb's watch cursor

I looked at this properly, because it is the obvious candidate and because it is what the
framing suggested. It cannot be a `resourceVersion`, for four independent reasons:

1. **It is a vector clock, not a scalar.** `collections/watch.go:encodeCursor` emits
   `[epoch u64][nShards u64][seq_0 … seq_n]`, and a mutable table defaults to 16 shards. A
   `jobs` cursor is 144 bytes with no total order. `resourceVersionMatch=NotOlderThan` and
   every "is this newer" comparison become unanswerable.
2. **The epoch is per-process random for mutable tables.** `watchepoch.go:40` is explicit:
   only append-only collections claim a persistent epoch, because a mutable collection's
   delete journal is in memory. `jobs` is mutable. **Every restart of the htcondordb daemon
   invalidates every cursor.** If you handed the raw cursor out as a `resourceVersion`, a
   routine daemon restart would become a fleet-wide 410 storm.
3. **There is no "cursor too old" error.** A refused cursor silently degrades to
   `WatchReset` + a full replay. You have to detect that yourself.
4. **The buffer is 1024 events per watcher**, and htcondordb never raises it. A
   `job_queue.log` compaction triggers `reconcileReload`, which rewrites every key — a burst
   that will overflow that buffer and produce a `WatchResync`.

So the cursor stays private to `source.HTCondorDB` and is used only to resume *its own*
stream. The mapping into store operations is where the honesty lives:

- the **first** reset/replay/synced sequence becomes a `Replace` (store is empty; the diff is
  exactly the right ADDED set);
- any **later** reset becomes a `Resync` into a **new generation** — a reset means the server
  could not replay what happened since our cursor, so the snapshot is authoritative about the
  present but says nothing about transitions in between, and a watcher resuming across that
  gap would be told it had seen everything when it had not;
- a `resync` ends the session and this source reconnects from its last cursor **without
  telling the store anything**, because nothing is known to be lost until the reconnect says
  so.

### The limits, stated plainly

- **`Gen` is `time.Now().UnixMilli()` at process start.** There is no persistent state, and
  the clock is the only thing available that reliably increases. **Every facade restart
  invalidates every open watch.** Clients recover (they re-list), but it is not free, and a
  restart loop would be a thundering herd.
- **One replica only.** Two facades in front of one schedd mint independent sequences. There
  is no shared counter. HA means a shared store, which means you have rebuilt etcd — see §7,
  because this is the single strongest argument for the operator.
- **No `409 Conflict`,** because there are no updates: every mutating path is create or
  delete. The optimistic-concurrency half of the `resourceVersion` contract is simply
  unimplemented, not approximated.
- **No watch bookmarks.** A quiet watch never advances the client's version, so a client that
  reconnects after a long idle period re-lists.
- **The replay buffer is in memory and bounded at 4096 events.** A client away for longer than
  that gets a 410 rather than a hole.
- **Latency is the source's, not the store's.** With `--source=poll` a change surfaces within
  one `--poll-interval` (default 2s). With htcondordb it is the mirror's tailer lag, ~200 ms
  busy / ≤2 s idle.

---

## 3. Namespaces — a naming convention, not a boundary

A Kubernetes namespace becomes `+K8sPodNamespace`, and every constraint the facade builds
includes it. That gives you: `kubectl get pods -n research` lists only that namespace; names
are unique per namespace; `-A` works; namespaces are synthesized from the jobs present so
`kubectl describe`/`apply` do not 404 on a namespace that "does not exist".

It does **not** give you isolation, and the gaps are security-relevant:

- **There is no authentication and no authorization.** None. Anyone who can reach the HTTP
  port can create, delete and read in every namespace. `--listen` defaults to `127.0.0.1`
  for that reason and that reason only.
- **Every namespace maps to one HTCondor identity** — whatever the facade process is. Two
  namespaces are two strings in a ClassAd, not two users, so they share a fair-share, a quota
  and an accounting group.
- **The attribute is not protected.** Any user who can `condor_submit` to the same schedd can
  set `+K8sPodName` and `+K8sPodNamespace` by hand and inject an object into any namespace the
  facade serves — or read one, since `K8sPodSpec` is just a job-ad attribute that
  `condor_q -l` will print.
- **`kubectl delete pod` is `condor_rm`** as the facade's identity, so namespace-scoped
  deletion is only as scoped as the constraint.

Making this real means per-namespace identity mapping (namespace → pool user, probably through
the same machinery `webapi` uses) plus authentication on the HTTP side. That is a project, not
a flag.

---

## 4. What kubectl actually does and does not do

All "works" rows were run against the real kubectl 1.26. Point it at an **empty kubeconfig** —
see §5.

### Works

| | Notes |
|---|---|
| `kubectl version`, `api-resources`, `api-versions` | Discovery is asserted in unit tests through client-go's own `RESTMapper` + `ShortcutExpander`, so `po`, `pod`, `jobs.batch` all resolve |
| `get pods` / `get jobs` | Server-side **Table** printing: NAME READY STATUS RESTARTS AGE |
| `get -o wide` | Adds NODE, plus **JOB** (`cluster.proc`) and **OWNER** (the HTCondor user) |
| `get -o yaml` / `-o json` | Round-trips the PodSpec out of `K8sPodSpec` |
| `get -A`, `-n <ns>` | |
| `get -w` | List-then-watch; verified streaming ADDED/MODIFIED/DELETED as Tables |
| `get -l app=demo` | Label selectors |
| `--field-selector` | `metadata.name`, `metadata.namespace`, `status.phase`, `spec.nodeName`. **Anything else is a 400**, not a silently dropped clause |
| `create -f` | |
| `apply -f` (new object) | |
| `delete pod/job` | Deleting a Job removes every proc — one `condor_rm`, no orphan window |
| `describe pod` | |
| `logs`, `logs --tail=N`, `logs -f` | |
| informers / client-go `Watch()` | Asserted through client-go's own `StreamWatcher` |

### Does not

| | Why |
|---|---|
| `apply` on an **existing** object, `patch`, `edit` | 405 with an explicit message. A queued job's image, command and resource request are already baked into the match; accepting the patch and running the old thing is worse |
| `scale`, `rollout` | No controller owns the object |
| `exec`, `attach`, `port-forward`, `cp` | golang-htcondor has `schedd_ssh.go` and this is the obvious next thing to wire up — it is not a dead end, just unwritten |
| `top` | No metrics API |
| `explain` | The OpenAPI document is deliberately empty (§6) |
| `get events` | Always empty; serving the endpoint only stops `describe` ending in an error |
| `deletecollection`, server-side apply, `--field-selector` on unindexed fields, pagination (`limit`/`continue` are ignored) | Unimplemented |
| protobuf content negotiation | JSON only; client-go falls back without complaint |
| Everything that is not a Pod or a Job | Deployments, Services, ConfigMaps, Secrets, PVCs. Most have no HTCondor meaning; `Secret`/`ConfigMap` have an obvious one and are the next useful resources |

---

## 5. Running it

```bash
go build ./cmd/htck8s-apiserver

./htck8s-apiserver \
  --collector=cm.example.org:9618 \
  --schedd=ap1.example.org \
  --spool-root=/var/lib/htck8s \
  --source=poll --poll-interval=2s \
  --listen=127.0.0.1:8080
```

`--schedd-address=<sinful>` skips the collector. `--max-cpu`, `--max-memory`, `--max-disk`,
`--gpu-resources` and `--node-selector-attrs=zone=Zone,gpu=GPUsModel` configure
`translate.Capabilities`, i.e. what admission accepts.

For the htcondordb source, enable the mirror on the AP (`HTCONDORDB_SYNC_SCHEDD = true` plus
`HTCONDORDB` in `DAEMON_LIST`, then `condor_restart -master` — a reconfig is not enough), then:

```bash
./htck8s-apiserver --collector=... --source=htcondordb \
  --htcondordb-address=/var/log/condor/.htcondordb_address
```

The address flag takes either a sinful string or the address file.

Then — and this part matters:

```bash
printf 'apiVersion: v1\nkind: Config\n' > /tmp/htck8s.kubeconfig
export KUBECONFIG=/tmp/htck8s.kubeconfig
kubectl --server=http://localhost:8080 get pods -A
```

**An empty kubeconfig is not optional.** kubectl merges your existing `~/.kube/config` with
`--server`, so it picks up a real cluster's CA and credentials, speaks TLS to a plain HTTP
port, and reports `connection refused`. That cost me a while; it will cost anyone else the
same.

`--spool-root` must be writable by the facade and readable on the same filesystem the schedd
sees, or `kubectl logs` falls back to `Schedd.PeekJobOutput` (running jobs only, bounded tail).

---

## 6. What is stubbed, and what is not

**Not stubbed — both event sources work.** `source.HTCondorDB` is tested end to end against a
real `classad/db` catalog behind a real `dbrpc` server (`internal/source/htcondordb_test.go`):
it replays pre-existing rows, streams live upserts and deletes, and filters out jobs with no
`K8sPodName`. The only thing the test does not exercise is the CEDAR handshake, which is
confined to `CedarDialer`; the test server speaks plain framed TCP. **`CedarDialer` itself has
never been run against a live htcondordb daemon** — that is the untested seam.

Genuinely stubbed or missing:

- **OpenAPI is an empty Swagger 2.0 document.** Deliberate: kubectl's client-side validator
  skips any kind it cannot find a definition for, so empty means "validate nothing here" —
  which is right, because the real validation is `translate.Admit` on the server, and a
  partial schema would reject Pods this server accepts. The cost is that `kubectl explain` has
  nothing to say.
- **Events are always an empty list.**
- **Namespaces are synthesized and read-only.**
- **No pagination.** `?limit=` is ignored; a large queue returns everything in one response.
- **`logs -f` over Peek sends one tail and stops**, because `PeekJobOutput` has no cursor on
  that path and following it would reprint the same lines forever. Following works properly
  via the spool file.
- **A finished job on a *remote* AP has no log path.** The spool file is on the other machine
  and Peek only works while running. `condor_transfer_data` is the missing piece.
- **`.spec.parallelism < .spec.completions` is rejected** rather than throttled (that is
  `shared/`'s call, and it is the right one).
- **`.spec.suspend: true` submits on hold**, but there is no resume path through the API.
- **No TLS, no authn, no authz, no quota, no rate limiting.**

---

## 7. Facade vs. CRD + webhook operator — what I actually learned

I was expecting to conclude "facade for standalone pools, operator for clusters". After
building it I think that is right, but for a sharper reason than I expected, and the
cost/benefit is more lopsided than it looks from a design doc.

### Where the facade genuinely wins

**No Kubernetes required.** This is the whole argument. It is one binary next to a schedd. If
a site has HTCondor and no cluster — which is most HTCondor sites — the operator approach is
not available to them at any price. The facade gives them `kubectl`, informers, and the client
ecosystem against the AP they already run.

**The job queue really is an adequate object store.** I did not expect this to hold up and it
does. `K8sPodSpec` round-trips through a real C++ schedd, verified; `kubectl get pod -o yaml`
returns the spec that was submitted. There is no second database, no reconciliation loop, and
therefore no drift between a CR and the job it is supposed to represent — which is the most
common way an operator goes wrong in production.

**Deletion is atomic.** `kubectl delete job` is one `condor_rm` over the cluster. The Job and
all its Pods disappear together; there is no window where orphaned Pods exist, and no
finalizer to get stuck. An operator has to reconcile that, and stuck finalizers are the #2 way
an operator goes wrong.

**The facade can tell the truth about HTCondor.** `kubectl get pods -o wide` shows `JOB` and
`OWNER` columns; `STATUS` shows `Held` and `Idle` rather than flattening both into `Pending`.
A CRD can do this too, but only in its own `status`, and `kubectl get pods` in a real cluster
will still show you the kubelet's view.

### Where the facade loses, concretely

**You are reimplementing the API server, and the long tail is the whole job.** This PoC is
~3,600 lines of non-test Go for *two resources and five verbs*, and that number is
misleading because the hard part was not the resources. It was the protocol:

- Server-side **Table** printing is not optional. Without it `kubectl get pods` prints
  `NAME AGE` and nothing else — you cannot see whether anything is running. That is ~130 lines
  of columns you have to know to write.
- **OpenAPI is mandatory for `kubectl create`**, and the failure mode is
  `Error from server (NotFound): the server could not find the requested resource` — a message
  that points nowhere near the cause.
- client-go's discovery asks for OpenAPI v2 **as protobuf with no JSON fallback**, so you need
  a `gnostic-models` document on the wire.
- And then: the `Content-Type` the *real* API server uses for it
  (`application/com.github.proto-openapi.spec.v2@v1.0+protobuf`) is **rejected by Go's own
  mime parser** inside client-go — `mime: unexpected content after media subtype` — so echoing
  the correct media type turns a valid document into an `InternalError`. Serving
  `application/octet-stream` works.

Each of those is a half-day of a message that tells you nothing. An operator inherits every one
of them for free, forever, including the ones that land in kubectl 1.34.

**`resourceVersion` is the wall, and it is not a matter of effort.** etcd hands the operator a
correct, persistent, monotonic, shared revision counter. Here it is a process-local
`uint64` seeded from the wall clock, so: a restart 410s every watcher, and two replicas cannot
exist. Making it persistent and shared means a durable, ordered, replicated counter in front of
the schedd — which is etcd, or htcondordb used as etcd, and at that point the facade's
"no second database" advantage is gone and the operator's "someone else runs this" advantage is
decisive. **If you want HA, build the operator.**

**Security is a project, not a feature.** RBAC, authentication, admission chains, quotas,
NetworkPolicy — all free in a cluster, all absent here. The namespace gap in §3 is the visible
tip; the real statement is that this PoC has no access control of any kind.

**The ecosystem assumes more API than you will implement.** Helm wants `patch` and three-way
merge. ArgoCD wants server-side apply. Informers want bookmarks and pagination. Every one of
those is a conversation about whether to implement another slice of the API server.

### The thing that surprised me

I assumed admission would be where the two approaches diverged. It is not — `shared/translate`
is genuinely shared and both get the same answer. What diverges is **everything around the
object's lifecycle**: who owns `resourceVersion`, who enforces identity, who reconciles. The
facade has to answer all three itself and can only answer the first one badly. The operator
answers none of them because Kubernetes already has.

### What I would do next

1. **Wire up `exec`** via `schedd_ssh.go`. It is the highest-value missing verb and the
   machinery exists.
2. **Per-namespace identity mapping.** Until a namespace is a pool user, the namespace story
   is a lie and the facade cannot be exposed beyond localhost.
3. **Do not chase HA.** If multi-replica comes up, that is the signal to switch to the
   operator rather than to rebuild etcd.

### Dead ends, flagged

- **htcondordb's watch cursor as a `resourceVersion`.** Four independent blockers (§2); not
  worth revisiting.
- **A facade-local counter that survives restart by persisting to disk.** It would fix the
  restart 410 but not the multi-replica problem, and the multi-replica problem is the one that
  matters. It buys a demo nicety at the cost of durability code.
- **Serving a real OpenAPI schema for Pods.** You would have to vendor the Kubernetes
  monorepo's generated definitions, and then kubectl would validate client-side against a
  schema that accepts Pods this server rejects — strictly worse than validating nothing.

---

## 8. Changes `shared/translate` would need

Reported rather than made, since `shared/` is read-only to this half.

1. **Per-proc `K8sPodName` / `K8sPodUID`.** `PodToSubmit` stamps one of each for the whole
   cluster, so a Job's procs all claim the same Pod name and UID. The facade derives
   `<name>-<proc>` and `<uid>-<proc>` locally, and the operator will have to derive the same
   thing — which means the two will drift. Stamping
   `+K8sPodUID = strcat("<uid>-", string(ProcId))` (or an explicit per-proc name field in
   `SubmitOptions`) would put it in one place.
2. **Export `classAdString`.** Both implementations need it to build `ExtraAttributes` values
   and query constraints. The facade has a ten-line copy in `internal/backend`.
3. **A place for the `batchv1.JobSpec`.** `K8sPodSpec` carries the PodSpec, but
   `.spec.completions` / `.backoffLimit` / `.completionMode` live nowhere, so
   `kubectl get job -o yaml` cannot round-trip. The facade added `K8sJobSpec`; it should be a
   shared constant, or both halves will pick different names.
4. **A place for the spool directory.** Same argument; the facade added `K8sSpoolDir`.
5. **`PodStatus` leaves `ContainerStatus.Image` empty.** The facade fills it from the spec.
   Taking the container (or the image) as a parameter would make the status complete at the
   source.
6. Cosmetic: `Capabilities.MaxMemoryBytes` is not gofmt-aligned with its neighbours.

---

## 9. Tests

| Package | Count | What it covers |
|---|---|---|
| `internal/store` | 8 | version monotonicity and round-trip, snapshot diffing, ADDED/MODIFIED/DELETED, removed-job-is-deleted, watch resume from a snapshot version, 410 across generations, 410 below the replay buffer's floor (and success *at* the floor), resync starting a new generation and expiring watchers |
| `internal/backend` | 6 | ClassAd decoding, `LastRemoteHost` fallback, ignoring non-facade jobs, removed-job visibility, malformed attributes degrading rather than failing, constraint escaping |
| `internal/apiserver` | 24 (+6 subtests) | discovery through client-go's real `RESTMapper`, create/get/list/delete through a real clientset, 422 with every rejection as a `StatusCause`, 409 on duplicates, selectors, Pod phase mapping, Job aggregation, Table printing for list/get/watch, logs from spool and via Peek |
| `internal/source` | 3 | polling convergence; htcondordb replay-then-stream and non-facade-job filtering, against a real `classad/db` server |
| `integration` (tagged) | 4 | real schedd submit + `condor_q` assertions, kubectl end-to-end, kubectl rejection message, a job that actually runs with its output served through `kubectl logs` |

Two notes on how they are built. The in-memory backend is **not a mock**: submit files go
through golang-htcondor's real submit parser and the ads it produces are the ads the facade
reads back, so the unit tests catch submit text that parses but does not say what the Pod said.
And the REST tests go through **client-go**, not `net/http`, because client-go is fussier than
a hand-written request about content types, status bodies and List shape — `TestWatch*` uses
client-go's own `StreamWatcher`, which is the code an informer runs.

One finding from the integration run worth keeping: **HTCondor's container universe is
`JobUniverse = 5` (vanilla) plus `WantContainer` and `ContainerImage`** — there is no distinct
universe number. An assertion on the universe alone passes for a plain vanilla job that ignored
the image entirely, which is exactly the bug it was meant to catch. The test now asserts all
three.
