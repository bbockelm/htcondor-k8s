# htcondor-k8s: shared design

Notes for review. The per-approach notes are in [FACADE.md](FACADE.md) and
[OPERATOR.md](OPERATOR.md); this file is the part that belongs to neither.

## Why there is a shared module at all

The two approaches differ in where they sit relative to the Kubernetes API
server, but they ask the same two questions, and if they answer them
differently the comparison is worthless — we would be comparing two Pod→job
mappings, not two integration architectures. So `shared/translate` holds:

- **Admission**: can this PodSpec be modeled faithfully by one HTCondor job?
- **Translation**: if so, what submit file says so, and what does the resulting
  job ad look like read back as Kubernetes status?

It depends on `k8s.io/api` and `k8s.io/apimachinery` and nothing else. In
particular it holds **no HTCondor client code**, which is load-bearing: the
facade and the operator can be on different `classad` library versions, and a
webhook running inside a cluster does not want CEDAR linked into it.

59 unit tests pin the contract.

## Admission is a whitelist, and that is the whole argument

The one genuinely non-mechanical decision here is what to reject. The rule the
code follows:

> A Pod that HTCondor runs *differently* than Kubernetes would is worse than a
> Pod HTCondor refuses to run.

Silently dropping a field is the failure mode to design against. If a user sets
`nodeSelector: {disktype: ssd}` and we ignore it, their job runs on spinning
rust and the only evidence is a performance regression three weeks later. If we
reject it, they are annoyed for thirty seconds. So every rule in
`shared/translate/admission.go` is of the form "we know how to preserve this"
rather than "we know this is dangerous", and anything unrecognized is refused.

That cuts both ways, and it is worth being clear-eyed about the cost: the
admitted subset is *small*. In rough terms, what survives is a single-container,
non-root, no-volume, no-network, resource-explicit batch Pod. That is, exactly
an HTCondor container-universe job wearing a Kubernetes hat. Whether that subset
is big enough to be interesting to real users is the open question this PoC
exists to answer, and neither approach changes the answer — which is itself a
finding.

Concrete rejections, grouped:

| Group | Rejected | Why |
|---|---|---|
| Shape | multiple containers, init containers, ephemeral containers | there is no sidecar lifecycle to emulate, and no way to join a running job |
| Security | `runAsUser: 0`, `runAsNonRoot: false`, `privileged`, added capabilities, host namespaces, sysctls | the EP runs the job as a mapped pool user; running as somebody else is a correctness bug, not a nicety |
| Storage | PVCs, hostPath, secret/configMap/projected/downwardAPI volumes, `subPath`, memory-backed emptyDir | no cluster storage to bind; secret/configMap injection is explicitly deferred past the PoC |
| Networking | container ports, probes, `hostAliases`, `dnsConfig`, `hostname`/`subdomain` | a job has no routable address and nothing probes it |
| Scheduling | `nodeName`, affinity, tolerations, topology spread, priority classes | no matchmaking equivalent; `nodeSelector` survives only for keys the pool was explicitly told how to express |
| Lifecycle | `restartPolicy: Always`, lifecycle hooks | `Always` describes a service, not a batch job |
| Resources | missing cpu/memory, limit below request, unknown extended resources, requests above the pool's largest slot | HTCondor has no burst: exceeding a provision means eviction, not throttling |
| Identity | non-default service account, `automountServiceAccountToken` | there is no in-job Kubernetes API identity to project |

Three choices in there worth arguing about:

1. **The limit, not the request, is what we provision.** Kubernetes lets a Pod
   burst between its request and its limit; HTCondor evicts. The honest
   translation of "what will this workload actually be allowed to use" is the
   limit, so `Resources()` prefers it and refuses a limit below its own request
   rather than silently picking one.
2. **Fractional CPU rounds up.** `500m` becomes `request_cpus = 1`.
   Over-provisioning is safe; rounding down hands the job less CPU than
   Kubernetes promised it.
3. **`nodeSelector` keys are pool configuration, not a passthrough.**
   `Capabilities.NodeSelectorAttrs` maps a selector key to a machine ClassAd
   attribute. An unmapped key is a rejection. This is the one place where
   admission is genuinely site-dependent, and it is the right shape: a site with
   GPUs sets `GPUResourceNames`, a site without them rejects GPU Pods at
   admission rather than letting them sit idle forever.

## The job ad is the object store

Both approaches stamp the same custom attributes onto every job:

```
+K8sPodName, +K8sPodNamespace, +K8sPodUID
+K8sOwnerKind, +K8sOwnerName, +K8sOwnerUID
+K8sLabels, +K8sAnnotations     (JSON)
+K8sPodSpec                     (the admitted PodSpec, as JSON)
+K8sCompletionIndex             (indexed Jobs)
```

`+K8sPodSpec` is the interesting one. It makes `kubectl get pod -o yaml`
round-trip through a system with no etcd: the facade reconstructs the object by
re-decoding the attribute. For the operator it is redundant with the CR, but
keeping both approaches on one attribute schema means a pool could run both at
once and each could see the other's jobs.

Two things to check before this scales: the job ad size cost of carrying a
PodSpec on every proc (it is set at cluster level, but confirm the schedd is not
duplicating it per-proc in the queue log), and whether `+K8sLabels` as an opaque
JSON string is good enough for `labelSelector` queries or whether label
selection needs to push down into a ClassAd constraint to avoid a full scan.
Today it does not push down.

## Status mapping, and the one place Kubernetes has no word

Most of `JobStatus` → `PodPhase` is obvious. Idle→Pending, Running→Running,
Completed→Succeeded-or-Failed by exit code, Removed→Failed. Transferring-output
and Suspended both report Running, because from the user's point of view the
work has started and has not finished.

**Held has no Kubernetes equivalent**, and the mapping has to pick. A held job
is waiting for a human, which is neither Pending (nothing will happen on its
own) nor Failed (it may yet run). The rule in `HoldIsTerminal`:

- Codes 1 (`UserRequest`), 15 (`SubmittedOnHold`), 16 (`SpoolingInput`) and 21
  (`StartdHeldJob`) → **Pending**. These are states a job passes through or is
  routinely released from. 21 is the judgment call in that set: one EP held it,
  another may not.
- The classified failures → **Failed**: 12/13 (`TransferOutputError` /
  `TransferInputError`), 14 (`IwdError`), 32/33 (max transfer size), 34
  (`JobOutOfResources`), 35 (`InvalidDockerImage` — this is
  `ImagePullBackOff` by another name), 45 (`SingularityTestFailed`), 46/47
  (duration/execute exceeded), and the credential, hook, user-account and
  policy codes. The full list with the reasoning per code is in
  `shared/translate/status.go`.
- **Any other non-zero code → Failed.** A hold nobody has classified is not
  going to clear on its own, and reporting Pending forever is the worse failure
  mode — it is invisible, whereas a wrong Failed gets reported as a bug.

That default is a deliberate bet that the hold-code table will be incomplete
forever. If it proves wrong in practice the fix is to extend the table, not to
flip the default.

Signalled exits report `128+signal` in the container status, matching what
Kubernetes does, so `kubectl describe pod` reads the way people expect.

## Known gaps in the shared layer

- **`container.command` is required.** Real `condor_submit` falls back to the
  image's own entrypoint for the container universe; golang-htcondor's submit
  parser hard-requires an `executable`. Resolving the entrypoint ourselves would
  mean pulling the image manifest at admission time, which drags a registry
  client and registry credentials into the webhook. The right fix is upstream in
  golang-htcondor — relax `setExecutable` for `universe = container` — and until
  then admission rejects a Pod with no explicit `command`. This is the most
  user-visible rough edge in the whole PoC; most real images rely on their
  entrypoint.
- **`parallelism` below `completions` is rejected, not throttled.** HTCondor
  starts every idle proc of a cluster as soon as a slot matches. Honoring
  `parallelism` would mean the controller holding and releasing procs, which is
  real work. Accepting it silently would overrun the limit, so it is refused.
- **Secrets and ConfigMaps are deferred**, as planned. The natural landing spot
  is the credd — `transfer_input_files` from a per-job spool directory the
  submitter populates, with the secret never touching the job ad. Worth noting
  that this is a place where the operator has a real structural advantage: it
  runs inside the cluster with a service account that can read the Secret, while
  the facade would need its own privileged path to one.
- **Namespaces are a naming convention, not an isolation boundary.** Both
  approaches map a namespace onto `+K8sPodNamespace` and filter queries on it.
  Nothing stops a job in one namespace from reading another's spool directory,
  and nothing maps a namespace onto an HTCondor accounting group or a Unix user.
  Any real deployment has to close that, and the two approaches close it
  differently — see the per-approach notes.

## Questions I did not resolve

- Is `+K8sPodSpec` the right object store, or should the authoritative object
  live outside the queue (etcd for the operator, htcondordb for the facade) with
  the job ad carrying only the join key?
- The submit file sets `stream_output`/`stream_error` so logs are readable
  while running. That costs a shadow-starter connection per running job. At what
  job count does that stop being free?
- Nothing here maps a Kubernetes identity onto an HTCondor one. Both approaches
  currently submit as whoever the process runs as, which is fine for a PoC and
  unacceptable for anything else.
