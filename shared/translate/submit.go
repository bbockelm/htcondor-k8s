package translate

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// Custom job-ad attributes that carry the Kubernetes identity of a job. These
// are the join key between the two worlds: the facade reconstructs Pod objects
// from them, and the HTCJob controller uses them to find the job belonging to a
// CR without keeping its own index.
const (
	AttrPodName      = "K8sPodName"
	AttrPodNamespace = "K8sPodNamespace"
	AttrPodUID       = "K8sPodUID"
	AttrOwnerKind    = "K8sOwnerKind"
	AttrOwnerName    = "K8sOwnerName"
	AttrOwnerUID     = "K8sOwnerUID"
	AttrLabels       = "K8sLabels"
	AttrAnnotations  = "K8sAnnotations"
	// AttrPodSpec holds the admitted PodSpec as JSON. It is what makes
	// `kubectl get pod -o yaml` round-trip through a system that has no
	// etcd: the schedd's job queue *is* the object store.
	AttrPodSpec = "K8sPodSpec"
	// AttrIndex is the completion index of an indexed Job.
	AttrIndex = "K8sCompletionIndex"
	// AttrJobSpec holds the batch/v1 JobSpec as JSON. The PodSpec alone
	// cannot reconstruct a Job: completions, backoffLimit and
	// completionMode live above it.
	AttrJobSpec = "K8sJobSpec"
	// AttrSpoolDir is the per-object directory holding stdout, stderr and
	// the user log --- what `kubectl logs` reads.
	AttrSpoolDir = "K8sSpoolDir"
)

// ProcPodName and ProcPodUID give the per-proc Pod identity of a multi-proc
// cluster.
//
// One submission produces one HTCondor cluster but N Pods, and each needs its
// own name and UID. Both front ends have to derive the same ones or a Pod will
// have two identities depending on who is looking, so the derivation lives
// here rather than in each of them. The submit file emits the same values via
// $(ProcId), which keeps the queue authoritative.
func ProcPodName(base string, proc int) string { return fmt.Sprintf("%s-%d", base, proc) }

// ProcPodUID derives a per-proc UID from the cluster's UID. It is not a real
// UUID, and nothing here depends on it being one: it only has to be stable and
// unique within the cluster.
func ProcPodUID(base string, proc int) string { return fmt.Sprintf("%s-%d", base, proc) }

// Identity names the Kubernetes object a job stands for.
type Identity struct {
	Name      string
	Namespace string
	UID       string
	// Owner, if set, is the Job or HTCJob that created this Pod.
	OwnerKind string
	OwnerName string
	OwnerUID  string

	Labels      map[string]string
	Annotations map[string]string
}

// SubmitOptions tune submit-file generation for the surrounding deployment.
type SubmitOptions struct {
	Caps Capabilities

	// IWD is the initial working directory for the job, normally a
	// per-object spool directory the caller has created. stdout, stderr and
	// the user log land there, which is what makes `kubectl logs` possible.
	IWD string

	// Count is the number of procs to queue (a Job's .spec.completions).
	// Zero or one queues a single proc.
	Count int

	// Indexed emits $(ProcId) as JOB_COMPLETION_INDEX, matching a batch/v1
	// Job with completionMode: Indexed.
	Indexed bool

	// MaxRetries maps a Job's backoffLimit onto HTCondor's retry support.
	MaxRetries int

	// ActiveDeadlineSeconds, if positive, becomes a periodic_remove on
	// wall-clock runtime.
	ActiveDeadlineSeconds int64

	// MergeOutput points error at the same file as output, so the job's
	// two streams interleave into one.
	//
	// This is what a container actually looks like: a Pod has one log per
	// container, stdout and stderr already merged, and `kubectl logs` has
	// no way to ask for one of them. Serving them separately means either
	// picking one and hiding the other, or inventing a parameter no
	// Kubernetes client will send. HTCondor writes two files by default,
	// and there is no honest way to interleave them afterwards -- the
	// ordering is gone by then.
	//
	// It is off by default because the facade exposes the two streams
	// separately and would lose that by merging.
	MergeOutput bool

	// Suspend submits the job on hold, matching a batch/v1 Job with
	// .spec.suspend: true.
	Suspend bool

	// JobSpecJSON, when set, is recorded as AttrJobSpec. A batch/v1 Job
	// cannot be reconstructed from its PodSpec alone.
	JobSpecJSON string

	// ExtraAttributes are additional +Attr = value lines, already rendered
	// as ClassAd literals. The deployment uses this for site policy.
	ExtraAttributes map[string]string
}

// PodToSubmit renders an admitted Pod as HTCondor submit-file text.
//
// It assumes Admit has already passed: anything it would have rejected is
// either ignored here or will produce a submit file that does not say what the
// Pod said. Callers must not skip admission.
func PodToSubmit(spec *corev1.PodSpec, id Identity, opts SubmitOptions) (string, error) {
	if spec == nil || len(spec.Containers) != 1 {
		return "", fmt.Errorf("translate: PodToSubmit requires exactly one container")
	}
	c := &spec.Containers[0]

	req, err := Resources(c, opts.Caps)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	w := func(format string, args ...any) {
		fmt.Fprintf(&b, format+"\n", args...)
	}

	w("# Generated by htcondor-k8s from %s/%s", id.Namespace, id.Name)
	w("universe = container")
	w("container_image = %s", escapeSubmitMacros(c.Image))

	// Kubernetes lets a container omit command and inherit the image's
	// ENTRYPOINT. An empty executable is how the container universe says
	// the same thing: both condor_submit and golang-htcondor's parser
	// accept it and produce Cmd = "", which is byte for byte the ad you
	// get from omitting the command entirely. Resolving the entrypoint
	// ourselves would mean pulling the image manifest at admission time,
	// which is a registry client and registry credentials in the webhook.
	var args []string
	if len(c.Command) > 0 {
		w("executable = %s", escapeSubmitMacros(c.Command[0]))
		args = append(args, c.Command[1:]...)
	} else {
		w("executable =")
	}
	args = append(args, c.Args...)
	if len(args) > 0 {
		w("arguments = %s", quoteArgsV2(args))
	}

	if c.WorkingDir != "" {
		w("container_target_dir = %s", escapeSubmitMacros(c.WorkingDir))
	}
	if env := quoteEnvV2(c.Env, opts.Indexed && opts.Count > 1); env != "" {
		w("environment = %s", env)
	}

	w("request_cpus = %d", req.CPUs)
	w("request_memory = %d", bytesToMB(req.MemoryBytes))
	w("request_disk = %d", bytesToKB(req.DiskBytes))
	if req.GPUs > 0 {
		w("request_gpus = %d", req.GPUs)
	}

	if opts.IWD != "" {
		w("initialdir = %s", escapeSubmitMacros(opts.IWD))
	}
	w("output = stdout")
	if opts.MergeOutput {
		w("error = stdout")
	} else {
		w("error = stderr")
	}
	w("log = job.log")
	w("should_transfer_files = YES")
	w("when_to_transfer_output = ON_EXIT")
	w("stream_output = True")
	w("stream_error = True")

	if reqs := nodeSelectorRequirements(spec.NodeSelector, opts.Caps); reqs != "" {
		w("requirements = %s", reqs)
	}

	// OnFailure is the only restart policy with a retry story; Never means
	// a failed job stays failed.
	if spec.RestartPolicy == corev1.RestartPolicyOnFailure && opts.MaxRetries > 0 {
		w("max_retries = %d", opts.MaxRetries)
		w("success_exit_code = 0")
	}
	if opts.ActiveDeadlineSeconds > 0 {
		// A Job's .spec.activeDeadlineSeconds is wall clock measured from
		// .status.startTime: it counts time spent pending, and a retry
		// does not reset it. QDate is the queue's analogue of that
		// instant. EnteredCurrentStatus would be wrong twice over --- it
		// restarts on every status transition, so a job that is held and
		// released gets a fresh deadline, and time spent idle never
		// counts against one.
		w("periodic_remove = (time() - QDate) > %d", opts.ActiveDeadlineSeconds)
	}
	if opts.Suspend {
		w("hold = True")
	}

	for _, line := range identityAttributes(spec, id, opts) {
		w("%s", line)
	}

	if opts.Count > 1 {
		if opts.Indexed {
			w("+%s = $(ProcId)", AttrIndex)
		}
		w("queue %d", opts.Count)
	} else {
		w("queue")
	}

	return b.String(), nil
}

// identityAttributes renders the +Attr lines that let either integration find
// its way back from a job ad to a Kubernetes object.
func identityAttributes(spec *corev1.PodSpec, id Identity, opts SubmitOptions) []string {
	// A cluster of more than one proc is a Job: each proc is its own Pod
	// and needs its own name and UID. $(ProcId) is expanded per proc at
	// submit time, so the ad carries the same values ProcPodName and
	// ProcPodUID compute.
	perProc := opts.Count > 1
	attrs := map[string]string{
		AttrPodName:      procLiteral(id.Name, perProc),
		AttrPodNamespace: ClassAdString(id.Namespace),
	}
	if id.UID != "" {
		attrs[AttrPodUID] = procLiteral(id.UID, perProc)
	}
	if id.OwnerKind != "" {
		attrs[AttrOwnerKind] = ClassAdString(id.OwnerKind)
		attrs[AttrOwnerName] = ClassAdString(id.OwnerName)
		attrs[AttrOwnerUID] = ClassAdString(id.OwnerUID)
	}
	if len(id.Labels) > 0 {
		attrs[AttrLabels] = ClassAdString(mustJSON(id.Labels))
	}
	if len(id.Annotations) > 0 {
		attrs[AttrAnnotations] = ClassAdString(mustJSON(id.Annotations))
	}
	if spec != nil {
		attrs[AttrPodSpec] = ClassAdString(mustJSON(spec))
	}
	if opts.IWD != "" {
		attrs[AttrSpoolDir] = ClassAdString(opts.IWD)
	}
	if opts.JobSpecJSON != "" {
		attrs[AttrJobSpec] = ClassAdString(opts.JobSpecJSON)
	}
	for k, v := range opts.ExtraAttributes {
		attrs[k] = v
	}

	out := make([]string, 0, len(attrs))
	for _, k := range sortedKeys(attrs) {
		out = append(out, fmt.Sprintf("+%s = %s", k, attrs[k]))
	}
	return out
}

// nodeSelectorRequirements renders the selector keys the pool knows as a
// matchmaking requirement. Admission has already rejected unknown keys.
func nodeSelectorRequirements(sel map[string]string, caps Capabilities) string {
	if len(sel) == 0 {
		return ""
	}
	parts := make([]string, 0, len(sel))
	for _, k := range sortedKeys(sel) {
		attr, ok := caps.NodeSelectorAttrs[k]
		if !ok {
			continue
		}
		parts = append(parts, fmt.Sprintf("(TARGET.%s == %s)", attr, ClassAdString(sel[k])))
	}
	if len(parts) == 0 {
		return ""
	}
	sort.Strings(parts)
	return strings.Join(parts, " && ")
}

// bytesToMB converts to the MB unit request_memory uses, rounding up so a job
// is never provisioned less memory than the Pod asked for.
func bytesToMB(b int64) int64 {
	const mb = 1 << 20
	n := (b + mb - 1) / mb
	if n < 1 {
		return 1
	}
	return n
}

func bytesToKB(b int64) int64 {
	const kb = 1 << 10
	n := (b + kb - 1) / kb
	if n < 1 {
		return 1
	}
	return n
}

// quoteArgsV2 renders arguments in HTCondor's "new" (V2) quoting: the whole
// list in double quotes, each argument with embedded whitespace in single
// quotes, a literal double quote doubled, a literal single quote doubled.
func quoteArgsV2(args []string) string {
	parts := make([]string, 0, len(args))
	for _, a := range args {
		parts = append(parts, quoteV2Word(a))
	}
	return `"` + strings.Join(parts, " ") + `"`
}

// quoteEnvV2 renders the environment in HTCondor's V2 format:
// environment = "ONE=1 TWO='a b'"
//
// withIndex appends JOB_COMPLETION_INDEX=$(ProcId); the submit-file macro is
// expanded before quoting is parsed, so it is safe inside the quoted list.
func quoteEnvV2(env []corev1.EnvVar, withIndex bool) string {
	if len(env) == 0 && !withIndex {
		return ""
	}
	parts := make([]string, 0, len(env)+1)
	for _, e := range env {
		// ValueFrom was rejected at admission; only literals reach here.
		// HTCondor's V2 environment format quotes the *value*, as in
		// environment = "one=1 three='spacey value'".
		parts = append(parts, escapeSubmitMacros(e.Name)+"="+quoteV2Word(e.Value))
	}
	if withIndex {
		parts = append(parts, "JOB_COMPLETION_INDEX=$(ProcId)")
	}
	return `"` + strings.Join(parts, " ") + `"`
}

func quoteV2Word(s string) string {
	// An empty word still has to occupy a place in the list. Rendered as
	// nothing it would leave two adjacent separators and the argument
	// would vanish; an empty quoted region keeps it.
	if s == "" {
		return "''"
	}

	// A literal double quote is doubled everywhere: the whole value sits
	// inside the submit command's own double quotes.
	escaped := strings.ReplaceAll(s, `"`, `""`)

	// A single quote is different. Doubling it is a literal only *inside* a
	// single-quoted region; outside one, '' opens and immediately closes an
	// empty region and the apostrophe silently disappears --- it's arrives
	// at the job as its. So a word containing one is quoted as a whole.
	if strings.ContainsAny(s, " \t\n'") {
		escaped = "'" + strings.ReplaceAll(escaped, `'`, `''`) + "'"
	}
	return escapeSubmitMacros(escaped)
}

// escapeSubmitMacros neutralizes submit-file macro expansion in text that came
// from a user.
//
// condor_submit expands macros in every command value before anything else
// looks at it, and the expansions are not limited to the submit file: $ENV(X)
// reads the *submitting process's* environment, and $(X) reads its configured
// macros. Since both front ends build submit files from Pod specs they did not
// write, an unescaped annotation reading "$ENV(TOKEN)" would hand that user
// whatever is in the server's environment. Every expansion form starts with a
// dollar sign, and $(DOLLAR) is the escape for one, so escaping the sigil
// closes all of them at once.
//
// This is applied to user-controlled text only. SubmitOptions.ExtraAttributes
// is site configuration, already rendered as ClassAd literals, and is passed
// through so a deployment can use macros deliberately.
func escapeSubmitMacros(s string) string {
	return strings.ReplaceAll(s, "$", "$(DOLLAR)")
}

// procLiteral renders a ClassAd string literal that, when perProc is set, has
// the submit file's $(ProcId) macro appended. The macro is added outside the
// escaping on purpose: it is ours, not the user's.
func procLiteral(base string, perProc bool) string {
	lit := ClassAdString(base)
	if !perProc {
		return lit
	}
	return lit[:len(lit)-1] + `-$(ProcId)"`
}

// ClassAdString renders a Go string as a ClassAd string literal, escaped so
// that neither ClassAd syntax nor submit-file macro expansion can read
// anything into it. Both front ends need it for their own custom attributes
// and query constraints.
func ClassAdString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\t':
			b.WriteString(`\t`)
		case '\r':
			b.WriteString(`\r`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return escapeSubmitMacros(b.String())
}

func mustJSON(v any) string {
	out, err := json.Marshal(v)
	if err != nil {
		// Every value we marshal here came out of the API server's own
		// decoder, so this cannot fail in practice; degrade rather than
		// take down a webhook.
		return "{}"
	}
	return string(out)
}
