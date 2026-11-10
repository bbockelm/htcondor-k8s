package translate

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// These tests check PodToSubmit's output against the only authority that
// matters: the real condor_submit. Everything else in this package is checked
// against our own reading of the submit language, which is exactly the kind of
// agreement that can be unanimously wrong. Here we hand the generated text to
// `condor_submit -dry-run` and assert on the job ad it actually produces.
//
// They skip when condor_submit is absent, so `go test ./...` stays green on a
// machine without HTCondor.

func condorSubmit(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("condor_submit")
	if err != nil {
		t.Skip("condor_submit not in PATH; skipping submit-language oracle")
	}
	return path
}

// dryRun renders the submit text through condor_submit and returns the
// resulting job ad as a flat attribute map.
func dryRun(t *testing.T, submitText string) map[string]string {
	t.Helper()
	bin := condorSubmit(t)

	dir := t.TempDir()
	subPath := filepath.Join(dir, "job.sub")
	adPath := filepath.Join(dir, "job.ad")
	require.NoError(t, os.WriteFile(subPath, []byte(submitText), 0o600))

	cmd := exec.Command(bin, "-dry-run", adPath, subPath)
	cmd.Dir = dir
	// A developer's own condor_config can carry site defaults that change
	// the generated ad (and on a laptop it often points at a real pool).
	// Pin an empty config so the oracle reflects the submit file alone.
	emptyCfg := filepath.Join(dir, "condor_config")
	require.NoError(t, os.WriteFile(emptyCfg, []byte("CONDOR_HOST=localhost\n"), 0o600))
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+emptyCfg)

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "condor_submit rejected our submit file:\n%s\n--- submit text ---\n%s", out, submitText)

	raw, err := os.ReadFile(adPath) //nolint:gosec // path is from t.TempDir
	require.NoError(t, err)

	ad := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		ad[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}
	return ad
}

func adString(t *testing.T, ad map[string]string, attr string) string {
	t.Helper()
	v, ok := ad[strings.ToLower(attr)]
	require.True(t, ok, "job ad has no %s; got attributes: %v", attr, sortedKeys(ad))
	unquoted, err := strconv.Unquote(v)
	if err != nil {
		return v
	}
	return unquoted
}

func TestOracleMinimalPodProducesAContainerJob(t *testing.T) {
	spec := goodPod()
	text, err := PodToSubmit(spec, Identity{Name: "my-pod", Namespace: "team-a", UID: "uid-1"},
		SubmitOptions{Caps: DefaultCapabilities()})
	require.NoError(t, err)

	ad := dryRun(t, text)

	assert.Equal(t, "5", ad["jobuniverse"], "container universe is 5")
	assert.Equal(t, "true", ad["wantcontainer"])
	assert.Equal(t, "docker://busybox:latest", adString(t, ad, "ContainerImage"))
	assert.Equal(t, "/bin/sh", adString(t, ad, "Cmd"))
	assert.Equal(t, "1", ad["requestcpus"])
	assert.Equal(t, "512", ad["requestmemory"], "request_memory is in MB")
	assert.Equal(t, "1048576", ad["requestdisk"], "request_disk is in KB")
}

func TestOracleIdentityAttributesSurvive(t *testing.T) {
	spec := goodPod()
	id := Identity{
		Name: "my-pod", Namespace: "team-a", UID: "uid-1",
		OwnerKind: "Job", OwnerName: "batch", OwnerUID: "uid-owner",
		Labels: map[string]string{"app": "demo", "tier": "back end"},
	}
	ad := dryRun(t, mustSubmit(t, spec, id, SubmitOptions{Caps: DefaultCapabilities()}))

	assert.Equal(t, "my-pod", adString(t, ad, AttrPodName))
	assert.Equal(t, "team-a", adString(t, ad, AttrPodNamespace))
	assert.Equal(t, "uid-1", adString(t, ad, AttrPodUID))
	assert.Equal(t, "Job", adString(t, ad, AttrOwnerKind))
	assert.Equal(t, "uid-owner", adString(t, ad, AttrOwnerUID))
	assert.JSONEq(t, `{"app":"demo","tier":"back end"}`, adString(t, ad, AttrLabels))
}

// The PodSpec round trip is the facade's entire object store. If condor_submit
// mangles the escaping of that attribute, `kubectl get pod -o yaml` returns
// garbage, so check it against the real parser rather than only our own.
func TestOraclePodSpecRoundTrip(t *testing.T) {
	spec := goodPod()
	spec.Containers[0].Env = []corev1.EnvVar{{Name: "QUOTED", Value: `a "quoted" value`}}

	ad := dryRun(t, mustSubmit(t, spec, Identity{Name: "p", Namespace: "n"},
		SubmitOptions{Caps: DefaultCapabilities()}))

	var back corev1.PodSpec
	require.NoError(t, jsonUnmarshalString(adString(t, ad, AttrPodSpec), &back))
	require.Len(t, back.Containers, 1)
	assert.Equal(t, spec.Containers[0].Image, back.Containers[0].Image)
	assert.Equal(t, spec.Containers[0].Command, back.Containers[0].Command)
	require.Len(t, back.Containers[0].Env, 1)
	assert.Equal(t, `a "quoted" value`, back.Containers[0].Env[0].Value)
}

// decodeV2 is HTCondor's "new" argument/environment quoting, read back: the
// value is a whitespace-separated list in which a single-quoted region
// protects whitespace, and a doubled single quote is a literal one only while
// inside such a region.
//
// Having the test decode rather than string-match is the whole point of the
// oracle. condor_submit re-encodes into its own canonical form --- a space
// inside a word comes back as two' 'words, not 'two words' --- so comparing
// text would only tell us whether we guessed its formatter, when the question
// is whether the job will receive the argv the Pod asked for.
func decodeV2(s string) []string {
	var out []string
	var cur strings.Builder
	started := false
	inQuote := false

	flush := func() {
		if started {
			out = append(out, cur.String())
			cur.Reset()
			started = false
		}
	}

	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inQuote && c == '\'':
			// Inside a quoted region a doubled quote is a literal
			// one; a lone quote ends the region.
			if i+1 < len(s) && s[i+1] == '\'' {
				cur.WriteByte('\'')
				i++
			} else {
				inQuote = false
			}
		case inQuote:
			cur.WriteByte(c)
		case c == '\'':
			inQuote = true
			started = true
		case c == ' ' || c == '\t':
			flush()
		default:
			cur.WriteByte(c)
			started = true
		}
	}
	flush()
	return out
}

func TestDecodeV2(t *testing.T) {
	// Pin the decoder itself, so a failure in the oracle tests below points
	// at the translation rather than at this helper.
	assert.Equal(t, []string{"one", "two words", "three"}, decodeV2("one two' 'words three"))
	assert.Equal(t, []string{"a b"}, decodeV2("'a b'"))
	assert.Empty(t, decodeV2(""))

	// A doubled single quote is a literal one only inside a quoted region.
	// Outside one it opens and immediately closes an empty region and
	// disappears, which is exactly how an apostrophe used to get eaten on
	// its way to the job. condor_submit's own canonical encoding of it's is
	// it''''s: quote, doubled literal, unquote.
	assert.Equal(t, []string{"its"}, decodeV2("it''s"))
	assert.Equal(t, []string{"it's"}, decodeV2("it''''s"))
	assert.Equal(t, []string{"it's"}, decodeV2("'it''s'"))
}

// The job must receive the argv the Pod asked for, byte for byte. Word
// boundaries are the thing that silently breaks: an argument containing a
// space that arrives as two arguments is a bug no unit test of our own
// formatter would catch.
func TestOracleArgumentsSurviveAsArgv(t *testing.T) {
	spec := goodPod()
	spec.Containers[0].Command = []string{"/bin/echo"}
	spec.Containers[0].Args = []string{"one", "two words", `with"quote`, "it's", "trailing "}

	ad := dryRun(t, mustSubmit(t, spec, Identity{Name: "p", Namespace: "n"},
		SubmitOptions{Caps: DefaultCapabilities()}))

	assert.Equal(t, spec.Containers[0].Args, decodeV2(adString(t, ad, "Arguments")),
		"the argv HTCondor parsed must equal the args the pod declared")
}

func TestOracleEnvironmentSurvives(t *testing.T) {
	spec := goodPod()
	spec.Containers[0].Env = []corev1.EnvVar{
		{Name: "PLAIN", Value: "x"},
		{Name: "SPACEY", Value: "a b"},
		{Name: "EMPTY", Value: ""},
		{Name: "QUOTED", Value: `say "hi"`},
		{Name: "APOSTROPHE", Value: "it's"},
	}

	ad := dryRun(t, mustSubmit(t, spec, Identity{Name: "p", Namespace: "n"},
		SubmitOptions{Caps: DefaultCapabilities()}))

	got := map[string]string{}
	for _, kv := range decodeV2(adString(t, ad, "Environment")) {
		k, v, _ := strings.Cut(kv, "=")
		got[k] = v
	}
	for _, e := range spec.Containers[0].Env {
		assert.Equal(t, e.Value, got[e.Name], "environment variable %s", e.Name)
	}
}

func TestOracleGPURequestReachesTheAd(t *testing.T) {
	caps := DefaultCapabilities()
	caps.GPUResourceNames = []string{"nvidia.com/gpu"}
	spec := goodPod()
	spec.Containers[0].Resources.Requests["nvidia.com/gpu"] = resource.MustParse("2")

	ad := dryRun(t, mustSubmit(t, spec, Identity{Name: "p", Namespace: "n"}, SubmitOptions{Caps: caps}))
	assert.Equal(t, "2", ad["requestgpus"])
}

func TestOracleNodeSelectorLandsInRequirements(t *testing.T) {
	caps := DefaultCapabilities()
	caps.NodeSelectorAttrs = map[string]string{"htcondor.org/os": "OpSysAndVer"}
	spec := goodPod()
	spec.NodeSelector = map[string]string{"htcondor.org/os": "AlmaLinux9"}

	ad := dryRun(t, mustSubmit(t, spec, Identity{Name: "p", Namespace: "n"}, SubmitOptions{Caps: caps}))
	// condor_submit ANDs our clause into the system requirements, so look
	// for it inside rather than comparing the whole expression.
	assert.Contains(t, ad["requirements"], `TARGET.OpSysAndVer == "AlmaLinux9"`)
}

func TestOracleActiveDeadlineExpressionParses(t *testing.T) {
	ad := dryRun(t, mustSubmit(t, goodPod(), Identity{Name: "p", Namespace: "n"},
		SubmitOptions{Caps: DefaultCapabilities(), ActiveDeadlineSeconds: 600}))
	assert.Contains(t, ad["periodicremove"], "600")
}

// An indexed Job must give each proc a distinct JOB_COMPLETION_INDEX. The
// $(ProcId) macro is expanded by condor_submit, so this is the check that our
// macro actually expands rather than reaching the job as a literal.
func TestOracleIndexedJobExpandsProcId(t *testing.T) {
	bin := condorSubmit(t)
	_ = bin

	text := mustSubmit(t, goodPod(), Identity{Name: "p", Namespace: "n"},
		SubmitOptions{Caps: DefaultCapabilities(), Count: 3, Indexed: true})
	ad := dryRun(t, text)

	// -dry-run writes the last proc's ad; with queue 3 that is ProcId 2.
	assert.Equal(t, "2", ad["procid"])
	assert.Contains(t, adString(t, ad, "Environment"), "JOB_COMPLETION_INDEX=2")
	assert.Equal(t, "2", ad[strings.ToLower(AttrIndex)])
}

func mustSubmit(t *testing.T, spec *corev1.PodSpec, id Identity, opts SubmitOptions) string {
	t.Helper()
	text, err := PodToSubmit(spec, id, opts)
	require.NoError(t, err)
	return text
}

func jsonUnmarshalString(s string, v any) error {
	return json.Unmarshal([]byte(s), v)
}

func TestOracleOnFailureRetriesParse(t *testing.T) {
	spec := goodPod()
	spec.RestartPolicy = corev1.RestartPolicyOnFailure

	ad := dryRun(t, mustSubmit(t, spec, Identity{Name: "p", Namespace: "n"},
		SubmitOptions{Caps: DefaultCapabilities(), MaxRetries: 3}))

	// max_retries is sugar: condor_submit expands it into the retry
	// bookkeeping attributes, so assert on what it actually produced
	// rather than on the keyword surviving.
	assert.Equal(t, "3", ad["jobmaxretries"])
	assert.Equal(t, "0", ad["jobsuccessexitcode"])
	assert.NotEmpty(t, ad["onexitremove"], "max_retries must have produced the retry policy expressions")
}

func TestOracleSuspendSubmitsOnHold(t *testing.T) {
	ad := dryRun(t, mustSubmit(t, goodPod(), Identity{Name: "p", Namespace: "n"},
		SubmitOptions{Caps: DefaultCapabilities(), Suspend: true}))

	assert.Equal(t, strconv.Itoa(JobStatusHeld), ad["jobstatus"],
		"a suspended Job must land in the queue held, not idle")
}

// Kubernetes names are tame, but labels and annotations are not: they carry
// arbitrary user text into a ClassAd string literal. If the escaping is wrong
// the submit file does not parse, or worse, parses into something else.
func TestOracleHostileTextSurvivesEscaping(t *testing.T) {
	hostile := `quote" backslash\ newline` + "\n" + `tab	end`
	id := Identity{
		Name: "p", Namespace: "n",
		Annotations: map[string]string{"note": hostile},
	}
	ad := dryRun(t, mustSubmit(t, goodPod(), id, SubmitOptions{Caps: DefaultCapabilities()}))

	var back map[string]string
	require.NoError(t, jsonUnmarshalString(adString(t, ad, AttrAnnotations), &back))
	assert.Equal(t, hostile, back["note"])
}
