package translate

import (
	"strings"
	"testing"
	"unicode"

	corev1 "k8s.io/api/core/v1"
)

// FuzzArgumentRoundTrip is the differential version of the oracle tests: for
// an arbitrary argument list, the argv HTCondor parses out of our submit file
// must equal the argv the Pod declared.
//
// The apostrophe bug that started this file is exactly the shape a fuzzer
// finds and a table of examples misses --- the table had quotes and spaces in
// it, because those are the characters you think of, and no apostrophes.
//
// Run it with:
//
//	go test ./translate -run XXX -fuzz FuzzArgumentRoundTrip -fuzztime 60s
//
// Each execution shells out to condor_submit, so throughput is a few hundred
// per minute, not millions. That is still plenty to cover the quoting
// alphabet, which is what matters here.
func FuzzArgumentRoundTrip(f *testing.F) {
	seeds := []string{
		"plain",
		"two words",
		"it's",
		`say "hi"`,
		`back\slash`,
		"'leading quote",
		"trailing quote'",
		"''",
		"'",
		`"`,
		"a'b'c",
		" leading space",
		"trailing space ",
		"tab\there",
		"$(ProcId)",
		"$$(Memory)",
		"semi;colon",
		"dollar$sign",
		"equals=sign",
		"#hash",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, arg string) {
		if !submitSafe(arg) {
			t.Skip("argument is outside the alphabet a submit file can carry on one line")
		}
		if strings.Contains(arg, `\`) {
			// Not a translation bug but a limit of the measurement:
			// HTCondor's old ClassAd output prints a lone backslash
			// unescaped, so a printed literal cannot distinguish one
			// backslash from two and the comparison is meaningless.
			// TestOracleBackslashIsAmbiguousOnTheWayOut pins that.
			t.Skip("backslash is not round-trippable through printed ClassAd text")
		}

		spec := goodPod()
		spec.Containers[0].Command = []string{"/bin/echo"}
		spec.Containers[0].Args = []string{"before", arg, "after"}

		// Input the pool would refuse is not a translation bug. Skipping
		// it here is what keeps admission and translation honest about
		// each other: anything Admit accepts must round-trip.
		if !Admit(spec, nil, DefaultCapabilities()).OK() {
			t.Skip("pod would be rejected at admission")
		}

		text, err := PodToSubmit(spec, Identity{Name: "p", Namespace: "n"},
			SubmitOptions{Caps: DefaultCapabilities()})
		if err != nil {
			t.Fatalf("PodToSubmit: %v", err)
		}

		ad := dryRun(t, text)
		got := decodeV2(adString(t, ad, "Arguments"))

		want := []string{"before", arg, "after"}
		if len(got) != len(want) {
			t.Fatalf("argv length changed: pod declared %q, HTCondor parsed %q", want, got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("argv[%d]: pod declared %q, HTCondor parsed %q (full: %q)",
					i, want[i], got[i], got)
			}
		}
	})
}

// FuzzEnvironmentRoundTrip is the same property for environment values, which
// go through the same V2 quoting but are split on '=' as well as whitespace.
func FuzzEnvironmentRoundTrip(f *testing.F) {
	for _, s := range []string{"plain", "a b", "it's", `q"uote`, "", "=leading-equals", "semi;colon"} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, value string) {
		if !submitSafe(value) {
			t.Skip("value is outside the alphabet a submit file can carry on one line")
		}
		if strings.Contains(value, `\`) {
			// Same measurement limit as the argument fuzzer: a printed
			// ClassAd literal cannot distinguish one backslash from two.
			t.Skip("backslash is not round-trippable through printed ClassAd text")
		}

		spec := goodPod()
		spec.Containers[0].Env = []corev1.EnvVar{
			{Name: "BEFORE", Value: "x"},
			{Name: "SUBJECT", Value: value},
			{Name: "AFTER", Value: "y"},
		}

		if !Admit(spec, nil, DefaultCapabilities()).OK() {
			t.Skip("pod would be rejected at admission")
		}

		text, err := PodToSubmit(spec, Identity{Name: "p", Namespace: "n"},
			SubmitOptions{Caps: DefaultCapabilities()})
		if err != nil {
			t.Fatalf("PodToSubmit: %v", err)
		}

		got := map[string]string{}
		for _, kv := range decodeV2(adString(t, dryRun(t, text), "Environment")) {
			k, v, _ := strings.Cut(kv, "=")
			got[k] = v
		}
		if got["SUBJECT"] != value {
			t.Fatalf("env SUBJECT: pod declared %q, HTCondor parsed %q", value, got["SUBJECT"])
		}
		// The neighbours catch a value that swallowed or split the list.
		if got["BEFORE"] != "x" || got["AFTER"] != "y" {
			t.Fatalf("value %q disturbed its neighbours: %v", value, got)
		}
	})
}

// submitSafe excludes input a submit file cannot carry at all, as opposed to
// input we quote wrongly. A newline ends the submit command, and a NUL cannot
// survive a C string; neither is a bug in our quoting, and admitting them into
// the corpus would bury the real findings.
func submitSafe(s string) bool {
	if !utf8Valid(s) {
		return false
	}
	for _, r := range s {
		if r == '\n' || r == '\r' || r == 0 {
			return false
		}
		// Other control characters are not meaningfully testable through
		// a text submit file either.
		if unicode.IsControl(r) && r != '\t' {
			return false
		}
	}
	return true
}

func utf8Valid(s string) bool {
	for _, r := range s {
		if r == unicode.ReplacementChar {
			return false
		}
	}
	return true
}
