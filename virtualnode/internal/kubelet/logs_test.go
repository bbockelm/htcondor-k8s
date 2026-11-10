package kubelet

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
)

func testConfig() Config {
	return Config{StdoutName: DefaultStdoutName, StderrName: DefaultStderrName}
}

func options(t *testing.T, query string) (logOptions, error) {
	t.Helper()
	r := httptest.NewRequest("GET", "/containerLogs/ns/pod/c?"+query, nil)
	return parseLogOptions(r, testConfig())
}

func TestParseLogOptionsRefusesPreviousRatherThanIgnoringIt(t *testing.T) {
	// Serving the current run's output when the previous run's was asked
	// for is a wrong answer to the one question the user cared about.
	_, err := options(t, "previous=true")
	require.ErrorContains(t, err, "truncates its output file")
}

func TestParseLogOptionsRecordsWhatItCannotHonor(t *testing.T) {
	opts, err := options(t, "timestamps=true&sinceSeconds=300&follow=true")
	require.NoError(t, err)
	require.True(t, opts.follow)
	require.ElementsMatch(t, []string{"timestamps", "sinceSeconds"}, opts.ignored)
}

func TestParseLogOptionsLeavesUnaskedParametersAlone(t *testing.T) {
	opts, err := options(t, "timestamps=false")
	require.NoError(t, err)
	require.Empty(t, opts.ignored)
	require.Equal(t, -1, opts.tailLines, "no tailLines means the whole file")
}

func TestParseLogOptionsRejectsNonsense(t *testing.T) {
	for _, q := range []string{"tailLines=-3", "tailLines=all", "limitBytes=0", "stream=syslog"} {
		_, err := options(t, q)
		require.Error(t, err, "query %q should be refused", q)
	}
}

func TestTailZeroMeansNothing(t *testing.T) {
	// `kubectl logs --tail=0` asks for no lines. Returning the whole file
	// is the easy mistake and the opposite of what was asked.
	data := []byte("a\nb\nc\n")
	require.Empty(t, applyLimits(data, logOptions{tailLines: 0}))
	require.Equal(t, "c\n", string(applyLimits(data, logOptions{tailLines: 1})))
	require.Equal(t, "b\nc\n", string(applyLimits(data, logOptions{tailLines: 2})))
	require.Equal(t, "a\nb\nc\n", string(applyLimits(data, logOptions{tailLines: 99})))
	require.Equal(t, "a\nb\nc\n", string(applyLimits(data, logOptions{tailLines: -1})))
}

func TestLimitBytesTruncatesAfterTailLines(t *testing.T) {
	data := []byte("aaaa\nbbbb\n")
	require.Equal(t, "bb", string(applyLimits(data, logOptions{tailLines: 1, limitBytes: 2})))
}

func TestLastLinesHandlesAFileWithNoTrailingNewline(t *testing.T) {
	require.Equal(t, "b", string(lastLines([]byte("a\nb"), 1)))
}

func TestPickStreamFallsBackToStderrOnlyWhenStdoutIsAbsent(t *testing.T) {
	// A job that wrote only to stderr must not report as silent: that is
	// the one empty log a user will believe.
	dir := t.TempDir()
	s := &Server{cfg: testConfig()}
	opts := logOptions{stream: DefaultStdoutName}

	require.Equal(t, filepath.Join(dir, "stdout"), s.pickStream(dir, opts),
		"with neither file present the answer is still stdout")

	require.NoError(t, os.WriteFile(filepath.Join(dir, "stderr"), []byte("boom\n"), 0o600))
	require.Equal(t, filepath.Join(dir, "stderr"), s.pickStream(dir, opts))

	require.NoError(t, os.WriteFile(filepath.Join(dir, "stdout"), []byte("hi\n"), 0o600))
	require.Equal(t, filepath.Join(dir, "stdout"), s.pickStream(dir, opts),
		"once stdout exists it wins")

	require.Equal(t, filepath.Join(dir, "stderr"),
		s.pickStream(dir, logOptions{stream: DefaultStderrName}),
		"an explicit request is never second-guessed")
}

func TestJobIDParsesTheAnnotationAndRefusesJunk(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{api.AnnotationJobID: "1234.7"},
	}}
	cluster, proc, ok := jobID(pod)
	require.True(t, ok)
	require.Equal(t, 1234, cluster)
	require.Equal(t, 7, proc)

	for _, bad := range []string{"", "1234", "x.y", "1234.", "cluster.proc"} {
		pod.Annotations[api.AnnotationJobID] = bad
		_, _, ok := jobID(pod)
		require.False(t, ok, "%q should not parse", bad)
	}
}

func TestNewRequiresPodsAndPairedCertificateFiles(t *testing.T) {
	_, err := New(Config{})
	require.ErrorContains(t, err, "Pods is required")

	_, err = New(Config{
		Pods:     PodGetterFunc(nil),
		CertFile: "/tmp/cert.pem",
	})
	require.ErrorContains(t, err, "must be set together")
}
