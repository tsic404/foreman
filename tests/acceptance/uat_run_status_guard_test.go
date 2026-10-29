// The TC-uat carriers read the server-side run's terminal class through
// uat-lib.sh's run_status, and the field it reads decides the verdict. A run
// object carries `status` beside the `result` output object, so reading
// `result` first turns every completed run into "not completed", while an
// output whose text merely says "completed" would pass for a completed one.
// Both faces are pinned here on the shape test-multica returns.
package acceptance

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// uatLibPath returns the shared carrier helper next to this test file.
func uatLibPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate uat-lib.sh from the test source")
	}
	return filepath.Join(filepath.Dir(file), "uat-lib.sh")
}

// uatRunStatus sources the helper and returns the class it derives from body.
func uatRunStatus(t *testing.T, body string) string {
	t.Helper()
	cmd := exec.Command("sh", "-c", `set -eu; . "$1"; run_status "$2"`, "sh", uatLibPath(t), body)
	// The helper resolves the test-multica CLI at source time; no class
	// assertion reads it, so an empty directory stands in for it.
	cmd.Env = append(os.Environ(), "TEST_MULTICA_DIR="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run_status(%s): %v\n%s", body, err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestUatRunStatusReadsTheStatusFaceNotTheOutput(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to read the run class")
	}
	cases := []struct {
		name string
		body string
		want string
	}{
		// The shape the server returns: status beside the output object.
		{"completed run", `{"status":"completed","result":{"output":"OK","pr_url":"","session_id":"…","work_dir":"…"}}`, "completed"},
		{"failed run", `{"status":"failed","result":{"output":"boom","session_id":"…"}}`, "failed"},
		{"state only", `{"state":"completed"}`, "completed"},
		{"legacy string result", `{"result":"completed"}`, "completed"},
		// Output text must never decide the class.
		{"output mentions completed", `{"result":{"output":"all completed"}}`, ""},
		{"failed run whose output says completed", `{"status":"failed","result":{"output":"completed"}}`, "failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := uatRunStatus(t, tc.body); got != tc.want {
				t.Fatalf("run_status(%s) = %q, want %q", tc.body, got, tc.want)
			}
		})
	}
}
