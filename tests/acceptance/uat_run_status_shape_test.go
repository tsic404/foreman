// The user-perspective carriers read a run's terminal class through uat-lib.sh's
// run_status, and the field it reads decides their verdict. The test server
// returns the task output in `result` and the class in `status`, so reading
// `result` first reports the output object as the class and fails every settled
// run, while an output whose text merely says "completed" would pass a failed
// one. Both faces are pinned here, on the shape the server returns.
package acceptance

import (
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
	cmd := exec.Command("sh", "-c", `. "$1"; run_status "$2"`, "sh", uatLibPath(t), body)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run_status(%s): %v\n%s", body, err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestUatRunStatusReadsTheServerStatusNotTheOutput(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to derive the run class")
	}
	cases := []struct {
		name string
		body string
		want string
	}{
		// The shape the server returns: class in `status`, output in `result`.
		{"completed run", `{"status":"completed","result":{"output":"OK","session_id":"s-1"}}`, "completed"},
		{"failed run", `{"status":"failed","result":{"output":"boom"}}`, "failed"},
		{"failed run whose output says completed", `{"status":"failed","result":{"output":"all completed"}}`, "failed"},
		{"uppercase status", `{"status":"Cancelled"}`, "cancelled"},
		// An output object is not a class: without a status there is none.
		{"output without a status", `{"result":{"output":"completed"}}`, ""},
		// The fields a shape variant may carry instead.
		{"legacy string result", `{"result":"completed"}`, "completed"},
		{"state fallback", `{"state":"Running"}`, "running"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := uatRunStatus(t, tc.body); got != tc.want {
				t.Fatalf("run_status(%s) = %q, want %q", tc.body, got, tc.want)
			}
		})
	}
}
