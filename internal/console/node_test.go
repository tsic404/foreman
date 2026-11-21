package console_test

import (
	"os/exec"
	"path/filepath"
	"testing"
)

// TestConsoleBehaviourPassesItsExecutableSuite runs the console's Node suite
// from the same gate that runs the Go tests, so the page's UI-01 / SYNC-02 /
// BIND-03 expectations cannot rot behind a command nobody runs in CI. The
// files are passed explicitly because a directory argument is resolved
// differently across Node versions: Node 24 loads it as a module path and
// fails. A runner without node skips with the reason instead of reporting a
// pass it did not earn.
func TestConsoleBehaviourPassesItsExecutableSuite(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is required to run the console behaviour suite")
	}
	files, err := filepath.Glob(filepath.Join("test", "*.test.mjs"))
	if err != nil {
		t.Fatalf("locate the console behaviour suite: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("the console behaviour suite is missing: test/*.test.mjs matched no file")
	}
	args := append([]string{"--test"}, files...)
	cmd := exec.Command(node, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the console behaviour suite failed: %v\n%s", err, output)
	}
}
