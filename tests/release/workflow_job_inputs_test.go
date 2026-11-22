package release

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestJobWorkflowBuildsEveryContainerfileInput covers the publish path, not a
// Makefile target: the real workflow's build step must put each per-arch file
// the Job Containerfile COPYs into the build context.
func TestJobWorkflowBuildsEveryContainerfileInput(t *testing.T) {
	requireTools(t, "bash")
	top := workflowEnv(t, foremanJobWorkflow)
	steps := workflowSteps(t, foremanJobWorkflow)
	build := requireStep(t, steps, "Build the per-platform binary", foremanJobWorkflow)
	publish := requireStep(t, steps, "Build and push the image", foremanJobWorkflow)

	repo := t.TempDir()
	stubDir := t.TempDir()
	buildLog := filepath.Join(stubDir, "go.log")
	stub(t, stubDir, "go", `#!/bin/sh
set -eu
out=
prev=
for arg in "$@"; do
  if [ "$prev" = "-o" ]; then out=$arg; fi
  prev=$arg
done
[ -n "$out" ] || { echo "go stub: no -o argument" >&2; exit 1; }
mkdir -p "$(dirname "$out")"
printf '#!/bin/sh\n' >"$out"
echo "GOARCH=$GOARCH $*" >>"$STUB_GO_LOG"
`)
	runStep(t, build.run, repo, stubDir, mergeEnv(top, map[string]string{"STUB_GO_LOG": buildLog}))

	body, err := os.ReadFile("../../images/foreman-job.Containerfile")
	if err != nil {
		t.Fatalf("read the Job Containerfile: %v", err)
	}
	copies := regexp.MustCompile(`(?m)^COPY\s+([A-Za-z0-9._-]+)-linux-\$\{TARGETARCH\}\s+`).
		FindAllStringSubmatch(string(body), -1)
	if len(copies) < 2 {
		t.Fatalf("Containerfile carries %d per-arch COPY inputs, want foreman-gc and foreman-job", len(copies))
	}
	seen := map[string]bool{}
	for _, match := range copies {
		name := match[1]
		seen[name] = true
		for _, arch := range []string{"amd64", "arm64"} {
			path := filepath.Join(repo, "dist", name+"-linux-"+arch)
			if _, err := os.Stat(path); err != nil {
				t.Errorf("build context lacks %s: %v", path, err)
			}
		}
	}
	if !seen["foreman-job"] {
		t.Fatalf("Containerfile COPY inputs = %v, want the fixed Job entry point", seen)
	}
	if !strings.Contains(publish.run, "--push dist") {
		t.Fatalf("the publish step does not use dist as its build context:\n%s", publish.run)
	}

	log, err := os.ReadFile(buildLog)
	if err != nil {
		t.Fatalf("the build step never invoked go: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(log)), "\n")
	for _, arch := range []string{"amd64", "arm64"} {
		for _, cmd := range []string{"foreman-gc", "foreman-job"} {
			found := false
			for _, line := range lines {
				if strings.Contains(line, "GOARCH="+arch) && strings.Contains(line, "./cmd/"+cmd) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("build step never built ./cmd/%s for %s:\n%s", cmd, arch, log)
			}
		}
	}
}
