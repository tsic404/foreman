// TC-tech-forbidden-01 (F2): the rsbx carrier never preinstalls a CLI, the Job
// never contacts a release source directly, never follows `latest`, and never
// falls back to an image-bundled or preinstalled binary.
package acceptance

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// tcReadRepoFile reads one file relative to the repository root.
func tcReadRepoFile(t *testing.T, rel string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(raw)
}

func TestForbidden01NoCarrierCLIOrDirectSource(t *testing.T) {
	fixed := tcReadRepoFile(t, "internal/jobbuilder/fixed.go")
	for _, forbidden := range []string{
		"/usr/local/bin/foreman-job",
		"/usr/local/bin/multica",
		"releases/download",
	} {
		if strings.Contains(fixed, forbidden) {
			t.Errorf("the fixed Job render still contains %q", forbidden)
		}
	}
	if !strings.Contains(fixed, `binding.Image + "@" + binding.ImageDigest`) {
		t.Error("the fixed Job render does not pin the carrier by the frozen digest")
	}

	// The release line that baked the daemon CLI into the carrier is gone:
	// no Job image Containerfile and no workflow that publishes one.
	if _, err := os.Stat(filepath.Join("..", "..", "images", "foreman-job.Containerfile")); err == nil {
		t.Error("images/foreman-job.Containerfile still exists")
	}
	if _, err := os.Stat(filepath.Join("..", "..", ".github", "workflows", "build-foreman-job.yml")); err == nil {
		t.Error(".github/workflows/build-foreman-job.yml still publishes a Job image")
	}

	// The Job's only download address is the attempt's frozen proxy origin;
	// the supervisor has no other source, and no fallback branch.
	downloader := tcReadRepoFile(t, "internal/attempt/download.go")
	for _, forbidden := range []string{"releases/download", "github.com"} {
		if strings.Contains(downloader, forbidden) {
			t.Errorf("the Job downloader names a release source directly (%q)", forbidden)
		}
	}
	if !strings.Contains(downloader, "cached") || !strings.Contains(downloader, "DigestHex(cli.Digest)") {
		t.Error("the Job downloader does not reuse only verified bytes")
	}

	// The deployment names the carrier by digest, not a movable tag.
	deployment := tcReadRepoFile(t, "deploy/30-foreman.yaml")
	if !strings.Contains(deployment, "ghcr.io/tsic404/rsbx@sha256:") {
		t.Error("deploy/30-foreman.yaml does not name the rsbx carrier by digest")
	}
	if strings.Contains(deployment, "ghcr.io/tsic404/foreman-job") {
		t.Error("the deployment still points FOREMAN_JOB_IMAGE at the old foreman-job image")
	}
}
