// TC-tech-provenance-01 (AC-13): the Job image's multica binary is the
// upstream release artifact, not a local build. The release line resolves the
// archive URL and its SHA-256 from the upstream release's own checksum
// manifest, fails when either is missing, and the image build verifies that
// digest before installing the archive member. The anchor pins the same
// artifact by repo and commit.
package acceptance

import (
	"regexp"
	"strings"
	"testing"
)

func TestProvenance01CLIIsTheUpstreamArtifact(t *testing.T) {
	containerfile := tcReadRepoFile(t, "images/foreman-job.Containerfile")
	workflow := tcReadRepoFile(t, ".github/workflows/build-foreman-job.yml")

	// The release line takes both the URL and the digest from the upstream
	// release, never from a repo constant.
	for _, required := range []string{
		"UPSTREAM_REPO: multica-ai/multica",
		`cli_base="https://github.com/$UPSTREAM_REPO/releases/download/v${VERSION}"`,
		`cli_asset="multica-cli-${VERSION}-linux-${arch}.tar.gz"`,
		`cli_sha=$(digest_of "$cli_base/checksums.txt" "$cli_asset")`,
		`[[ $sha =~ ^[0-9a-f]{64}$ ]]`,
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("the release line lost the upstream artifact resolution step %q", required)
		}
	}
	if !strings.Contains(workflow, "MULTICA_CLI_SHA256_%s") {
		t.Error("the release line does not pass the upstream digest to the image build")
	}

	// The image verifies that digest before installing the archive member.
	for _, required := range []string{
		`sha256sum -c -`,
		"tar -xzf /tmp/multica-cli.tar.gz -C /tmp multica",
		"mv /tmp/multica /usr/local/bin/multica",
		"chmod 0755 /usr/local/bin/multica",
	} {
		if !strings.Contains(containerfile, required) {
			t.Errorf("the image build lost the verified install step %q", required)
		}
	}
	// The installed path is the one the reference template executes.
	template := tcReadRepoFile(t, "deploy/25-job-template.yaml")
	if !strings.Contains(template, `"/usr/local/bin/multica"`) {
		t.Error("the reference template no longer executes the installed CLI path")
	}

	// No local build or patch of the CLI: the only Go binary copied in is
	// foreman-gc.
	for _, line := range tcInstructionLines(containerfile) {
		if strings.HasPrefix(line, "COPY ") || strings.HasPrefix(line, "ADD ") {
			if strings.Contains(line, "multica") && !strings.Contains(line, "foreman-gc") {
				t.Errorf("a local multica artifact is copied into the image: %s", line)
			}
		}
		if strings.HasPrefix(line, "RUN ") {
			for _, forbidden := range []string{"patch ", "git apply", "sed -i", "go build"} {
				if strings.Contains(line, forbidden) {
					t.Errorf("the image build patches/rebuilds the CLI (%q): %s", forbidden, line)
				}
			}
		}
	}

	// The anchor pins the same artifact by repo and commit.
	anchors := tcReadRepoFile(t, "docs/upstream-anchors.md")
	match := regexp.MustCompile(`github\.com/multica-ai/multica@([0-9a-f]{7,40})`).FindStringSubmatch(anchors)
	if match == nil {
		t.Fatal("docs/upstream-anchors.md does not pin the upstream artifact as repo@commit")
	}
	if !strings.Contains(anchors, "artifact the test cluster pins and the Job image carries") {
		t.Error("the anchor no longer declares the artifact as the image's CLI source")
	}
}
