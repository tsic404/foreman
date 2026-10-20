// TC-tech-forbidden-01 (F1): the Job image runs the upstream CLI artifact,
// never a locally patched build. The Containerfile's only CLI input is the
// upstream release URL, its digest comes from that release's checksum
// manifest, and the extracted binary is installed verbatim; no COPY/ADD
// brings a local multica tree in, and no instruction patches one. The
// upstream anchor records the pinned artifact.
package acceptance

import (
	"os"
	"path/filepath"
	"regexp"
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

// tcInstructionLines returns the Containerfile's non-comment instructions.
func tcInstructionLines(content string) []string {
	var lines []string
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		lines = append(lines, trimmed)
	}
	return lines
}

func TestForbidden01UpstreamCLIArtifactUnmodified(t *testing.T) {
	containerfile := tcReadRepoFile(t, "images/foreman-job.Containerfile")
	for _, required := range []string{
		"ARG MULTICA_CLI_URL_AMD64",
		"ARG MULTICA_CLI_SHA256_AMD64",
		`wget -q -O /tmp/multica-cli.tar.gz "$MULTICA_CLI_URL"`,
		`echo "$MULTICA_CLI_SHA256  /tmp/multica-cli.tar.gz" | sha256sum -c -`,
		"tar -xzf /tmp/multica-cli.tar.gz -C /tmp multica",
		"mv /tmp/multica /usr/local/bin/multica",
	} {
		if !strings.Contains(containerfile, required) {
			t.Errorf("the Job image build lost the upstream artifact step %q", required)
		}
	}
	// The digest check precedes the extraction: the binary that lands in the
	// image is the verified release member.
	digestAt := strings.Index(containerfile, "sha256sum -c -")
	extractAt := strings.Index(containerfile, "tar -xzf /tmp/multica-cli.tar.gz")
	if digestAt < 0 || extractAt < 0 || digestAt > extractAt {
		t.Errorf("digest verification (at %d) must precede extraction (at %d)", digestAt, extractAt)
	}

	// No local source is copied in: the only COPY is foreman-gc.
	for _, line := range tcInstructionLines(containerfile) {
		if !strings.HasPrefix(line, "COPY ") && !strings.HasPrefix(line, "ADD ") {
			continue
		}
		if strings.Contains(line, "multica") && !strings.Contains(line, "foreman-gc") {
			t.Errorf("the image copies a local multica artifact: %s", line)
		}
	}
	// No instruction patches or rebuilds the CLI.
	for _, line := range tcInstructionLines(containerfile) {
		if strings.HasPrefix(line, "RUN ") {
			for _, forbidden := range []string{"patch ", "git apply", "sed -i", "go build"} {
				if strings.Contains(line, forbidden) {
					t.Errorf("the image build patches/rebuilds the CLI (%q): %s", forbidden, line)
				}
			}
		}
	}

	// The anchor records the pinned upstream artifact, and the release line
	// states the CLI is used verbatim.
	anchors := tcReadRepoFile(t, "docs/upstream-anchors.md")
	if !regexp.MustCompile(`github\.com/multica-ai/multica@[0-9a-f]{7,40}`).MatchString(anchors) {
		t.Error("docs/upstream-anchors.md does not pin the upstream artifact as repo@commit")
	}
	if !strings.Contains(containerfile, "used verbatim") || !strings.Contains(containerfile, "never patched") {
		t.Error("the Job image build no longer declares the CLI as verbatim/never patched")
	}
}
