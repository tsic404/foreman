// TC-tech-provenance-01 (AC-13): the CLIs the Job runs are the upstream
// release artifacts the Foreman release registry verified, never a local build
// or an image-bundled copy. The Job fetches both frozen artifacts from the
// Foreman proxy, which serves only the published version/digest pair.
package acceptance

import (
	"regexp"
	"strings"
	"testing"
)

func TestProvenance01CLIsAreVerifiedUpstreamArtifacts(t *testing.T) {
	// The carrier and the Foreman image install no CLI; the only CLI bytes the
	// Job sees arrive through the proxy.
	image := tcReadRepoFile(t, "images/foreman.Containerfile")
	for _, forbidden := range []string{"multica-cli", "MULTICA_CLI_URL", "MULTICA_CLI_SHA256"} {
		if strings.Contains(image, forbidden) {
			t.Errorf("images/foreman.Containerfile still installs a CLI input %q", forbidden)
		}
	}

	// The download handler answers only the published version and digest of
	// the token's own source, and serves verified bytes from the release
	// domain.
	handler := tcReadRepoFile(t, "internal/proxy/clidist.go")
	for _, required := range []string{
		"VerifiedRelease",
		"VerifiedArtifactByDomain",
		"not this source's published frozen artifact",
		"verified",
	} {
		if !strings.Contains(handler, required) {
			t.Errorf("the CLI download handler lost %q", required)
		}
	}
	downloader := tcReadRepoFile(t, "internal/attempt/download.go")
	for _, required := range []string{
		"the download address is not on the Foreman proxy",
		"the artifact digest",
		"the frozen",
	} {
		if !strings.Contains(downloader, required) {
			t.Errorf("the Job-side CLI fetch lost %q", required)
		}
	}

	// The release registry still resolves the upstream artifact and its own
	// checksum; the Job never patches or rebuilds it.
	anchors := tcReadRepoFile(t, "docs/upstream-anchors.md")
	match := regexp.MustCompile(`github\.com/multica-ai/multica@([0-9a-f]{7,40})`).FindStringSubmatch(anchors)
	if match == nil {
		t.Fatal("docs/upstream-anchors.md does not pin the upstream artifact as repo@commit")
	}
}
