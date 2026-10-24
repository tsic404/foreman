// TC-tech-forbidden-07 (F7): the design repository holds intent and contracts,
// not runtime state. The scan covers every markdown file under docs/: no
// non-markdown data files (a task or cluster snapshot would land as one), no
// table column named for a state or an execution result, no ✅/❌ execution
// cells, and no execution-record heading or bullet. The input is always the
// frozen anchor: an explicit FOREMAN_DESIGN_DIR must be at it, and CI — which
// has no route to the LAN-only design host — scans the commit-pinned bundle
// under testdata. A missing or off-anchor input fails the case; it never
// skips.
package acceptance

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// tcForbidden07DesignAnchor is the frozen design revision every input must
// name; the HEAD of the checkout (or the carried bundle) is checked against
// it before the scan.
const tcForbidden07DesignAnchor = "6c4be145d97c03f2405b62d43d3bc05eeb6df013"

// tcForbidden07DesignDir locates the design checkout.
func tcForbidden07DesignDir(t *testing.T) string {
	t.Helper()
	if candidate := os.Getenv("FOREMAN_DESIGN_DIR"); candidate != "" {
		if info, err := os.Stat(filepath.Join(candidate, "docs", "03-acceptance.md")); err != nil || info.IsDir() {
			t.Fatalf("%s does not look like the design checkout", candidate)
		}
		if got := tcForbidden07GitHead(t, candidate); got != tcForbidden07DesignAnchor {
			t.Fatalf("design checkout HEAD = %s, want the frozen anchor %s (unset FOREMAN_DESIGN_DIR to scan the carried bundle)", got, tcForbidden07DesignAnchor)
		}
		return candidate
	}
	dir := t.TempDir()
	bundle := filepath.Join("testdata", "foreman-design-"+tcForbidden07DesignAnchor[:7]+".bundle")
	if out, err := exec.Command("git", "clone", "--quiet", bundle, dir).CombinedOutput(); err != nil {
		t.Fatalf("clone the design bundle %s: %v\n%s", bundle, err, out)
	}
	if got := tcForbidden07GitHead(t, dir); got != tcForbidden07DesignAnchor {
		t.Fatalf("design bundle HEAD = %s, want the frozen anchor %s", got, tcForbidden07DesignAnchor)
	}
	return dir
}

// tcForbidden07GitHead reads one checkout's HEAD revision.
func tcForbidden07GitHead(t *testing.T, dir string) string {
	t.Helper()
	out, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("read the design checkout's HEAD at %s (is it a git checkout?): %v", dir, err)
	}
	return strings.TrimSpace(string(out))
}

// tcForbidden07StateHeaders are the column names a runtime-state or
// execution-record table would carry.
var tcForbidden07StateHeaders = map[string]bool{
	"状态": true, "执行状态": true, "结论": true, "结果": true,
	"验收结论": true, "验收结果": true, "执行结果": true,
	"pass": true, "fail": true, "result": true, "status": true,
}

var (
	tcForbidden07ExecutionHeading = regexp.MustCompile(`^\s*#{1,6}\s*(执行记录|验收记录|执行结果|验收结果|验收结论)\s*$`)
	tcForbidden07ExecutionRecord  = regexp.MustCompile(`^\s*[-*]\s*(执行记录|验收结果|验收结论)\s*[:：]`)
	tcForbidden07TableSeparator   = regexp.MustCompile(`^\s*\|[\s:|-]+\|?\s*$`)
)

// tcForbidden07TableCells splits one markdown table row.
func tcForbidden07TableCells(line string) []string {
	trimmed := strings.TrimSpace(line)
	trimmed = strings.TrimPrefix(trimmed, "|")
	trimmed = strings.TrimSuffix(trimmed, "|")
	parts := strings.Split(trimmed, "|")
	cells := make([]string, 0, len(parts))
	for _, part := range parts {
		cells = append(cells, strings.TrimSpace(part))
	}
	return cells
}

func TestForbidden07DesignRepoHoldsNoRuntimeState(t *testing.T) {
	designDir := tcForbidden07DesignDir(t)
	docsDir := filepath.Join(designDir, "docs")
	var markdown []string
	err := filepath.WalkDir(docsDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".md" {
			t.Errorf("the design repo carries a non-markdown data file: %s", path)
			return nil
		}
		markdown = append(markdown, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", docsDir, err)
	}
	if len(markdown) == 0 {
		t.Fatalf("no markdown under %s", docsDir)
	}
	t.Logf("scanned design repo %s: %d markdown files", designDir, len(markdown))

	for _, path := range markdown {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		lines := strings.Split(string(raw), "\n")
		for i, line := range lines {
			if strings.Contains(line, "✅") || strings.Contains(line, "❌") {
				t.Errorf("%s:%d carries an execution-result cell: %s", path, i+1, line)
			}
			if tcForbidden07ExecutionHeading.MatchString(line) || tcForbidden07ExecutionRecord.MatchString(line) {
				t.Errorf("%s:%d carries an execution record: %s", path, i+1, line)
			}
			if !strings.HasPrefix(strings.TrimSpace(line), "|") || i+1 >= len(lines) {
				continue
			}
			if !tcForbidden07TableSeparator.MatchString(lines[i+1]) {
				continue
			}
			for _, cell := range tcForbidden07TableCells(line) {
				if tcForbidden07StateHeaders[strings.ToLower(cell)] {
					t.Errorf("%s:%d table declares the state/execution column %q", path, i+1, cell)
				}
			}
		}
	}
}
