// The acceptance matrix joins a criterion to its case by one key: the TC
// number. This census walks it both ways and applies the design's gate: a P0
// entry without evidence and any reverse drift fail the run, while a P1/P2
// entry without evidence is recorded and the run continues. The design host
// is LAN-only, so its acceptance document travels as a commit-pinned bundle
// under testdata; an input whose HEAD is not the pinned revision fails, never
// skips, because a skipped census is a green step that decides nothing.
package testinfra

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// acceptanceCases is the implementation side of the matrix, relative to this
// package's own directory (the working directory of a `go test` run).
const acceptanceCases = "../acceptance"

// acceptanceMatrixDesignAnchor is the design revision the census judges
// against. Pinning it is what makes the verdict reproducible: the same tree
// must decide the same way. Refresh it with `make acceptance-census-refresh`
// when the design moves and the cases follow.
const acceptanceMatrixDesignAnchor = "b8892e7c9dbf07b7ab7c92c9a8294d3edbcb556c"

// acceptanceFixtures are shared helpers and repo-side guards, not cases: the
// helpers build the scenarios the numbered files judge, and the guards pin
// this repository's own artifacts; neither claims a TC of their own.
var acceptanceFixtures = map[string]bool{
	"deploy_manifest_probe_window_guard_test.go": true,
	"harness_test.go":               true,
	"multi_shared_helpers_test.go":  true,
	"multiws_harness_test.go":       true,
	"probe_session_helpers_test.go": true,
	"uat-lib.sh":                    true,
	"uat-scenarios.sh":              true,
	"uat_run_status_guard_test.go":  true,
	"uat_scenarios_carrier_test.go": true,
}

var (
	// designTCHeading matches a TC section heading and captures its number;
	// the section body carries the entry, including the priority.
	designTCHeading = regexp.MustCompile(`^#+\s+(TC-[a-z0-9]+(?:-[a-z0-9]+)*-\d{2})\s*$`)
	// designTCPriority matches the priority field inside a TC section.
	designTCPriority = regexp.MustCompile(`^-\s*\*\*优先级\*\*\s*[：:]\s*(P[0-2])\s*$`)
	// designTCID finds a TC number in a case file's leading comment.
	designTCID = regexp.MustCompile(`\bTC-[a-z0-9]+(?:-[a-z0-9]+)*-\d{2}\b`)
)

// parseDesignMatrix reads the design's TC entries: each entry is a section
// headed by its number and states its priority in the body. The number set is
// the design side of the census, so every shape the reader cannot decide
// fails instead of dropping the entry: a heading that names a number but not
// in the readable form, a repeated number, and an entry with anything but one
// priority all fail, because a silently skipped entry or a softened P0 is
// exactly what the census exists to catch.
func parseDesignMatrix(path string, raw []byte) (map[string]string, []string, error) {
	priorities := make(map[string]string)
	var order []string
	var current string
	var body []string

	closeEntry := func() error {
		if current == "" {
			return nil
		}
		var stated []string
		for _, line := range body {
			if match := designTCPriority.FindStringSubmatch(line); match != nil {
				stated = append(stated, match[1])
			}
		}
		switch len(stated) {
		case 0:
			return fmt.Errorf("%s: entry %s states no priority (P0/P1/P2): the document changed shape", path, current)
		case 1:
		default:
			return fmt.Errorf("%s: entry %s states %d priorities (%s): want exactly one, so a P0 cannot be softened into a recorded gap", path, current, len(stated), strings.Join(stated, ", "))
		}
		priorities[current] = stated[0]
		order = append(order, current)
		current, body = "", nil
		return nil
	}

	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") {
			if err := closeEntry(); err != nil {
				return nil, nil, err
			}
			match := designTCHeading.FindStringSubmatch(line)
			if match == nil {
				// Any heading that carries `TC-` but does not read back as
				// an entry is a number the census cannot place; letting it
				// pass would drop that entry, and a P0 without a case has no
				// other check to catch it.
				if strings.Contains(line, "TC-") {
					return nil, nil, fmt.Errorf("%s: heading %q names a TC number in a shape the census cannot read: the entry would leave the design side silently", path, line)
				}
				continue
			}
			if _, seen := priorities[match[1]]; seen {
				return nil, nil, fmt.Errorf("%s: entry %s appears again at %q: numbers are globally unique, so a second entry is a defect, not a duplicate to keep", path, match[1], line)
			}
			current = match[1]
			continue
		}
		if current != "" {
			body = append(body, line)
		}
	}
	if err := closeEntry(); err != nil {
		return nil, nil, err
	}
	return priorities, order, nil
}

// caseTCID reads the TC number a case's filename claims: the design id with
// `_` in place of `-` for Go files, optionally followed by a descriptor.
func caseTCID(name string) (string, bool) {
	base := name
	switch {
	case strings.HasSuffix(base, "_test.go"):
		base = strings.TrimSuffix(base, "_test.go")
	case strings.HasSuffix(base, ".sh"):
		base = strings.TrimSuffix(base, ".sh")
	default:
		return "", false
	}
	parts := strings.Split(strings.ReplaceAll(base, "_", "-"), "-")
	if len(parts) < 3 || (parts[0] != "tech" && parts[0] != "uat") {
		return "", false
	}
	for i := 2; i < len(parts); i++ {
		if len(parts[i]) == 2 && isDigits(parts[i]) {
			return "TC-" + strings.Join(parts[:i+1], "-"), true
		}
	}
	return "", false
}

// isDigits reports whether s is one or more ASCII digits.
func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// leadingTCIDs reads the TC numbers a Go case declares in its leading comment
// block. A case may declare more than one: the matrix allows a scenario to be
// evidence for every number it names, and the filename only carries one.
func leadingTCIDs(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	var block strings.Builder
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "//") {
			break
		}
		block.WriteString(line)
		block.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return designTCID.FindAllString(block.String(), -1), nil
}

// acceptanceMatrixBundlePath names the carried design snapshot.
func acceptanceMatrixBundlePath() string {
	return filepath.Join("testdata", "foreman-design-"+acceptanceMatrixDesignAnchor[:7]+".bundle")
}

// acceptanceMatrixDesignDir resolves the design checkout the census reads:
// FOREMAN_DESIGN_DIR when set, the carried bundle otherwise. Either way its
// HEAD must be the pinned revision, so a changed snapshot cannot change the
// verdict.
func acceptanceMatrixDesignDir(t *testing.T) (string, error) {
	t.Helper()
	if candidate := os.Getenv("FOREMAN_DESIGN_DIR"); candidate != "" {
		if info, err := os.Stat(filepath.Join(candidate, "docs", "03-acceptance.md")); err != nil || info.IsDir() {
			return "", fmt.Errorf("%s is not a tsic/foreman-design checkout", candidate)
		}
		return candidate, acceptanceMatrixCheckAnchor(candidate)
	}
	dir, err := acceptanceMatrixCloneBundle(t)
	if err != nil {
		return "", err
	}
	return dir, acceptanceMatrixCheckAnchor(dir)
}

// acceptanceMatrixCloneBundle unpacks the carried bundle into a temp dir.
func acceptanceMatrixCloneBundle(t *testing.T) (string, error) {
	t.Helper()
	dir := t.TempDir()
	bundle := acceptanceMatrixBundlePath()
	if out, err := exec.Command("git", "clone", "--quiet", bundle, dir).CombinedOutput(); err != nil {
		return "", fmt.Errorf("clone the carried design bundle %s: %v\n%s", bundle, err, out)
	}
	return dir, nil
}

// acceptanceMatrixCheckAnchor rejects any input whose HEAD is not the pin.
func acceptanceMatrixCheckAnchor(dir string) error {
	out, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
	if err != nil {
		return fmt.Errorf("read HEAD at %s (is it a git checkout?): %v", dir, err)
	}
	if got := strings.TrimSpace(string(out)); got != acceptanceMatrixDesignAnchor {
		return fmt.Errorf("%s is at %s, want the pinned revision %s (run `make acceptance-census-refresh` when the design moves)", dir, got, acceptanceMatrixDesignAnchor)
	}
	return nil
}

// acceptanceMatrixOutcome is what one census observation decides.
type acceptanceMatrixOutcome int

const (
	// outcomeClear means the observation decides nothing.
	outcomeClear acceptanceMatrixOutcome = iota
	// outcomeRecorded is a gap the gate logs while the run continues.
	outcomeRecorded
	// outcomeBlocking fails the run.
	outcomeBlocking
)

// acceptanceMatrixObservation is one census result: a TC with or without
// evidence, or a case pointing at a number the design does not define.
type acceptanceMatrixObservation struct {
	hasEvidence bool
	isReverse   bool
	priority    string
}

// acceptanceMatrixVerdict applies the design's gate to one observation:
// reverse drift always blocks, a TC without evidence blocks at P0, is
// recorded at P1/P2, and evidence clears it.
func acceptanceMatrixVerdict(o acceptanceMatrixObservation) acceptanceMatrixOutcome {
	switch {
	case o.isReverse:
		return outcomeBlocking
	case o.hasEvidence:
		return outcomeClear
	case o.priority == "P0":
		return outcomeBlocking
	default:
		return outcomeRecorded
	}
}

// TestAcceptanceMatrixIsBidirectionallyClosed is the census gate: design TC
// numbers and implementation case names must name each other.
func TestAcceptanceMatrixIsBidirectionallyClosed(t *testing.T) {
	designDir, err := acceptanceMatrixDesignDir(t)
	if err != nil {
		t.Fatal(err)
	}
	designPath := filepath.Join(designDir, "docs", "03-acceptance.md")
	raw, err := os.ReadFile(designPath)
	if err != nil {
		t.Fatalf("read the design's TC entries: %v", err)
	}

	priorities, designOrder, err := parseDesignMatrix(designPath, raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(priorities) == 0 {
		t.Fatalf("no TC entries in %s: the document changed shape", designPath)
	}

	entries, err := os.ReadDir(acceptanceCases)
	if err != nil {
		t.Fatalf("read %s: %v", acceptanceCases, err)
	}

	covered := map[string]bool{}
	var reverse []string
	var caseFiles int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		id, numbered := caseTCID(name)
		if !numbered {
			if !acceptanceFixtures[name] {
				reverse = append(reverse, name+" names no TC")
			}
			continue
		}
		caseFiles++
		declared := map[string]bool{}
		if _, defined := priorities[id]; defined {
			declared[id] = true
		} else {
			reverse = append(reverse, name+" is named after "+id+", which the design does not define")
		}
		if strings.HasSuffix(name, ".go") {
			headerIDs, err := leadingTCIDs(filepath.Join(acceptanceCases, name))
			if err != nil {
				t.Fatalf("read %s: %v", name, err)
			}
			for _, headerID := range headerIDs {
				if _, defined := priorities[headerID]; defined {
					declared[headerID] = true
				} else {
					reverse = append(reverse, name+" declares "+headerID+", which the design does not define")
				}
			}
		}
		// An observation script supplements a judgement; it does not build the
		// scenario, so it never counts as the evidence a TC needs.
		if strings.HasSuffix(name, ".sh") {
			continue
		}
		for declaredID := range declared {
			covered[declaredID] = true
		}
	}

	var blockingTCs, recordedTCs []string
	for _, id := range designOrder {
		switch acceptanceMatrixVerdict(acceptanceMatrixObservation{hasEvidence: covered[id], priority: priorities[id]}) {
		case outcomeBlocking:
			blockingTCs = append(blockingTCs, id)
		case outcomeRecorded:
			recordedTCs = append(recordedTCs, id)
		}
	}
	if verdict := acceptanceMatrixVerdict(acceptanceMatrixObservation{isReverse: true}); verdict != outcomeBlocking {
		t.Fatalf("reverse drift must be a blocking finding, got %v", verdict)
	}

	for _, id := range blockingTCs {
		t.Errorf("%s (P0) has no case under tests/acceptance: a P0 entry without evidence fails the gate", id)
	}
	for _, drift := range reverse {
		t.Errorf("reverse drift: %s", drift)
	}
	if len(blockingTCs) > 0 {
		t.Logf("P0 without a case (blocking): %s", strings.Join(blockingTCs, ", "))
	}
	if len(recordedTCs) > 0 {
		t.Logf("P1/P2 without a case (recorded, not blocking): %s", strings.Join(recordedTCs, ", "))
	}
	if len(reverse) > 0 {
		t.Logf("reverse drift (blocking): %s", strings.Join(reverse, "; "))
	}
	t.Logf("censused %d TCs against %d numbered case files at %s", len(priorities), caseFiles, acceptanceMatrixDesignAnchor[:7])

	if findings := len(blockingTCs) + len(reverse); findings > 0 {
		t.Fatalf("%d blocking matrix findings: %d P0 entries without evidence, %d reverse drift", findings, len(blockingTCs), len(reverse))
	}
}
