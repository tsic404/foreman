// Parser cases for the census gate: the filename and header readers decide
// every verdict the gate issues, so their edges are pinned here rather than
// discovered by a silently degraded census.
package testinfra

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
)

func TestCaseTCID(t *testing.T) {
	cases := []struct {
		name   string
		wantID string
		wantOK bool
	}{
		{"tech_auth_01_credential_routing_test.go", "TC-tech-auth-01", true},
		{"tech_multiws_02_cross_workspace_test.go", "TC-tech-multiws-02", true},
		{"tech-cache-02-cluster.sh", "TC-tech-cache-02", true},
		{"uat-recovery-01-cluster.sh", "TC-uat-recovery-01", true},
		{"uat-lib.sh", "", false},
		{"deploy_shared_state_probe_test.go", "", false},
		{"tech_multi_assembly_test.go", "", false},
		{"tech_auth_1_short_test.go", "", false},
		{"tech_auth_010_long_test.go", "", false},
		{"misc_tech_auth_01_test.go", "", false},
		{"tech_auth_01_test.go.bak", "", false},
	}
	for _, tc := range cases {
		gotID, gotOK := caseTCID(tc.name)
		if gotID != tc.wantID || gotOK != tc.wantOK {
			t.Errorf("caseTCID(%q) = (%q, %v), want (%q, %v)", tc.name, gotID, gotOK, tc.wantID, tc.wantOK)
		}
	}
}

func TestLeadingTCIDs(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    []string
	}{
		{
			name:    "one declared number",
			content: "// TC-tech-auth-01 covers the routing face.\npackage acceptance\n",
			want:    []string{"TC-tech-auth-01"},
		},
		{
			name:    "several declared numbers",
			content: "// TC-tech-job-01 and TC-tech-security-01 share the window.\n// A second line may add TC-tech-job-04.\npackage acceptance\n",
			want:    []string{"TC-tech-job-01", "TC-tech-security-01", "TC-tech-job-04"},
		},
		{
			name:    "numbers outside the leading block do not count",
			content: "package acceptance\n\n// TC-tech-auth-01 appears too late.\n",
			want:    nil,
		},
		{
			name:    "ac references are not TC numbers",
			content: "// Assembly evidence for AC-74…AC-78.\npackage acceptance\n",
			want:    nil,
		},
		{
			name:    "a blank first line ends the block",
			content: "\n// TC-tech-auth-01\npackage acceptance\n",
			want:    nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "case_test.go")
			if err := os.WriteFile(path, []byte(tc.content), 0o600); err != nil {
				t.Fatalf("write fixture: %v", err)
			}
			got, err := leadingTCIDs(path)
			if err != nil {
				t.Fatalf("leadingTCIDs: %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("leadingTCIDs = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAcceptanceMatrixVerdict(t *testing.T) {
	cases := []struct {
		name        string
		observation acceptanceMatrixObservation
		want        acceptanceMatrixOutcome
	}{
		{"P0 without evidence blocks", acceptanceMatrixObservation{priority: "P0"}, outcomeBlocking},
		{"P1 without evidence is recorded", acceptanceMatrixObservation{priority: "P1"}, outcomeRecorded},
		{"P2 without evidence is recorded", acceptanceMatrixObservation{priority: "P2"}, outcomeRecorded},
		{"evidence clears a P0 entry", acceptanceMatrixObservation{hasEvidence: true, priority: "P0"}, outcomeClear},
		{"evidence clears a P2 entry", acceptanceMatrixObservation{hasEvidence: true, priority: "P2"}, outcomeClear},
		{"reverse drift blocks at any priority", acceptanceMatrixObservation{isReverse: true, priority: "P2"}, outcomeBlocking},
	}
	for _, tc := range cases {
		if got := acceptanceMatrixVerdict(tc.observation); got != tc.want {
			t.Errorf("%s: verdict = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestAcceptanceMatrixRejectsOffAnchorCheckout(t *testing.T) {
	dir, err := acceptanceMatrixCloneBundle(t)
	if err != nil {
		t.Fatalf("clone the carried bundle: %v", err)
	}
	t.Setenv("FOREMAN_DESIGN_DIR", dir)
	if _, err := acceptanceMatrixDesignDir(t); err != nil {
		t.Fatalf("the pinned checkout was rejected: %v", err)
	}

	// Same tree, later revision: only the HEAD comparison tells them apart.
	commit := exec.Command("git", "-C", dir,
		"-c", "user.name=census", "-c", "user.email=census@example.invalid",
		"commit", "--quiet", "--allow-empty", "-m", "off-anchor snapshot")
	if out, err := commit.CombinedOutput(); err != nil {
		t.Fatalf("create the off-anchor revision: %v\n%s", err, out)
	}
	if _, err := acceptanceMatrixDesignDir(t); err == nil {
		t.Fatalf("a checkout whose HEAD moved past the pin was accepted: the census would judge another snapshot")
	}
}

func TestParseDesignMatrix(t *testing.T) {
	cases := []struct {
		name    string
		doc     string
		want    map[string]string
		wantErr bool
	}{
		{
			name: "entries carry their own priority",
			doc: "# Acceptance\n\n## 验收标准\n\n#### AC-01\n\n- **优先级**：P0\n\n" +
				"## 用例\n\n#### TC-tech-auth-01\n\n- **AC**：AC-01\n- **优先级**：P0\n\n" +
				"#### TC-uat-cache-01\n\n- **AC**：AC-80\n- **优先级**：P2\n",
			want: map[string]string{"TC-tech-auth-01": "P0", "TC-uat-cache-01": "P2"},
		},
		{
			name:    "an entry without a priority fails the shape",
			doc:     "#### TC-tech-auth-01\n\n- **场景语义**\n  - 注册与心跳\n",
			wantErr: true,
		},
		{
			name:    "an unknown priority fails the shape",
			doc:     "#### TC-tech-auth-01\n\n- **优先级**：P3\n",
			wantErr: true,
		},
		{
			name: "a repeated number fails instead of keeping the first",
			doc: "#### TC-tech-auth-01\n\n- **优先级**：P0\n\n" +
				"#### TC-tech-auth-01\n\n- **优先级**：P2\n",
			wantErr: true,
		},
		{
			name: "an entry with more than one priority fails",
			doc: "#### TC-tech-auth-01\n\n- **优先级**：P0\n" +
				"- **优先级**：P1\n",
			wantErr: true,
		},
		{
			name: "a heading that names a number in another shape fails",
			doc: "#### TC-tech-auth-01\n\n- **优先级**：P0\n\n" +
				"#### TC-tech-auth-02 的补充说明\n\n- **优先级**：P0\n",
			wantErr: true,
		},
		{
			name: "a heading with a short number is not dropped",
			doc: "#### TC-tech-auth-01\n\n- **优先级**：P0\n\n" +
				"#### TC-tech-auth-1\n\n- **优先级**：P0\n",
			wantErr: true,
		},
		{
			name: "a heading with a three-digit number is not dropped",
			doc: "#### TC-tech-auth-01\n\n- **优先级**：P0\n\n" +
				"#### TC-tech-auth-001\n\n- **优先级**：P0\n",
			wantErr: true,
		},
		{
			name: "a heading with upper-case letters is not dropped",
			doc: "#### TC-tech-auth-01\n\n- **优先级**：P0\n\n" +
				"#### TC-Tech-Auth-01\n\n- **优先级**：P0\n",
			wantErr: true,
		},
		{
			name: "text outside any entry is ignored",
			doc:  "> intro quoting TC-tech-auth-01\n\n- **优先级**：P1\n",
			want: map[string]string{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, order, err := parseDesignMatrix("acceptance.md", []byte(tc.doc))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseDesignMatrix accepted a document the census cannot decide: %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseDesignMatrix: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("parseDesignMatrix read %d entries, want %d: %v", len(got), len(tc.want), got)
			}
			for id, priority := range tc.want {
				if got[id] != priority {
					t.Errorf("%s priority = %q, want %q", id, got[id], priority)
				}
			}
			if len(order) != len(got) {
				t.Errorf("order has %d entries, want one per entry: %v", len(order), order)
			}
		})
	}
}
