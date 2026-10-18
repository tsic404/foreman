// TC-tech-job-07 implements AC-29's risk-disclosure surface: the deployment
// README carries the five fixed sentences, and the startup log carries the two
// independent declaration sentences (template face and policy face — neither
// substitutes for the other) plus each file's content hash when it was
// readable. The hash assertion is by value, not "a 64-hex string appeared":
// the logged digest must equal the sha256 of the file the process read.
package acceptance

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// tcJob07ReadmeSentences are the five literals AC-29 fixes (the same strings
// the design case greps).
var tcJob07ReadmeSentences = []string{
	"Job 内负载来自模型输出，等同于在节点上执行不可信代码",
	"Foreman 不对 Job 模板做任何校验",
	"模板内容不构成安全边界",
	"Foreman 按策略文件向 Job 写入资源与能力字段，该提权面由 Foreman 自身写入",
	"策略文件内容不构成安全边界",
}

// TestJob07RiskDisclosure pins both halves of the disclosure surface.
func TestJob07RiskDisclosure(t *testing.T) {
	readme, err := os.ReadFile(filepath.Join("..", "..", "deploy", "README.md"))
	if err != nil {
		t.Fatalf("read deploy/README.md: %v", err)
	}
	for _, sentence := range tcJob07ReadmeSentences {
		if !strings.Contains(string(readme), sentence) {
			t.Errorf("deploy/README.md is missing the fixed sentence %q", sentence)
		}
	}

	// The reference-template window: both files readable, so both declaration
	// sentences and both hash lines appear, each hash equal to its file's
	// digest.
	template := publishedTemplateContent(t)
	policy := "agents: {}\n"
	h := newHarness(t, false)
	h.useRealBuilderWithPolicy(t, writeTemplateFile(t, template), writePolicyFile(t, policy), nil)

	logs := h.logs.String()
	templateDeclaration := "Foreman 不对 Job 模板做任何校验，模板内容不构成安全边界"
	policyDeclaration := "策略文件在启动期一次性加载，运行中变更不生效"
	for _, declaration := range []string{templateDeclaration, policyDeclaration} {
		lines := 0
		for _, line := range strings.Split(logs, "\n") {
			if strings.Contains(line, declaration) {
				lines++
			}
		}
		if lines != 1 {
			t.Errorf("declaration %q appears on %d log lines, want exactly one (the two faces are independent lines)", declaration, lines)
		}
	}
	templateSum := sha256.Sum256([]byte(template))
	if want := "模板内容哈希 sha256=" + hex.EncodeToString(templateSum[:]); !strings.Contains(logs, want) {
		t.Errorf("startup log carries no %q", want)
	}
	policySum := sha256.Sum256([]byte(policy))
	if want := "策略文件内容哈希 sha256=" + hex.EncodeToString(policySum[:]); !strings.Contains(logs, want) {
		t.Errorf("startup log carries no %q", want)
	}

	// The absent policy file is the legal default: the declaration is still
	// printed, and no hash value is claimed (the missing/unreadable hash
	// wording belongs to TC-tech-job-06's startup-failure variants).
	absent := newHarness(t, false)
	absent.useRealBuilderWithPolicy(t, writeTemplateFile(t, template), filepath.Join(t.TempDir(), "absent-policy.yaml"), nil)
	absentLogs := absent.logs.String()
	if !strings.Contains(absentLogs, policyDeclaration) {
		t.Error("an absent policy file must still carry the policy-face declaration line")
	}
	if strings.Contains(absentLogs, "策略文件内容哈希 sha256=") {
		t.Error("an absent policy file must not carry a policy hash value")
	}
	if !strings.Contains(absentLogs, "模板内容哈希 sha256="+hex.EncodeToString(templateSum[:])) {
		t.Error("the template hash line must not depend on the policy file's state")
	}
}
