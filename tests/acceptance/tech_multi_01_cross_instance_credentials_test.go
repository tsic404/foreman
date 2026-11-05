// TC-tech-multi-01 implements AC-74: a Job Token issued by one instance
// authenticates and passes the resource checks on another; a revocation
// written by one instance rejects the token on the other inside the bounded
// window, while a terminal-endpoint replay of its own settled task still
// succeeds (§6.1 跨实例一致, §6.4 回收后的裁决).
package acceptance

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// repadJobToken rewrites the final character of a Job Token's signature
// segment into another character of the same six-bit group. The trailing
// padding bits are not part of the decoded signature, so the two strings carry
// the same token in two spellings.
func repadJobToken(t *testing.T, token string) string {
	t.Helper()
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	i := strings.IndexByte(alphabet, token[len(token)-1])
	if i < 0 {
		t.Fatalf("token %q does not end in a base64url character", token)
	}
	return token[:len(token)-1] + string(alphabet[i^1])
}

func TestTechMulti01CrossInstanceCredentials(t *testing.T) {
	tw := newTwoInstances(t, twoInstanceOptions{})
	tw.declareAll(t)
	tw.a.startLeader(t)

	tw.a.upstream.enqueueTask("task-1")
	tw.startClaims(t)
	tw.awaitClaimedJob(t, "task-1")

	// The credential is issued through A's builder (the instance that created
	// the Job), the daemon reaches the instance set (B), and A authenticates
	// the same token — the token is instance-independent (§6.1).
	token := tw.token(t, tw.a, "task-1")
	tw.registerVia(t, tw.b, "task-1", token)
	tw.a.upstream.setTaskStatus("task-1", "running")
	if code := tw.statusVia(t, tw.b, "task-1", token); code != http.StatusOK {
		t.Fatalf("B status with A's token = %d, want 200", code)
	}
	if code := tw.statusVia(t, tw.a, "task-1", token); code != http.StatusOK {
		t.Fatalf("A status with A's token = %d, want 200", code)
	}

	// Settle the task, then revoke on A. The terminal replay must survive the
	// revocation; a non-terminal endpoint must not (§6.4, AC-74).
	tw.startVia(t, tw.b, "task-1", token)
	tw.completeVia(t, tw.a, "task-1", token)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if e, ok := tw.entry(t, "task-1"); ok && e.IsTerminal() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task-1 never reached terminal state")
		}
		time.Sleep(5 * time.Millisecond)
	}

	tw.a.issuer.Revoke(tw.a.issuer.Hash(token), "ws-1")
	// Within the bounded window B rejects the revoked digest on a
	// non-terminal endpoint.
	if code := tw.statusVia(t, tw.b, "task-1", token); code != http.StatusUnauthorized {
		t.Fatalf("B status after A's revocation = %d, want 401", code)
	}
	// The key names the token, not one spelling of it: the signature's
	// trailing padding bits are not data, so a rewritten final character
	// decodes to the same signature. Keyed by the received string, the lookup
	// would miss the digest A wrote and answer the rewritten token on the
	// still-live drain entry (§6.1: 写入摘要 == 校验摘要).
	if code := tw.statusVia(t, tw.b, "task-1", repadJobToken(t, token)); code != http.StatusUnauthorized {
		t.Fatalf("B status with a re-spelled signature = %d, want 401", code)
	}
	// The terminal endpoint replay of the token's own task is idempotent and
	// still succeeds.
	if code, body := tw.b.call("POST", "/api/daemon/tasks/task-1/complete", token, []byte(`{"result":"completed"}`)); code != http.StatusOK {
		t.Fatalf("terminal replay after revocation = %d (%s), want 200", code, body)
	}

	// The Job was deleted exactly once by the chain (single terminal write).
	if n := tw.a.jobs.deletedCount("fm-task-1"); n != 1 {
		t.Fatalf("Job deletions = %d, want exactly one", n)
	}
}
