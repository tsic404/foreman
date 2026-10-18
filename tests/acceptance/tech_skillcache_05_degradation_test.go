// TC-tech-skillcache-05 / AC-62 (the cache only ever degrades): a deleted entry,
// an entry whose stored bytes no longer match their landing digest, and a
// restarted process each turn a resolve into a plain miss-forward — the task
// still succeeds, and a damaged body is never served.
package acceptance

import (
	"bytes"
	"testing"

	"github.com/tsic404/foreman/internal/proxy"
)

func TestSkillCache05MissingDamagedAndRestartDegradeToMiss(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.upstream.setSkillAnswer(200, `{"bundles":[{"name":"bundle-g","variant":"upstream"}]}`)
	path, token := skillTask(t, h, "task-1")
	const payload = `{"refs":["skill-g@1"]}`
	key := proxy.SkillKeyOf([]byte(payload))

	// Control: an intact entry is served, so the two faces below really are the
	// degradation of a working cache.
	seeded := []byte(`{"bundles":[{"name":"bundle-g","variant":"seeded"}]}`)
	h.bundles.Put(key, seeded, h.skillClock.now())
	code, hit := resolveSkill(t, h, path, token, payload)
	if code != 200 || !bytes.Equal(hit, seeded) {
		t.Fatalf("seeded-entry resolve = %d %s, want the entry verbatim", code, hit)
	}
	if n := h.upstream.skillCalls(); n != 0 {
		t.Fatalf("upstream resolves with an intact entry = %d, want 0", n)
	}

	// Deleted entry: a miss, forwarded.
	if !h.bundles.Delete(key) {
		t.Fatal("the seeded entry could not be deleted")
	}
	code, forwarded := resolveSkill(t, h, path, token, payload)
	if code != 200 || string(forwarded) != `{"bundles":[{"name":"bundle-g","variant":"upstream"}]}` {
		t.Fatalf("deleted-entry resolve = %d %s, want the upstream body", code, forwarded)
	}
	if n := h.upstream.skillCalls(); n != 1 {
		t.Errorf("upstream resolves after the deletion = %d, want 1", n)
	}

	// Damaged entry: the stored bytes no longer hash to the landing digest, so
	// the entry is a miss — never an answer.
	damaged := []byte(`{"bundles":[{"name":"bundle-g","variant":"damaged"}]}`)
	h.bundles.Put(key, damaged, h.skillClock.now())
	damaged[0] ^= 0xff // flip one byte of the body the store took over
	code, afterDamage := resolveSkill(t, h, path, token, payload)
	if code != 200 || string(afterDamage) != `{"bundles":[{"name":"bundle-g","variant":"upstream"}]}` {
		t.Fatalf("damaged-entry resolve = %d %s, want the upstream body", code, afterDamage)
	}
	if bytes.Equal(afterDamage, damaged) {
		t.Fatal("the damaged body was served")
	}
	if n := h.upstream.skillCalls(); n != 2 {
		t.Errorf("upstream resolves after the damage = %d, want 2", n)
	}
	if n := h.metrics.skillEvictionCount(proxy.SkillEvictDamaged); n != 1 {
		t.Errorf("damaged reclaims = %d, want 1", n)
	}
	if events := h.handlerEvents("skill.cache_entry_damaged"); len(events) != 1 {
		t.Errorf("skill.cache_entry_damaged events = %d, want 1", len(events))
	}

	// The task still succeeds: the cache only ever degrades.
	completeSkillTask(t, h, "task-1", token)

	// A restarted process starts empty: the same resolve is a plain forward.
	restarted := newHarness(t, false)
	restarted.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	restarted.declare(declaredRuntimes())
	restarted.upstream.setSkillAnswer(200, `{"bundles":[{"name":"bundle-g","variant":"upstream"}]}`)
	restartPath, restartToken := skillTask(t, restarted, "task-1")
	code, forwarded = resolveSkill(t, restarted, restartPath, restartToken, payload)
	if code != 200 || string(forwarded) != `{"bundles":[{"name":"bundle-g","variant":"upstream"}]}` {
		t.Fatalf("post-restart resolve = %d %s, want the upstream body", code, forwarded)
	}
	if n := restarted.upstream.skillCalls(); n != 1 {
		t.Errorf("post-restart upstream resolves = %d, want 1", n)
	}
	if n := restarted.metrics.skillResultCount(proxy.SkillResultHit); n != 0 {
		t.Errorf("post-restart hit count = %d, want 0", n)
	}
	completeSkillTask(t, restarted, "task-1", restartToken)
}
