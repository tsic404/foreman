// TC-tech-skillcache-02 / AC-60 (key identity and its negative): the cache key
// is the request payload bytes alone. The path's runtime id never enters it, so
// the same payload from another task hits; any byte change is a different key,
// so it misses — a different answer can never be served for changed bytes, and
// the bytes are never normalized (§1.8 键定义, F10).
package acceptance

import (
	"bytes"
	"testing"

	"github.com/tsic404/foreman/internal/proxy"
)

func TestSkillCache02RuntimeIDDoesNotEnterTheKey(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.upstream.setSkillAnswer(200, `{"bundles":[{"name":"bundle-c","variant":1}]}`)

	firstPath, firstToken := skillTask(t, h, "task-1")
	secondPath, secondToken := skillTask(t, h, "task-2")
	const payload = `{"refs":["skill-c@1"]}`

	code, first := resolveSkill(t, h, firstPath, firstToken, payload)
	if code != 200 {
		t.Fatalf("first S26 = %d (%s), want 200", code, first)
	}
	if n := h.upstream.skillCalls(); n != 1 {
		t.Fatalf("upstream resolves after the miss = %d, want 1", n)
	}

	// Same payload, a different job runtime id in the path: the same key.
	code, body := resolveSkill(t, h, secondPath, secondToken, payload)
	if code != 200 || !bytes.Equal(body, first) {
		t.Fatalf("resolve under another runtime = %d %s, want the first body verbatim", code, body)
	}
	if n := h.upstream.skillCalls(); n != 1 {
		t.Errorf("upstream resolves after the other-runtime request = %d, want 1 (the runtime id is not part of the key)", n)
	}
	if n := h.metrics.skillResultCount(proxy.SkillResultHit); n != 1 {
		t.Errorf("hit count = %d, want 1", n)
	}
}

func TestSkillCache02ChangedPayloadBytesMiss(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.upstream.setSkillAnswer(200, `{"bundles":[{"name":"bundle-c","variant":1}]}`)
	path, token := skillTask(t, h, "task-1")

	const payload = `{"refs":["skill-c@1"]}`
	if code, _ := resolveSkill(t, h, path, token, payload); code != 200 {
		t.Fatalf("first S26 = %d, want 200", code)
	}

	// A different reference set is a different payload: the upstream answers it
	// with a different body, and the cache must not serve the old one.
	h.upstream.setSkillAnswer(200, `{"bundles":[{"name":"bundle-c","variant":2}]}`)
	code, changed := resolveSkill(t, h, path, token, `{"refs":["skill-c@2"]}`)
	if code != 200 {
		t.Fatalf("changed-payload S26 = %d, want 200", code)
	}
	if string(changed) != `{"bundles":[{"name":"bundle-c","variant":2}]}` {
		t.Fatalf("changed-payload answer = %s, want the upstream's own body", changed)
	}
	if n := h.upstream.skillCalls(); n != 2 {
		t.Errorf("upstream resolves after the changed payload = %d, want 2 (changed bytes miss)", n)
	}

	// The first payload's entry is untouched: it still answers with variant 1.
	code, again := resolveSkill(t, h, path, token, payload)
	if code != 200 || string(again) != `{"bundles":[{"name":"bundle-c","variant":1}]}` {
		t.Fatalf("first-payload resolve = %d %s, want its own cached body", code, again)
	}
	if n := h.upstream.skillCalls(); n != 2 {
		t.Errorf("upstream resolves after the first payload's hit = %d, want 2", n)
	}

	// Semantically equal but byte-different payloads are different keys: the
	// cache compares bytes, it never normalizes key order or whitespace.
	code, reordered := resolveSkill(t, h, path, token, `{ "refs" : ["skill-c@1"] }`)
	if code != 200 || string(reordered) != `{"bundles":[{"name":"bundle-c","variant":2}]}` {
		t.Fatalf("reordered-payload resolve = %d %s, want the upstream's own answer", code, reordered)
	}
	if n := h.upstream.skillCalls(); n != 3 {
		t.Errorf("upstream resolves after the reordered payload = %d, want 3 (bytes, not content, are the key)", n)
	}
}
