package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func probeFor(jobName string) ProbeSession {
	return ProbeSession{
		JobName:       jobName,
		RequestID:     "req-" + jobName,
		SelfMintedID:  "",
		TokenHash:     "hash-" + jobName,
		JobRuntime:    "rt-" + jobName,
		RealRuntimeID: "real-" + jobName,
		Deadline:      testNow.Add(time.Minute),
		CLIVersion:    "0.0.0",
		Runtimes: []DiscoveredRuntime{
			{Name: "claude", Type: "anthropic", Version: "1"},
		},
		FailedProfiles: []json.RawMessage{json.RawMessage(`{"profile":"p"}`)},
	}
}

func TestProbePutGetDelete(t *testing.T) {
	r := NewProbeRegistry()
	if err := r.PutProbe(ProbeSession{JobRuntime: "rt"}); err == nil {
		t.Fatal("PutProbe without job_name accepted")
	}
	s := probeFor("fm-catalog-1")
	if err := r.PutProbe(s); err != nil {
		t.Fatalf("PutProbe: %v", err)
	}
	got, ok := r.ProbeByJob("fm-catalog-1")
	if !ok || got.RequestID != "req-fm-catalog-1" || got.TokenHash != "hash-fm-catalog-1" {
		t.Fatalf("ProbeByJob = %+v, %v", got, ok)
	}
	if got, ok := r.ProbeByRuntime("rt-fm-catalog-1"); !ok || got.JobName != "fm-catalog-1" {
		t.Fatalf("ProbeByRuntime = %+v, %v", got, ok)
	}
	if _, ok := r.ProbeByJob("unknown"); ok {
		t.Fatal("ProbeByJob hit for unknown job")
	}
	if _, ok := r.ProbeByRuntime("unknown"); ok {
		t.Fatal("ProbeByRuntime hit for unknown runtime")
	}

	if err := r.DeleteProbe("fm-catalog-1"); err != nil {
		t.Fatalf("DeleteProbe: %v", err)
	}
	if err := r.DeleteProbe("fm-catalog-1"); err != nil {
		t.Fatalf("repeated DeleteProbe: %v", err)
	}
	if err := r.DeleteProbe("never-seen"); err != nil {
		t.Fatalf("DeleteProbe of absent session: %v", err)
	}
	if _, ok := r.ProbeByJob("fm-catalog-1"); ok {
		t.Fatal("session survived DeleteProbe")
	}
	if _, ok := r.ProbeByRuntime("rt-fm-catalog-1"); ok {
		t.Fatal("runtime index survived DeleteProbe")
	}
	if n := len(r.ListProbes()); n != 0 {
		t.Fatalf("ListProbes len = %d, want 0", n)
	}
}

func TestProbeRuntimeIndexFollowsSession(t *testing.T) {
	r := NewProbeRegistry()
	bare := probeFor("fm-catalog-1")
	bare.JobRuntime = ""
	if err := r.PutProbe(bare); err != nil {
		t.Fatalf("PutProbe without runtime: %v", err)
	}
	if _, ok := r.ProbeByJob("fm-catalog-1"); !ok {
		t.Fatal("session without runtime is not readable by job")
	}
	if _, ok := r.ProbeByRuntime(""); ok {
		t.Fatal("empty runtime id resolved a session")
	}

	// Replacing the session moves the index off the old runtime id.
	named := probeFor("fm-catalog-1")
	if err := r.PutProbe(named); err != nil {
		t.Fatalf("PutProbe: %v", err)
	}
	if n := len(r.ListProbes()); n != 1 {
		t.Fatalf("ListProbes len = %d, want 1", n)
	}
	empty := probeFor("fm-catalog-1")
	empty.JobRuntime = ""
	if err := r.PutProbe(empty); err != nil {
		t.Fatalf("PutProbe: %v", err)
	}
	if _, ok := r.ProbeByRuntime("rt-fm-catalog-1"); ok {
		t.Fatal("runtime index kept an id the session no longer carries")
	}
}

// TestProbeUpdateFailsClosedOnStoreError pins the shared write face: a store
// error is not an update, so the caller must not believe the session landed.
// The local row is seeded first, so a fallback to the local cache would be
// caught rather than passing as the same (empty, false) result.
func TestProbeUpdateFailsClosedOnStoreError(t *testing.T) {
	r := NewProbeRegistry()
	if err := r.PutProbe(ProbeSession{JobName: "fm-catalog-1", JobRuntime: "rt-fm-catalog-1"}); err != nil {
		t.Fatalf("PutProbe: %v", err)
	}
	r.AttachShared(failingProbeStore{})

	updated, ok := r.UpdateProbe("fm-catalog-1", func(s *ProbeSession) {
		s.NodeName = "node-a"
	})
	if ok {
		t.Fatal("an unpersisted probe session update was reported as applied")
	}
	if updated.JobName != "" {
		t.Fatalf("failed update returned a session: %+v", updated)
	}
}

// failingProbeStore is a shared face whose reads fail: the update must report
// the write as unapplied rather than fall back to the local cache.
type failingProbeStore struct {
	ProbeStore
}

func (failingProbeStore) GetProbeSession(context.Context, string) ([]byte, bool, error) {
	return nil, false, errors.New("shared state unavailable")
}

func TestProbeUpdate(t *testing.T) {
	r := NewProbeRegistry()
	if got, ok := r.UpdateProbe("missing", func(*ProbeSession) {}); ok || got.JobName != "" {
		t.Fatalf("UpdateProbe of absent session = %+v, %v", got, ok)
	}
	if err := r.PutProbe(probeFor("fm-catalog-1")); err != nil {
		t.Fatalf("PutProbe: %v", err)
	}

	got, ok := r.UpdateProbe("fm-catalog-1", func(s *ProbeSession) {
		s.NodeName = "node-a"
		s.RelayedCode = 200
		s.Runtimes = append(s.Runtimes, DiscoveredRuntime{Name: "codex"})
		s.JobName = "ignored-rename"
	})
	if !ok {
		t.Fatal("UpdateProbe reported the session as gone")
	}
	if got.NodeName != "node-a" || got.RelayedCode != 200 || len(got.Runtimes) != 2 {
		t.Fatalf("UpdateProbe result = %+v", got)
	}
	if got.JobName != "fm-catalog-1" {
		t.Fatalf("UpdateProbe renamed the session to %q", got.JobName)
	}
	stored, _ := r.ProbeByJob("fm-catalog-1")
	if stored.NodeName != "node-a" || stored.RelayedCode != 200 || len(stored.Runtimes) != 2 {
		t.Fatalf("stored session = %+v", stored)
	}
	if _, ok := r.ProbeByJob("ignored-rename"); ok {
		t.Fatal("UpdateProbe inserted a second row")
	}

	// A mutation that moves JobRuntime repairs the reverse index.
	if _, ok := r.UpdateProbe("fm-catalog-1", func(s *ProbeSession) { s.JobRuntime = "rt-new" }); !ok {
		t.Fatal("UpdateProbe reported the session as gone")
	}
	if _, ok := r.ProbeByRuntime("rt-fm-catalog-1"); ok {
		t.Fatal("reverse index kept the pre-update runtime id")
	}
	if got, ok := r.ProbeByRuntime("rt-new"); !ok || got.JobName != "fm-catalog-1" {
		t.Fatalf("ProbeByRuntime after update = %+v, %v", got, ok)
	}
	// ... and dropping the runtime id drops the index row.
	if _, ok := r.UpdateProbe("fm-catalog-1", func(s *ProbeSession) { s.JobRuntime = "" }); !ok {
		t.Fatal("UpdateProbe reported the session as gone")
	}
	if _, ok := r.ProbeByRuntime("rt-new"); ok {
		t.Fatal("reverse index kept a cleared runtime id")
	}
}

func TestProbeLookupsCopy(t *testing.T) {
	r := NewProbeRegistry()
	if err := r.PutProbe(probeFor("fm-catalog-1")); err != nil {
		t.Fatalf("PutProbe: %v", err)
	}

	byJob, _ := r.ProbeByJob("fm-catalog-1")
	byJob.NodeName = "mutated"
	byJob.TokenHash = "mutated"
	byJob.Runtimes[0].Name = "mutated"
	byJob.FailedProfiles[0][0] = 'X'
	byJob.Runtimes = append(byJob.Runtimes, DiscoveredRuntime{Name: "extra"})

	byRuntime, _ := r.ProbeByRuntime("rt-fm-catalog-1")
	byRuntime.NodeName = "mutated"
	byRuntime.Runtimes[0].Name = "mutated"

	updated, _ := r.UpdateProbe("fm-catalog-1", func(*ProbeSession) {})
	updated.NodeName = "mutated"
	updated.FailedProfiles[0][0] = 'X'

	listed := r.ListProbes()
	if len(listed) != 1 {
		t.Fatalf("ListProbes len = %d, want 1", len(listed))
	}
	listed[0].JobName = "mutated"
	listed[0].Runtimes[0].Name = "mutated"

	stored, _ := r.ProbeByJob("fm-catalog-1")
	want := probeFor("fm-catalog-1")
	if stored.JobName != want.JobName || stored.NodeName != "" || stored.TokenHash != want.TokenHash {
		t.Fatalf("scalar fields written through a lookup: %+v", stored)
	}
	if len(stored.Runtimes) != 1 || stored.Runtimes[0] != want.Runtimes[0] {
		t.Fatalf("Runtimes written through a lookup: %+v", stored.Runtimes)
	}
	if !bytes.Equal(stored.FailedProfiles[0], want.FailedProfiles[0]) {
		t.Fatalf("FailedProfiles written through a lookup: %s", stored.FailedProfiles[0])
	}

	// The session handed to PutProbe is not aliased either.
	fresh := probeFor("fm-catalog-2")
	rt := fresh.Runtimes
	if err := r.PutProbe(fresh); err != nil {
		t.Fatalf("PutProbe: %v", err)
	}
	rt[0].Name = "mutated"
	fresh.FailedProfiles[0][0] = 'X'
	stored, _ = r.ProbeByJob("fm-catalog-2")
	if stored.Runtimes[0].Name == "mutated" || stored.FailedProfiles[0][0] == 'X' {
		t.Fatalf("caller's slices aliased into the table: %+v", stored)
	}
}

func TestStartupCatalogPutTakeSnapshot(t *testing.T) {
	c := NewStartupCatalog()
	body := []byte(`{"runtimes":[{"name":"claude"}]}`)
	c.Put("anthropic", "", body)
	c.Put("openai", "", []byte(`{"runtimes":[]}`))
	c.Put("anthropic", "prof-1", []byte(`{"runtimes":[{"profile_id":"prof-1"}]}`))

	snap := c.Snapshot()
	if len(snap) != 3 || !bytes.Equal(snap[RuntimeIdentityKey("anthropic", "")], body) {
		t.Fatalf("Snapshot = %q", snap)
	}
	snap[RuntimeIdentityKey("anthropic", "")][0] = 'X'
	snap["injected"] = []byte("x")
	if again := c.Snapshot(); len(again) != 3 || !bytes.Equal(again[RuntimeIdentityKey("anthropic", "")], body) {
		t.Fatalf("Snapshot aliased the catalog: %q", again)
	}

	// Put copies the caller's buffer.
	body[0] = 'Y'
	if got, ok := c.Take("openai", ""); !ok {
		t.Fatalf("Take after buffer reuse = %q, %v", got, ok)
	}

	got, ok := c.Take("anthropic", "")
	if !ok || !bytes.Equal(got, []byte(`{"runtimes":[{"name":"claude"}]}`)) {
		t.Fatalf("Take = %s, %v", got, ok)
	}
	if got, ok := c.Take("anthropic", ""); ok || got != nil {
		t.Fatalf("second Take = %s, %v", got, ok)
	}
	// The builtin and the profile entry are separate identities.
	if got, ok := c.Take("anthropic", "prof-1"); !ok || !bytes.Contains(got, []byte("prof-1")) {
		t.Fatalf("profile Take = %s, %v", got, ok)
	}
	if got, ok := c.Take("never-seen", ""); ok || got != nil {
		t.Fatalf("Take of unknown identity = %s, %v", got, ok)
	}
	if n := len(c.Snapshot()); n != 0 {
		t.Fatalf("Snapshot after Take = %d entries", n)
	}
}
