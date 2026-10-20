package state

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/tsic404/foreman/internal/registry"
)

func TestSnapshotRoundTripKeepsProjectedFieldsAndDropsSecrets(t *testing.T) {
	e := registry.TaskEntry{
		TaskID:           "t1",
		AgentID:          "a1",
		IssueID:          "i1",
		IssueIdentifier:  "iss-1",
		AutopilotRunID:   "ap-1",
		ChatSessionID:    "chat-1",
		WorkspaceID:      "ws-1",
		JobName:          "fm-t1",
		JobNamespace:     "multica-agents",
		DaemonID:         "fm-t1",
		JobRuntimeID:     "rt-1",
		NodeName:         "node-1",
		State:            registry.StateDelivering,
		Attempt:          2,
		ClaimedAt:        time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC),
		JobCreatedAt:     time.Date(2026, 10, 4, 12, 0, 1, 0, time.UTC),
		DeliveredAt:      time.Date(2026, 10, 4, 12, 0, 2, 0, time.UTC),
		Deliveries:       1,
		StartedAt:        time.Date(2026, 10, 4, 12, 0, 3, 0, time.UTC),
		BootDeadline:     time.Date(2026, 10, 4, 12, 5, 0, 0, time.UTC),
		LeaseKeepUntil:   time.Date(2026, 10, 4, 12, 0, 45, 0, time.UTC),
		LastStatusSeen:   "running",
		JobPhase:         registry.JobPhaseAlive,
		DrainDeadline:    time.Date(2026, 10, 4, 12, 1, 0, 0, time.UTC),
		DeliveryDeadline: time.Date(2026, 10, 4, 12, 0, 30, 0, time.UTC),
		// The memory-only secrets must never reach the store.
		TokenHash:       "deadbeef",
		Payload:         []byte(`{"auth_token":"secret"}`),
		DecidedReport:   []byte(`{"result":"completed"}`),
		PriorSessionID:  "sess",
		AgentName:       "agent-1",
		ProjectID:       "pid-1",
		RealRuntimeID:   "rid-1",
		RuntimeIdentity: "omp\x00",
	}
	got := FromEntry(e).ToEntry()
	if got.TaskID != e.TaskID || got.JobName != e.JobName || got.WorkspaceID != e.WorkspaceID {
		t.Fatalf("round trip lost identity: %+v", got)
	}
	if got.State != e.State || got.JobPhase != e.JobPhase || got.Deliveries != e.Deliveries {
		t.Fatalf("round trip lost lifecycle: %+v", got)
	}
	if got.ClaimedAt != e.ClaimedAt || got.DeliveredAt != e.DeliveredAt || got.BootDeadline != e.BootDeadline {
		t.Fatalf("round trip lost timestamps: %+v", got)
	}
	// The mapping record carries the delivery body, the verdict body and the
	// token digest (ADR-046 决策 0): a restart must be able to deliver the
	// claimed task and reclaim its credential. They are json:"-" in the
	// projection, so they never render in /foreman/tasks.
	if string(got.Payload) != string(e.Payload) || string(got.DecidedReport) != string(e.DecidedReport) || got.TokenHash != e.TokenHash {
		t.Fatal("the mapping record's non-projected faces were lost")
	}
	projected, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal projection: %v", err)
	}
	for _, secret := range []string{"auth_token", "decided_report", "token_hash", "deadbeef"} {
		if strings.Contains(string(projected), secret) {
			t.Fatalf("projection leaked %q: %s", secret, projected)
		}
	}
	// The claim-time dispatch record is part of the mapping record a restart
	// continues from (C4 lease addressing, S8/S26 translation).
	if got.RealRuntimeID != e.RealRuntimeID || got.RuntimeIdentity != e.RuntimeIdentity {
		t.Fatal("claim-time dispatch record was not persisted")
	}
	if got.PriorSessionID != "" {
		t.Fatal("build-only identity fields should not be persisted")
	}
}

func TestStorePerWorkspaceNamespacing(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := Config{URL: "redis://" + mr.Addr(), DialTimeout: time.Second, EntryTTL: time.Hour}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	e := registry.TaskEntry{TaskID: "t1", WorkspaceID: "ws-a", State: registry.StatePending}
	if err := s.PutEntry(ctx, "ws-a", "t1", e); err != nil {
		t.Fatalf("PutEntry: %v", err)
	}
	if _, ok, err := s.GetEntry(ctx, "ws-b", "t1"); err != nil || ok {
		t.Fatalf("cross-workspace GetEntry = %v, %v", ok, err)
	}
	got, ok, err := s.GetEntry(ctx, "ws-a", "t1")
	if err != nil || !ok || got.TaskID != "t1" {
		t.Fatalf("GetEntry = %v, %v, %v", got, ok, err)
	}
	if err := s.Revoke(ctx, "ws-a", "hash-1", time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if rev, err := s.IsRevoked(ctx, "ws-b", "hash-1"); err != nil || rev {
		t.Fatalf("cross-workspace IsRevoked = %v, %v", rev, err)
	}
	if rev, err := s.IsRevoked(ctx, "ws-a", "hash-1"); err != nil || !rev {
		t.Fatalf("IsRevoked = %v, %v", rev, err)
	}
}

// TestMalformedURLRefusesStartup keeps the fail-fast face from becoming a
// credential leak: the URLs a deployment may legitimately embed a credential
// in (§10 安全) are the ones whose parse error used to echo the whole URL.
// Each case must be refused with an error naming FOREMAN_STATE_URL and
// carrying neither the raw URL nor the credential.
func TestMalformedURLRefusesStartup(t *testing.T) {
	cases := []struct {
		name   string
		url    string
		secret string
	}{
		{"no scheme", "not-a-url", ""},
		{"invalid port after a credentialed host", "redis://:s3cret@redis:not-a-port", "s3cret"},
		{"invalid escape in the URL password", "redis://:s3cret%zz@redis:6379", "s3cret"},
		{"sentinel form with a data-node password and a bad parameter", "redis://sentinel:26379?master_name=foreman&password=s3cret&bogus=1", "s3cret"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(Config{URL: tc.url, DialTimeout: time.Second})
			if err == nil {
				t.Fatal("malformed URL accepted")
			}
			msg := err.Error()
			if !strings.Contains(msg, EnvURL) {
				t.Fatalf("startup error does not name %s: %s", EnvURL, msg)
			}
			if strings.Contains(msg, tc.url) {
				t.Fatalf("startup error echoes the raw URL: %s", msg)
			}
			if tc.secret != "" && strings.Contains(msg, tc.secret) {
				t.Fatalf("startup error leaks the credential: %s", msg)
			}
		})
	}
}

func TestStoreEntryTTLExpires(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := Config{URL: "redis://" + mr.Addr(), DialTimeout: time.Second, EntryTTL: 100 * time.Millisecond}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	e := registry.TaskEntry{TaskID: "t1", WorkspaceID: "ws-1"}
	if err := s.PutEntry(ctx, "ws-1", "t1", e); err != nil {
		t.Fatalf("PutEntry: %v", err)
	}
	mr.FastForward(200 * time.Millisecond)
	if _, ok, err := s.GetEntry(ctx, "ws-1", "t1"); err != nil || ok {
		t.Fatalf("expired entry still present: %v, %v", ok, err)
	}
}

func TestStoreSettledRecord(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := Config{URL: "redis://" + mr.Addr(), DialTimeout: time.Second, EntryTTL: time.Hour}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	e := registry.TaskEntry{TaskID: "t1", WorkspaceID: "ws-1", State: registry.StateTerminal, Result: registry.ResultCompleted}
	if err := s.PutSettled(ctx, "ws-1", "t1", e); err != nil {
		t.Fatalf("PutSettled: %v", err)
	}
	got, ok, err := s.GetSettled(ctx, "ws-1", "t1")
	if err != nil || !ok || got.Result != registry.ResultCompleted {
		t.Fatalf("GetSettled = %v, %v, %v", got, ok, err)
	}
	if err := s.DeleteSettled(ctx, "ws-1", "t1"); err != nil {
		t.Fatalf("DeleteSettled: %v", err)
	}
	if _, ok, err := s.GetSettled(ctx, "ws-1", "t1"); err != nil || ok {
		t.Fatalf("settled record survived delete: %v, %v", ok, err)
	}
}

func TestNoStoreDegradesToMisses(t *testing.T) {
	s, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if err := s.PutEntry(ctx, "ws-1", "t1", registry.TaskEntry{TaskID: "t1"}); err != nil {
		t.Fatalf("PutEntry on no-store: %v", err)
	}
	if _, ok, err := s.GetEntry(ctx, "ws-1", "t1"); err != nil || ok {
		t.Fatalf("GetEntry on no-store = %v, %v", ok, err)
	}
	if rev, err := s.IsRevoked(ctx, "ws-1", "h"); err != nil || rev {
		t.Fatalf("IsRevoked on no-store = %v, %v", rev, err)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("Ping on no-store: %v", err)
	}
}

// TestSentinelURLParsesSentinelsAndCredential locks the failover parse the
// client is built from: the URL host is the first sentinel, each addr value
// adds another, the dedicated password overrides the URL's data-node password
// and the configured dial bound applies (02-contracts.md §7: the topology is
// the deployer's; the connection face is ours).
func TestSentinelURLParsesSentinelsAndCredential(t *testing.T) {
	raw := "redis://sentinel-a:26379?master_name=foreman&addr=sentinel-b:26379&addr=sentinel-c:26379&password=url-secret"
	opts, err := failoverOptions(raw, "", 2500*time.Millisecond)
	if err != nil {
		t.Fatalf("failoverOptions: %v", err)
	}
	if opts.MasterName != "foreman" {
		t.Fatalf("MasterName = %q, want foreman", opts.MasterName)
	}
	want := []string{"sentinel-a:26379", "sentinel-b:26379", "sentinel-c:26379"}
	if len(opts.SentinelAddrs) != len(want) {
		t.Fatalf("SentinelAddrs = %v, want %v", opts.SentinelAddrs, want)
	}
	for i, addr := range want {
		if opts.SentinelAddrs[i] != addr {
			t.Fatalf("SentinelAddrs = %v, want %v", opts.SentinelAddrs, want)
		}
	}
	if opts.Password != "url-secret" {
		t.Fatalf("Password = %q, want the URL's data-node password", opts.Password)
	}
	if opts.DialTimeout != 2500*time.Millisecond {
		t.Fatalf("DialTimeout = %s, want the configured bound", opts.DialTimeout)
	}

	// The dedicated credential wins over the URL's, exactly as the
	// single-endpoint form applies it.
	opts, err = failoverOptions(raw, "dedicated", time.Second)
	if err != nil {
		t.Fatalf("failoverOptions with dedicated password: %v", err)
	}
	if opts.Password != "dedicated" {
		t.Fatalf("Password = %q, want the dedicated credential to win", opts.Password)
	}

	// rediss keeps the TLS config its parse derives; the RFC 1738 scheme must
	// not reach the failover parse, which only accepts redis/rediss.
	tlsOpts, err := failoverOptions("rediss://sentinel-a:26379?master_name=foreman", "", time.Second)
	if err != nil {
		t.Fatalf("failoverOptions(rediss): %v", err)
	}
	if tlsOpts.TLSConfig == nil {
		t.Fatal("rediss sentinel URL lost its TLS config")
	}
}

// TestSentinelURLBuildsAFailoverClient proves master_name selects the failover
// client rather than the single-endpoint one: the failover client opens with
// SENTINEL get-master-addr-by-name, which a plain Redis answers as an unknown
// command, while the same address served as a single endpoint pings fine.
func TestSentinelURLBuildsAFailoverClient(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()

	plain, err := New(Config{URL: "redis://" + mr.Addr(), DialTimeout: time.Second, EntryTTL: time.Hour})
	if err != nil {
		t.Fatalf("New(single endpoint): %v", err)
	}
	if err := plain.Ping(ctx); err != nil {
		t.Fatalf("single-endpoint Ping against the same address: %v", err)
	}

	failover, err := New(Config{
		URL:         "redis://" + mr.Addr() + "?master_name=foreman",
		DialTimeout: time.Second,
		EntryTTL:    time.Hour,
	})
	if err != nil {
		t.Fatalf("New(sentinel): %v", err)
	}
	err = failover.Ping(ctx)
	if err == nil {
		t.Fatal("sentinel URL pinged a plain Redis; the failover client was not used")
	}
	if !strings.Contains(strings.ToUpper(err.Error()), "SENTINEL") {
		t.Fatalf("sentinel Ping error = %v, want the sentinel resolution failure", err)
	}
}

// TestSentinelMisconfigurationRefusesStartup keeps the documented forms
// honest: the undeclared RFC 1738 scheme, an unknown failover parameter and
// the cluster-routing options NewFailoverClient panics on are all configuration
// errors, so the deployment never silently runs a plain client against a
// sentinel address and never takes the process down at startup.
func TestSentinelMisconfigurationRefusesStartup(t *testing.T) {
	for _, raw := range []string{
		"redis+sentinel://sentinel-a:26379?master_name=foreman",
		"redis://sentinel-a:26379?master_name=foreman&unknown_param=1",
		"redis://sentinel-a:26379?master_name=foreman&route_by_latency=true",
		"redis://sentinel-a:26379?master_name=foreman&route_randomly=true",
	} {
		if _, err := New(Config{URL: raw, DialTimeout: time.Second}); err == nil {
			t.Fatalf("New(%q) accepted a configuration it cannot honour", raw)
		}
	}
}
