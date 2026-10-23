package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/tsic404/foreman/internal/registry"
)

// Key namespaces of the shared store. A single shared store carries every
// workspace's entries and revocations; the keys are namespaced per workspace
// so one workspace's rows can never adjudicate another's (§0 术语, F11 多
// workspace 口径). The workspace id itself is a config value, never a
// deployment constant, and the same store instance serves all workspaces of
// one deployment.
const (
	keyEntry  = "foreman:entry:"      // + workspace + ":" + task_id
	keyRevoke = "foreman:revocation:" // + workspace + ":" + token hash
	keyDone   = "foreman:done:"       // + workspace + ":" + task_id (settled record)
)

// Store is the shared state face stage one needs: entry snapshots, settled
// terminal records and the revocation set, all keyed by workspace. The
// interface is what the registry and the auth issuer consume; the Redis
// implementation is in redis.go.
type Store interface {
	// PutEntry stores the live row's snapshot under its workspace and task
	// id with the configured retention.
	PutEntry(ctx context.Context, ws, taskID string, e registry.TaskEntry) error
	// GetEntry reads the live row of one task. ok=false means the store
	// holds no snapshot (never written or expired). Reserved for stage two's
	// partitioned lookups (stage one restores through ListEntries and then
	// reads its in-memory index).
	GetEntry(ctx context.Context, ws, taskID string) (registry.TaskEntry, bool, error)
	// ListEntries lists every live snapshot (the startup-restore scan). The
	// entries carry their workspace; the scan crosses workspaces only so a
	// single-instance restore can re-home every row it finds.
	ListEntries(ctx context.Context) ([]registry.TaskEntry, error)
	// DeleteEntry removes one live row.
	DeleteEntry(ctx context.Context, ws, taskID string) error

	// PutSettled stores the settled terminal record of one task.
	PutSettled(ctx context.Context, ws, taskID string, e registry.TaskEntry) error
	// GetSettled reads the settled terminal record of one task.
	GetSettled(ctx context.Context, ws, taskID string) (registry.TaskEntry, bool, error)
	// ListSettled lists every settled terminal record (the startup-restore
	// scan of the post-reclaim answer face).
	ListSettled(ctx context.Context) ([]registry.TaskEntry, error)
	// DeleteSettled removes one settled record. Reserved for stage two's
	// explicit retention sweep (stage one relies on the store TTL and the
	// registry's done-index expiry).
	DeleteSettled(ctx context.Context, ws, taskID string) error

	// Revoke records a revoked token digest until validUntil.
	Revoke(ctx context.Context, ws, hash string, validUntil time.Time) error
	// IsRevoked answers the revocation check for one digest.
	IsRevoked(ctx context.Context, ws, hash string) (bool, error)
	// ListRevoked lists the live revocations grouped by workspace
	// (workspace → digest → valid-until) so a restart can seed the in-process
	// positive cache without flattening the partition (§6.1). The cache only
	// ever holds revoked digests — the safe direction: it can reject a token
	// early, never accept one the shared set revoked.
	ListRevoked(ctx context.Context) (map[string]map[string]time.Time, error)

	// Ping verifies the store is reachable; a failed ping is the startup
	// signal that the store is down (the restore then degrades to the K8s
	// rebuild, §4.3).
	Ping(ctx context.Context) error
}

// Snapshot is the serialized form of one mapping row in the shared store.
// The mapping record carries the delivery body (交付体), the verdict body
// (定论体) and the token digest (ADR-046 决策 0: 任务映射记录含交付体与定论
// 体): a restart must be able to deliver an already-claimed task and to keep
// the credential's reclaim handle, so those fields persist here even though
// they are json:"-" in the ops projection and never reach a K8s object or a
// log (F3, §4.3: 任务载荷存在共享状态的条目里). The store is therefore treated
// as a credential store by the deployment (§10 安全).
type Snapshot struct {
	TaskID              string    `json:"task_id"`
	AgentID             string    `json:"agent_id"`
	IssueID             string    `json:"issue_id"`
	IssueIdentifier     string    `json:"issue_identifier"`
	AutopilotRunID      string    `json:"autopilot_run_id"`
	ChatSessionID       string    `json:"chat_session_id"`
	WorkspaceID         string    `json:"workspace_id"`
	JobName             string    `json:"job_name"`
	JobNamespace        string    `json:"job_namespace"`
	DaemonID            string    `json:"daemon_id"`
	JobRuntimeID        string    `json:"job_runtime_id"`
	NodeName            string    `json:"node_name"`
	State               string    `json:"state"`
	Result              string    `json:"result"`
	FailureReason       string    `json:"failure_reason"`
	Attempt             int       `json:"attempt"`
	ClaimedAt           time.Time `json:"claimed_at"`
	JobCreatedAt        time.Time `json:"job_created_at"`
	DaemonRegisteredAt  time.Time `json:"daemon_registered_at"`
	DeliveredAt         time.Time `json:"delivered_at"`
	Deliveries          int       `json:"deliveries"`
	ClaimsAfterDelivery int       `json:"claims_after_delivery"`
	StartedAt           time.Time `json:"started_at"`
	BootDeadline        time.Time `json:"boot_deadline"`
	LeaseKeepUntil      time.Time `json:"lease_keep_until"`
	LastStatusSeen      string    `json:"last_status_seen"`
	JobPhase            string    `json:"job_phase"`
	DrainDeadline       time.Time `json:"drain_deadline"`
	DeliveryDeadline    time.Time `json:"delivery_deadline"`
	CompletedAt         time.Time `json:"completed_at"`

	// The mapping record's non-projected faces (ADR-046 决策 0): the delivery
	// body (§4.3: 任务载荷存在共享状态的条目里), the instance's decided
	// terminal body (§4.4) and the credential digest the reclaim path
	// revokes (ADR-017). All three are json:"-" in TaskEntry's projection —
	// they never reach a K8s object, a log or /foreman/tasks — but they are
	// exactly what a restart needs to keep delivering and reclaiming.
	Payload       json.RawMessage `json:"payload,omitempty"`
	DecidedReport []byte          `json:"decided_report,omitempty"`
	TokenHash     string          `json:"token_hash,omitempty"`

	// The claim-time dispatch record (§4.1's non-projected fields). A
	// restored pre-delivery entry keeps renewing its prepare lease (C4) and
	// translating S8/S26 through the runtime it was dispatched under; without
	// these the lease keeper skips the entry and the server's lease lapses
	// into a redispatch. Memory-only in the projection, persisted here because
	// the mapping record is what the restart continues from.
	RealRuntimeID   string `json:"real_runtime_id,omitempty"`
	RuntimeIdentity string `json:"runtime_identity,omitempty"`
}

// FromEntry serializes a live row into its shared-store snapshot.
func FromEntry(e registry.TaskEntry) Snapshot {
	return Snapshot{
		TaskID:              e.TaskID,
		AgentID:             e.AgentID,
		IssueID:             e.IssueID,
		IssueIdentifier:     e.IssueIdentifier,
		AutopilotRunID:      e.AutopilotRunID,
		ChatSessionID:       e.ChatSessionID,
		WorkspaceID:         e.WorkspaceID,
		JobName:             e.JobName,
		JobNamespace:        e.JobNamespace,
		DaemonID:            e.DaemonID,
		JobRuntimeID:        e.JobRuntimeID,
		NodeName:            e.NodeName,
		State:               string(e.State),
		Result:              string(e.Result),
		FailureReason:       e.FailureReason,
		Attempt:             e.Attempt,
		ClaimedAt:           e.ClaimedAt,
		JobCreatedAt:        e.JobCreatedAt,
		DaemonRegisteredAt:  e.DaemonRegisteredAt,
		DeliveredAt:         e.DeliveredAt,
		Deliveries:          e.Deliveries,
		ClaimsAfterDelivery: e.ClaimsAfterDelivery,
		StartedAt:           e.StartedAt,
		BootDeadline:        e.BootDeadline,
		LeaseKeepUntil:      e.LeaseKeepUntil,
		LastStatusSeen:      e.LastStatusSeen,
		JobPhase:            string(e.JobPhase),
		DrainDeadline:       e.DrainDeadline,
		DeliveryDeadline:    e.DeliveryDeadline,
		CompletedAt:         e.CompletedAt,
		Payload:             e.Payload,
		DecidedReport:       e.DecidedReport,
		TokenHash:           e.TokenHash,
		RealRuntimeID:       e.RealRuntimeID,
		RuntimeIdentity:     e.RuntimeIdentity,
	}
}

// ToEntry rebuilds the live row from a snapshot, including the mapping
// record's non-projected faces (the delivery body, the verdict body and the
// token digest) — that is what makes a restarted instance able to keep
// delivering and reclaiming an in-flight task (§4.3, AC-73).
func (s Snapshot) ToEntry() registry.TaskEntry {
	return registry.TaskEntry{
		TaskID:              s.TaskID,
		AgentID:             s.AgentID,
		IssueID:             s.IssueID,
		IssueIdentifier:     s.IssueIdentifier,
		AutopilotRunID:      s.AutopilotRunID,
		ChatSessionID:       s.ChatSessionID,
		WorkspaceID:         s.WorkspaceID,
		JobName:             s.JobName,
		JobNamespace:        s.JobNamespace,
		DaemonID:            s.DaemonID,
		JobRuntimeID:        s.JobRuntimeID,
		NodeName:            s.NodeName,
		State:               registry.State(s.State),
		Result:              registry.Result(s.Result),
		FailureReason:       s.FailureReason,
		Attempt:             s.Attempt,
		ClaimedAt:           s.ClaimedAt,
		JobCreatedAt:        s.JobCreatedAt,
		DaemonRegisteredAt:  s.DaemonRegisteredAt,
		DeliveredAt:         s.DeliveredAt,
		Deliveries:          s.Deliveries,
		ClaimsAfterDelivery: s.ClaimsAfterDelivery,
		StartedAt:           s.StartedAt,
		BootDeadline:        s.BootDeadline,
		LeaseKeepUntil:      s.LeaseKeepUntil,
		LastStatusSeen:      s.LastStatusSeen,
		JobPhase:            registry.JobPhase(s.JobPhase),
		DrainDeadline:       s.DrainDeadline,
		DeliveryDeadline:    s.DeliveryDeadline,
		CompletedAt:         s.CompletedAt,
		Payload:             s.Payload,
		DecidedReport:       s.DecidedReport,
		TokenHash:           s.TokenHash,
		RealRuntimeID:       s.RealRuntimeID,
		RuntimeIdentity:     s.RuntimeIdentity,
	}
}

var _ Store = noStore{}

// noStore is the Store a deployment without FOREMAN_STATE_URL gets: every
// operation is a no-op or a miss, so the process behaves exactly like the
// pre-stage-one code (in-memory index + K8s rebuild) — the shared state is
// an optional persistence face, never a hard dependency at stage one.
type noStore struct{}

func (noStore) PutEntry(context.Context, string, string, registry.TaskEntry) error { return nil }
func (noStore) GetEntry(context.Context, string, string) (registry.TaskEntry, bool, error) {
	return registry.TaskEntry{}, false, nil
}
func (noStore) ListEntries(context.Context) ([]registry.TaskEntry, error) { return nil, nil }
func (noStore) DeleteEntry(context.Context, string, string) error         { return nil }
func (noStore) PutSettled(context.Context, string, string, registry.TaskEntry) error {
	return nil
}
func (noStore) GetSettled(context.Context, string, string) (registry.TaskEntry, bool, error) {
	return registry.TaskEntry{}, false, nil
}
func (noStore) ListSettled(context.Context) ([]registry.TaskEntry, error) { return nil, nil }
func (noStore) DeleteSettled(context.Context, string, string) error       { return nil }
func (noStore) Revoke(context.Context, string, string, time.Time) error   { return nil }
func (noStore) IsRevoked(context.Context, string, string) (bool, error)   { return false, nil }
func (noStore) ListRevoked(context.Context) (map[string]map[string]time.Time, error) {
	return nil, nil
}
func (noStore) Ping(context.Context) error { return nil }

// New returns the Store implementation for cfg. An empty URL yields the
// no-op store (the deployment did not configure the shared state face); a
// URL that cannot be parsed is a configuration error and refuses startup
// (fail-fast, §1.5) rather than running without the persistence face.
func New(cfg Config, opts ...Option) (Store, error) {
	if cfg.URL == "" {
		return noStore{}, nil
	}
	rc := &redisStore{cfg: cfg}
	for _, opt := range opts {
		opt(rc)
	}
	if err := rc.init(); err != nil {
		return nil, err
	}
	return rc, nil
}

// Option customizes a redisStore (tests).
type Option func(*redisStore)

// WithClock overrides the clock (tests).
func WithClock(now func() time.Time) Option {
	return func(s *redisStore) { s.now = now }
}

// WithEventHook observes the two shared-state availability transitions
// (§9.1 共享状态面不可用与恢复): event is "unavailable" (err carries the first
// failure) or "recovered" (downFor is the outage duration).
func WithEventHook(fn func(event string, downFor time.Duration, err error)) Option {
	return func(s *redisStore) { s.event = fn }
}

// redisStore is the Redis-backed shared state implementation. It uses the
// go-redis client, whose connection information is the URL + password pair
// from the config (the deployment interface list carries both, §7).
type redisStore struct {
	cfg Config
	now func() time.Time
	rdb *redis.Client
	// downSince is the Unix-nano instant of the first failed operation since
	// the last success (0 = available); the outage face of §10.
	downSince int64
	// resSeq numbers slot reservations within this process.
	resSeq atomic.Int64
	// event observes the availability transitions (§9.1 共享状态面不可用与
	// 恢复).
	event func(event string, downFor time.Duration, err error)
	// detached counts the operations still running after their caller's wait
	// ended (§10 判定).
	detached opTracker
}

func (s *redisStore) init() error {
	if s.now == nil {
		s.now = time.Now
	}
	rdb, err := s.client()
	if err != nil {
		return err
	}
	s.rdb = rdb
	return nil
}

// client builds the client the URL names. A URL carrying master_name names a
// sentinel topology: it is served by a failover client that resolves the
// master through the sentinels and follows a failover, so the deployment can
// upgrade availability without changing the contract (§7). Every other URL is
// the single-endpoint form. Both take the credential the same way — the
// dedicated password wins over the URL's own data-node password — and the
// configured dial bound.
func (s *redisStore) client() (*redis.Client, error) {
	if sentinelMaster(s.cfg.URL) == "" {
		opts, err := redis.ParseURL(s.cfg.URL)
		if err != nil {
			return nil, parseError(err)
		}
		if s.cfg.Password != "" {
			opts.Password = s.cfg.Password
		}
		opts.DialTimeout = s.cfg.DialTimeout
		// Without this go-redis swaps the command's context for
		// context.Background() and bounds the socket by ReadTimeout alone, so
		// the store-side answer bound would never reach the operation (§10
		// 共享状态不可用的判定; ADR-054 决策 1).
		opts.ContextTimeoutEnabled = true
		return redis.NewClient(opts), nil
	}
	opts, err := failoverOptions(s.cfg.URL, s.cfg.Password, s.cfg.DialTimeout)
	if err != nil {
		return nil, err
	}
	return redis.NewFailoverClient(opts), nil
}

// parseError wraps a client-URL parse failure without the raw URL. go-redis
// hands back url.Parse's *url.Error, whose text is `parse "<raw url>": <reason>`
// — and the URL may embed the data-node credential (§10 安全; the same reason
// stateEndpoint strips userinfo before logging the endpoint). Only the reason
// is kept, so the startup error names the variable, never its value.
func parseError(err error) error {
	var uerr *url.Error
	if errors.As(err, &uerr) {
		err = uerr.Err
	}
	return fmt.Errorf("shared state: parse %s: %v", EnvURL, err)
}

// failoverOptions parses a sentinel URL and applies the module's credential
// and dial bound to it, so the parse the client is built from is what the
// tests assert.
func failoverOptions(raw, password string, dialTimeout time.Duration) (*redis.FailoverOptions, error) {
	opts, err := redis.ParseFailoverURL(raw)
	if err != nil {
		return nil, parseError(err)
	}
	// NewFailoverClient panics on these two: the routing variants are cluster
	// clients' options. A deployment value must fail startup with a message,
	// never take the process down (§1.5).
	if opts.RouteByLatency || opts.RouteRandomly {
		return nil, fmt.Errorf("shared state: parse %s: route_by_latency/route_randomly need the cluster form, which this client does not serve", EnvURL)
	}
	if password != "" {
		opts.Password = password
	}
	opts.DialTimeout = dialTimeout
	// Same reason as the single-endpoint client: the store-side bound only
	// bounds the operation when go-redis lets the command context reach the
	// socket.
	opts.ContextTimeoutEnabled = true
	return opts, nil
}

// sentinelMaster returns the master name the URL names, or "" when the URL is
// the single-endpoint form. master_name is go-redis's own failover marker: the
// URL's host is a sentinel, each addr query value adds another sentinel, and
// the client resolves that master name to the current master on every
// failover (§7: the topology is the deployer's, the connection face is ours).
// An unparsable URL yields "" and is then rejected by the single-endpoint
// parse, which reports the syntax error.
func sentinelMaster(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Query().Get("master_name")
}

// marshal and unmarshal are the snapshot codec: JSON, with the retention
// TTL applied at write.
func (s *redisStore) marshal(e registry.TaskEntry) ([]byte, error) {
	return json.Marshal(FromEntry(e))
}

func (s *redisStore) unmarshal(raw []byte) (registry.TaskEntry, error) {
	var snap Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return registry.TaskEntry{}, err
	}
	return snap.ToEntry(), nil
}

func (s *redisStore) entryKey(ws, taskID string) string {
	return keyEntry + ws + ":" + taskID
}
func (s *redisStore) doneKey(ws, taskID string) string {
	return keyDone + ws + ":" + taskID
}
func (s *redisStore) revokeKey(ws, hash string) string {
	return keyRevoke + ws + ":" + hash
}

func (s *redisStore) putEntry(ctx context.Context, ws, taskID string, e registry.TaskEntry) error {
	raw, err := s.marshal(e)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, s.entryKey(ws, taskID), raw, s.cfg.EntryTTL).Err()
}

func (s *redisStore) getEntry(ctx context.Context, ws, taskID string) (registry.TaskEntry, bool, error) {
	raw, err := s.rdb.Get(ctx, s.entryKey(ws, taskID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return registry.TaskEntry{}, false, nil
	}
	if err != nil {
		return registry.TaskEntry{}, false, err
	}
	e, err := s.unmarshal(raw)
	return e, err == nil, err
}

// ListEntries scans the workspace's entry namespace. The scan cursor is
// bounded by the client's own iteration; the workspace prefix is what keeps
// one workspace's scan from touching another's rows.
func (s *redisStore) listEntries(ctx context.Context) ([]registry.TaskEntry, error) {
	var out []registry.TaskEntry
	iter := s.rdb.Scan(ctx, 0, keyEntry+"*", 256).Iterator()
	for iter.Next(ctx) {
		raw, err := s.rdb.Get(ctx, iter.Val()).Bytes()
		if err != nil {
			continue // a row deleted between scan and get is not an error
		}
		e, err := s.unmarshal(raw)
		if err != nil {
			return nil, fmt.Errorf("shared state: corrupt entry %s: %w", iter.Val(), err)
		}
		out = append(out, e)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *redisStore) putSettled(ctx context.Context, ws, taskID string, e registry.TaskEntry) error {
	raw, err := s.marshal(e)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, s.doneKey(ws, taskID), raw, s.cfg.EntryTTL).Err()
}

func (s *redisStore) getSettled(ctx context.Context, ws, taskID string) (registry.TaskEntry, bool, error) {
	raw, err := s.rdb.Get(ctx, s.doneKey(ws, taskID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return registry.TaskEntry{}, false, nil
	}
	if err != nil {
		return registry.TaskEntry{}, false, err
	}
	e, err := s.unmarshal(raw)
	return e, err == nil, err
}

func (s *redisStore) deleteSettled(ctx context.Context, ws, taskID string) error {
	return s.rdb.Del(ctx, s.doneKey(ws, taskID)).Err()
}

// ListSettled scans the settled-record namespace.
func (s *redisStore) listSettled(ctx context.Context) ([]registry.TaskEntry, error) {
	var out []registry.TaskEntry
	iter := s.rdb.Scan(ctx, 0, keyDone+"*", 256).Iterator()
	for iter.Next(ctx) {
		raw, err := s.rdb.Get(ctx, iter.Val()).Bytes()
		if err != nil {
			continue
		}
		e, err := s.unmarshal(raw)
		if err != nil {
			return nil, fmt.Errorf("shared state: corrupt settled record %s: %w", iter.Val(), err)
		}
		out = append(out, e)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Revoke records the digest until validUntil (the token's remaining
// validity, §6.1). The TTL is the remaining validity itself, so the shared
// store expires the row exactly when the token would — the bounded window
// across instances (§6.1 跨实例一致) needs no sweep.
func (s *redisStore) revoke(ctx context.Context, ws, hash string, validUntil time.Time) error {
	if hash == "" || !s.now().Before(validUntil) {
		return nil
	}
	ttl := validUntil.Sub(s.now())
	if ttl < time.Second {
		ttl = time.Second
	}
	return s.rdb.Set(ctx, s.revokeKey(ws, hash), validUntil.Unix(), ttl).Err()
}

// ListRevoked returns the live revocations grouped by workspace
// (workspace → digest → valid-until). The grouping is what keeps §6.1's
// partition across a restart seed: a digest revoked in one workspace must not
// adjudicate another workspace's token.
func (s *redisStore) listRevoked(ctx context.Context) (map[string]map[string]time.Time, error) {
	out := make(map[string]map[string]time.Time)
	iter := s.rdb.Scan(ctx, 0, keyRevoke+"*", 256).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		// Key shape: foreman:revocation:<ws>:<hash>.
		rest := strings.TrimPrefix(key, keyRevoke)
		idx := strings.LastIndex(rest, ":")
		if idx <= 0 || idx == len(rest)-1 {
			continue
		}
		ws, digest := rest[:idx], rest[idx+1:]
		raw, err := s.rdb.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		sec, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			continue
		}
		if out[ws] == nil {
			out[ws] = make(map[string]time.Time)
		}
		out[ws][digest] = time.Unix(sec, 0)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *redisStore) isRevoked(ctx context.Context, ws, hash string) (bool, error) {
	n, err := s.rdb.Exists(ctx, s.revokeKey(ws, hash)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *redisStore) ping(ctx context.Context) error {
	return s.rdb.Ping(ctx).Err()
}
