package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// RuntimeRef is one declared real runtime: the identity it serves plus the id
// and name the server answered for it (ID is empty before C1). Identity =
// (Provider, ProfileID), with the empty ProfileID naming the builtin entry
// (ADR-043).
type RuntimeRef struct {
	ID        string
	Provider  string
	ProfileID string
	Name      string
}

// Key is the identity key of a RuntimeRef (registry.RuntimeIdentityKey).
func (r RuntimeRef) Key() string { return registry.RuntimeIdentityKey(r.Provider, r.ProfileID) }

// RuntimeChange reports a change of the declared runtime set. NewID empty means
// the runtime disappeared; OldID empty means it appeared. The WS subscriber
// reconnects only the connections whose id moved (§1.4 按 runtime 粒度).
type RuntimeChange struct {
	Provider  string
	ProfileID string
	OldID     string
	NewID     string
}

// errAckUnparsable marks a C2 ack body Foreman could not read; the caller logs
// an error and continues as if there were no pending work (§1.3).
var errAckUnparsable = errors.New("heartbeat ack unparsable")

// HeartbeatAck is C2's ack body. pending_model_list is the only consumed field;
// every other pending_* field is ignored on purpose (§1.3).
type HeartbeatAck struct {
	Status           string            `json:"status"`
	RuntimeID        string            `json:"runtime_id"`
	PendingModelList *PendingModelList `json:"pending_model_list"`
}

// PendingModelList is the server-issued catalog request. The id is opaque: its
// shape is never parsed (§1.3).
type PendingModelList struct {
	ID string `json:"id"`
}

// WithStartupCatalog wires the provider-keyed staging of the startup discovery
// result, bound to real runtime ids by the C1 response.
func WithStartupCatalog(c *registry.StartupCatalog) Option {
	return func(cl *Client) { cl.startup = c }
}

// WithCatalogSlots wires the per-real-runtime catalog cache the fake server
// fills (probe results) and the reporter reads (cache replay).
func WithCatalogSlots(s *CatalogSlots) Option {
	return func(cl *Client) { cl.slots = s }
}

// WithTaskRuntime wires the lookup translating a task's real runtime id: the S8
// body names the job runtime the daemon was handed, the server knows only the
// real runtime id its claim was dispatched under (S-8).
func WithTaskRuntime(fn func(taskID string) string) Option {
	return func(cl *Client) { cl.taskRuntime = fn }
}

// Changes returns the client-wide change channel. It is a single queue, kept
// for tests and one-shot observers; consumers that must see every change (the
// heartbeat supervisor and the WS subscriber) each use SubscribeChanges, so
// neither can steal the other's notification.
func (c *Client) Changes() <-chan RuntimeChange { return c.changes }

// SubscribeChanges returns a private change stream plus its release. The
// channel is buffered and never closed; release removes it from the fan-out.
func (c *Client) SubscribeChanges() (<-chan RuntimeChange, func()) {
	ch := make(chan RuntimeChange, 16)
	c.changesMu.Lock()
	c.changeSubs = append(c.changeSubs, ch)
	c.changesMu.Unlock()
	return ch, func() {
		c.changesMu.Lock()
		defer c.changesMu.Unlock()
		for i, sub := range c.changeSubs {
			if sub == ch {
				c.changeSubs = append(c.changeSubs[:i], c.changeSubs[i+1:]...)
				return
			}
		}
	}
}

// RuntimeRefs returns the declared runtime set (declaration order).
func (c *Client) RuntimeRefs() []RuntimeRef {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]RuntimeRef(nil), c.refs...)
}

// RuntimeIDs returns the real runtime ids of the declared set (empty before C1).
func (c *Client) RuntimeIDs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := make([]string, 0, len(c.refs))
	for _, ref := range c.refs {
		if ref.ID != "" {
			ids = append(ids, ref.ID)
		}
	}
	return ids
}

// Declaration returns the startup discovery that drives C1.
func (c *Client) Declaration() registry.Discovery {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.disc
}

// CLIVersion is the X-Client-Version value: the startup discovery's
// cli_version once the probe has reported it (the template CLI's truth),
// Foreman's own version before that — the discriminator is the request moment
// against the probe's registration, so C1 itself already carries the template
// version (C-1 的取值来源与声明域).
func (c *Client) CLIVersion() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cliVersionOf(c.disc)
}

// cliVersionOf is the X-Client-Version one discovery's own C-1 carries: that
// discovery's cli_version, or Foreman's version when the probe reported none.
func (c *Client) cliVersionOf(d registry.Discovery) string {
	if d.CLIVersion == "" {
		return c.cfg.Version
	}
	return d.CLIVersion
}

// Declare caches the startup discovery as the C1 declaration without sending
// it. The first Register (the claim loop's preflight, with the §1.5 backoff)
// carries it, so the startup path has exactly one C1 trigger.
func (c *Client) Declare(d registry.Discovery) {
	c.mu.Lock()
	c.disc = d
	c.refs = declaredRefs(d)
	c.source = declarationSourceStartup
	c.mu.Unlock()
}

// RegisterWithDiscovery declares exactly the runtime set a probe discovered and
// caches the ids the server answered with (C-1). It is the startup
// declaration path; a runtime-profile refresh commits through its own
// sequence.
func (c *Client) RegisterWithDiscovery(ctx context.Context, d registry.Discovery) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	return c.commitRegisterLocked(ctx, d, declarationSourceStartup)
}

// Register re-declares the cached startup discovery. It is the 404 rebuild path
// and never starts a new probe (§1.5).
func (c *Client) Register(ctx context.Context) error { return c.register(ctx) }

// declaredRefs turns a discovery into the declared runtime set, deduplicating
// repeated identities: a repeat merges into the first entry and the identity is
// (type, profile_id), so a builtin entry and profile entries of the same type
// stay distinct (C-1; ADR-043).
func declaredRefs(d registry.Discovery) []RuntimeRef {
	refs := make([]RuntimeRef, 0, len(d.Runtimes))
	seen := make(map[string]bool, len(d.Runtimes))
	for _, rt := range d.Runtimes {
		if rt.Type == "" {
			continue
		}
		key := registry.RuntimeIdentityKey(rt.Type, rt.ProfileID)
		if seen[key] {
			continue
		}
		seen[key] = true
		refs = append(refs, RuntimeRef{Provider: rt.Type, ProfileID: rt.ProfileID, Name: rt.Name})
	}
	return refs
}

// runtimePayloads is the C1 `runtimes[]` array: the discovery handed over entry
// by entry (status = online, profile_id carried verbatim, C-1).
func runtimePayloads(d registry.Discovery) []runtimePayload {
	payloads := make([]runtimePayload, 0, len(d.Runtimes))
	seen := make(map[string]bool, len(d.Runtimes))
	for _, rt := range d.Runtimes {
		if rt.Type == "" {
			continue
		}
		key := registry.RuntimeIdentityKey(rt.Type, rt.ProfileID)
		if seen[key] {
			continue
		}
		seen[key] = true
		payloads = append(payloads, runtimePayload{
			Name:      rt.Name,
			Type:      rt.Type,
			Version:   rt.Version,
			Status:    "online",
			ProfileID: rt.ProfileID,
		})
	}
	return payloads
}

// register re-declares the cached discovery. Callers hold no lock; the
// registration sequence runs under opMu so a profile refresh and a
// "runtime not found" rebuild can never write C1 concurrently (§1.6).
func (c *Client) register(ctx context.Context) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	return c.registerLocked(ctx)
}

// registerWithDiscoveryLocked is the name the refresh sequence uses: it commits
// a discovery-probe result as the declaration. opMu must be held.
func (c *Client) registerWithDiscoveryLocked(ctx context.Context, d registry.Discovery) error {
	return c.commitRegisterLocked(ctx, d, declarationSourceDiscovery)
}

// registerLocked re-declares the cached discovery (the startup declaration or
// a rebuild after "runtime not found"). The cached set is what is sent and the
// declaration is not written until the server accepts it.
func (c *Client) registerLocked(ctx context.Context) error {
	c.mu.RLock()
	disc := c.disc
	source := c.source
	c.mu.RUnlock()
	return c.commitRegisterLocked(ctx, disc, source)
}

// commitRegisterLocked sends one C-1 for d and swaps the declaration to d plus
// the ids the server answered only after that answer arrived (§1.6 提交点 =
// C-1 成功). While the request is in flight the previous declaration stays
// live, so heartbeats, subscriptions and the claim list keep running on it
// instead of collapsing to an id-less set; a failure leaves the old set in
// place, which is why no rollback is needed. opMu must be held.
func (c *Client) commitRegisterLocked(ctx context.Context, d registry.Discovery, source string) error {
	if len(d.Runtimes) == 0 {
		return errors.New("register: no startup discovery; the startup probe must register before C1")
	}
	body := registerRequest{
		WorkspaceID:     c.cfg.WorkspaceID,
		DaemonID:        c.cfg.DaemonID,
		LegacyDaemonIDs: []string{},
		DeviceName:      "foreman",
		CLIVersion:      d.CLIVersion,
		LaunchedBy:      "",
		Runtimes:        runtimePayloads(d),
		FailedProfiles:  d.FailedProfiles,
	}
	if len(body.Runtimes) == 0 {
		return errors.New("register: startup discovery declares no runtime")
	}
	c.mu.RLock()
	prev := append([]RuntimeRef(nil), c.refs...)
	lost := make(map[string]bool, len(c.lost))
	maps.Copy(lost, c.lost)
	c.mu.RUnlock()
	var resp registerResponse
	code, raw, err := c.doOnceVersion(ctx, c.ctrl, http.MethodPost, "/api/daemon/register", c.cliVersionOf(d), mustMarshal(body))
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("register: %w", &StatusError{Code: code, Body: raw})
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("register: decode response: %w", err)
	}
	next, err := c.selectRuntimes(body.Runtimes, resp.Runtimes)
	if err != nil {
		return err
	}
	now := time.Now()
	c.mu.Lock()
	c.disc = d
	c.refs = next
	c.source = source
	// Consume only the marks this attempt started with: a rebuild that failed
	// while this C1 was in flight recorded its own identity, and that row still
	// owes its C15 (C-15, §1.4 按 runtime 粒度).
	for key := range lost {
		delete(c.lost, key)
	}
	c.state = WorkspaceState{
		Repos:        resp.Repos,
		ReposVersion: resp.ReposVersion,
		Settings:     resp.Settings,
		Seeded:       true,
	}
	c.mu.Unlock()

	c.applyRuntimeChanges(prev, next, lost)
	c.bindStartupCatalog(next, now)
	return nil
}

// runtimeAnswer is one entry of the C1 response `runtimes[]`. ProfileID is a
// pointer because the server encodes the builtin entry as `null` while a
// profile entry carries its identifier (ADR-043; §2.3 builtin 空串与 null 归一).
type runtimeAnswer struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	Provider  string  `json:"provider"`
	Status    string  `json:"status"`
	ProfileID *string `json:"profile_id"`
}

func (a runtimeAnswer) profileID() string {
	if a.ProfileID == nil {
		return ""
	}
	return *a.ProfileID
}

// selectRuntimes binds every declared runtime to exactly one answered runtime
// row: the identity (provider, profile_id) must match by value, name matching
// first, then the lexicographically smallest id with a warn naming the
// candidates (C-1 + §2.3 tie-break; ADR-043).
func (c *Client) selectRuntimes(declared []runtimePayload, answers []runtimeAnswer) ([]RuntimeRef, error) {
	byIdentity := make(map[string][]runtimeAnswer, len(answers))
	for _, a := range answers {
		key := registry.RuntimeIdentityKey(a.Provider, a.profileID())
		byIdentity[key] = append(byIdentity[key], a)
	}
	refs := make([]RuntimeRef, 0, len(declared))
	for _, d := range declared {
		key := registry.RuntimeIdentityKey(d.Type, d.ProfileID)
		candidates := byIdentity[key]
		if len(candidates) == 0 {
			return nil, fmt.Errorf("register: response carries no runtime for identity (%q, %q)", d.Type, d.ProfileID)
		}
		chosen := candidates[0]
		if len(candidates) > 1 {
			tied := make([]string, 0, len(candidates))
			for _, cand := range candidates {
				tied = append(tied, cand.ID)
			}
			sort.Strings(tied)
			// §2.3 响应侧同键多条: online rows whose name matches this
			// declaration come first; among those the lexicographically
			// smallest id wins. Only when no candidate matches both does the
			// whole set's smallest id decide.
			eligible := make([]runtimeAnswer, 0, len(candidates))
			for _, cand := range candidates {
				if cand.Status == "online" && cand.Name == d.Name {
					eligible = append(eligible, cand)
				}
			}
			if len(eligible) == 0 {
				eligible = candidates
			}
			sort.SliceStable(eligible, func(i, j int) bool { return eligible[i].ID < eligible[j].ID })
			picked := eligible[0].ID
			for _, cand := range eligible {
				if cand.ID == picked {
					chosen = cand
				}
			}
			c.log.Warn("runtime.set tied: picking one candidate",
				"provider", d.Type, "profile_id", d.ProfileID, "picked", chosen.ID, "candidates", tied)
		}
		if chosen.ID == "" {
			return nil, fmt.Errorf("register: empty runtime id for identity (%q, %q)", d.Type, d.ProfileID)
		}
		refs = append(refs, RuntimeRef{ID: chosen.ID, Provider: d.Type, ProfileID: d.ProfileID, Name: d.Name})
	}
	return refs, nil
}

// applyRuntimeChanges emits one change per identity that appeared or moved,
// runs C15 only for the rebuilt rows, and moves the WS subscription of those
// runtimes only. lost is per identity: only a runtime whose own row was proved
// gone (a 404) counts as rebuilt, never every row of the declaration.
func (c *Client) applyRuntimeChanges(prev, next []RuntimeRef, lost map[string]bool) {
	prevByIdentity := make(map[string]RuntimeRef, len(prev))
	for _, ref := range prev {
		prevByIdentity[ref.Key()] = ref
	}
	for _, ref := range next {
		before, had := prevByIdentity[ref.Key()]
		// A rebuild is an id that moved, or any successful C1 that follows a
		// 404 (the row came back, possibly under the same id). A first
		// declaration is neither: C15 belongs to rebuilt rows only (C-15).
		rebuilt := had && before.ID != "" && (before.ID != ref.ID || lost[ref.Key()])
		if had && before.ID != "" && !rebuilt {
			continue
		}
		if rebuilt {
			c.log.Warn("runtime.rebuilt", "provider", ref.Provider, "profile_id", ref.ProfileID, "runtime_id", ref.ID, "previous", before.ID)
		} else {
			c.log.Info("runtime.registered", "provider", ref.Provider, "profile_id", ref.ProfileID, "runtime_id", ref.ID)
		}
		if rebuilt {
			c.recoverOrphans(context.WithoutCancel(context.Background()), ref.ID)
		}
		c.notifyChange(RuntimeChange{Provider: ref.Provider, ProfileID: ref.ProfileID, OldID: before.ID, NewID: ref.ID})
	}
	nextByIdentity := make(map[string]bool, len(next))
	for _, ref := range next {
		nextByIdentity[ref.Key()] = true
	}
	for _, ref := range prev {
		if !nextByIdentity[ref.Key()] {
			c.notifyChange(RuntimeChange{Provider: ref.Provider, ProfileID: ref.ProfileID, OldID: ref.ID})
		}
	}
}

func (c *Client) notifyChange(change RuntimeChange) {
	select {
	case c.changes <- change:
	default:
	}
	c.changesMu.Lock()
	defer c.changesMu.Unlock()
	for _, ch := range c.changeSubs {
		select {
		case ch <- change:
		default:
			// The subscriber re-reads the whole set on every wake-up, so a
			// dropped notification only delays its reconcile by one tick.
		}
	}
}

// bindStartupCatalog moves the startup discovery result staged by its identity
// into the real runtime's catalog slot (§1.3: the slot key is the real runtime
// identity, not the provider).
func (c *Client) bindStartupCatalog(refs []RuntimeRef, at time.Time) {
	if c.startup == nil || c.slots == nil {
		return
	}
	for _, ref := range refs {
		body, ok := c.startup.Take(ref.Provider, ref.ProfileID)
		if !ok {
			continue
		}
		c.slots.Put(ref.ID, body, at)
	}
}

// RefForRuntime resolves the declared runtime that owns a real runtime id.
func (c *Client) RefForRuntime(runtimeID string) (RuntimeRef, bool) {
	if runtimeID == "" {
		return RuntimeRef{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, ref := range c.refs {
		if ref.ID == runtimeID {
			return ref, true
		}
	}
	return RuntimeRef{}, false
}

// RuntimeIdentity resolves the declared identity a real runtime id belongs to,
// for the claim-time dispatch record the entry keeps (§2.3):
// registry.RuntimeIdentityKey(provider, profile id). A runtime id the current
// declaration does not carry has no identity.
func (c *Client) RuntimeIdentity(runtimeID string) (string, bool) {
	ref, ok := c.RefForRuntime(runtimeID)
	if !ok {
		return "", false
	}
	return ref.Key(), true
}

// stageStartupCatalog records the startup discovery result under the identity
// that produced it, until C1 binds it to a real runtime id.
func (c *Client) stageStartupCatalog(provider, profileID string, body []byte) {
	if c.startup == nil || provider == "" {
		return
	}
	c.startup.Put(provider, profileID, body)
}

// rebuildRuntime re-registers (C1) after a 404 "runtime not found" and returns
// the fresh id of the runtime that was lost. §1.6: C-18 is re-fetched first —
// an unchanged profile set re-declares the cached discovery, a changed set goes
// through the refresh sequence, and an unavailable C-18 falls back to the cached
// discovery for availability. Only that runtime's identity moves: the other
// rows answer with the ids they already had.
func (c *Client) rebuildRuntime(ctx context.Context, runtimeID string) (string, error) {
	identity, _, _, ok := c.identityOf(runtimeID)
	if !ok {
		// The id is already stale by the time the rebuild starts (a concurrent
		// path moved it): re-declaring would only write another C1 without
		// moving anything. The caller re-reads the set instead.
		c.log.Warn("runtime rebuild skipped: id already replaced", "runtime_id", runtimeID)
		return "", nil
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	// A concurrent path may have moved the identity while this caller waited
	// for the sequence lock: hand back the fresh id instead of writing another
	// C1 that cannot move anything.
	if current := c.runtimeIDFor(identity); current != "" && current != runtimeID {
		return current, nil
	}
	if err := c.rebuildLocked(ctx, identity); err != nil {
		return "", err
	}
	return c.runtimeIDFor(identity), nil
}

// rebuildClaimRuntime is the same sequence for C-3: the claim carries the whole
// declared set, so its 404 names no row and the re-register covers them all
// (ADR-043 §9 — C-18 is re-fetched before any C-1, whatever triggered it). The
// fresh ids arrive in that C1's answer, which is the set the caller re-claims
// with; nothing per-identity is marked, because no row was identified.
func (c *Client) rebuildClaimRuntime(ctx context.Context) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	return c.rebuildLocked(ctx, "")
}

// rebuildLocked runs the "runtime not found" sequence of §1.6/ADR-043 §9 and
// records one refresh outcome on every branch (§9.1 event, §9.2 metric).
// identity is the lost row when the caller knows it ("" for the claim face);
// only a known row's failed re-register is recorded as lost, so its next
// success still counts as a rebuild. opMu must be held.
func (c *Client) rebuildLocked(ctx context.Context, identity string) error {
	status, body, err := c.fetchRuntimeProfiles(ctx)
	var rerr error
	switch {
	case err == nil && status == http.StatusOK:
		if profileProjectionChanged(c.profileSnapshot().committed, body) {
			// The changed branch is the refresh sequence, which records its own
			// outcome (§1.6 已变 ⇒ 走刷新序列).
			return c.refreshLocked(ctx, body, "runtime-not-found")
		}
		if rerr = c.registerLocked(ctx); rerr == nil {
			c.acceptProfiles(body)
			c.metrics.RuntimeProfileRefresh(ProfileRefreshUnchanged)
			c.log.Info("runtime.profiles_refresh",
				"trigger", "runtime-not-found", "c18", ProfileStateOK, "changed", false)
		}
	case status == http.StatusNotFound:
		// The face is gone: S-3 answers 404 from now on. The declaration is
		// unchanged (there is no profile list to compare against).
		c.setProfilesMissing()
		if rerr = c.registerLocked(ctx); rerr == nil {
			c.metrics.RuntimeProfileRefresh(ProfileRefreshUnchanged)
			c.log.Info("runtime.profiles_refresh",
				"trigger", "runtime-not-found", "c18", ProfileStateMissing, "changed", false)
		}
	default:
		// Availability first: C-18 unavailable keeps the current replay source
		// and declares the cached discovery.
		if rerr = c.registerLocked(ctx); rerr == nil {
			c.metrics.RuntimeProfileRefresh(ProfileRefreshFailed)
			fields := []any{
				"trigger", "runtime-not-found", "stage", "c18", "status", status,
				"replay", "committed", "register", "ok",
			}
			if err != nil {
				fields = append(fields, "err", err)
			}
			c.log.Error("runtime.profiles_refresh_failed", fields...)
		}
	}
	if rerr != nil {
		if identity != "" {
			c.markLost(identity)
		}
		c.metrics.RuntimeProfileRefresh(ProfileRefreshFailed)
		c.log.Error("runtime.profiles_refresh_failed",
			"trigger", "runtime-not-found", "stage", "register", "err", rerr)
		return rerr
	}
	c.log.Debug("runtime.rebuild_settled")
	return nil
}

// identityOf resolves the identity of a declared runtime id.
func (c *Client) identityOf(runtimeID string) (key, provider, profileID string, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, ref := range c.refs {
		if ref.ID == runtimeID {
			return ref.Key(), ref.Provider, ref.ProfileID, true
		}
	}
	return "", "", "", false
}

// runtimeIDFor returns the declared id of an identity ("" when it is not
// declared).
func (c *Client) runtimeIDFor(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, ref := range c.refs {
		if ref.Key() == key {
			return ref.ID
		}
	}
	return ""
}

// markLost records that a 404 proved this identity's row gone and its
// re-register failed, so the next successful C1 still treats it as rebuilt
// (C15 + runtime-set signal).
func (c *Client) markLost(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lost == nil {
		c.lost = map[string]bool{}
	}
	c.lost[key] = true
}

// Heartbeat is C2 for one real runtime. A 404 "runtime not found" rebuilds that
// runtime's row only and retries once (§1.4 多 runtime 的失败面按 runtime 粒度).
func (c *Client) Heartbeat(ctx context.Context, runtimeID string) (HeartbeatAck, error) {
	code, raw, err := c.heartbeatOnce(ctx, runtimeID)
	if err != nil {
		return HeartbeatAck{}, err
	}
	if code == http.StatusNotFound && containsRuntimeNotFound(raw) {
		c.log.Warn("runtime not found; re-registering", "runtime_id", runtimeID)
		fresh, rerr := c.rebuildRuntime(ctx, runtimeID)
		if rerr != nil {
			return HeartbeatAck{}, fmt.Errorf("heartbeat: re-register after runtime loss: %w", rerr)
		}
		if fresh == "" || fresh == runtimeID {
			return HeartbeatAck{}, fmt.Errorf("heartbeat: %w", &StatusError{Code: code, Body: raw})
		}
		runtimeID = fresh
		code, raw, err = c.heartbeatOnce(ctx, runtimeID)
		if err != nil {
			return HeartbeatAck{}, err
		}
	}
	if code < 200 || code >= 300 {
		return HeartbeatAck{}, fmt.Errorf("heartbeat: %w", &StatusError{Code: code, Body: raw})
	}
	ack, derr := decodeHeartbeatAck(raw)
	if derr != nil {
		return HeartbeatAck{}, fmt.Errorf("%w: %v (ack %d bytes)", errAckUnparsable, derr, len(raw))
	}
	c.log.Debug("daemon.heartbeat", "runtime_id", runtimeID, "status", "ok")
	return ack, nil
}

func (c *Client) heartbeatOnce(ctx context.Context, runtimeID string) (int, []byte, error) {
	body, err := json.Marshal(map[string]any{"runtime_id": runtimeID, "supports_batch_import": true})
	if err != nil {
		return 0, nil, err
	}
	return c.doOnce(ctx, c.ctrl, http.MethodPost, "/api/daemon/heartbeat", body)
}

// decodeHeartbeatAck parses the ack tolerantly: unknown fields are ignored, and
// a body that is not a JSON object (or carries an empty id) is an error the
// caller logs before continuing without pending work.
func decodeHeartbeatAck(raw []byte) (HeartbeatAck, error) {
	var ack HeartbeatAck
	if err := json.Unmarshal(raw, &ack); err != nil {
		return HeartbeatAck{}, err
	}
	if ack.PendingModelList != nil && ack.PendingModelList.ID == "" {
		return HeartbeatAck{}, errors.New("pending_model_list.id is empty")
	}
	return ack, nil
}

// ClaimTasks is C3 over the whole declared runtime set: runtime_ids carries
// every real runtime id, and max_tasks the caller's free slots.
func (c *Client) ClaimTasks(ctx context.Context, maxTasks int) ([]json.RawMessage, error) {
	if maxTasks < 1 {
		return nil, nil
	}
	code, raw, err := c.claimOnce(ctx, c.RuntimeIDs(), maxTasks)
	if err == nil && code == http.StatusNotFound && containsRuntimeNotFound(raw) {
		c.log.Warn("runtime not found on claim; re-registering")
		// The claim carries the whole declared set, so the 404 names no row:
		// the §1.6 sequence still re-fetches C-18 before it writes any C-1, and
		// the answer moves every stale id at once (ADR-043 §9).
		if rerr := c.rebuildClaimRuntime(ctx); rerr != nil {
			return nil, fmt.Errorf("claim: re-register after runtime loss: %w", rerr)
		}
		code, raw, err = c.claimOnce(ctx, c.RuntimeIDs(), maxTasks)
	}
	if err == nil && code >= 200 && code < 300 {
		var resp struct {
			Tasks []json.RawMessage `json:"tasks"`
		}
		if err = json.Unmarshal(raw, &resp); err == nil {
			return resp.Tasks, nil
		}
	}
	if err == nil {
		err = &StatusError{Code: code, Body: raw}
	}
	codeLabel := "transport"
	se := new(StatusError)
	if errors.As(err, &se) {
		codeLabel = fmt.Sprintf("%d", se.Code)
	}
	c.metrics.ClaimError(codeLabel)
	return nil, fmt.Errorf("claim: %w", err)
}

func (c *Client) claimOnce(ctx context.Context, runtimeIDs []string, maxTasks int) (int, []byte, error) {
	if len(runtimeIDs) == 0 {
		return 0, nil, errors.New("claim: no real runtime registered")
	}
	body, err := json.Marshal(map[string]any{
		"daemon_id":   c.cfg.DaemonID,
		"runtime_ids": runtimeIDs,
		"max_tasks":   maxTasks,
	})
	if err != nil {
		return 0, nil, err
	}
	return c.doOnce(ctx, c.ctrl, http.MethodPost, "/api/daemon/tasks/claim", body)
}

// PrepareLease is C4 on the runtime that claimed the task. The C-4
// semantics are normalized to ready: 200 → true; 400 → false (the task left the
// renewable pre-start state); 404 → scheduler.ErrTaskNotFound.
func (c *Client) PrepareLease(ctx context.Context, runtimeID, taskID string) (ready bool, err error) {
	code, raw, err := c.prepareLeaseOnce(ctx, runtimeID, taskID)
	if err != nil {
		return false, err
	}
	if code == http.StatusNotFound && containsRuntimeNotFound(raw) {
		fresh, rerr := c.rebuildRuntime(ctx, runtimeID)
		if rerr != nil {
			return false, fmt.Errorf("prepare-lease: re-register after runtime loss: %w", rerr)
		}
		if fresh == "" {
			return false, &StatusError{Code: code, Body: raw}
		}
		code, raw, err = c.prepareLeaseOnce(ctx, fresh, taskID)
		if err != nil {
			return false, err
		}
	}
	switch {
	case code >= 200 && code < 300:
		return true, nil
	case code == http.StatusBadRequest:
		return false, nil
	case code == http.StatusNotFound:
		return false, scheduler.ErrTaskNotFound
	default:
		return false, &StatusError{Code: code, Body: raw}
	}
}

func (c *Client) prepareLeaseOnce(ctx context.Context, runtimeID, taskID string) (int, []byte, error) {
	path := fmt.Sprintf("/api/daemon/runtimes/%s/tasks/%s/prepare-lease",
		url.PathEscape(runtimeID), url.PathEscape(taskID))
	return c.doOnce(ctx, c.lease, http.MethodPost, path, []byte("{}"))
}

// RelayModelCatalog is C21: it forwards, byte for byte, the body a probe daemon
// reported (or the body cached from an earlier discovery). No field is parsed,
// assembled or validated — the path is the only thing rewritten (C-21).
func (c *Client) RelayModelCatalog(ctx context.Context, runtimeID, requestID string, body []byte) (int, []byte, error) {
	path := fmt.Sprintf("/api/daemon/runtimes/%s/models/%s/result",
		url.PathEscape(runtimeID), url.PathEscape(requestID))
	return c.doOnce(ctx, c.ctrl, http.MethodPost, path, body)
}

// Deregister is C14 (graceful shutdown): it carries every real runtime id.
func (c *Client) Deregister(ctx context.Context) error {
	ids := c.RuntimeIDs()
	if len(ids) == 0 {
		return nil
	}
	body, err := json.Marshal(map[string]any{"runtime_ids": ids})
	if err != nil {
		return err
	}
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodPost, "/api/daemon/deregister", body)
	if err != nil {
		return fmt.Errorf("deregister: %w", err)
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("deregister: %w", &StatusError{Code: code, Body: raw})
	}
	return nil
}
