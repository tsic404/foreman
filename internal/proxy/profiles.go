package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

// Runtime profile snapshot states, the S-3 three-state of §1.6: a successful
// C-18 ⇒ 200 replay; C-18 404 ⇒ 404; anything else ⇒ 503.
const (
	ProfileStateOK          = "ok"
	ProfileStateMissing     = "404"
	ProfileStateUnavailable = "unavailable"
)

// S-3 replay source labels of the S-31 view: the last committed body, or the
// candidate that already replaced it while a refresh sequence is in flight
// (before the registration-discovery probe is created).
const (
	ProfileSourceCommitted = "committed"
	ProfileSourceCandidate = "candidate"
)

// Declaration sources of the current runtime set (§9.3): the startup probe or
// the registration-discovery probe of a refresh.
const (
	declarationSourceStartup   = "startup"
	declarationSourceDiscovery = "discovery-probe"
)

// profileProjectionKeys is the participation projection §1.6 compares a
// candidate list against the committed one with: key order in the JSON body is
// irrelevant, the profile list order is irrelevant, and any key outside this
// set is not part of the comparison.
var profileProjectionKeys = []string{
	"id", "display_name", "runtime_type", "protocol_family",
	"command_name", "fixed_args", "visibility", "enabled",
}

// profileSnapshot is Foreman's C-18 state: the current S-3 replay source plus
// the last committed body an interrupted refresh rolls back to.
type profileSnapshot struct {
	status    string
	body      []byte // current replay source; nil when 404 / unavailable
	committed []byte // last committed body; nil before the first success
	candidate bool   // true while body is a refresh candidate
}

// RuntimeProfileView is the S-31 projection of the replay source: its state,
// whether it is the committed body or a refresh candidate, and the body the
// probe daemon would read.
type RuntimeProfileView struct {
	State             string          `json:"state"`
	ReplaySource      string          `json:"replay_source"`
	DeclarationSource string          `json:"declaration_source"`
	Body              json.RawMessage `json:"replay_body,omitempty"`
}

// DiscoveryProbeRunner runs one registration-discovery probe: a one-shot probe
// Job that registers and is then cleaned up, with no model catalog discovery
// (§1.6).
type DiscoveryProbeRunner interface {
	RunDiscoveryProbe(ctx context.Context, workspaceID string) (registry.Discovery, error)
}

// DiscoveryProbeFunc adapts a function to DiscoveryProbeRunner.
type DiscoveryProbeFunc func(context.Context, string) (registry.Discovery, error)

// RunDiscoveryProbe implements DiscoveryProbeRunner.
func (f DiscoveryProbeFunc) RunDiscoveryProbe(ctx context.Context, workspaceID string) (registry.Discovery, error) {
	return f(ctx, workspaceID)
}

// WithDiscoveryProbe wires the registration-discovery probe the §1.6 refresh
// sequence creates. Without it a changed profile list cannot be committed.
func WithDiscoveryProbe(r DiscoveryProbeRunner) Option {
	return func(c *Client) { c.discoveryProbe = r }
}

// RuntimeProfiles is C-18: one single attempt, whose outcome is the S-3 state
// and — on success — the replay source. It is the startup snapshot fetched
// before the startup probe, and the rebuild path's first step. It never
// triggers a refresh sequence.
func (c *Client) RuntimeProfiles(ctx context.Context) error {
	code, body, err := c.fetchRuntimeProfiles(ctx)
	switch {
	case err != nil:
		c.setProfilesUnavailable()
		c.log.Error("runtime.profiles_snapshot_failed", "trigger", "startup", "c18", ProfileStateUnavailable, "err", err)
		return fmt.Errorf("runtime profiles: %w", err)
	case code == http.StatusNotFound:
		c.setProfilesMissing()
		c.log.Info("runtime.profiles_snapshot", "trigger", "startup", "c18", ProfileStateMissing)
		return nil
	case code < 200 || code >= 300:
		c.setProfilesUnavailable()
		c.log.Error("runtime.profiles_snapshot_failed", "trigger", "startup", "c18", ProfileStateUnavailable, "status", code)
		return &StatusError{Code: code, Body: body}
	}
	c.acceptProfiles(body)
	c.log.Info("runtime.profiles_snapshot", "trigger", "startup", "c18", ProfileStateOK, "bytes", len(body))
	return nil
}

// RefreshRuntimeProfiles is the §1.6 refresh sequence for one
// daemon:runtime_profiles_changed event. It is serialized against every other
// C1 write, so a refresh and a "runtime not found" rebuild can never race.
func (c *Client) RefreshRuntimeProfiles(ctx context.Context) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	status, body, err := c.fetchRuntimeProfiles(ctx)
	switch {
	case err != nil:
		c.log.Error("runtime.profiles_refresh_failed",
			"trigger", "change-event", "stage", "c18", "err", err)
		c.metrics.RuntimeProfileRefresh(ProfileRefreshFailed)
		return fmt.Errorf("refresh profiles: %w", err)
	case status == http.StatusNotFound:
		// The face disappeared: S-3 answers 404 from here on, and the
		// declaration is left as it is (there is no list to compare). The
		// outcome is "unchanged" — nothing moved — and the event's c18 field is
		// what tells this apart from an unchanged projection (§9.2).
		c.setProfilesMissing()
		c.metrics.RuntimeProfileRefresh(ProfileRefreshUnchanged)
		c.log.Info("runtime.profiles_refresh",
			"trigger", "change-event", "c18", ProfileStateMissing, "changed", false)
		return nil
	case status < 200 || status >= 300:
		c.metrics.RuntimeProfileRefresh(ProfileRefreshFailed)
		c.log.Error("runtime.profiles_refresh_failed",
			"trigger", "change-event", "stage", "c18", "status", status)
		return &StatusError{Code: status, Body: body}
	}
	return c.refreshLocked(ctx, body, "change-event")
}

// refreshLocked runs the replay-source-first refresh sequence of §1.6. opMu
// must be held; trigger names the event that started it (change-event /
// runtime-not-found) for the refresh event (§9.1).
func (c *Client) refreshLocked(ctx context.Context, candidate []byte, trigger string) error {
	prev := c.profileSnapshot()
	changed := profileProjectionChanged(prev.committed, candidate)
	// The candidate becomes the S-3 replay source before the probe is created:
	// the probe must read the new list, and a task Job in this window reads it
	// too (ADR-043 §1.6 回放源先行).
	c.setProfilesCandidate(candidate)
	if !changed {
		c.commitProfiles(candidate)
		c.metrics.RuntimeProfileRefresh(ProfileRefreshUnchanged)
		c.log.Info("runtime.profiles_refresh",
			"trigger", trigger, "c18", ProfileStateOK, "changed", false)
		return nil
	}
	if c.discoveryProbe == nil {
		c.restoreProfiles(prev)
		c.metrics.RuntimeProfileRefresh(ProfileRefreshFailed)
		c.log.Error("runtime.profiles_refresh_failed",
			"trigger", trigger, "stage", "probe", "err", "no discovery probe runner")
		return errors.New("refresh profiles: no registration-discovery probe runner wired")
	}
	prevRefs := c.RuntimeRefs()
	disc, err := c.discoveryProbe.RunDiscoveryProbe(ctx, c.cfg.WorkspaceID)
	if err != nil {
		c.restoreProfiles(prev)
		c.metrics.RuntimeProfileRefresh(ProfileRefreshFailed)
		c.log.Error("runtime.profiles_refresh_failed",
			"trigger", trigger, "stage", "probe", "err", err)
		return fmt.Errorf("refresh profiles: discovery probe: %w", err)
	}
	// Commit point = C-1 success: the declaration is replaced by the probe's
	// registration body, and only then do the dropped entries get deregistered.
	// A failed C-1 leaves the live declaration untouched — no rollback step is
	// needed, because nothing was written before the answer arrived.
	if err := c.registerWithDiscoveryLocked(ctx, disc); err != nil {
		c.restoreProfiles(prev)
		c.metrics.RuntimeProfileRefresh(ProfileRefreshFailed)
		c.log.Error("runtime.profiles_refresh_failed",
			"trigger", trigger, "stage", "register", "err", err)
		return fmt.Errorf("refresh profiles: register discovery probe: %w", err)
	}
	c.commitProfiles(candidate)
	c.metrics.RuntimeProfileRefresh(ProfileRefreshSuccess)
	c.log.Info("runtime.profiles_refresh",
		"trigger", trigger, "c18", ProfileStateOK, "changed", true,
		"probe", "registered", "register", "ok", "runtimes", len(disc.Runtimes))
	c.deregisterDropped(ctx, prevRefs, c.RuntimeRefs())
	return nil
}

// deregisterDropped issues one C-14 per entry that left the declaration (§1.6
// 增删语义: 逐条注销, row kept server-side). Best-effort: a failure never
// touches the commit; the passive fallback is the server's own
// no-heartbeat scan.
func (c *Client) deregisterDropped(ctx context.Context, prev, next []RuntimeRef) {
	nextKeys := make(map[string]bool, len(next))
	for _, ref := range next {
		nextKeys[ref.Key()] = true
	}
	for _, ref := range prev {
		if ref.ID == "" || nextKeys[ref.Key()] {
			continue
		}
		if err := c.deregisterOne(ctx, ref.ID); err != nil {
			c.log.Warn("runtime.deregister_failed",
				"provider", ref.Provider, "profile_id", ref.ProfileID, "runtime_id", ref.ID, "err", err)
		} else {
			c.log.Info("runtime.deregistered",
				"provider", ref.Provider, "profile_id", ref.ProfileID, "runtime_id", ref.ID)
		}
		// The catalog slot belongs to the identity, not the provider: dropping
		// the entry drops its slot too (a new entry starts empty).
		if c.slots != nil {
			c.slots.Delete(ref.ID)
		}
	}
}

// deregisterOne is C-14 carrying exactly one runtime id (§1.6 一次请求只带退出
// 声明的条目).
func (c *Client) deregisterOne(ctx context.Context, runtimeID string) error {
	body, err := json.Marshal(map[string]any{"runtime_ids": []string{runtimeID}})
	if err != nil {
		return err
	}
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodPost, "/api/daemon/deregister", body)
	if err != nil {
		return fmt.Errorf("deregister %s: %w", runtimeID, err)
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("deregister %s: %w", runtimeID, &StatusError{Code: code, Body: raw})
	}
	return nil
}

// fetchRuntimeProfiles performs the single C-18 attempt and returns its raw
// outcome. It never mutates the snapshot.
func (c *Client) fetchRuntimeProfiles(ctx context.Context) (int, []byte, error) {
	path := fmt.Sprintf("/api/daemon/workspaces/%s/runtime-profiles", url.PathEscape(c.cfg.WorkspaceID))
	return c.doOnce(ctx, c.ctrl, http.MethodGet, path, nil)
}

// RuntimeProfileView projects the current replay source for S-31 (read-only).
func (c *Client) RuntimeProfileView() RuntimeProfileView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	source := ProfileSourceCommitted
	if c.profiles.candidate {
		source = ProfileSourceCandidate
	}
	return RuntimeProfileView{
		State:             c.profiles.status,
		ReplaySource:      source,
		DeclarationSource: c.source,
		Body:              append(json.RawMessage(nil), c.profiles.body...),
	}
}

// DeclarationSource names the probe the current declaration came from
// ("startup" or "discovery-probe", §9.3).
func (c *Client) DeclarationSource() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.source
}

// S3Response returns the S-3 answer of the current replay source: the status
// code and the raw body (nil for 404 / 503).
func (c *Client) S3Response() (int, []byte) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	switch c.profiles.status {
	case ProfileStateOK:
		return http.StatusOK, append([]byte(nil), c.profiles.body...)
	case ProfileStateMissing:
		return http.StatusNotFound, nil
	default:
		return http.StatusServiceUnavailable, nil
	}
}

func (c *Client) profileSnapshot() profileSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	snap := c.profiles
	snap.body = append([]byte(nil), snap.body...)
	snap.committed = append([]byte(nil), snap.committed...)
	return snap
}

func (c *Client) restoreProfiles(snap profileSnapshot) {
	c.mu.Lock()
	c.profiles = snap
	c.mu.Unlock()
}

// acceptProfiles records a successful C-18 as the committed replay source.
func (c *Client) acceptProfiles(body []byte) {
	stored := append([]byte(nil), body...)
	c.mu.Lock()
	c.profiles = profileSnapshot{status: ProfileStateOK, body: stored, committed: stored}
	c.mu.Unlock()
}

// setProfilesCandidate makes a refresh candidate the S-3 replay source before
// the registration-discovery probe is created.
func (c *Client) setProfilesCandidate(body []byte) {
	stored := append([]byte(nil), body...)
	c.mu.Lock()
	c.profiles.status = ProfileStateOK
	c.profiles.body = stored
	c.profiles.candidate = true
	c.mu.Unlock()
}

// commitProfiles accepts a candidate as the new committed replay source.
func (c *Client) commitProfiles(body []byte) {
	stored := append([]byte(nil), body...)
	c.mu.Lock()
	c.profiles.status = ProfileStateOK
	c.profiles.body = stored
	c.profiles.committed = stored
	c.profiles.candidate = false
	c.mu.Unlock()
}

func (c *Client) setProfilesMissing() {
	c.mu.Lock()
	c.profiles.status = ProfileStateMissing
	c.profiles.body = nil
	c.profiles.candidate = false
	c.mu.Unlock()
}

func (c *Client) setProfilesUnavailable() {
	c.mu.Lock()
	c.profiles.status = ProfileStateUnavailable
	c.profiles.body = nil
	c.profiles.candidate = false
	c.mu.Unlock()
}

// profileProjectionChanged compares a candidate C-18 body against the last
// committed one by content projection only (§1.6: id / display_name /
// runtime_type / protocol_family / command_name / fixed_args / visibility /
// enabled; list order and key order are irrelevant). An unreadable candidate is
// "changed": the declaration must come from a probe, never from a body Foreman
// could not read.
func profileProjectionChanged(committed, candidate []byte) bool {
	prev, okPrev := profileProjection(committed)
	next, okNext := profileProjection(candidate)
	if !okNext {
		return true
	}
	if !okPrev {
		return true
	}
	return prev != next
}

// profileProjection canonicalizes the participation projection of a C-18 body.
// A nil or empty body is the empty list.
func profileProjection(raw []byte) (string, bool) {
	if len(raw) == 0 {
		return "[]", true
	}
	var body struct {
		RuntimeProfiles []map[string]json.RawMessage `json:"runtime_profiles"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return "", false
	}
	projections := make([]string, 0, len(body.RuntimeProfiles))
	for _, profile := range body.RuntimeProfiles {
		selected := make(map[string]json.RawMessage, len(profileProjectionKeys))
		for _, key := range profileProjectionKeys {
			if value, ok := profile[key]; ok {
				selected[key] = value
			}
		}
		encoded, err := json.Marshal(selected)
		if err != nil {
			return "", false
		}
		projections = append(projections, string(encoded))
	}
	sort.Strings(projections)
	out, err := json.Marshal(projections)
	if err != nil {
		return "", false
	}
	return string(out), true
}

// ProfileRefresher is the §1.6 trigger face: the WS subscriber notifies it on
// every daemon:runtime_profiles_changed frame, and it runs the refresh
// sequence with the short-window merging the contract requires — events
// arriving while a refresh is in flight collapse into one more run.
type ProfileRefresher struct {
	client  *Client
	log     *slog.Logger
	trigger chan struct{}
	now     func() time.Time
	// leader gates the refresh sequence: it runs a probe Job and re-registers,
	// both control-plane actions (§1.1 发出者口径). Nil keeps the gate open.
	leader func() bool
}

// SetLeaderGate wires the control-plane ownership check.
func (r *ProfileRefresher) SetLeaderGate(g func() bool) { r.leader = g }

// ProfileRefresherOption customizes a ProfileRefresher.
type ProfileRefresherOption func(*ProfileRefresher)

// WithProfileRefresherLogger overrides the logger (tests).
func WithProfileRefresherLogger(l *slog.Logger) ProfileRefresherOption {
	return func(r *ProfileRefresher) { r.log = l.With("component", "profile-refresher") }
}

// WithProfileRefresherClock overrides the clock (tests).
func WithProfileRefresherClock(now func() time.Time) ProfileRefresherOption {
	return func(r *ProfileRefresher) { r.now = now }
}

// NewProfileRefresher builds the refresher over the client.
func NewProfileRefresher(client *Client, opts ...ProfileRefresherOption) *ProfileRefresher {
	r := &ProfileRefresher{
		client:  client,
		log:     slog.Default().With("component", "profile-refresher"),
		trigger: make(chan struct{}, 1),
		now:     time.Now,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Notify records one change event. Non-blocking: a pending signal means a
// refresh (or one more run after the current one) is already scheduled.
func (r *ProfileRefresher) Notify() {
	select {
	case r.trigger <- struct{}{}:
	default:
	}
}

// Run serves refresh triggers until ctx ends. A failed sequence is logged and
// does not stop the loop: the next event or a restart converges.
func (r *ProfileRefresher) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-r.trigger:
		}
		if r.leader != nil && !r.leader() {
			// The invalidation hint arrived on a non-holder: the event is
			// dropped here, and the next restart converges (§1.6 降级面), so a
			// non-holder never runs a probe Job or re-registers.
			r.log.Debug("runtime.profiles_refresh_skipped", "reason", "not control-plane holder")
			continue
		}
		if err := r.client.RefreshRuntimeProfiles(ctx); err != nil {
			r.log.Warn("runtime.profiles_refresh_failed", "err", err)
		}
	}
}
