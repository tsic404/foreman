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

// RuntimeRef is one declared real runtime: the provider it serves plus the id
// and name the server answered for it (ID is empty before C1).
type RuntimeRef struct {
	ID       string
	Provider string
	Name     string
}

// RuntimeChange reports a change of the declared runtime set. NewID empty means
// the runtime disappeared; OldID empty means it appeared. The WS subscriber
// reconnects only the connections whose id moved (§7 按 runtime 粒度).
type RuntimeChange struct {
	Provider string
	OldID    string
	NewID    string
}

// errAckUnparsable marks a C2 ack body Foreman could not read; the caller logs
// an error and continues as if there were no pending work (§1.1 C21 第 8 条).
var errAckUnparsable = errors.New("heartbeat ack unparsable")

// HeartbeatAck is C2's ack body. pending_model_list is the only consumed field;
// every other pending_* field is ignored on purpose (§1.1「C2 ack 消费」).
type HeartbeatAck struct {
	Status           string            `json:"status"`
	RuntimeID        string            `json:"runtime_id"`
	PendingModelList *PendingModelList `json:"pending_model_list"`
}

// PendingModelList is the server-issued catalog request. The id is opaque: its
// shape is never parsed (§1.1 C2 ack 消费 第 2 条).
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
// real runtime id its claim was dispatched under (§1.2 S8).
func WithTaskRuntime(fn func(taskID string) string) Option {
	return func(cl *Client) { cl.taskRuntime = fn }
}

// Changes returns the channel that receives every declared-runtime-set change.
func (c *Client) Changes() <-chan RuntimeChange { return c.changes }

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
// version (§1.1 C1 的取值来源与声明域).
func (c *Client) CLIVersion() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.disc.CLIVersion == "" {
		return c.cfg.Version
	}
	return c.disc.CLIVersion
}

// Declare caches the startup discovery as the C1 declaration without sending
// it. The first Register (the claim loop's preflight, with the §4 backoff)
// carries it, so the startup path has exactly one C1 trigger.
func (c *Client) Declare(d registry.Discovery) {
	c.mu.Lock()
	c.disc = d
	c.refs = declaredRefs(d)
	c.mu.Unlock()
}

// RegisterWithDiscovery declares exactly the runtime set the startup probe
// discovered and caches the ids the server answered with (§1.1 C1).
func (c *Client) RegisterWithDiscovery(ctx context.Context, d registry.Discovery) error {
	c.mu.Lock()
	c.disc = d
	c.refs = declaredRefs(d)
	c.mu.Unlock()
	return c.register(ctx)
}

// Register re-declares the cached startup discovery. It is the 404 rebuild path
// and never starts a new probe (§1.1 failure table).
func (c *Client) Register(ctx context.Context) error { return c.register(ctx) }

// declaredRefs turns a discovery into the declared runtime set, deduplicating
// repeated provider types (a repeat merges into the first entry, §1.1 C1).
func declaredRefs(d registry.Discovery) []RuntimeRef {
	refs := make([]RuntimeRef, 0, len(d.Runtimes))
	seen := make(map[string]bool, len(d.Runtimes))
	for _, rt := range d.Runtimes {
		if rt.Type == "" || seen[rt.Type] {
			continue
		}
		seen[rt.Type] = true
		refs = append(refs, RuntimeRef{Provider: rt.Type, Name: rt.Name})
	}
	return refs
}

// runtimePayloads is the C1 `runtimes[]` array: the startup discovery handed
// over entry by entry (status = online, profile_id always empty, §1.1 C1).
func runtimePayloads(d registry.Discovery) []runtimePayload {
	payloads := make([]runtimePayload, 0, len(d.Runtimes))
	seen := make(map[string]bool, len(d.Runtimes))
	for _, rt := range d.Runtimes {
		if rt.Type == "" || seen[rt.Type] {
			continue
		}
		seen[rt.Type] = true
		payloads = append(payloads, runtimePayload{
			Name:    rt.Name,
			Type:    rt.Type,
			Version: rt.Version,
			Status:  "online",
		})
	}
	return payloads
}

func (c *Client) register(ctx context.Context) error {
	c.mu.RLock()
	disc := c.disc
	prev := append([]RuntimeRef(nil), c.refs...)
	lost := make(map[string]bool, len(c.lost))
	maps.Copy(lost, c.lost)
	c.mu.RUnlock()
	if len(disc.Runtimes) == 0 {
		return errors.New("register: no startup discovery; the startup probe must register before C1")
	}
	body := registerRequest{
		WorkspaceID:     c.cfg.WorkspaceID,
		DaemonID:        c.cfg.DaemonID,
		LegacyDaemonIDs: []string{},
		DeviceName:      "foreman",
		CLIVersion:      disc.CLIVersion,
		LaunchedBy:      "",
		Runtimes:        runtimePayloads(disc),
		FailedProfiles:  disc.FailedProfiles,
	}
	if len(body.Runtimes) == 0 {
		return errors.New("register: startup discovery declares no runtime")
	}
	var resp registerResponse
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodPost, "/api/daemon/register", mustMarshal(body))
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
	c.refs = next
	// Consume only the marks this attempt started with: a rebuild that failed
	// while this C1 was in flight recorded its own provider, and that row still
	// owes its C15 (§1.1 C15, §7 按 runtime 粒度).
	for p := range lost {
		delete(c.lost, p)
	}
	c.repos = resp.Repos
	c.reposVersion = resp.ReposVersion
	c.mu.Unlock()

	c.applyRuntimeChanges(prev, next, lost)
	c.bindStartupCatalog(next, now)
	return nil
}

// runtimeAnswer is one entry of the C1 response `runtimes[]`.
type runtimeAnswer struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Provider string `json:"provider"`
	Status   string `json:"status"`
}

// selectRuntimes binds every declared runtime to exactly one answered runtime
// row: provider ↔ type by value, name matching first, then the lexicographically
// smallest id with a warn naming the candidates (§1.2 S4 tie-break).
func (c *Client) selectRuntimes(declared []runtimePayload, answers []runtimeAnswer) ([]RuntimeRef, error) {
	byProvider := make(map[string][]runtimeAnswer, len(answers))
	for _, a := range answers {
		byProvider[a.Provider] = append(byProvider[a.Provider], a)
	}
	refs := make([]RuntimeRef, 0, len(declared))
	for _, d := range declared {
		candidates := byProvider[d.Type]
		if len(candidates) == 0 {
			return nil, fmt.Errorf("register: response carries no runtime for provider %q", d.Type)
		}
		chosen := candidates[0]
		var tied []string
		for _, cand := range candidates {
			tied = append(tied, cand.ID)
		}
		sort.Strings(tied)
		if len(candidates) > 1 {
			picked := ""
			for _, cand := range candidates {
				if cand.Status == "online" && cand.Name == d.Name && cand.ID == tied[0] {
					picked = cand.ID
				}
			}
			for _, cand := range candidates {
				if picked == "" && cand.Status == "online" && cand.Name == d.Name {
					picked = cand.ID
				}
			}
			if picked == "" {
				picked = tied[0]
			}
			for _, cand := range candidates {
				if cand.ID == picked {
					chosen = cand
				}
			}
			c.log.Warn("runtime.set tied: picking one candidate",
				"provider", d.Type, "picked", chosen.ID, "candidates", tied)
		}
		if chosen.ID == "" {
			return nil, fmt.Errorf("register: empty runtime id for provider %q", d.Type)
		}
		refs = append(refs, RuntimeRef{ID: chosen.ID, Provider: d.Type, Name: d.Name})
	}
	return refs, nil
}

// applyRuntimeChanges emits one change per provider that appeared or moved, runs
// C15 only for the rebuilt rows, and moves the WS subscription of those
// runtimes only. lost is per provider: only a runtime whose own row was proved
// gone (a 404) counts as rebuilt, never every row of the declaration.
func (c *Client) applyRuntimeChanges(prev, next []RuntimeRef, lost map[string]bool) {
	prevByProvider := make(map[string]RuntimeRef, len(prev))
	for _, ref := range prev {
		prevByProvider[ref.Provider] = ref
	}
	for _, ref := range next {
		before, had := prevByProvider[ref.Provider]
		// A rebuild is an id that moved, or any successful C1 that follows a
		// 404 (the row came back, possibly under the same id). A first
		// declaration is neither: C15 belongs to rebuilt rows only (§1.1 C15).
		rebuilt := had && before.ID != "" && (before.ID != ref.ID || lost[ref.Provider])
		if had && before.ID != "" && !rebuilt {
			continue
		}
		if rebuilt {
			c.log.Warn("runtime.rebuilt", "provider", ref.Provider, "runtime_id", ref.ID, "previous", before.ID)
		} else {
			c.log.Info("runtime.registered", "provider", ref.Provider, "runtime_id", ref.ID)
		}
		if rebuilt {
			c.recoverOrphans(context.WithoutCancel(context.Background()), ref.ID)
		}
		c.notifyChange(RuntimeChange{Provider: ref.Provider, OldID: before.ID, NewID: ref.ID})
	}
	nextByProvider := make(map[string]bool, len(next))
	for _, ref := range next {
		nextByProvider[ref.Provider] = true
	}
	for _, ref := range prev {
		if !nextByProvider[ref.Provider] {
			c.notifyChange(RuntimeChange{Provider: ref.Provider, OldID: ref.ID})
		}
	}
}

func (c *Client) notifyChange(change RuntimeChange) {
	select {
	case c.changes <- change:
	default:
		// The subscriber re-reads the whole set on every wake-up, so a dropped
		// notification only delays the reconnect by one tick.
	}
}

// bindStartupCatalog moves the startup discovery result staged by its provider
// into the real runtime's catalog slot (§1.1 C21 第 13 条).
func (c *Client) bindStartupCatalog(refs []RuntimeRef, at time.Time) {
	if c.startup == nil || c.slots == nil {
		return
	}
	for _, ref := range refs {
		body, ok := c.startup.Take(ref.Provider)
		if !ok {
			continue
		}
		c.slots.Put(ref.ID, body, at)
	}
}

// ProviderForRuntime resolves the provider served by a real runtime id.
func (c *Client) ProviderForRuntime(runtimeID string) (string, bool) {
	if runtimeID == "" {
		return "", false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, ref := range c.refs {
		if ref.ID == runtimeID {
			return ref.Provider, true
		}
	}
	return "", false
}

// stageStartupCatalog records the startup discovery result under the provider
// that produced it, until C1 binds it to a real runtime id.
func (c *Client) stageStartupCatalog(provider string, body []byte) {
	if c.startup == nil || provider == "" {
		return
	}
	c.startup.Put(provider, body)
}

// rebuildRuntime re-registers (C1) after a 404 "runtime not found" and returns
// the fresh id of the runtime that was lost. Only that runtime's id moves: the
// other rows answer with the ids they already had (§1.1 failure table).
func (c *Client) rebuildRuntime(ctx context.Context, runtimeID string) (string, error) {
	c.mu.RLock()
	provider := ""
	for _, ref := range c.refs {
		if ref.ID == runtimeID {
			provider = ref.Provider
		}
	}
	c.mu.RUnlock()
	if provider == "" {
		// The id is already stale by the time the rebuild starts (a concurrent
		// path moved it): re-declaring would only write another C1 without
		// moving anything. The caller re-reads the set instead.
		c.log.Warn("runtime rebuild skipped: id already replaced", "runtime_id", runtimeID)
		return "", nil
	}
	if err := c.register(ctx); err != nil {
		c.mu.Lock()
		if c.lost == nil {
			c.lost = map[string]bool{}
		}
		// Only this runtime's row is known lost; the other rows of the
		// declaration did not fail (C15 is per runtime, §1.1).
		c.lost[provider] = true
		c.mu.Unlock()
		return "", err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, ref := range c.refs {
		if ref.Provider == provider {
			return ref.ID, nil
		}
	}
	return "", nil
}

// Heartbeat is C2 for one real runtime. A 404 "runtime not found" rebuilds that
// runtime's row only and retries once (§7 多 runtime 的失败面按 runtime 粒度).
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
		if rerr := c.register(ctx); rerr != nil {
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

// PrepareLease is C4 on the runtime that claimed the task. The §1.1 C4
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
// assembled or validated — the path is the only thing rewritten (§1.1 C21).
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
