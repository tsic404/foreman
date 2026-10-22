package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/tsic404/foreman/internal/registry"
)

// DeclarationRecord is the shared registration identity of one workspace: the
// declaration body the startup probe produced and the server-assigned real
// runtime ids of its entries (02-contracts.md §0 术语「leader」, §1.1 注册体的
// 取值来源). The control-plane holder writes it after its C1; a follower
// adopts it instead of running its own probe or registration, so a takeover
// reuses the same server-side rows (AC-77 沿用同一注册身份, AC-97).
type DeclarationRecord struct {
	WorkspaceID    string                       `json:"workspace_id"`
	CLIVersion     string                       `json:"cli_version"`
	Runtimes       []registry.DiscoveredRuntime `json:"runtimes"`
	FailedProfiles []json.RawMessage            `json:"failed_profiles,omitempty"`
	// RuntimeIDs maps the identity key (provider \x00 profile id) to the
	// server-assigned real runtime id.
	RuntimeIDs map[string]string `json:"runtime_ids"`
}

// DeclarationRecord renders the current declaration for the shared store.
func (c *Client) DeclarationRecord() DeclarationRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	rec := DeclarationRecord{
		WorkspaceID:    c.cfg.WorkspaceID,
		CLIVersion:     c.disc.CLIVersion,
		Runtimes:       slices.Clone(c.disc.Runtimes),
		FailedProfiles: c.disc.FailedProfiles,
		RuntimeIDs:     make(map[string]string, len(c.refs)),
	}
	for _, ref := range c.refs {
		if ref.ID != "" {
			rec.RuntimeIDs[ref.Key()] = ref.ID
		}
	}
	return rec
}

// Adopt installs a stored declaration without sending C1: the server-side rows
// already exist, and re-registering the same identity is exactly what the
// takeover path forbids (AC-77 不重新注册). The runtime change notifications
// start this instance's heartbeat and subscription streams.
func (c *Client) Adopt(d DeclarationRecord) error {
	if len(d.Runtimes) == 0 {
		return errors.New("declaration carries no runtime")
	}
	disc := registry.Discovery{
		CLIVersion:     d.CLIVersion,
		Runtimes:       slices.Clone(d.Runtimes),
		FailedProfiles: d.FailedProfiles,
	}
	refs := declaredRefs(disc)
	for i := range refs {
		id := d.RuntimeIDs[refs[i].Key()]
		if id == "" {
			return fmt.Errorf("declaration has no runtime id for identity %q", refs[i].Key())
		}
		refs[i].ID = id
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.mu.Lock()
	prev := append([]RuntimeRef(nil), c.refs...)
	c.disc = disc
	c.refs = refs
	c.source = declarationSourceStartup
	c.mu.Unlock()
	// Adoption is not a rebuild: no C15, no id movement.
	c.applyRuntimeChanges(prev, refs, nil)
	return nil
}
