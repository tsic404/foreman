package scheduler

import (
	"slices"

	"github.com/tsic404/foreman/internal/scope"
)

// partitionKey composes the source/workspace partition key. It exists so
// callers holding a ClaimScope value (whose identifier shadows the package)
// can still name the partition.
func partitionKey(sourceID, workspaceID string) string {
	return scope.Key(sourceID, workspaceID)
}

// ClaimScope is one C3 request's own evidence: the real runtime set that
// request carried and the declared identity each id answered for at that
// moment. It is frozen where the request is built, and admission plus the
// entry's dispatch record are decided from it.
//
// The live declaration is never the authority here. A successful C1 or profile
// refresh may replace it between the claim response and the first entry; the
// upstream's confirmation still belongs to the request that asked for it
// (02-contracts V2-C02 已确认归原 runtime、沿原确认链收敛).
type ClaimScope struct {
	// RuntimeIDs is the exact real runtime set the request carried, in
	// declaration order. Empty means the caller holds no request evidence —
	// a direct claim outside the claim loop — and leaves the legacy path.
	RuntimeIDs []string
	// Identities maps each carried runtime id to its declared identity
	// (registry.RuntimeIdentityKey). The entry keeps it for its whole life, so
	// a later generation change cannot blind S-4's runtime selection.
	Identities map[string]string
}

// Declared reports whether the scope carries the request's runtime set.
func (s ClaimScope) Declared() bool { return len(s.RuntimeIDs) > 0 }

// Owns reports whether the request carried realRuntimeID.
func (s ClaimScope) Owns(realRuntimeID string) bool {
	return realRuntimeID != "" && slices.Contains(s.RuntimeIDs, realRuntimeID)
}

// Identity returns the identity the request's declaration answered for
// realRuntimeID. A runtime the request did not carry has none.
func (s ClaimScope) Identity(realRuntimeID string) (string, bool) {
	identity, ok := s.Identities[realRuntimeID]
	if !ok || identity == "" {
		return "", false
	}
	return identity, true
}
