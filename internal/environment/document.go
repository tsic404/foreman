package environment

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// environmentDocumentKey is the one data key the durable carriers use.
const environmentDocumentKey = "environments.json"

type environmentDocument struct {
	// LedgerID names the ledger instance that wrote the document. A carrier
	// recreated from an empty template is a new instance even if its counter
	// starts at the same value, so probe evidence is bound to this identity.
	LedgerID string `json:"ledger_id,omitempty"`
	// CarrierUID is the carrier object the document was written through. A
	// document restored into a different object is a different instance.
	CarrierUID string `json:"carrier_uid,omitempty"`
	// Generation advances by one on every committed write of this instance.
	Generation   uint64        `json:"generation,omitempty"`
	Environments []Environment `json:"environments"`
	Tombstones   []Tombstone   `json:"tombstones"`
}

// newLedgerID answers a fresh ledger instance identity.
func newLedgerID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("time-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(raw[:])
}

// encodeEnvironmentDocument renders the registry deterministically so a
// no-op rewrite is byte-identical.
func encodeEnvironmentDocument(environments map[ID]Environment, tombstones map[ID]Tombstone, generation uint64, ledgerID, carrierUID string) ([]byte, error) {
	document := environmentDocument{
		LedgerID:     ledgerID,
		CarrierUID:   carrierUID,
		Generation:   generation,
		Environments: make([]Environment, 0, len(environments)),
		Tombstones:   make([]Tombstone, 0, len(tombstones)),
	}
	for _, env := range environments {
		document.Environments = append(document.Environments, env)
	}
	for _, tomb := range tombstones {
		document.Tombstones = append(document.Tombstones, tomb)
	}
	sort.Slice(document.Environments, func(i, j int) bool {
		return document.Environments[i].ID < document.Environments[j].ID
	})
	sort.Slice(document.Tombstones, func(i, j int) bool {
		return document.Tombstones[i].ID < document.Tombstones[j].ID
	})
	raw, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode the environment registry: %w", err)
	}
	return append(raw, '\n'), nil
}

// decodeEnvironmentDocument parses one carrier's document and applies the
// shape migration the previous release needs: an untrusted tree observation
// must never become a frozen ownership fact.
func decodeEnvironmentDocument(raw []byte, source string) (environmentDocument, error) {
	var document environmentDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		return environmentDocument{}, fmt.Errorf("parse the environment registry %s: %w", source, err)
	}
	seenEnvironment := map[ID]bool{}
	for index, env := range document.Environments {
		if env.ID == "" {
			return environmentDocument{}, fmt.Errorf("parse the environment registry %s: an environment has no id", source)
		}
		if seenEnvironment[env.ID] {
			return environmentDocument{}, fmt.Errorf("parse the environment registry %s: environment %s is duplicated", source, env.ID)
		}
		seenEnvironment[env.ID] = true
		document.Environments[index] = normalizeLoadedEnvironment(env)
	}
	seenTombstone := map[ID]bool{}
	for _, tomb := range document.Tombstones {
		if tomb.ID == "" {
			return environmentDocument{}, fmt.Errorf("parse the environment registry %s: a tombstone has no id", source)
		}
		if seenTombstone[tomb.ID] {
			return environmentDocument{}, fmt.Errorf("parse the environment registry %s: tombstone %s is duplicated", source, tomb.ID)
		}
		seenTombstone[tomb.ID] = true
	}
	return document, nil
}

// normalizeLoadedEnvironment derives the registration fact for documents
// written before it existed and moves a tree id that was recorded on a failed
// first attempt into the observation field. Only evidence of a completed
// verification can promote a legacy record: a stale or pending row with a tree
// id may be the failed observation an earlier release froze, and treating it
// as ownership would both count an unverified target and block its repair.
func normalizeLoadedEnvironment(env Environment) Environment {
	if !env.Registered &&
		(!env.Verification.VerifiedAt.IsZero() ||
			env.Verification.State == VerificationOK ||
			env.Phase == PhaseActive) {
		env.Registered = true
	}
	if env.Verification.ObservedTreeID == "" && env.Verification.TreeID != "" {
		env.Verification.ObservedTreeID = env.Verification.TreeID
	}
	if !env.Registered {
		env.Verification.TreeID = ""
	}
	return env
}
