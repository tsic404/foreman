// The deployment README's hand-written registry template is a startup fixture:
// an operator pastes it into the Secret's environments.json, so it must parse
// and let Foreman start. These cases pin that fixture against the parser and
// the identity rules the same section states, so the prose cannot drift.
package environment_test

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/tsic404/foreman/internal/environment"
)

const (
	readmePath           = "../../deploy/README.md"
	readmeTemplateMarker = "<!-- environment-registry-empty-document -->"
	readmeCarrierUID     = "carrier-1"
)

// readmeRegistryTemplate answers the empty document the deployment README
// tells an operator to copy.
func readmeRegistryTemplate(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("read deploy/README.md: %v", err)
	}
	marker := strings.Index(string(raw), readmeTemplateMarker)
	if marker < 0 {
		t.Fatalf("deploy/README.md carries no %q marker", readmeTemplateMarker)
	}
	rest := string(raw)[marker:]
	open := strings.Index(rest, "```json\n")
	if open < 0 {
		t.Fatal("the registry section carries no json fence after its marker")
	}
	body := rest[open+len("```json\n"):]
	end := strings.Index(body, "\n```")
	if end < 0 {
		t.Fatal("the registry template's json fence is not closed")
	}
	return body[:end] + "\n"
}

// readmeRegistryClient answers a carrier Secret holding one document.
func readmeRegistryClient(document string) *fake.Clientset {
	return fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testRegistrySecret,
			Namespace:       "foreman",
			ResourceVersion: "1",
			UID:             types.UID(readmeCarrierUID),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"environments.json": []byte(document)},
	})
}

func readmeRegistryDocument(t *testing.T, client *fake.Clientset) map[string]json.RawMessage {
	t.Helper()
	secret, err := client.CoreV1().Secrets("foreman").Get(t.Context(), testRegistrySecret, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the carrier: %v", err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(secret.Data["environments.json"], &document); err != nil {
		t.Fatalf("parse the written document: %v", err)
	}
	return document
}

// The copied template starts Foreman, and the first committed write
// materializes the identity the README describes.
func TestReadmeRegistryTemplateStartsAFreshLedger(t *testing.T) {
	template := readmeRegistryTemplate(t)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(template), &fields); err != nil {
		t.Fatalf("the README template is not a JSON object: %v", err)
	}
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	want := []string{"carrier_uid", "environments", "generation", "ledger_id", "tombstones"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("the README template names %v, want %v (the document struct's fields)", names, want)
	}
	if raw := string(fields["environments"]); raw != "[]" {
		t.Fatalf("the README template's environments = %s, want []", raw)
	}
	if raw := string(fields["tombstones"]); raw != "[]" {
		t.Fatalf("the README template's tombstones = %s, want []", raw)
	}

	client := readmeRegistryClient(template)
	store, err := environment.NewSecretStore(t.Context(), client.CoreV1().Secrets("foreman"), testRegistrySecret)
	if err != nil {
		t.Fatalf("startup with the README template = %v, want success", err)
	}
	if generation, err := store.Generation(t.Context()); err != nil || generation != 0 {
		t.Fatalf("Generation after startup = %d, %v, want 0", generation, err)
	}
	if ledgerID, err := store.LedgerID(t.Context()); err != nil || ledgerID == "" {
		t.Fatalf("LedgerID after startup = %q, %v, want a minted identity", ledgerID, err)
	}
	if err := store.PutEnvironment(t.Context(), environment.Environment{ID: "build-a"}); err != nil {
		t.Fatalf("first write: %v", err)
	}
	document := readmeRegistryDocument(t, client)
	var ledgerID, carrierUID string
	var generation uint64
	for name, target := range map[string]any{
		"ledger_id":   &ledgerID,
		"carrier_uid": &carrierUID,
		"generation":  &generation,
	} {
		if err := json.Unmarshal(document[name], target); err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
	}
	if ledgerID == "" {
		t.Fatal("the first write left ledger_id empty")
	}
	if carrierUID != readmeCarrierUID {
		t.Fatalf("the first write stored carrier_uid %q, want the Secret uid %q", carrierUID, readmeCarrierUID)
	}
	if generation != 1 {
		t.Fatalf("the first write stored generation %d, want 1", generation)
	}
}

// The README promises that a top-level array refuses startup and quotes the
// error; the quoted text is the parser's own.
func TestReadmeRegistryRejectsATopLevelArray(t *testing.T) {
	client := readmeRegistryClient("[]\n")
	_, err := environment.NewSecretStore(t.Context(), client.CoreV1().Secrets("foreman"), testRegistrySecret)
	if err == nil {
		t.Fatal("startup with a top-level array succeeded")
	}
	quoted := "json: cannot unmarshal array into Go value of type environment.environmentDocument"
	if !strings.Contains(err.Error(), quoted) {
		t.Fatalf("startup with a top-level array = %v, want it to carry %q", err, quoted)
	}
	readme, readErr := os.ReadFile(readmePath)
	if readErr != nil {
		t.Fatalf("read deploy/README.md: %v", readErr)
	}
	if !strings.Contains(string(readme), quoted) {
		t.Errorf("deploy/README.md does not quote the refusal %q", quoted)
	}
}

// A hand-written identity is adopted only for the carrier object that wrote
// it; any other carrier starts a new ledger instance.
func TestReadmeRegistryAdoptsIdentityOnlyForTheSameCarrier(t *testing.T) {
	same := `{"ledger_id":"hand-written","carrier_uid":"` + readmeCarrierUID + `","generation":4,"environments":[],"tombstones":[]}`
	store, err := environment.NewSecretStore(t.Context(), readmeRegistryClient(same).CoreV1().Secrets("foreman"), testRegistrySecret)
	if err != nil {
		t.Fatalf("startup with a matching identity: %v", err)
	}
	if ledgerID, _ := store.LedgerID(t.Context()); ledgerID != "hand-written" {
		t.Fatalf("LedgerID = %q, want the document's hand-written identity", ledgerID)
	}
	if generation, _ := store.Generation(t.Context()); generation != 4 {
		t.Fatalf("Generation = %d, want the document's 4", generation)
	}

	other := `{"ledger_id":"hand-written","carrier_uid":"another-carrier","generation":4,"environments":[],"tombstones":[]}`
	store, err = environment.NewSecretStore(t.Context(), readmeRegistryClient(other).CoreV1().Secrets("foreman"), testRegistrySecret)
	if err != nil {
		t.Fatalf("startup with a foreign identity: %v", err)
	}
	if ledgerID, _ := store.LedgerID(t.Context()); ledgerID == "hand-written" {
		t.Fatal("a document written through another carrier was adopted")
	}
}
