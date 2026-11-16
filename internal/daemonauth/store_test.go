package daemonauth_test

import (
	"errors"
	"testing"

	"github.com/tsic404/foreman/internal/daemonauth"
)

func env(pairs map[string]string) func(string) string {
	return func(k string) string { return pairs[k] }
}

func TestLoadRequiresBindingAndRejectsDuplicates(t *testing.T) {
	// A token shared by two identities must refuse startup: otherwise one
	// daemon could impersonate another.
	_, err := daemonauth.Load(env(map[string]string{
		daemonauth.EnvDaemonTokens:          "a=same,b=same",
		daemonauth.EnvDaemonTokenWorkspaces: "a=ws,b=ws",
	}))
	if err == nil {
		t.Fatal("a shared token must be refused")
	}

	// A missing workspace binding must refuse startup in the strict form.
	_, err = daemonauth.Load(env(map[string]string{
		daemonauth.EnvDaemonTokens: "a=tok-a",
	}))
	if !errors.Is(err, daemonauth.ErrWorkspaceUnbound) {
		t.Fatalf("err = %v, want ErrWorkspaceUnbound", err)
	}

	// The discovery form allows it, and Bind fills it in later.
	store, err := daemonauth.LoadUnbound(env(map[string]string{
		daemonauth.EnvDaemonTokens: "a=tok-a",
	}))
	if err != nil {
		t.Fatalf("unbound load: %v", err)
	}
	cred, err := store.Verify("tok-a")
	if err != nil || cred.DaemonID != "a" || cred.WorkspaceID != "" {
		t.Fatalf("verify = %+v err=%v", cred, err)
	}
	store.Bind("a", "ws-1")
	if cred, _ := store.Verify("tok-a"); cred.WorkspaceID != "ws-1" {
		t.Fatalf("bind did not stick: %+v", cred)
	}
}

func TestVerifyFailsClosed(t *testing.T) {
	store, err := daemonauth.Load(env(map[string]string{
		daemonauth.EnvDaemonTokens:          "a=tok-a",
		daemonauth.EnvDaemonTokenWorkspaces: "a=ws",
	}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := store.Verify(""); !errors.Is(err, daemonauth.ErrMissingCredential) {
		t.Fatalf("empty token err = %v", err)
	}
	if _, err := store.Verify("tok-b"); !errors.Is(err, daemonauth.ErrUnknownCredential) {
		t.Fatalf("unknown token err = %v", err)
	}
	if _, err := store.Verify("tok-a"); err != nil {
		t.Fatalf("valid token rejected: %v", err)
	}
}
