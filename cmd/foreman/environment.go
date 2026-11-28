package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
)

const (
	// EnvEnvironmentStateFile names the development-only document that keeps
	// registered environments and retirement tombstones across a restart. The
	// Deployment uses the Secret carrier below so the registry survives a Pod
	// rebuild.
	EnvEnvironmentStateFile = "FOREMAN_ENVIRONMENT_STATE_FILE"
	// EnvEnvironmentStateSecret names the Secret that carries the environment
	// registry across Pods.
	EnvEnvironmentStateSecret = "FOREMAN_ENVIRONMENT_STATE_SECRET"
	// EnvEnvironmentStateNamespace is the namespace of that Secret.
	EnvEnvironmentStateNamespace = "FOREMAN_ENVIRONMENT_STATE_NAMESPACE"
	// EnvEnvironmentProbe names the deployment's read-only verification helper.
	// The helper receives the target as FOREMAN_ENVIRONMENT_TARGET JSON and
	// answers the verified facts on stdout.
	EnvEnvironmentProbe = "FOREMAN_ENVIRONMENT_PROBE"
	// EnvEnvironmentTrustDomains is the comma-separated data-residency
	// allowlist. Empty means no domain is approved, so registrations are
	// refused.
	EnvEnvironmentTrustDomains = "FOREMAN_ENVIRONMENT_TRUST_DOMAINS"

	defaultEnvironmentStateFile      = "/var/lib/foreman/environments.json"
	defaultEnvironmentStateSecret    = "foreman-environment-registry"
	defaultEnvironmentStateNamespace = "foreman"
)

// newEnvironmentService builds the registered-environment lifecycle. Without
// a probe helper every registration stays pending: reachability is never
// guessed, so a target cannot enter the scheduling pool unverified.
func newEnvironmentService(getenv func(string) string, clientset kubernetes.Interface, log *slog.Logger) (*environment.Service, error) {
	var store environment.Store
	stateFile := strings.TrimSpace(getenv(EnvEnvironmentStateFile))
	switch {
	case stateFile != "":
		fileStore, err := environment.NewFileStore(stateFile)
		if err != nil {
			return nil, err
		}
		store = fileStore
		log.Warn("environment.state_file",
			"state_file", stateFile,
			"detail", "an explicit file carrier is for development and tests; it does not survive a Pod rebuild")
	case clientset != nil:
		name := strings.TrimSpace(getenv(EnvEnvironmentStateSecret))
		if name == "" {
			name = defaultEnvironmentStateSecret
		}
		namespace := strings.TrimSpace(getenv(EnvEnvironmentStateNamespace))
		if namespace == "" {
			namespace = defaultEnvironmentStateNamespace
		}
		secretStore, err := environment.NewSecretStore(context.Background(), clientset.CoreV1().Secrets(namespace), name)
		if err != nil {
			return nil, err
		}
		store = secretStore
		log.Info("environment.state_secret", "name", name)
	default:
		stateFile = defaultEnvironmentStateFile
		fileStore, err := environment.NewFileStore(stateFile)
		if err != nil {
			return nil, err
		}
		store = fileStore
		log.Warn("environment.state_file",
			"state_file", stateFile,
			"detail", "the file carrier is for development and tests; it does not survive a Pod rebuild")
	}

	probePath := strings.TrimSpace(getenv(EnvEnvironmentProbe))
	var probe environment.Probe
	if probePath == "" {
		log.Warn("environment.probe_absent",
			"detail", "no probe helper is configured; every registration stays pending and out of the scheduling pool")
		probe = environment.UnavailableProbe{Reason: "no environment probe is configured; registrations stay pending"}
	} else {
		probe = environment.ExecProbe{Path: probePath}
	}

	domains := splitCommaList(getenv(EnvEnvironmentTrustDomains))
	if len(domains) == 0 {
		log.Warn("environment.trust_domains_unset",
			"detail", "no data-residency allowlist is configured; every registration is refused until one is set")
	}
	service, err := environment.New(environment.Config{
		Store:               store,
		Probe:               probe,
		AllowedTrustDomains: domains,
		RuleReferences:      environment.NewMemoryRuleReferences(),
		Logger:              log,
	})
	if err != nil {
		return nil, err
	}
	log.Info("environment.configured",
		"probe_configured", probePath != "",
		"trust_domains", len(domains))
	return service, nil
}

// newEnvironmentAdmin mounts the environment routes on the management surface.
// A workspace-bound ops credential is resolved to the sources it may read, so
// an environment row is visible only when it is unrestricted or its
// allowed-source set intersects that grant. Mutations need the deployment-wide
// credential.
func newEnvironmentAdmin(service *environment.Service, catalogService *catalog.Service) (*environment.AdminHandler, error) {
	if service == nil || catalogService == nil {
		return nil, errors.New("environment: the admin surface needs both the environment and catalog services")
	}
	return environment.NewAdminHandler(environment.AdminConfig{
		Service: service,
		Grant: func(r *http.Request) (environment.Grant, bool) {
			scopes, wide, ok := observability.CallerScopes(r.Context())
			if !ok {
				return environment.Grant{}, false
			}
			if wide {
				return environment.Grant{DeploymentWide: true}, true
			}
			converted := make([]catalog.Scope, 0, len(scopes))
			for _, scope := range scopes {
				converted = append(converted, catalog.Scope{
					ServerID: catalog.ServerID(scope.ServerID), WorkspaceID: scope.WorkspaceID,
				})
			}
			resolved := catalogService.GrantForScopes(r.Context(), converted)
			return environmentGrantFromCatalog(resolved), true
		},
		Actor: func(*http.Request) string { return "ops-credential" },
	})
}

// environmentGrantFromCatalog maps the catalog's (server, workspace) scope to
// the environment registry's source grant. Overlapping scopes collapse to one
// source so an environment is never counted twice.
func environmentGrantFromCatalog(grant catalog.Grant) environment.Grant {
	if grant.IsDeploymentWide() {
		return environment.Grant{DeploymentWide: true}
	}
	sources := make([]string, 0, len(grant.Scopes))
	seen := map[string]bool{}
	for _, scope := range grant.Scopes {
		if scope.ServerID == "" || seen[string(scope.ServerID)] {
			continue
		}
		seen[string(scope.ServerID)] = true
		sources = append(sources, string(scope.ServerID))
	}
	return environment.Grant{Sources: sources}
}

// newAdminRoot sends the environment subtree to its own route table and every
// other management path to the handler that owns it. One handler is mounted at
// /admin/ by newOpsRoot, so the environment table is joined here instead of
// widening the ops route set.
func newAdminRoot(managementAdmin, environmentAdmin, rulesAdmin http.Handler) http.Handler {
	mux := http.NewServeMux()
	if environmentAdmin != nil {
		mux.Handle("/admin/environments", environmentAdmin)
		mux.Handle("/admin/environments/", environmentAdmin)
	}
	if rulesAdmin != nil {
		mux.Handle("/admin/rules", rulesAdmin)
		mux.Handle("/admin/rules/", rulesAdmin)
		mux.Handle("/admin/readiness", rulesAdmin)
	}
	if managementAdmin != nil {
		mux.Handle("/", managementAdmin)
	}
	return mux
}

func splitCommaList(raw string) []string {
	out := make([]string, 0, 4)
	for _, value := range strings.Split(raw, ",") {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

// newPodCloseObserver answers the release gate's second half from the bound
// Job's own pods. "Closed" means no Pod exists or every Pod has reached a
// terminal phase; a missing Job name is unknown, never closed.
func newPodCloseObserver(clientset kubernetes.Interface, defaultNamespace string) func(context.Context, registry.TaskEntry, attempt.Binding) (bool, error) {
	if clientset == nil {
		return nil
	}
	return func(ctx context.Context, entry registry.TaskEntry, _ attempt.Binding) (bool, error) {
		if strings.TrimSpace(entry.JobName) == "" {
			return false, errors.New("the entry names no Job")
		}
		namespace := strings.TrimSpace(entry.JobNamespace)
		if namespace == "" {
			namespace = defaultNamespace
		}
		pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: "job-name=" + entry.JobName,
		})
		if err != nil {
			return false, err
		}
		if len(pods.Items) == 0 {
			// The caller is a live process inside the bound Job, so an empty
			// list means the observation failed to match its Pod — unknown,
			// not closed.
			return false, errors.New("no pod matches the bound Job")
		}
		for _, pod := range pods.Items {
			if pod.Status.Phase != "Succeeded" && pod.Status.Phase != "Failed" {
				return false, nil
			}
		}
		return true, nil
	}
}

// newFixedDeliveryGate holds a fixed attempt's payload until the supervisor
// has verified the registration and committed "started" to the shared ledger.
// The commit is the only server-visible proof that the Job-side preconditions
// passed; without it the daemon receives an empty batch and keeps polling.
func newFixedDeliveryGate(slots *environment.Service) func(context.Context, registry.TaskEntry) (bool, error) {
	if slots == nil {
		return nil
	}
	return func(ctx context.Context, entry registry.TaskEntry) (bool, error) {
		if len(entry.FixedAttempt) == 0 {
			// The legacy template path has no shared reservation to wait for.
			return true, nil
		}
		var binding attempt.Binding
		if err := json.Unmarshal(entry.FixedAttempt, &binding); err != nil {
			return false, err
		}
		if binding.EnvironmentID == "" || binding.SlotID == "" {
			return false, errors.New("the frozen attempt names no reservation")
		}
		slot, found, err := slots.SlotState(ctx, environment.ID(binding.EnvironmentID), binding.SlotID)
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}
		return slot.State == environment.SlotActive && slot.Started && slot.ClaimID != "", nil
	}
}
