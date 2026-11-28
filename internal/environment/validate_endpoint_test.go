package environment

import "testing"

// TestRegistrationAcceptsTheCarrierTransportForms pins that the management
// surface stores exactly the endpoint forms the Job side can open. The
// registration used to accept only host:port and the Job mapped that one form
// to a client the shipped carrier does not contain, so no registered target
// was reachable.
func TestRegistrationAcceptsTheCarrierTransportForms(t *testing.T) {
	for _, endpoint := range []string{
		"build-a.example.test:7443",
		"tls://build-a.example.test:7443",
		"unix:///var/lib/foreman/workspaces/.rsbxd/rsbxd.sock",
		"/var/lib/foreman/workspaces/.rsbxd/rsbxd.sock",
		"ssh://agent@build-a.example.test:2222",
		"cmd:ssh build-a.example.test rsbxd -stdio",
	} {
		t.Run(endpoint, func(t *testing.T) {
			if _, _, _, err := normalizeRegistration(registrationFor(endpoint)); err != nil {
				t.Fatalf("registration with endpoint %q = %v, want acceptance", endpoint, err)
			}
		})
	}
}

func TestRegistrationRefusesFormsTheCarrierCannotOpen(t *testing.T) {
	for _, endpoint := range []string{
		"",
		"relative/socket",
		"tcp://build-a.example.test:7443",
		"build-a.example.test",
		"build-a.example.test:0",
		"unix://relative.sock",
		"build-a.example.test:7443 extra",
	} {
		t.Run(endpoint, func(t *testing.T) {
			if _, _, _, err := normalizeRegistration(registrationFor(endpoint)); err == nil {
				t.Fatalf("registration with endpoint %q was accepted", endpoint)
			}
		})
	}
}

func registrationFor(endpoint string) Registration {
	return Registration{
		ID:             "build-a",
		Endpoint:       endpoint,
		TLSRef:         "ref/build-a",
		StateRoot:      "/srv/rsbx/build-a",
		TrustDomain:    "build",
		AllowedSources: []string{"atlas"},
		Capabilities:   []string{"nix"},
		Concurrency:    2,
	}
}
