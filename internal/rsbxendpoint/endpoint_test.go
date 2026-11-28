package rsbxendpoint

import "testing"

// TestParseAcceptsEveryCarrierChannel pins the shared vocabulary: the
// management surface and the Job side must agree on every form, because a
// form the registration accepts but the carrier cannot open is a target no
// attempt can ever use.
func TestParseAcceptsEveryCarrierChannel(t *testing.T) {
	for _, tc := range []struct {
		endpoint string
		want     Target
	}{
		{endpoint: "/run/rsbx/daemon.sock", want: Target{Transport: TransportUnix, Socket: "/run/rsbx/daemon.sock"}},
		{endpoint: "unix:///run/rsbx/daemon.sock", want: Target{Transport: TransportUnix, Socket: "/run/rsbx/daemon.sock"}},
		{endpoint: "ssh://agent@env.example.test:2222", want: Target{Transport: TransportSSH, Command: "ssh -p 2222 agent@env.example.test rsbxd -stdio"}},
		{endpoint: "ssh://env.example.test", want: Target{Transport: TransportSSH, Command: "ssh env.example.test rsbxd -stdio"}},
		{endpoint: "cmd:rsbxd -stdio", want: Target{Transport: TransportSSH, Command: "rsbxd -stdio"}},
		{endpoint: "tls://env.example.test:7443", want: Target{Transport: TransportTLS, Address: "env.example.test:7443"}},
		// A bare address is the authenticated network channel. Mapping it to
		// ssh made the only accepted registration form require a program the
		// shipped carrier does not contain.
		{endpoint: "env.example.test:7443", want: Target{Transport: TransportTLS, Address: "env.example.test:7443"}},
	} {
		t.Run(tc.endpoint, func(t *testing.T) {
			got, err := Parse(tc.endpoint)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.endpoint, err)
			}
			if got != tc.want {
				t.Fatalf("Parse(%q) = %+v, want %+v", tc.endpoint, got, tc.want)
			}
		})
	}
}

func TestParseRefusesFormsNoCarrierChannelServes(t *testing.T) {
	for _, endpoint := range []string{
		"",
		"relative/socket",
		"tcp://env.example.test:7443",
		"env.example.test",
		"env.example.test:0",
		"env.example.test:70000",
		"unix://relative.sock",
		"cmd:",
		"ssh://",
		"env.example.test:7443\n",
	} {
		t.Run(endpoint, func(t *testing.T) {
			if got, err := Parse(endpoint); err == nil {
				t.Fatalf("Parse(%q) = %+v, want a refusal", endpoint, got)
			}
		})
	}
}
