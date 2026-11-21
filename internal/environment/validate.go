package environment

import (
	"net"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var (
	environmentIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{1,31}$`)
	domainPattern        = regexp.MustCompile(`^[a-z][a-z0-9-]{1,31}$`)
	capabilityPattern    = regexp.MustCompile(`^[a-z][a-z0-9-]{1,31}$`)
	sourceIDPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	tlsRefPattern        = regexp.MustCompile(`^ref/[a-z0-9][a-z0-9._-]{0,63}$`)
	slotIDPattern        = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

// normalizeRegistration validates the create input and returns the stored
// target. The ID and the target are separate: the target is what the probe
// inspects, the ID is the stable registration identity.
func normalizeRegistration(reg Registration) (ID, Target, string, error) {
	id := strings.TrimSpace(reg.ID)
	if !environmentIDPattern.MatchString(id) {
		return "", Target{}, "", invalid("environment id must be 2-32 lowercase letters, digits or hyphens and start with a letter")
	}
	target, err := normalizeTarget(Target{
		Endpoint:       reg.Endpoint,
		TLSRef:         reg.TLSRef,
		StateRoot:      reg.StateRoot,
		TrustDomain:    reg.TrustDomain,
		AllowedSources: reg.AllowedSources,
		Capabilities:   reg.Capabilities,
		Concurrency:    reg.Concurrency,
	})
	if err != nil {
		return "", Target{}, "", err
	}
	expected := strings.TrimSpace(reg.ExpectedIdentity)
	if err := validateOptionalText("expected identity", expected, 256); err != nil {
		return "", Target{}, "", err
	}
	return ID(id), target, expected, nil
}

// normalizeTarget validates and canonicalizes one target. Every value is
// operator input, so none of it is trusted.
func normalizeTarget(target Target) (Target, error) {
	endpoint := strings.TrimSpace(target.Endpoint)
	if err := validateEndpoint(endpoint); err != nil {
		return Target{}, err
	}
	tlsRef := strings.TrimSpace(target.TLSRef)
	if !tlsRefPattern.MatchString(tlsRef) {
		return Target{}, invalid("TLS reference must be a ref/<name> token")
	}
	root := strings.TrimSpace(target.StateRoot)
	if err := validateStateRoot(root); err != nil {
		return Target{}, err
	}
	domain := strings.TrimSpace(target.TrustDomain)
	if !domainPattern.MatchString(domain) {
		return Target{}, invalid("trust domain must be 2-32 lowercase letters, digits or hyphens and start with a letter")
	}
	sources, err := normalizeSources(target.AllowedSources)
	if err != nil {
		return Target{}, err
	}
	capabilities, err := normalizeCapabilities(target.Capabilities)
	if err != nil {
		return Target{}, err
	}
	if target.Concurrency < 1 || target.Concurrency > MaxConcurrency {
		return Target{}, invalid("concurrency must be between 1 and %d", MaxConcurrency)
	}
	return Target{
		Endpoint:       endpoint,
		TLSRef:         tlsRef,
		StateRoot:      root,
		TrustDomain:    domain,
		AllowedSources: sources,
		Capabilities:   capabilities,
		Concurrency:    target.Concurrency,
	}, nil
}

func validateEndpoint(endpoint string) error {
	if endpoint == "" {
		return invalid("endpoint is required")
	}
	if strings.ContainsAny(endpoint, " \t\r\n") {
		return invalid("endpoint must not contain whitespace")
	}
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" {
		return invalid("endpoint must be host:port")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return invalid("endpoint port must be between 1 and 65535")
	}
	return nil
}

func validateStateRoot(root string) error {
	if root == "" {
		return invalid("state root is required")
	}
	if !strings.HasPrefix(root, "/") {
		return invalid("state root must be an absolute path")
	}
	if strings.ContainsAny(root, "\x00\r\n") {
		return invalid("state root must not contain control characters")
	}
	cleaned := path.Clean(root)
	if cleaned == "/" {
		return invalid("state root must not be the filesystem root")
	}
	if cleaned != root {
		return invalid("state root must be a clean absolute path without a trailing slash or dot segments")
	}
	return nil
}

func normalizeSources(sources []string) ([]string, error) {
	out := make([]string, 0, len(sources))
	seen := map[string]bool{}
	for _, source := range sources {
		source = strings.TrimSpace(source)
		if !sourceIDPattern.MatchString(source) {
			return nil, invalid("source id %q is not a valid source id", source)
		}
		if seen[source] {
			continue
		}
		seen[source] = true
		out = append(out, source)
	}
	sort.Strings(out)
	return out, nil
}

func normalizeCapabilities(capabilities []string) ([]string, error) {
	out := make([]string, 0, len(capabilities))
	seen := map[string]bool{}
	for _, capability := range capabilities {
		capability = strings.TrimSpace(capability)
		if !capabilityPattern.MatchString(capability) {
			return nil, invalid("capability %q is not a valid capability tag", capability)
		}
		if seen[capability] {
			continue
		}
		seen[capability] = true
		out = append(out, capability)
	}
	sort.Strings(out)
	return out, nil
}

func normalizeTrustDomains(domains []string) ([]string, error) {
	out := make([]string, 0, len(domains))
	seen := map[string]bool{}
	for _, domain := range domains {
		domain = strings.TrimSpace(domain)
		if !domainPattern.MatchString(domain) {
			return nil, invalid("trust domain %q is not a valid trust domain", domain)
		}
		if seen[domain] {
			continue
		}
		seen[domain] = true
		out = append(out, domain)
	}
	sort.Strings(out)
	return out, nil
}

func validateOptionalText(name, value string, max int) error {
	if len(value) > max {
		return invalid("%s must be at most %d bytes", name, max)
	}
	if strings.ContainsAny(value, "\x00\r\n") {
		return invalid("%s must not contain control characters", name)
	}
	return nil
}

func validateSlotID(slotID string) error {
	if !slotIDPattern.MatchString(slotID) {
		return invalid("slot id is not a valid token")
	}
	return nil
}

func missingCapabilities(required, observed []string) []string {
	if len(required) == 0 {
		return nil
	}
	have := make(map[string]bool, len(observed))
	for _, capability := range observed {
		have[capability] = true
	}
	var missing []string
	for _, capability := range required {
		if !have[capability] {
			missing = append(missing, capability)
		}
	}
	return missing
}

func hasAllCapabilities(have, required []string) bool {
	return len(missingCapabilities(required, have)) == 0
}

func sortStrings(values []string) {
	sort.Strings(values)
}
