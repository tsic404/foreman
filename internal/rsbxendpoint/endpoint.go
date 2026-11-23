// Package rsbxendpoint parses the registered rsbxd target into the transport
// the carrier opens. The management surface and the Job side share this one
// parser: a form the registration accepts is a form the Job can open, and a
// form the carrier cannot serve is refused before a target is stored or a Job
// rendered.
package rsbxendpoint

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
)

// Transport names the carrier's daemon channels.
const (
	// TransportUnix is a local unix socket, given as unix://<path> or an
	// absolute path.
	TransportUnix = "unix"
	// TransportSSH is an explicit ssh destination or command, given as
	// ssh://<destination> or cmd:<command>.
	TransportSSH = "ssh"
	// TransportTLS is the authenticated network channel, given as
	// tls://<host:port> or a bare host:port.
	TransportTLS = "tls"
)

// Target is one parsed endpoint. Exactly the fields of its transport are set.
type Target struct {
	Transport string
	Socket    string
	Address   string
	Command   string
}

// Parse maps the deployer's endpoint onto a carrier transport. A bare
// host:port selects the authenticated network channel: the carrier carries no
// ssh client, so the only historically accepted form must not resolve to a
// program the carrier does not have. Deployers who do carry an ssh client
// name it explicitly.
func Parse(raw string) (Target, error) {
	if strings.ContainsAny(raw, "\x00\r\n") {
		return Target{}, fmt.Errorf("endpoint %q contains a control character", raw)
	}
	value := strings.TrimSpace(raw)
	if value == "" {
		return Target{}, errors.New("the endpoint is empty")
	}
	switch {
	case strings.HasPrefix(value, "cmd:"):
		command := strings.TrimSpace(strings.TrimPrefix(value, "cmd:"))
		if command == "" {
			return Target{}, fmt.Errorf("endpoint %q names an empty command", raw)
		}
		return Target{Transport: TransportSSH, Command: command}, nil
	case strings.HasPrefix(value, "ssh://"):
		parsed, err := url.Parse(value)
		if err != nil || parsed.Host == "" || parsed.Hostname() == "" {
			return Target{}, fmt.Errorf("endpoint %q is not an ssh destination", raw)
		}
		if parsed.Path != "" && parsed.Path != "/" {
			return Target{}, fmt.Errorf("endpoint %q carries an ssh path", raw)
		}
		command := "ssh "
		target := parsed.Host
		if port := parsed.Port(); port != "" {
			if err := validPort(port); err != nil {
				return Target{}, err
			}
			command += "-p " + port + " "
			target = parsed.Hostname()
		}
		if parsed.User != nil {
			target = parsed.User.Username() + "@" + target
		}
		return Target{Transport: TransportSSH, Command: command + target + " rsbxd -stdio"}, nil
	case strings.HasPrefix(value, "unix://"):
		socket := strings.TrimPrefix(value, "unix://")
		if !filepath.IsAbs(socket) {
			return Target{}, fmt.Errorf("endpoint %q does not name an absolute socket path", raw)
		}
		return Target{Transport: TransportUnix, Socket: socket}, nil
	case strings.HasPrefix(value, "/"):
		return Target{Transport: TransportUnix, Socket: value}, nil
	case strings.HasPrefix(value, "tls://"):
		address := strings.TrimPrefix(value, "tls://")
		if err := validateAddress(address); err != nil {
			return Target{}, fmt.Errorf("endpoint %q: %w", raw, err)
		}
		return Target{Transport: TransportTLS, Address: address}, nil
	case strings.Contains(value, "://"):
		return Target{}, fmt.Errorf("endpoint %q names an unsupported transport", raw)
	}
	if strings.ContainsAny(value, "/\\") {
		return Target{}, fmt.Errorf("endpoint %q is not an absolute socket path", raw)
	}
	if err := validateAddress(value); err != nil {
		return Target{}, fmt.Errorf("endpoint %q: %w", raw, err)
	}
	return Target{Transport: TransportTLS, Address: value}, nil
}

// validateAddress requires the host:port shape the network channel dials.
func validateAddress(address string) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return errors.New("endpoint must be host:port")
	}
	return validPort(portText)
}

func validPort(portText string) error {
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("endpoint port must be between 1 and 65535")
	}
	return nil
}
