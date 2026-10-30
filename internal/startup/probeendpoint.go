package startup

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	probeEndpointPollInterval   = 500 * time.Millisecond
	probeEndpointRequestTimeout = 3 * time.Second
	// probeEndpointHealthPath is the ops surface's liveness answer (§9.3): a
	// 200 through the Job-side address proves a ready instance is reachable
	// there, which is exactly the condition the probe daemon needs.
	probeEndpointHealthPath = "/foreman/healthz"
)

// WaitForProbeEndpoint blocks until serverURL — the address every Job daemon
// dials — answers the health endpoint, so the startup probe Job is created
// only once a Job pod dialing that address lands on a ready instance. wait is
// the budget the caller derives from its startup window.
//
// A wait that runs out of time is not a failure of its own: it warns and lets
// the probe run, whose own window keeps the fixed failure classes (§10). The
// escape is deliberate — a never-ready Service must not invent a second
// verdict.
func WaitForProbeEndpoint(ctx context.Context, serverURL string, wait time.Duration, log *slog.Logger) error {
	return waitForProbeEndpoint(ctx, serverURL, log, wait, probeEndpointPollInterval, probeEndpointRequestTimeout)
}

// waitForProbeEndpoint is WaitForProbeEndpoint with the timing parameters
// exposed, so the package tests can drive the wait without a real Service.
func waitForProbeEndpoint(ctx context.Context, serverURL string, log *slog.Logger, window, interval, requestTimeout time.Duration) error {
	if log == nil {
		log = slog.Default()
	}
	target, err := probeEndpointURL(serverURL)
	if err != nil {
		// Not this gate's verdict: an unparseable address is the deployment's
		// configuration error, and the probe's own failure face names it.
		log.Warn("probe.endpoint_wait_skipped", "endpoint", serverURL, "err", err.Error())
		return nil
	}
	client := &http.Client{Timeout: requestTimeout}
	start := time.Now()
	deadline := start.Add(window)
	for {
		attemptErr := probeEndpointAnswer(ctx, client, target, requestTimeout)
		if attemptErr == nil {
			log.Info("probe.endpoint_ready", "endpoint", target, "waited_s", time.Since(start).Seconds())
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !time.Now().Before(deadline) {
			log.Warn("probe.endpoint_wait_timeout",
				"endpoint", target,
				"waited_s", time.Since(start).Seconds(),
				"err", attemptErr.Error(),
				"detail", "creating the probe anyway; a daemon that cannot dial is reported by the probe's failure class")
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// probeEndpointURL joins the health path onto the Job-side address. The
// address carries no path in the shipped deployment; a configured path is
// kept as the prefix rather than replaced.
func probeEndpointURL(serverURL string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(serverURL))
	if err != nil {
		return "", fmt.Errorf("parse job server url: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("job server url %q has no scheme or host", serverURL)
	}
	u.Path = strings.TrimRight(u.Path, "/") + probeEndpointHealthPath
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

// probeEndpointAnswer makes one attempt: only a 2xx answer counts as ready,
// because a non-2xx health answer is the half-dead shape the readiness input
// already refuses (§9.3).
func probeEndpointAnswer(ctx context.Context, client *http.Client, target string, requestTimeout time.Duration) error {
	attemptCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("health endpoint answered %s", resp.Status)
	}
	return nil
}
