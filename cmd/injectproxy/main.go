// Command injectproxy runs the fault-injection access path of the test
// multica cluster environment: a transparent HTTP/WS relay between the
// Foreman instance under test and the test multica server, with an injection
// control plane and a record face on a separate port.
//
// Contract: test-cases/_infra/test-multica-cluster.md §4 (transparency
// invariant, control plane, record face, control/data separation).
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tsic404/foreman/internal/injectproxy"
)

const (
	// shutdownTimeout bounds the control plane's drain.
	shutdownTimeout = 10 * time.Second
	// readHeaderTimeout bounds a control-plane request head.
	readHeaderTimeout = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "injectproxy:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := injectproxy.LoadConfig(os.Getenv)
	if err != nil {
		return err
	}
	log := injectproxy.NewLogger(os.Stderr, cfg.LogLevel, "injectproxy")

	recorder, err := injectproxy.OpenRecorder(cfg.RecordDir, log)
	if err != nil {
		return err
	}
	defer recorder.Close()

	injector := injectproxy.NewInjector()
	server := injectproxy.NewServer(cfg, injector, recorder, log)

	dataListener, err := injectproxy.ListenData(cfg)
	if err != nil {
		return err
	}
	defer dataListener.Close()
	controlListener, err := net.Listen("tcp", cfg.ControlAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.ControlAddr, err)
	}
	defer controlListener.Close()

	control := &http.Server{
		Handler:           injectproxy.NewControl(injector, recorder, log).Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("injectproxy started",
		"data_addr", dataListener.Addr().String(),
		"control_addr", controlListener.Addr().String(),
		"upstream", cfg.Upstream.String(),
		"record_dir", cfg.RecordDir,
		"record_response_bodies", cfg.RecordResponseBodySources(),
	)

	served := make(chan error, 2)
	go func() { served <- server.Serve(ctx, dataListener) }()
	go func() {
		if err := control.Serve(controlListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			served <- err
			return
		}
		served <- nil
	}()

	var serveErr error
	select {
	case serveErr = <-served:
		if serveErr != nil {
			log.Error("listener failed", "err", serveErr)
		}
		stop()
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := control.Shutdown(shutdownCtx); err != nil {
		log.Warn("control plane shutdown", "err", err)
	}
	return serveErr
}
