// Command foreman-gc is the node-local cache collector. It runs as a
// DaemonSet (one pod per node) with the node's state root mounted at
// /state, reclaiming task directories, bare repo caches and orphan task
// roots by TTL and by the .repos size cap, plus the node-local agent state
// (sessions, transcripts and memory) under /state/home.
//
// Design: docs/05-modules/job-template.md §节点清理, contracts §5.1,
// ADR-005 (node-local hostPath), ADR-006 (shared .repos).
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tsic404/foreman/internal/gc"
	"github.com/tsic404/foreman/internal/observability"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "foreman-gc:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := gc.LoadConfig(os.Getenv)
	if err != nil {
		return err
	}
	if cfg.Node == "" {
		// Outside a DaemonSet (local runs) fall back to the host name so
		// the cache.gc event still carries a node field.
		if host, err := os.Hostname(); err == nil {
			cfg.Node = host
		}
	}

	log := observability.NewLogger(os.Stdout, cfg.LogLevel, "gc")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("gc.started",
		"node", cfg.Node,
		"state_root", cfg.StateRoot,
		"interval", cfg.Interval.String(),
		"quiesce", cfg.Quiesce.String(),
		"cache_ttl", cfg.CacheTTL.String(),
		"task_dir_ttl", cfg.TaskDirTTL.String(),
		"agent_state_ttl", cfg.AgentStateTTL.String(),
		"agent_memory_ttl", cfg.AgentMemoryTTL.String(),
		"cache_max_bytes", cfg.CacheMaxBytes,
	)
	if err := gc.New(cfg, log).Run(ctx); err != nil {
		return err
	}
	log.Info("gc.stopped", "node", cfg.Node)
	return nil
}
