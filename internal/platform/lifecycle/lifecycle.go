// Package lifecycle runs a binary's long-lived parts (HTTP listeners, SQS
// consumers, pollers) and shuts them down in the right order on SIGTERM.
//
// Why this matters on Kubernetes: when a pod is deleted the kubelet sends
// SIGTERM, and at the SAME time the endpoint is removed from the Service. Those
// two things are not synchronised, so for a few seconds traffic may still arrive.
// The sequence below is the standard answer:
//
//  1. SIGTERM received: mark /readyz as 503 ("shutting down")
//  2. wait SHUTDOWN_DELAY (default 5 s) while the load balancer deregisters us;
//     we keep serving normally during the wait
//  3. cancel the components' context: listeners stop accepting and drain
//     in-flight requests, consumers stop polling and finish in-flight messages
//  4. wait up to the drain timeout (25 s; the pod's terminationGracePeriod must
//     be larger, the contracts say 30 s)
//  5. run closers: flush telemetry, close connection pools
//
// AWS services touched: none.
package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Component is something that runs until its context is cancelled.
type Component interface {
	Name() string
	// Run blocks. It must return after ctx is cancelled once it has drained.
	// Returning a non-nil error before ctx is cancelled means the component
	// failed and the whole process should stop.
	Run(ctx context.Context) error
}

// Func adapts a function to Component.
type Func struct {
	N string
	F func(ctx context.Context) error
}

func (f Func) Name() string                  { return f.N }
func (f Func) Run(ctx context.Context) error { return f.F(ctx) }

// Options controls Run.
type Options struct {
	// SetNotReady is called first on shutdown (usually health.SetShuttingDown).
	SetNotReady   func()
	ShutdownDelay time.Duration
	DrainTimeout  time.Duration // default 25s
	// Closers run after components stop, in the order given (telemetry flush,
	// pool close...). Each gets a context bounded by 5 s.
	Closers []func(ctx context.Context) error
}

// Run starts every component and blocks until shutdown completes. It installs
// the SIGTERM/SIGINT handler itself; tests may pass a pre-cancelled parent via
// RunContext.
func Run(opts Options, comps ...Component) error {
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return RunContext(sigCtx, opts, comps...)
}

// RunContext is Run with an explicit "please stop" context.
func RunContext(stopCtx context.Context, opts Options, comps ...Component) error {
	if opts.DrainTimeout == 0 {
		opts.DrainTimeout = 25 * time.Second
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(stopCtx))
	defer cancel()

	var wg sync.WaitGroup
	failed := make(chan error, len(comps))
	for _, c := range comps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Run(runCtx); err != nil && runCtx.Err() == nil {
				slog.Error("component failed", "component", c.Name(), "error", err.Error())
				failed <- err
			}
		}()
	}

	var runErr error
	select {
	case <-stopCtx.Done():
		slog.Info("shutdown requested")
		if opts.SetNotReady != nil {
			opts.SetNotReady()
		}
		time.Sleep(opts.ShutdownDelay) // keep serving while the load balancer deregisters us
	case runErr = <-failed:
	}

	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(opts.DrainTimeout):
		runErr = errors.Join(runErr, errors.New("drain timeout exceeded; exiting with work in flight"))
	}

	for _, closer := range opts.Closers {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := closer(cctx); err != nil {
			slog.Warn("closer failed", "error", err.Error())
		}
		ccancel()
	}
	_ = os.Stdout.Sync()
	return runErr
}
