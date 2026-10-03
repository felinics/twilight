// Package serve runs one component's HTTP server the way every binary of
// the reference agent does: health and readiness beside the component's
// handler, a listener bound before Ready reports true, and a shutdown on
// SIGTERM or SIGINT that drains connections within the grace period and
// then closes the component (the leases it holds are released there).
package serve

import (
	"context"
	"errors"
	"net"
	stdhttp "net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

// Component is what a binary serves: its handler and the close that
// releases what it holds.
type Component interface {
	Handler() stdhttp.Handler
	Close(context.Context) error
}

// Options tune Run.
type Options struct {
	// Grace bounds the drain of in-flight requests at shutdown; zero selects
	// DefaultGrace.
	Grace time.Duration
	// Ready, when set, gates /readyz; nil reports ready once listening.
	Ready func() bool
}

// DefaultGrace is the shutdown drain when Options give none.
const DefaultGrace = 15 * time.Second

// Handler wraps a component's handler with /healthz and /readyz.
func Handler(component stdhttp.Handler, ready func() bool) stdhttp.Handler {
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w stdhttp.ResponseWriter, _ *stdhttp.Request) { w.WriteHeader(stdhttp.StatusNoContent) })
	mux.HandleFunc("GET /readyz", func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
		if ready != nil && !ready() {
			w.WriteHeader(stdhttp.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(stdhttp.StatusNoContent)
	})
	mux.Handle("/", component)
	return mux
}

// Run serves the component on addr until ctx ends or a termination signal
// arrives, then drains and closes it. It returns nil on a clean stop.
func Run(ctx context.Context, addr string, component Component, opts Options) error {
	grace := opts.Grace
	if grace <= 0 {
		grace = DefaultGrace
	}
	var listening atomic.Bool
	ready := func() bool {
		if !listening.Load() {
			return false
		}
		return opts.Ready == nil || opts.Ready()
	}
	server := &stdhttp.Server{Addr: addr, Handler: Handler(component.Handler(), ready), ReadHeaderTimeout: 10 * time.Second}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		_ = component.Close(context.Background())
		return err
	}
	listening.Store(true)
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)
	defer stop()
	errs := make(chan error, 1)
	go func() {
		if err := server.Serve(ln); err != nil && !errors.Is(err, stdhttp.ErrServerClosed) {
			errs <- err
			return
		}
		errs <- nil
	}()
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errs:
		if serveErr != nil {
			_ = component.Close(context.Background())
			return serveErr
		}
	}
	drain, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	shutdownErr := server.Shutdown(drain)
	closeErr := component.Close(drain)
	if shutdownErr != nil {
		return shutdownErr
	}
	return closeErr
}
