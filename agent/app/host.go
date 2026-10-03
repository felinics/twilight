package app

import (
	"context"
	"sync"
	"time"
)

// host is the process-local lifetime of one open Session: the goroutines
// the Session runs in this process, in two groups. Services are the loops
// that live as long as the Session is open (the inbox applier, the idle
// release); tasks are the pieces of work a call hands to the background
// (a drive to settlement, a snapshot). Wait covers the tasks, IdleFor
// counts them, and Close ends the services before the tasks, so no service
// starts a task after the tasks were told to stop.
type host struct {
	svcCtx     context.Context
	svcCancel  context.CancelFunc
	taskCtx    context.Context
	taskCancel context.CancelFunc
	services   group

	// mu guards the task count, idle and lastActive: tasks counts the
	// tasks in flight, idle is closed when the count returns to zero, and
	// lastActive drives IdleFor.
	mu         sync.Mutex
	tasks      int
	idle       chan struct{}
	lastActive time.Time
}

func newHost() *host {
	h := &host{lastActive: time.Now()}
	h.svcCtx, h.svcCancel = context.WithCancel(context.Background())
	h.taskCtx, h.taskCancel = context.WithCancel(context.Background())
	return h
}

// serve runs fn as a service until Close cancels its ctx.
func (h *host) serve(fn func(ctx context.Context)) {
	h.services.start()
	go func() {
		defer h.services.done()
		fn(h.svcCtx)
	}()
}

// run runs fn as a background task: Close cancels its ctx and Wait covers
// it.
func (h *host) run(fn func(ctx context.Context)) {
	h.track()
	go func() {
		defer h.untrack()
		fn(h.taskCtx)
	}()
}

// wait blocks until every task started so far has finished, or ctx ends.
// It does not cancel anything; Close does.
func (h *host) wait(ctx context.Context) error {
	h.mu.Lock()
	idle, n := h.idle, h.tasks
	h.mu.Unlock()
	if n == 0 {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// touch records activity: the idle clock restarts.
func (h *host) touch() {
	h.mu.Lock()
	h.lastActive = time.Now()
	h.mu.Unlock()
}

// idleFor reports no task has run for at least d since the last recorded
// activity.
func (h *host) idleFor(d time.Duration) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.tasks == 0 && time.Since(h.lastActive) >= d
}

// close ends the services and waits for them, then cancels the tasks and
// waits for them.
func (h *host) close() {
	h.svcCancel()
	_ = h.services.wait(context.Background())
	h.taskCancel()
	_ = h.wait(context.Background()) // the tasks observe the cancelled ctx and return
}

func (h *host) track() {
	h.mu.Lock()
	if h.tasks == 0 {
		h.idle = make(chan struct{})
	}
	h.tasks++
	h.lastActive = time.Now()
	h.mu.Unlock()
}

func (h *host) untrack() {
	h.mu.Lock()
	h.tasks--
	if h.tasks == 0 {
		close(h.idle)
	}
	h.lastActive = time.Now()
	h.mu.Unlock()
}
