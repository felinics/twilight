package writer

import (
	"context"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/session"
)

// heartbeat renews a Writer's kernel lease at a third of its duration
// (EXT-WRT-11), the same shape as the Executor Worker's heartbeat over an
// execution record. A fenced Renew reports the loss once and the heartbeat
// ends; any other failure is retried at the next tick, because the lease
// stays held until it expires or is superseded.
type heartbeat struct {
	stopOnce sync.Once
	stopped  chan struct{}
	done     chan struct{}
}

func startHeartbeat(h session.Handle, lease time.Duration, lost func(error)) *heartbeat {
	hb := &heartbeat{stopped: make(chan struct{}), done: make(chan struct{})}
	interval := max(lease/3, time.Millisecond)
	go func() {
		defer close(hb.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-hb.stopped:
				return
			case <-t.C:
				if err := h.Renew(context.Background()); err != nil && session.IsCode(err, session.ErrOwnershipLost) {
					lost(err)
					return
				}
			}
		}
	}()
	return hb
}

// stop ends the heartbeat and waits for its goroutine.
func (hb *heartbeat) stop() {
	hb.stopOnce.Do(func() { close(hb.stopped) })
	<-hb.done
}
