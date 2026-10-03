package executor

import (
	"errors"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/executor/notice"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// backendNotices is the Worker's subscriber to its Backends' settled Refs
// (notice.Source): one stream per Backend for every execution the Worker
// observes on it, the way effect.Watcher keeps one stream per executor for
// the Owner (RUN-EXE-17). A waiter registers a Ref and is woken when the
// Backend announces it; a stream that drops or changes epoch wakes every
// waiter so each re-reads, which is what the notice protocol asks of a
// subscriber that lost track.
type backendNotices struct {
	worker *Worker

	mu   sync.Mutex
	subs map[ExecutionBackend]*noticeSubscription
}

type noticeSubscription struct {
	mu      sync.Mutex
	waiters map[string][]chan struct{}
	epoch   string
	after   uint64
}

// noticeWait is one waiter's view of a Backend's notice stream: the channel
// a notice for its ref (or a stream reset) signals on, whether the stream
// has delivered anything yet, and how to unregister.
type noticeWait struct {
	signal      <-chan struct{}
	established func() bool
	cancel      func()
}

// await registers ref on backend's subscription. A Backend without notices
// yields a nil signal and a never-established stream: the caller reads at
// intervals. A stream that has not yet delivered a notice may have started
// after the caller's first read, so until it has, the caller keeps a short
// interval too; the notice protocol asks a subscriber to re-read what it
// waits on until its subscription is known to be live.
func (n *backendNotices) await(backend ExecutionBackend, ref string) noticeWait {
	source, ok := backend.(notice.Source)
	if !ok {
		return noticeWait{established: func() bool { return false }, cancel: func() {}}
	}
	n.mu.Lock()
	if n.subs == nil {
		n.subs = make(map[ExecutionBackend]*noticeSubscription)
	}
	sub, started := n.subs[backend]
	if !started {
		sub = &noticeSubscription{waiters: make(map[string][]chan struct{})}
		n.subs[backend] = sub
		n.worker.spawn(func() { n.stream(source, sub) })
	}
	n.mu.Unlock()
	ch := make(chan struct{}, 1)
	sub.mu.Lock()
	sub.waiters[ref] = append(sub.waiters[ref], ch)
	sub.mu.Unlock()
	return noticeWait{
		signal: ch,
		established: func() bool {
			sub.mu.Lock()
			defer sub.mu.Unlock()
			return sub.epoch != ""
		},
		cancel: func() {
			sub.mu.Lock()
			defer sub.mu.Unlock()
			list := sub.waiters[ref]
			for i := range list {
				if list[i] == ch {
					sub.waiters[ref] = append(list[:i], list[i+1:]...)
					break
				}
			}
			if len(sub.waiters[ref]) == 0 {
				delete(sub.waiters, ref)
			}
		},
	}
}

// stream keeps one Settled subscription open for the Worker's lifetime,
// reconnecting after the epoch it last saw; an eviction or an epoch change
// wakes every waiter.
func (n *backendNotices) stream(source notice.Source, sub *noticeSubscription) {
	ctx := n.worker.lifecycle
	for ctx.Err() == nil {
		sub.mu.Lock()
		epoch, after := sub.epoch, sub.after
		sub.mu.Unlock()
		err := source.Settled(ctx, epoch, after, func(r notice.Ref) bool {
			sub.mu.Lock()
			if sub.epoch != "" && r.Epoch != sub.epoch {
				sub.wakeAllLocked()
			}
			sub.epoch, sub.after = r.Epoch, r.Sequence
			for _, ch := range sub.waiters[r.Ref] {
				select {
				case ch <- struct{}{}:
				default:
				}
			}
			sub.mu.Unlock()
			return true
		})
		if ctx.Err() != nil {
			return
		}
		sub.mu.Lock()
		if errors.Is(err, effect.ErrSettlementsEvicted) {
			sub.epoch, sub.after = "", 0
		}
		sub.wakeAllLocked()
		sub.mu.Unlock()
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *noticeSubscription) wakeAllLocked() {
	for _, list := range s.waiters {
		for _, ch := range list {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
}
