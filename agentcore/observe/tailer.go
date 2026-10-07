package observe

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
)

// History is the shared source of truth followed by SessionLedgerTailer.
type History interface {
	ReadCommits(context.Context, session.CommitReadRequest) (session.CommitPage, error)
}

var (
	// ErrNoHistory reports a durable subscription on a tailer with no ledger.
	ErrNoHistory = errors.New("observe: the tailer has no history")
	// ErrTailerClosed reports a subscription attempted after Close.
	ErrTailerClosed = errors.New("observe: session ledger tailer is closed")
)

const (
	historyPageCommits    = 64
	historyPoll           = 100 * time.Millisecond
	subscriberSendTimeout = 250 * time.Millisecond
)

// SessionLedgerTailer follows committed events in shared History. All durable
// subscribers of one Session share one continuous polling loop. Individual
// subscribers may perform a finite, paged catch-up to the loop's attachment
// boundary; after that boundary only the shared loop reads the ledger.
type SessionLedgerTailer struct {
	registry *module.Registry
	history  History

	mu        sync.Mutex
	closed    bool
	closeDone chan struct{}
	sessions  map[session.SessionID]*sessionTail
}

// NewSessionLedgerTailer returns a durable ledger follower.
func NewSessionLedgerTailer(registry *module.Registry, history History) *SessionLedgerTailer {
	return &SessionLedgerTailer{
		registry: registry, history: history, closeDone: make(chan struct{}),
		sessions: make(map[session.SessionID]*sessionTail),
	}
}

// Committed implements writer.CommitObserver. A local commit is only a
// coalesced low-latency hint; the polling loop still obtains it from History.
func (l *SessionLedgerTailer) Committed(_ context.Context, sid session.SessionID, _ ledger.Commit) {
	l.mu.Lock()
	t := l.sessions[sid]
	l.mu.Unlock()
	if t != nil {
		select {
		case t.wake <- struct{}{}:
		default:
		}
	}
}

// SubscribeFrom follows sid from the inclusive commit sequence from. The
// initial head read is completed before this method returns, so invalid
// Sessions and immediately failing histories are reported synchronously.
func (l *SessionLedgerTailer) SubscribeFrom(ctx context.Context, sid session.SessionID, from ledger.CommitSeq) (<-chan Event, error) {
	if l.history == nil {
		return nil, ErrNoHistory
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		t, err := l.acquire(ctx, sid)
		if err != nil {
			return nil, err
		}
		select {
		case <-t.ready:
		case <-ctx.Done():
			t.releasePending()
			return nil, ctx.Err()
		}
		if t.initErr != nil {
			t.releasePending()
			return nil, t.initErr
		}
		out, err := t.attach(ctx, from)
		if !errors.Is(err, ErrTailerClosed) {
			return out, err
		}
		// A shared tail can finish between acquisition and attachment after
		// its last previous subscriber leaves or a poll fails. The tailer
		// itself is still usable, so transparently acquire the replacement.
	}
}

func (l *SessionLedgerTailer) acquire(ctx context.Context, sid session.SessionID) (*sessionTail, error) {
	for {
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			return nil, ErrTailerClosed
		}
		if old := l.sessions[sid]; old != nil {
			old.mu.Lock()
			if !old.stopping {
				old.pending++
				old.mu.Unlock()
				l.mu.Unlock()
				return old, nil
			}
			done := old.done
			old.mu.Unlock()
			l.mu.Unlock()
			// Keep the stopping loop registered until it is completely done.
			// This lets CloseContext account for every loop and avoids replacing
			// it in the small detach-to-cancel window.
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		t := &sessionTail{
			owner: l, sid: sid, ctx: ctx, cancel: cancel,
			ready: make(chan struct{}), active: make(chan struct{}), wake: make(chan struct{}, 1), done: make(chan struct{}),
			subs: make(map[*durableSubscriber]struct{}), pending: 1,
		}
		l.sessions[sid] = t
		l.mu.Unlock()
		go t.run()
		return t, nil
	}
}

// drop atomically removes a finished loop and publishes its completion.
// CloseContext therefore either observes the loop in sessions and waits for
// done, or observes it absent only after done has already closed.
func (l *SessionLedgerTailer) drop(t *sessionTail) {
	l.mu.Lock()
	if l.sessions[t.sid] == t {
		delete(l.sessions, t.sid)
	}
	close(t.done)
	l.mu.Unlock()
}

// Close stops every Session loop and waits without a deadline. Use
// CloseContext when shutdown must respect a caller's budget.
func (l *SessionLedgerTailer) Close() { _ = l.CloseContext(context.Background()) }

// CloseContext stops every Session loop and closes all durable subscriptions.
// It is idempotent. The first caller starts the shutdown; every caller waits
// for it only until ctx ends, while shutdown itself continues in the
// background so a later caller can still observe completion.
func (l *SessionLedgerTailer) CloseContext(ctx context.Context) error {
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		tails := make([]*sessionTail, 0, len(l.sessions))
		for _, t := range l.sessions {
			t.mu.Lock()
			t.stopping = true
			t.mu.Unlock()
			tails = append(tails, t)
		}
		for _, t := range tails {
			t.cancel()
		}
		done := l.closeDone
		go func() {
			for _, t := range tails {
				<-t.done
			}
			close(done)
		}()
	}
	done := l.closeDone
	l.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type sessionTail struct {
	owner  *SessionLedgerTailer
	sid    session.SessionID
	ctx    context.Context
	cancel context.CancelFunc
	ready  chan struct{}
	active chan struct{}
	wake   chan struct{}
	done   chan struct{}

	initErr error

	mu         sync.Mutex
	cursor     ledger.CommitSeq
	pending    int
	subs       map[*durableSubscriber]struct{}
	stopping   bool
	activeOnce sync.Once
	subsWG     sync.WaitGroup
}

func (t *sessionTail) run() {
	defer func() {
		t.cancel()
		t.mu.Lock()
		t.stopping = true
		for s := range t.subs {
			s.stop(nil)
			delete(t.subs, s)
		}
		t.mu.Unlock()
		t.subsWG.Wait()
		t.owner.drop(t)
	}()

	// Limit one is enough to obtain Head without making a first subscriber's
	// requested history the shared loop's cursor.
	page, err := t.owner.history.ReadCommits(t.ctx, session.CommitReadRequest{SessionID: t.sid, Limit: 1})
	if err != nil {
		t.initErr = err
		close(t.ready)
		return
	}
	t.mu.Lock()
	t.cursor = page.Head.Next
	t.mu.Unlock()
	close(t.ready)

	select {
	case <-t.active:
	case <-t.ctx.Done():
		return
	}

	for {
		t.mu.Lock()
		cursor := t.cursor
		empty := len(t.subs) == 0 && t.pending == 0
		if empty {
			t.stopping = true
		}
		t.mu.Unlock()
		if empty {
			return
		}
		page, err = t.owner.history.ReadCommits(t.ctx, session.CommitReadRequest{
			SessionID: t.sid, From: cursor, Limit: historyPageCommits,
		})
		if err != nil {
			if t.ctx.Err() == nil {
				t.failAll(err)
			}
			return
		}

		type delivery struct {
			seq    ledger.CommitSeq
			events []Event
		}
		deliveries := make([]delivery, 0, len(page.Commits))
		for i := range page.Commits {
			commit := &page.Commits[i]
			deliveries = append(deliveries, delivery{seq: commit.Seq, events: decodeCommit(t.owner.registry, t.sid, commit)})
		}

		t.mu.Lock()
		for _, d := range deliveries {
			if d.seq < t.cursor {
				continue
			}
			for s := range t.subs {
				if d.seq >= s.liveFrom && !s.enqueue(d.events) {
					delete(t.subs, s)
					s.stop(nil)
				}
			}
			t.cursor = d.seq + 1
		}
		empty = len(t.subs) == 0 && t.pending == 0
		if empty {
			t.stopping = true
		}
		t.mu.Unlock()
		if empty {
			return
		}
		if page.HasMore {
			continue
		}
		timer := time.NewTimer(historyPoll)
		select {
		case <-t.wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		case <-t.ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
	}
}

func (t *sessionTail) attach(ctx context.Context, from ledger.CommitSeq) (<-chan Event, error) {
	subCtx, cancel := context.WithCancel(ctx)
	s := &durableSubscriber{
		tail: t, ctx: subCtx, cancel: cancel, from: from,
		out: make(chan Event, subscriberBuffer), mailbox: make(chan Event, subscriberBuffer), stopCh: make(chan struct{}),
	}
	t.mu.Lock()
	if t.pending > 0 {
		t.pending--
	}
	if t.stopping || t.ctx.Err() != nil {
		t.mu.Unlock()
		cancel()
		return nil, ErrTailerClosed
	}
	s.boundary = t.cursor
	s.liveFrom = t.cursor
	if from > s.liveFrom {
		s.liveFrom = from
	}
	t.subs[s] = struct{}{}
	t.subsWG.Add(1)
	t.activeOnce.Do(func() { close(t.active) })
	t.mu.Unlock()
	go s.run()
	return s.out, nil
}

func (t *sessionTail) releasePending() {
	t.mu.Lock()
	if t.pending > 0 {
		t.pending--
	}
	empty := t.pending == 0 && len(t.subs) == 0
	if empty {
		t.stopping = true
	}
	t.mu.Unlock()
	if empty {
		t.cancel()
	}
}

func (t *sessionTail) detach(s *durableSubscriber) {
	t.mu.Lock()
	delete(t.subs, s)
	empty := t.pending == 0 && len(t.subs) == 0
	if empty {
		t.stopping = true
	}
	t.mu.Unlock()
	if empty {
		t.cancel()
	}
}

func (t *sessionTail) failAll(err error) {
	t.mu.Lock()
	t.stopping = true
	for s := range t.subs {
		s.stop(err)
		delete(t.subs, s)
	}
	t.mu.Unlock()
}

type durableSubscriber struct {
	tail     *sessionTail
	ctx      context.Context
	cancel   context.CancelFunc
	from     ledger.CommitSeq
	boundary ledger.CommitSeq
	liveFrom ledger.CommitSeq
	out      chan Event
	mailbox  chan Event
	stopCh   chan struct{}
	stopOnce sync.Once

	errMu sync.Mutex
	err   error
}

// enqueue is called under the Session tail's lock and never blocks.
func (s *durableSubscriber) enqueue(events []Event) bool {
	for i := range events {
		select {
		case s.mailbox <- events[i]:
		default:
			return false
		}
	}
	return true
}

func (s *durableSubscriber) stop(err error) {
	if err != nil {
		s.errMu.Lock()
		if s.err == nil {
			s.err = err
		}
		s.errMu.Unlock()
	}
	s.stopOnce.Do(func() {
		close(s.stopCh)
		s.cancel()
	})
}

func (s *durableSubscriber) run() {
	defer s.tail.subsWG.Done()
	defer close(s.out)
	defer s.tail.detach(s)
	if err := s.catchUp(); err != nil {
		if s.terminalError() != nil {
			s.emitTerminalError()
		} else if s.ctx.Err() == nil && !errors.Is(err, context.Canceled) {
			s.emitError(err)
		}
		return
	}
	for {
		select {
		case event := <-s.mailbox:
			if err := s.send(event); err != nil {
				s.emitTerminalError()
				return
			}
		case <-s.stopCh:
			s.emitTerminalError()
			return
		case <-s.ctx.Done():
			s.emitTerminalError()
			return
		}
	}
}

func (s *durableSubscriber) catchUp() error {
	cursor := s.from
	for cursor < s.boundary {
		page, err := s.tail.owner.history.ReadCommits(s.ctx, session.CommitReadRequest{
			SessionID: s.tail.sid, From: cursor, Limit: historyPageCommits,
		})
		if err != nil {
			return err
		}
		advanced := false
		for i := range page.Commits {
			commit := &page.Commits[i]
			if commit.Seq >= s.boundary {
				break
			}
			for _, event := range decodeCommit(s.tail.owner.registry, s.tail.sid, commit) {
				if err := s.send(event); err != nil {
					return err
				}
			}
			cursor = commit.Seq + 1
			advanced = true
		}
		if !advanced {
			return fmt.Errorf("observe: history ended at commit %d before attachment boundary %d", cursor, s.boundary)
		}
	}
	return nil
}

func (s *durableSubscriber) send(event Event) error {
	select {
	case s.out <- event:
		return nil
	default:
	}
	timer := time.NewTimer(subscriberSendTimeout)
	defer timer.Stop()
	select {
	case s.out <- event:
		return nil
	case <-timer.C:
		return errors.New("observe: durable subscriber is too slow")
	case <-s.stopCh:
		return context.Canceled
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *durableSubscriber) terminalError() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *durableSubscriber) emitTerminalError() {
	if err := s.terminalError(); err != nil {
		s.emitError(err)
	}
}

func (s *durableSubscriber) emitError(err error) {
	select {
	case s.out <- Event{Session: s.tail.sid, Err: err}:
	default:
	}
}
