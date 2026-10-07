package observe_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
)

type blockingPollHistory struct {
	base    observe.History
	polls   atomic.Int32
	started chan struct{}
	stopped chan struct{}
}

func (h *blockingPollHistory) ReadCommits(ctx context.Context, req session.CommitReadRequest) (session.CommitPage, error) {
	if req.Limit == 1 {
		return h.base.ReadCommits(ctx, req)
	}
	h.polls.Add(1)
	select {
	case h.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	select {
	case h.stopped <- struct{}{}:
	default:
	}
	return session.CommitPage{}, ctx.Err()
}

// Durable subscribers of one Session share the same sustained read, and the
// read is canceled as soon as the last subscriber leaves.
func TestTailerSharesOnePollAndStopsAfterLastSubscriber(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: "shared"}); err != nil {
		t.Fatal(err)
	}
	history := &blockingPollHistory{base: store, started: make(chan struct{}, 4), stopped: make(chan struct{}, 4)}
	tailer := observe.NewSessionLedgerTailer(registry(t), history)
	defer tailer.Close()

	ctx1, cancel1 := context.WithCancel(ctx)
	if _, err := tailer.SubscribeFrom(ctx1, "shared", 0); err != nil {
		t.Fatal(err)
	}
	select {
	case <-history.started:
	case <-time.After(time.Second):
		t.Fatal("shared polling loop did not start")
	}
	ctx2, cancel2 := context.WithCancel(ctx)
	if _, err := tailer.SubscribeFrom(ctx2, "shared", 0); err != nil {
		t.Fatal(err)
	}
	select {
	case <-history.started:
		t.Fatal("a second subscriber started another sustained poll")
	case <-time.After(50 * time.Millisecond):
	}
	cancel1()
	select {
	case <-history.stopped:
		t.Fatal("the poll stopped while one subscriber remained")
	case <-time.After(50 * time.Millisecond):
	}
	cancel2()
	select {
	case <-history.stopped:
	case <-time.After(time.Second):
		t.Fatal("the last cancellation did not stop the poll")
	}
	if got := history.polls.Load(); got != 1 {
		t.Fatalf("sustained polls = %d, want 1", got)
	}

	// A new subscriber arriving as the idle loop retires must transparently
	// get a replacement loop, not a transient ErrTailerClosed.
	ctx3, cancel3 := context.WithCancel(ctx)
	if _, err := tailer.SubscribeFrom(ctx3, "shared", 0); err != nil {
		t.Fatal(err)
	}
	select {
	case <-history.started:
	case <-time.After(time.Second):
		t.Fatal("replacement polling loop did not start")
	}
	cancel3()
	select {
	case <-history.stopped:
	case <-time.After(time.Second):
		t.Fatal("replacement polling loop did not stop")
	}
	if got := history.polls.Load(); got != 2 {
		t.Fatalf("sustained polls after resubscribe = %d, want 2", got)
	}
}

type notifyingHistory struct {
	observe.History
	poll chan session.CommitReadRequest
}

func (h *notifyingHistory) ReadCommits(ctx context.Context, req session.CommitReadRequest) (session.CommitPage, error) {
	page, err := h.History.ReadCommits(ctx, req)
	if req.Limit != 1 {
		select {
		case h.poll <- req:
		default:
		}
	}
	return page, err
}

func TestTailerLocalCommitWakesPoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := filestoretest.Store(t)
	reg := registry(t)
	const sid session.SessionID = "wake"
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	history := &notifyingHistory{History: store, poll: make(chan session.CommitReadRequest, 8)}
	tailer := observe.NewSessionLedgerTailer(reg, history)
	defer tailer.Close()
	w, err := writer.NewWriters(store, reg, writer.Admission{}, session.OpenOptions{}, writer.WritersConfig{
		Observers: []writer.CommitObserver{tailer},
	}).Writer(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)
	events, err := tailer.SubscribeFrom(ctx, sid, 0)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-history.poll: // the immediate poll at attachment
	case <-time.After(time.Second):
		t.Fatal("initial poll did not run")
	}
	commitRow(t, w, "wake-commit", "woken")
	select {
	case e := <-events:
		if e.Err != nil || e.Value.(rowPayload).Text != "woken" {
			t.Fatalf("event = %+v", e)
		}
	case <-time.After(90 * time.Millisecond):
		t.Fatal("local commit did not wake the poll")
	}
}

func TestTailerSubscribersCatchUpFromDifferentPositions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := filestoretest.Store(t)
	reg := registry(t)
	const sid session.SessionID = "positions"
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	tailer := observe.NewSessionLedgerTailer(reg, store)
	defer tailer.Close()
	w, err := writer.NewWriters(store, reg, writer.Admission{}, session.OpenOptions{}, writer.WritersConfig{
		Observers: []writer.CommitObserver{tailer},
	}).Writer(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)
	commitRow(t, w, "p0", "a")
	commitRow(t, w, "p1", "b")
	commitRow(t, w, "p2", "c")
	all, err := tailer.SubscribeFrom(ctx, sid, 0)
	if err != nil {
		t.Fatal(err)
	}
	late, err := tailer.SubscribeFrom(ctx, sid, 2)
	if err != nil {
		t.Fatal(err)
	}
	commitRow(t, w, "p3", "d")
	gotAll, _ := texts(t, all, 4)
	gotLate, _ := texts(t, late, 2)
	if join(gotAll) != "abcd" || join(gotLate) != "cd" {
		t.Fatalf("all=%v late=%v", gotAll, gotLate)
	}
}

func join(values []string) string {
	var out string
	for _, value := range values {
		out += value
	}
	return out
}

type catchupWindowHistory struct {
	base    observe.History
	started chan struct{}
	release chan struct{}
}

func (h *catchupWindowHistory) ReadCommits(ctx context.Context, req session.CommitReadRequest) (session.CommitPage, error) {
	if req.Limit != 1 && req.From == 0 {
		select {
		case h.started <- struct{}{}:
		default:
		}
		select {
		case <-h.release:
		case <-ctx.Done():
			return session.CommitPage{}, ctx.Err()
		}
	}
	return h.base.ReadCommits(ctx, req)
}

func TestTailerCatchupAndSharedLiveHaveNoAttachmentGap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := filestoretest.Store(t)
	reg := registry(t)
	const sid session.SessionID = "catchup-window"
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	history := &catchupWindowHistory{base: store, started: make(chan struct{}, 1), release: make(chan struct{})}
	tailer := observe.NewSessionLedgerTailer(reg, history)
	defer tailer.Close()
	w, err := writer.NewWriters(store, reg, writer.Admission{}, session.OpenOptions{}, writer.WritersConfig{
		Observers: []writer.CommitObserver{tailer},
	}).Writer(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)
	commitRow(t, w, "window-0", "a")
	commitRow(t, w, "window-1", "b")

	liveCtx, stopLive := context.WithCancel(ctx)
	defer stopLive()
	if _, err := tailer.SubscribeFrom(liveCtx, sid, 2); err != nil {
		t.Fatal(err)
	}
	catchup, err := tailer.SubscribeFrom(ctx, sid, 0)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-history.started:
	case <-time.After(time.Second):
		t.Fatal("historical catch-up did not reach the attachment window")
	}

	// This commit lands after the new subscriber was attached but before its
	// historical catch-up can finish. The shared tail must retain it exactly
	// once while the subscriber replays the older prefix.
	commitRow(t, w, "window-2", "c")
	close(history.release)
	got, positions := texts(t, catchup, 3)
	if join(got) != "abc" {
		t.Fatalf("events across attachment window = %v, want [a b c]", got)
	}
	for i := 1; i < len(positions); i++ {
		if !positions[i-1].Less(positions[i]) {
			t.Fatalf("positions not strictly increasing: %v", positions)
		}
	}
	select {
	case event := <-catchup:
		t.Fatalf("duplicate event across attachment window: %+v", event)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSlowDurableSubscriberIsClosed(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	reg := registry(t)
	const sid session.SessionID = "slow-durable"
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	w, err := writer.NewWriters(store, reg, writer.Admission{}, session.OpenOptions{}, writer.WritersConfig{}).Writer(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		commitRow(t, w, "slow-"+string(rune('a'+i%26))+string(rune('A'+i/26)), "x")
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	tailer := observe.NewSessionLedgerTailer(reg, store)
	defer tailer.Close()
	events, err := tailer.SubscribeFrom(ctx, sid, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Do not consume until the bounded output has remained full long enough
	// for the subscription to classify us as slow.
	time.Sleep(500 * time.Millisecond)
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("slow durable subscriber was not closed")
		}
	}
}

func TestTailerOverflowWithinCommitResumesFromFullPosition(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	reg := registry(t)
	const sid session.SessionID = "overflow-position"
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	tailer := observe.NewSessionLedgerTailer(reg, store)
	defer tailer.Close()
	w, err := writer.NewWriters(store, reg, writer.Admission{}, session.OpenOptions{}, writer.WritersConfig{
		Observers: []writer.CommitObserver{tailer},
	}).Writer(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)

	slow, err := tailer.SubscribeFrom(ctx, sid, 0)
	if err != nil {
		t.Fatal(err)
	}
	typed := make([]writer.TypedEvent, 256)
	for i := range typed {
		typed[i] = writer.TypedEvent{Type: rowType, Value: rowPayload{Text: "x"}}
	}
	result, err := w.Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) {
		return &writer.SemanticGroup{CommitID: "large-commit", Batches: []writer.TypedBatch{{
			Domain: ledger.Domain{Name: "z"}, Events: typed,
		}}}, nil
	})
	if err != nil || result.Outcome != writer.CommitApplied {
		t.Fatalf("large commit: outcome=%s err=%v", result.Outcome, err)
	}
	time.Sleep(500 * time.Millisecond)

	var last ledger.Position
	count := 0
	deadline := time.After(2 * time.Second)
	for {
		select {
		case event, ok := <-slow:
			if !ok {
				if count == 0 || count >= len(typed) {
					t.Fatalf("overflow delivered %d events, want a non-empty strict prefix", count)
				}
				goto reconnect
			}
			last = event.Position
			count++
		case <-deadline:
			t.Fatal("overflowed subscriber did not close")
		}
	}

reconnect:
	replayCtx, stopReplay := context.WithCancel(ctx)
	defer stopReplay()
	replayed, err := tailer.SubscribeFrom(replayCtx, sid, last.Commit)
	if err != nil {
		t.Fatal(err)
	}
	next := last.Index + 1
	deadline = time.After(2 * time.Second)
	for next < uint32(len(typed)) {
		select {
		case event, ok := <-replayed:
			if !ok {
				t.Fatalf("replay closed at index %d", next)
			}
			if !last.Less(event.Position) {
				continue // Inclusive commit replay: the client de-duplicates this prefix.
			}
			if event.Position.Commit != last.Commit || event.Position.Index != next {
				t.Fatalf("replay position = %+v, want commit %d index %d", event.Position, last.Commit, next)
			}
			next++
		case <-deadline:
			t.Fatalf("replay stopped at index %d", next)
		}
	}
}

type failingPollHistory struct {
	base observe.History
	err  error
}

func (h failingPollHistory) ReadCommits(ctx context.Context, req session.CommitReadRequest) (session.CommitPage, error) {
	if req.Limit == 1 {
		return h.base.ReadCommits(ctx, req)
	}
	return session.CommitPage{}, h.err
}

func TestTailerReadErrorIsReportedThenClosesSubscription(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: "read-error"}); err != nil {
		t.Fatal(err)
	}
	want := errors.New("read failed")
	tailer := observe.NewSessionLedgerTailer(registry(t), failingPollHistory{base: store, err: want})
	defer tailer.Close()
	events, err := tailer.SubscribeFrom(ctx, "read-error", 0)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	gotError := false
	for {
		select {
		case event, ok := <-events:
			if !ok {
				if !gotError {
					t.Fatal("read error closed subscription without an error event")
				}
				return
			}
			if errors.Is(event.Err, want) {
				gotError = true
			}
		case <-deadline:
			t.Fatal("read error did not close subscription")
		}
	}
}

type blockingCatchupHistory struct {
	base    observe.History
	started chan struct{}
}

func (h *blockingCatchupHistory) ReadCommits(ctx context.Context, req session.CommitReadRequest) (session.CommitPage, error) {
	if req.Limit != 1 && req.From == 0 {
		select {
		case h.started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return session.CommitPage{}, ctx.Err()
	}
	return h.base.ReadCommits(ctx, req)
}

func TestTailerCloseWaitsForCatchupSubscriber(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	reg := registry(t)
	const sid session.SessionID = "close-catchup"
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	w, err := writer.NewWriters(store, reg, writer.Admission{}, session.OpenOptions{}, writer.WritersConfig{}).Writer(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	commitRow(t, w, "before-close", "history")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}

	history := &blockingCatchupHistory{base: store, started: make(chan struct{}, 1)}
	tailer := observe.NewSessionLedgerTailer(reg, history)
	events, err := tailer.SubscribeFrom(ctx, sid, 0)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-history.started:
	case <-time.After(time.Second):
		t.Fatal("catch-up read did not start")
	}
	closeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := tailer.CloseContext(closeCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-events:
		if ok {
			t.Fatal("subscriber channel remained open after CloseContext returned")
		}
	default:
		t.Fatal("subscriber channel remained open after CloseContext returned")
	}
}
