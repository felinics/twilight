package observe_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

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

func TestTailerReadErrorClosesSubscription(t *testing.T) {
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
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("read error did not close subscription")
		}
	}
}
