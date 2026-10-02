package owner_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
	"time"
)

// fixture is what every owner test starts from: a fresh Session store under
// t.TempDir(), the Writers over it and the Owner over those.
type fixture struct {
	owner    *owner.Owner
	store    session.Stores
	registry *module.Registry
	chat     *chatlog.Commands
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	registry, err := module.BuildRegistry(chatlog.Module, sessionstore.Module, turn.Module)
	if err != nil {
		t.Fatal(err)
	}
	bindings, retention := artifacttest.Stores(t)
	store := filestoretest.Store(t)
	writers := writer.NewWriters(store, registry, writer.Admission{Bindings: bindings, Ledger: retention}, session.OpenOptions{}, writer.WritersConfig{})
	a := owner.New(writers)
	t.Cleanup(func() {
		_ = a.Close(context.Background())
		_ = writer.CloseWriters(context.Background(), writers)
	})
	return fixture{owner: a, store: store, registry: registry, chat: &chatlog.Commands{Now: time.Now}}
}

func (f fixture) create(t *testing.T, sid session.SessionID) {
	t.Helper()
	if _, err := f.store.Create(context.Background(), session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: 1}); err != nil {
		t.Fatal(err)
	}
}

func (f fixture) inputs(t *testing.T, sid session.SessionID) int {
	t.Helper()
	chat, err := chatlog.ReadSurface(context.Background(), session.NewProjectionReader(f.store, f.registry, nil), sid)
	if err != nil {
		t.Fatal(err)
	}
	return chat.Inputs.Len()
}

// One generation of ownership at a time. A second Open of an open Session
// is refused; a Handle whose generation was released does not close the
// generation that replaced it; reads need no Handle.
func TestHandleGenerations(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	a := f.owner
	const sid session.SessionID = "s-gen"
	f.create(t, sid)
	first, err := a.Open(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Open(ctx, sid); !errors.Is(err, owner.ErrSessionOpen) {
		t.Fatalf("second open = %v, want ErrSessionOpen", err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := a.Open(ctx, sid)
	if err != nil {
		t.Fatalf("open after close: %v", err)
	}
	// The stale Handle's Close releases nothing: the second generation keeps
	// its Writer and can still commit.
	if err := first.Close(ctx); err != nil {
		t.Fatalf("stale close = %v, want nil", err)
	}
	if _, err := f.chat.Submit(ctx, second.Writer(), "in-1", jsonstable.MustParse(`{"text":"hello"}`)); err != nil {
		t.Fatalf("commit through the live generation after a stale close: %v", err)
	}
	// Reading takes no ownership: it works by SessionID while the Handle is
	// open and after it is closed.
	if n := f.inputs(t, sid); n != 1 {
		t.Fatalf("read while open = %d", n)
	}
	if err := second.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if n := f.inputs(t, sid); n != 1 {
		t.Fatalf("read after close = %d", n)
	}
	// Reading did not reopen the Session: a third Open succeeds.
	third, err := a.Open(ctx, sid)
	if err != nil {
		t.Fatalf("open after reads: %v", err)
	}
	if err := third.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// gatedWriters blocks every Writer call behind gate, so a test parks an
// Open inside the acquisition and observes the Owner's shutdown behaviour.
type gatedWriters struct {
	writer.Writers
	gate    chan struct{}
	started chan struct{}
}

func (g *gatedWriters) Writer(ctx context.Context, sid session.SessionID) (writer.Writer, error) {
	g.started <- struct{}{}
	select {
	case <-g.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return g.Writers.Writer(ctx, sid)
}

// Close waits for an Open in flight before returning, and an Open after
// Close began is ErrClosed: no Open outlives what the caller closes behind
// Close.
func TestCloseWaitsForInFlightOpen(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	a := f.owner
	f.create(t, "s-inflight")
	gate := make(chan struct{})
	a.Writers = &gatedWriters{Writers: a.Writers, gate: gate, started: make(chan struct{}, 1)}
	opened := make(chan error, 1)
	go func() {
		h, err := a.Open(ctx, "s-inflight")
		if err != nil {
			opened <- err
			return
		}
		opened <- h.Close(ctx)
	}()
	<-a.Writers.(*gatedWriters).started
	closed := make(chan error, 1)
	go func() { closed <- a.Close(ctx) }()
	select {
	case err := <-closed:
		t.Fatalf("close returned %v while an Open was in flight", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(gate)
	if err := <-closed; err != nil {
		t.Fatalf("close = %v", err)
	}
	if err := <-opened; err != nil {
		t.Fatalf("in-flight open's handle close = %v", err)
	}
	if _, err := a.Open(ctx, "s-inflight"); !errors.Is(err, owner.ErrClosed) {
		t.Fatalf("open after close = %v, want ErrClosed", err)
	}
}
