package owner_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/decision"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	rt "github.com/felinics/twilight/agentcore/runtime"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/sessionkernel"
	"time"
)

// newAuthority is the deployment every owner test starts from: a local
// executor, fresh durable stores under t.TempDir, and the Kernel plus
// Execution assembled over them.
func newAuthority(t *testing.T) (*owner.Owner, *sessionkernel.Kernel) {
	t.Helper()
	catalog, err := local.NewCatalog(nil)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := local.NewLocalExecutor(catalog, catalog, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	exec, err := executor.NewWorker(context.Background(), storetest.NewMap(nil), []executor.Route{local.Route(backend)})
	if err != nil {
		t.Fatal(err)
	}
	bindings, retention := artifacttest.Stores(t)
	decisions, err := decision.NewCatalog(nil)
	if err != nil {
		t.Fatal(err)
	}
	k, err := sessionkernel.New(sessionkernel.Ports{
		Store: filestoretest.Store(t), Content: filestoretest.Content(t, sessionstore.FrozenAuthority),
		Artifacts: sessionkernel.Artifacts{Bindings: bindings, Ledger: retention}})
	if err != nil {
		t.Fatal(err)
	}
	x, err := rt.NewExecution(rt.ExecutionConfig{Executor: exec, Decisions: decisions},
		rt.ExecutionSources{Runs: k.Runs, Projections: k.Projections, Content: k.Content})
	if err != nil {
		t.Fatal(err)
	}
	a := owner.New(k, x)
	t.Cleanup(func() {
		_ = a.Close(context.Background())
		x.Close()
		_ = k.Close(context.Background())
		exec.Close()
	})
	return a, k
}

// OWN-HDL-1: one generation of ownership at a time. A second Open of an
// open Session is refused; a Handle whose generation was released does not
// close the generation that replaced it; reads need no Handle.
func TestHandleGenerations(t *testing.T) {
	ctx := context.Background()
	a, k := newAuthority(t)
	const sid session.SessionID = "s-gen"
	if err := k.CreateSession(ctx, sid, nil); err != nil {
		t.Fatal(err)
	}
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
	if _, err := k.Chatlog.Submit(ctx, second.Writer(), "in-1", run.MustParseCanonicalJSON(`{"text":"hello"}`)); err != nil {
		t.Fatalf("commit through the live generation after a stale close: %v", err)
	}
	// Reading takes no ownership: it works by SessionID while the Handle is
	// open and after it is closed.
	if chat, err := chatlog.ReadSurface(ctx, k.Projections, sid); err != nil || chat.Inputs.Len() != 1 {
		t.Fatalf("read while open = %d %v", chat.Inputs.Len(), err)
	}
	if err := second.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if chat, err := chatlog.ReadSurface(ctx, k.Projections, sid); err != nil || chat.Inputs.Len() != 1 {
		t.Fatalf("read after close = %d %v", chat.Inputs.Len(), err)
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
// Close began is ErrClosed: no Open outlives the Execution and the Kernel
// closing behind Close (the Application.Close order).
func TestCloseWaitsForInFlightOpen(t *testing.T) {
	ctx := context.Background()
	a, k := newAuthority(t)
	if err := k.CreateSession(ctx, "s-inflight", nil); err != nil {
		t.Fatal(err)
	}
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
