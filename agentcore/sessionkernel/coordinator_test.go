package sessionkernel_test

import (
	"context"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/session/writer/writertest"
	"github.com/felinics/twilight/agentcore/sessionkernel"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
	"github.com/felinics/twilight/agentcore/turn/sessionstore/sessionstoretest"
	"testing"
)

// The Coordinator is pure protocol: Start, Deliver and Status commit and read
// without any driver or Loop in the assembly. The Turn stays Open until a
// host drives it.
func TestCoordinatorCommitsWithoutDriver(t *testing.T) {
	ctx := context.Background()
	const sid session.SessionID = "s-protocol"
	registry, err := module.BuildRegistry(chatlog.Module, sessionstore.Module)
	if err != nil {
		t.Fatal(err)
	}
	store := filestoretest.Store(t)
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: 1}); err != nil {
		t.Fatal(err)
	}
	bindings, retention := artifacttest.Stores(t)
	factory := &writer.Factory{Store: store, Registry: registry, Admission: writer.Admission{Bindings: bindings, Ledger: retention}}
	turns, err := sessionstore.NewSessionTurnStore(sessionstore.Config{Registry: registry, Store: store, Frozen: sessionstoretest.Frozen(t, bindings)})
	if err != nil {
		t.Fatal(err)
	}
	c := &sessionkernel.Coordinator{Projections: session.NewProjectionReader(store, registry, nil), TurnStore: turns}
	w, err := writertest.Open(t, ctx, factory, sid, session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}

	submit := func(id chatlog.InputID) turn.AgentInput {
		t.Helper()
		content := turn.MustParseCanonicalJSON(`{"text":"hi"}`)
		if _, err := w.Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) {
			return &writer.SemanticGroup{CommitID: ledger.CommitID("submit/" + string(id)),
				Batches: []writer.TypedBatch{{Domain: chatlog.Stream, Events: []writer.TypedEvent{{
					Type: chatlog.TypeInputSubmitted, RecordedAtUnixMilli: 1,
					Value: chatlog.InputSubmittedPayload{InputID: id, Content: content, SubmittedAtUnixMilli: 1},
				}}}}}, nil
		}); err != nil {
			t.Fatal(err)
		}
		d, err := chatlog.DigestInput(id, content)
		if err != nil {
			t.Fatal(err)
		}
		return turn.AgentInput{ID: turn.InputID(id), Digest: d}
	}

	ref := sessionstore.TurnRef{SessionID: sid, TurnID: "t1"}
	p := preset.PresetRef{ID: "p1", Digest: "sha256:p1"}
	start := sessionkernel.StartRequest{Ref: ref, Inputs: []turn.AgentInput{submit("in-1")}, Preset: p}
	resp, err := c.Start(ctx, w, start)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if resp.Status != turn.TurnActive || resp.Ref.TurnID == "" || resp.Disposition != "" {
		t.Fatalf("start response = %+v, want active with no disposition", resp)
	}
	if again, err := c.Start(ctx, w, start); err != nil || again.Ref.TurnID != resp.Ref.TurnID {
		t.Fatalf("start replay = %+v %v", again, err)
	}
	if _, err := c.Deliver(ctx, w, sessionkernel.DeliverRequest{Ref: ref, Inputs: []turn.AgentInput{submit("in-2")}}); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	status, err := c.Status(ctx, ref)
	if err != nil || status.Status != turn.TurnActive || status.Ref.TurnID != resp.Ref.TurnID {
		t.Fatalf("status = %+v %v", status, err)
	}
	snap, err := turns.Bind(w).Load(ctx, resp.Ref.TurnID)
	if err != nil || snap.State.Status.Terminal() {
		t.Fatalf("run advanced without a driver: %+v %v", snap.State.Status, err)
	}
}
