package sessionkernel_test

import (
	"context"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/run/sessionstore/sessionstoretest"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/sessionkernel"
	"github.com/felinics/twilight/agentcore/turn"
	"testing"
)

// The Coordinator is pure protocol: Start, Deliver and Status commit and read
// without any driver or Loop in the assembly. The Run stays Open until a
// host drives it.
func TestCoordinatorCommitsWithoutDriver(t *testing.T) {
	ctx := context.Background()
	const sid session.SessionID = "s-protocol"
	registry, err := module.BuildRegistry(chatlog.Module, sessionstore.Module, turn.Module)
	if err != nil {
		t.Fatal(err)
	}
	store := filestoretest.Store(t)
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: 1}); err != nil {
		t.Fatal(err)
	}
	bindings, retention := artifacttest.Stores(t)
	writers := writer.NewWriters(store, registry, writer.Admission{Bindings: bindings, Ledger: retention}, session.OpenOptions{}, writer.WritersConfig{})
	runs, err := sessionstore.NewSessionRunStore(sessionstore.Config{Registry: registry, Store: store, Frozen: sessionstoretest.Frozen(t, bindings)})
	if err != nil {
		t.Fatal(err)
	}
	c := &sessionkernel.Coordinator{Projections: session.NewProjectionReader(store, registry, nil), Runs: runs}
	w, err := writers.Writer(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}

	submit := func(id chatlog.InputID) run.AgentInput {
		t.Helper()
		content := run.MustParseCanonicalJSON(`{"text":"hi"}`)
		w, err := writers.Writer(ctx, sid)
		if err != nil {
			t.Fatal(err)
		}
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
		return run.AgentInput{ID: run.InputID(id), Digest: d}
	}

	ref := turn.TurnRef{SessionID: sid, TurnID: "t1"}
	p := preset.PresetRef{ID: "p1", Digest: "sha256:p1"}
	start := sessionkernel.StartRequest{Ref: ref, Inputs: []run.AgentInput{submit("in-1")}, Preset: p}
	resp, err := c.Start(ctx, w, start)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if resp.Status != turn.TurnActive || resp.RunID == "" || resp.Disposition != "" {
		t.Fatalf("start response = %+v, want active with no disposition", resp)
	}
	if again, err := c.Start(ctx, w, start); err != nil || again.RunID != resp.RunID {
		t.Fatalf("start replay = %+v %v", again, err)
	}
	if _, err := c.Deliver(ctx, w, sessionkernel.DeliverRequest{Ref: ref, Inputs: []run.AgentInput{submit("in-2")}}); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	status, err := c.Status(ctx, ref)
	if err != nil || status.Status != turn.TurnActive || status.RunID != resp.RunID {
		t.Fatalf("status = %+v %v", status, err)
	}
	snap, err := runs.Bind(w).Load(ctx, resp.RunID)
	if err != nil || snap.State.Status.Terminal() {
		t.Fatalf("run advanced without a driver: %+v %v", snap.State.Status, err)
	}
}
