package unit_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/attempt"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
)

func newWriter(t *testing.T) writer.Writer {
	t.Helper()
	ctx := context.Background()
	registry, err := extension.BuildRegistry(chatlog.Module, runmod.Module, attempt.Module)
	if err != nil {
		t.Fatal(err)
	}
	store := filestoretest.Store(t)
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	bindings, ledger := artifacttest.Stores(t)
	w, err := writer.OpenWriter(ctx, store, registry, writer.Admission{Bindings: bindings, Ledger: ledger}, "s", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func submitted(id chatlog.InputID) unit.Part {
	return unit.PartFunc(func(_ context.Context, _ writer.View, now int64) ([]writer.TypedBatch, error) {
		return []writer.TypedBatch{{Stream: chatlog.Stream, Events: []writer.TypedEvent{{
			Type: chatlog.TypeInputSubmitted, RecordedAtUnixMilli: now,
			Value: chatlog.InputSubmittedPayload{InputID: id, Content: run.MustParseCanonicalJSON(`{"text":"x"}`), SubmittedAtUnixMilli: now}}}}}, nil
	})
}

// Parts writing the same stream are merged into one batch in Part order,
// streams keep the order of their first Part, a Part error refuses the unit
// with nothing written, and a replay of the CommitID is already applied
// without preparing any Part.
func TestCommitMergesRefusesAndReplays(t *testing.T) {
	ctx := context.Background()
	w := newWriter(t)
	newRun, err := run.BuildNewRun("r1", "")
	if err != nil {
		t.Fatal(err)
	}
	prepared := 0
	counting := unit.PartFunc(func(ctx context.Context, v writer.View, now int64) ([]writer.TypedBatch, error) {
		prepared++
		return submitted("b").Prepare(ctx, v, now)
	})
	work := unit.Work{CommitID: "u1", Parts: []unit.Part{submitted("a"), runmod.CreateRun(newRun, nil), counting}}
	res, err := unit.Commit(ctx, w, 7, work)
	if err != nil || res.Outcome != writer.CommitApplied {
		t.Fatalf("commit = %+v %v", res, err)
	}
	if len(res.Commit.Batches) != 2 || res.Commit.Batches[0].Stream != chatlog.Stream || len(res.Commit.Batches[0].Events) != 2 ||
		res.Commit.Batches[1].Stream != runmod.Stream("r1") {
		t.Fatalf("batches = %+v", res.Commit.Batches)
	}
	if res.Commit.Batches[0].Events[0].RecordedAtUnixMilli != 7 {
		t.Fatal("now not stamped")
	}
	again, err := unit.Commit(ctx, w, 8, work)
	if err != nil || again.Outcome != writer.CommitAlreadyApplied || again.Commit.Seq != res.Commit.Seq || prepared != 1 {
		t.Fatalf("replay = %+v %v prepared=%d", again, err, prepared)
	}
	boom := errors.New("boom")
	refused := unit.Work{CommitID: "u2", Parts: []unit.Part{submitted("c"), unit.PartFunc(func(context.Context, writer.View, int64) ([]writer.TypedBatch, error) { return nil, boom })}}
	if _, err := unit.Commit(ctx, w, 9, refused); !errors.Is(err, boom) {
		t.Fatalf("refused unit = %v", err)
	}
	_, err = w.Commit(ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		if v.Committed("u2") {
			t.Fatal("refused unit reached the ledger")
		}
		if _, ok := v.StreamHead(runmod.Stream("r1")); !ok {
			t.Fatal("run stream not indexed")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unit.Commit(ctx, w, 1, unit.Work{CommitID: "u3"}); err == nil {
		t.Fatal("unit without events accepted")
	}
}

// A CommitID the log holds is already applied without preparing a Part,
// whatever the later unit would have written (EXT-WRT-2): the identity is
// the operation.
func TestCommitReplaysByCommitID(t *testing.T) {
	ctx := context.Background()
	w := newWriter(t)
	res, err := unit.Commit(ctx, w, 1, unit.Work{CommitID: "u1", Parts: []unit.Part{submitted("a")}})
	if err != nil || res.Outcome != writer.CommitApplied {
		t.Fatalf("commit = %+v %v", res, err)
	}
	boom := unit.PartFunc(func(context.Context, writer.View, int64) ([]writer.TypedBatch, error) {
		t.Fatal("replay prepared a part")
		return nil, nil
	})
	if again, err := unit.Commit(ctx, w, 2, unit.Work{CommitID: "u1", Parts: []unit.Part{boom}}); err != nil || again.Outcome != writer.CommitAlreadyApplied || again.Commit.Seq != res.Commit.Seq {
		t.Fatalf("replay = %+v %v", again, err)
	}
	// A CommitID written by a plain writer.Commit is a replay for a unit too.
	if _, err := w.Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) {
		return &writer.SemanticGroup{CommitID: "plain", Batches: []writer.TypedBatch{{Stream: chatlog.Stream, Events: []writer.TypedEvent{{
			Type: chatlog.TypeInputSubmitted, Value: chatlog.InputSubmittedPayload{InputID: "p", Content: run.MustParseCanonicalJSON(`{"text":"x"}`)}}}}}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if none, err := unit.Commit(ctx, w, 2, unit.Work{CommitID: "plain", Parts: []unit.Part{boom}}); err != nil || none.Outcome != writer.CommitAlreadyApplied {
		t.Fatalf("replay of a plain commit = %+v %v, want already applied", none, err)
	}
}
