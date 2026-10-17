package chatlog_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset/ref"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/session/writer/writertest"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
)

func TestPlanCompaction(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries []chatlog.Entry
		retain  int
		start   int
		can     bool
	}{
		{"empty", nil, 0, 0, false},
		{"zero settled", pairEntries(), 0, 4, true},
		{"plain suffix", pairEntries(), 1, 3, true},
		{"result pulls assistant", pairEntries(), 2, 1, true},
		{"whole context", pairEntries(), 4, 0, false},
		{"oversized window", pairEntries(), 100, 0, false},
		{"zero preserves open call", openEntries(), 0, 3, true},
		{"open call prevents shrinking", openEntries()[3:], 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := append([]chatlog.Entry(nil), tc.entries...)
			plan, can, err := chatlog.PlanCompaction(tc.entries, tc.retain)
			if err != nil || can != tc.can || len(plan.Retained) != len(tc.entries)-tc.start {
				t.Fatalf("plan=%+v can=%v err=%v", plan, can, err)
			}
			for i, pair := range plan.Retained {
				if pair != tc.entries[tc.start+i].Pair() {
					t.Fatalf("retained[%d] = %+v", i, pair)
				}
			}
			if len(tc.entries) > 0 {
				if err := plan.Validate(tc.entries); err != nil {
					t.Fatal(err)
				}
				if plan.CoveredThrough != tc.entries[len(tc.entries)-1].Position {
					t.Fatal("wrong boundary")
				}
			}
			if !reflect.DeepEqual(before, tc.entries) {
				t.Fatal("planning mutated entries")
			}
		})
	}
	if _, _, err := chatlog.PlanCompaction(pairEntries(), -1); err == nil {
		t.Fatal("negative retain accepted")
	}
	// Pulling an open call into the suffix can expose another result whose
	// owner is earlier; closure must iterate until all such owners are kept.
	entries := pairEntries()
	open := openEntries()[3]
	entries = append(entries[:2], open, entries[2])
	plan, can, err := chatlog.PlanCompaction(entries, 0)
	if err != nil || !can || len(plan.Retained) != 3 || plan.Retained[0] != entries[1].Pair() {
		t.Fatalf("recursive closure: %+v, %v, %v", plan, can, err)
	}
}

func TestCompactionPlanValidate(t *testing.T) {
	entries := pairEntries()
	plan, _, err := chatlog.PlanCompaction(entries, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*chatlog.CompactionPlan)
		cas  bool
	}{
		{"base", func(p *chatlog.CompactionPlan) { p.BaseContextDigest = "sha256:other" }, true},
		{"boundary", func(p *chatlog.CompactionPlan) { p.CoveredThrough.Commit++ }, true},
		{"foreign retain", func(p *chatlog.CompactionPlan) { p.Retained = []chatlog.EntryDigestPair{{ID: "missing"}} }, false},
		{"wrong order", func(p *chatlog.CompactionPlan) {
			p.Retained = []chatlog.EntryDigestPair{entries[3].Pair(), entries[0].Pair()}
		}, false},
		{"duplicate", func(p *chatlog.CompactionPlan) {
			p.Retained = []chatlog.EntryDigestPair{entries[3].Pair(), entries[3].Pair()}
		}, false},
		{"split result", func(p *chatlog.CompactionPlan) { p.Retained = []chatlog.EntryDigestPair{entries[2].Pair()} }, false},
		{"split assistant", func(p *chatlog.CompactionPlan) { p.Retained = []chatlog.EntryDigestPair{entries[1].Pair()} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := plan
			tc.edit(&bad)
			err := bad.Validate(entries)
			if err == nil || errors.Is(err, chatlog.ErrContextChanged) != tc.cas {
				t.Fatalf("Validate = %v, want CAS=%v", err, tc.cas)
			}
		})
	}
	reordered := append([]chatlog.Entry(nil), entries...)
	reordered[0], reordered[1] = reordered[1], reordered[0]
	if err := plan.Validate(reordered); !errors.Is(err, chatlog.ErrContextChanged) {
		t.Fatalf("reordered context = %v", err)
	}
	plan, _, _ = chatlog.PlanCompaction(openEntries(), 0)
	plan.Retained = nil
	if err := plan.Validate(openEntries()); err == nil || errors.Is(err, chatlog.ErrContextChanged) {
		t.Fatalf("dropped open assistant = %v", err)
	}
}

func compactionWriter(t *testing.T) writer.Writer {
	t.Helper()
	ctx := context.Background()
	reg, err := module.BuildRegistry(chatlog.Module, sessionstore.Module)
	if err != nil {
		t.Fatal(err)
	}
	store := filestoretest.Store(t)
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	bindings, artifacts := artifacttest.Stores(t)
	w, err := writertest.Open(t, ctx, &writer.Factory{Store: store, Registry: reg, Admission: writer.Admission{Bindings: bindings, Ledger: artifacts}}, "s", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w
}

func compactionContext(t *testing.T, w writer.Writer) chatlog.Context {
	t.Helper()
	var current chatlog.Context
	if _, err := w.Commit(context.Background(), func(v writer.View) (*writer.SemanticGroup, error) {
		state, err := v.Projection(chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
		if err != nil {
			return nil, err
		}
		current = state.(chatlog.Context)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	return current
}

func appendCompactionInput(t *testing.T, w writer.Writer, id turn.InputID) {
	t.Helper()
	commands := chatlog.Commands{Now: time.Now}
	in, err := commands.Submit(context.Background(), w, id, turn.MustParseCanonicalJSON(`{"text":"input"}`))
	if err != nil {
		t.Fatal(err)
	}
	creation, err := turn.BuildNewTurn(turn.TurnID("turn-"+id), "", ref.Ref{ID: "test", Digest: "sha256:test"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := unit.Commit(context.Background(), w, 1, unit.Work{
		CommitID: ledger.CommitID("accept/" + id),
		Parts:    []unit.Part{chatlog.AdmitInputs([]turn.AgentInput{in}), sessionstore.CreateTurn(creation, []turn.AgentInput{in})},
	})
	if err != nil || res.Outcome != writer.CommitApplied {
		t.Fatalf("deliver = %+v, %v", res, err)
	}
}

func TestCompactRejectsChangedContextWithoutWriting(t *testing.T) {
	w := compactionWriter(t)
	appendCompactionInput(t, w, "first")
	plan, _, err := chatlog.PlanCompaction(compactionContext(t, w).Entries, 0)
	if err != nil {
		t.Fatal(err)
	}
	// This input arrives while the summary is being generated.
	appendCompactionInput(t, w, "during-summary")
	before := compactionContext(t, w)
	commands := chatlog.Commands{Now: time.Now}
	guardCalls := 0
	if _, err := commands.Compact(context.Background(), w, "stale summary", plan, func(writer.View) error {
		guardCalls++
		return nil
	}); !errors.Is(err, chatlog.ErrContextChanged) {
		t.Fatalf("Compact = %v", err)
	}
	if guardCalls != 1 || !reflect.DeepEqual(before, compactionContext(t, w)) {
		t.Fatal("refused compaction changed context or skipped guard")
	}
	_, err = w.Commit(context.Background(), func(v writer.View) (*writer.SemanticGroup, error) {
		state, err := v.Projection(chatlog.SurfaceProjectionID, chatlog.SurfaceProjection.Version)
		if err == nil && state.(chatlog.Surface).Summaries.Len() != 0 {
			t.Fatal("refused compaction wrote its summary")
		}
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCompactRetryAndRetentionIdentity(t *testing.T) {
	w := compactionWriter(t)
	appendCompactionInput(t, w, "first")
	appendCompactionInput(t, w, "second")
	original := compactionContext(t, w)
	plan, _, err := chatlog.PlanCompaction(original.Entries, 0)
	if err != nil {
		t.Fatal(err)
	}
	other, _, err := chatlog.PlanCompaction(original.Entries, 1)
	if err != nil {
		t.Fatal(err)
	}
	commands := chatlog.Commands{Now: time.Now}
	guardCalls := 0
	guard := func(writer.View) error { guardCalls++; return nil }
	id, err := commands.Compact(context.Background(), w, "summary", plan, guard)
	if err != nil || id == "" {
		t.Fatalf("Compact = %s, %v", id, err)
	}
	before := compactionContext(t, w)
	if len(before.Entries) != 1 || before.Entries[0].Kind != chatlog.EntrySummary {
		t.Fatalf("context = %+v", before)
	}
	// Retrying after unrelated context growth must still find the original
	// commit without preparing either the guard or the compaction Part.
	appendCompactionInput(t, w, "later")
	before = compactionContext(t, w)
	if again, err := commands.Compact(context.Background(), w, "summary", plan, guard); err != nil || again != id {
		t.Fatalf("retry = %s, %v", again, err)
	}
	if guardCalls != 1 || !reflect.DeepEqual(before, compactionContext(t, w)) {
		t.Fatal("retry was not idempotent")
	}
	if _, err := commands.Compact(context.Background(), w, "summary", other, guard); !errors.Is(err, chatlog.ErrContextChanged) {
		t.Fatalf("different retained policy falsely replayed: %v", err)
	}
	if guardCalls != 2 || !reflect.DeepEqual(before, compactionContext(t, w)) {
		t.Fatal("different retention bypassed validation or changed context")
	}
}

func TestCompactContextAtomicPartAndValidation(t *testing.T) {
	w := compactionWriter(t)
	appendCompactionInput(t, w, "first")
	plan, _, _ := chatlog.PlanCompaction(compactionContext(t, w).Entries, 0)
	summary := chatlog.Summary{ID: "sum", Parts: chatlog.Parts{chatlog.TextPart{Text: "summary"}}}
	summary.Digest, _ = chatlog.DigestSummary(&summary)
	emptySummary := chatlog.Summary{ID: "empty"}
	emptySummary.Digest, _ = chatlog.DigestSummary(&emptySummary)
	missingID := chatlog.Summary{Parts: summary.Parts}
	missingID.Digest, _ = chatlog.DigestSummary(&missingID)
	for _, tc := range []struct {
		name    string
		id      chatlog.CompactionID
		summary chatlog.Summary
	}{
		{"empty id", "", summary},
		{"empty summary", "c", emptySummary},
		{"missing summary id", "c", missingID},
		{"bad digest", "c", chatlog.Summary{ID: summary.ID, Parts: summary.Parts, Digest: "sha256:bad"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := compactionContext(t, w)
			if _, err := unit.Commit(context.Background(), w, 9, unit.Work{CommitID: ledger.CommitID(tc.name), Parts: []unit.Part{chatlog.CompactContext(tc.id, tc.summary, plan)}}); err == nil {
				t.Fatal("invalid compaction accepted")
			}
			if !reflect.DeepEqual(before, compactionContext(t, w)) {
				t.Fatal("invalid Part changed context")
			}
		})
	}
	boom := errors.New("other module refuses")
	if _, err := unit.Commit(context.Background(), w, 9, unit.Work{CommitID: "refused", Parts: []unit.Part{
		chatlog.CompactContext("c", summary, plan),
		unit.PartFunc(func(context.Context, writer.View, int64) ([]writer.TypedBatch, error) { return nil, boom }),
	}}); !errors.Is(err, boom) {
		t.Fatalf("refused unit = %v", err)
	}
	if len(compactionContext(t, w).Entries) != 1 || compactionContext(t, w).Entries[0].Kind != chatlog.EntryInput {
		t.Fatal("refused unit wrote a summary")
	}
	res, err := unit.Commit(context.Background(), w, 10, unit.Work{CommitID: "atomic", Parts: []unit.Part{chatlog.CompactContext("c", summary, plan)}})
	if err != nil || res.Outcome != writer.CommitApplied || len(unit.Events(res.Commit)) != 2 {
		t.Fatalf("CompactContext = %+v, %v", res, err)
	}
	for _, event := range unit.Events(res.Commit) {
		if event.RecordedAtUnixMilli != 10 {
			t.Fatal("Part did not use unit timestamp")
		}
	}
}
