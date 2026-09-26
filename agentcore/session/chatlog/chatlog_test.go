package chatlog

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/attempt"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
)

func registry(t *testing.T) *extension.Registry {
	t.Helper()
	r, err := extension.BuildRegistry(runmod.Module, attempt.Module, Module)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

type step struct {
	typ   session.EventType
	value any
}

func runStep(runID run.RunID, f run.Fact) step {
	return step{runmod.EventType(f), runmod.Event{RunID: runID, Fact: f}}
}

func completed(runID run.RunID, stepID run.StepID, digest es.Digest) step {
	return runStep(runID, run.ModelStepCompleted{StepID: stepID, FinishReason: model.FinishReasonStop, ResultDigest: digest})
}

func opened(runID run.RunID, source run.StepID, calls ...run.CallID) step {
	bindings := make([]run.ToolCallBinding, len(calls))
	for i, c := range calls {
		bindings[i] = run.ToolCallBinding{CallID: c, ToolRef: "echo", DefinitionDigest: "sha256:def", Arguments: jsonstable.MustParse(`{}`)}
	}
	return runStep(runID, run.ToolStepOpened{StepID: run.StepID(string(source) + "/tools"), Source: source, Calls: bindings})
}

// foldSteps encodes, decodes and folds steps through both projections,
// returning the states and the first fold error. Each step is one commit, so
// step i lands at ledger Position{Commit: i} and a compaction names entries
// by the step that produced them.
func foldSteps(t *testing.T, steps []step) (Context, Surface, error) {
	t.Helper()
	r := registry(t)
	surfaceState, _ := SurfaceProjection.Initial()
	contextState, _ := ContextProjection.Initial()
	for i, st := range steps {
		wire, err := r.Encode(st.typ, st.value)
		if err != nil {
			t.Fatalf("step %d encode: %v", i, err)
		}
		d, err := r.Decode(session.Event{Type: st.typ, Payload: wire})
		if err != nil {
			t.Fatal(err)
		}
		d.Position = session.Position{Commit: session.CommitSeq(i)}
		nextSurface, err := SurfaceProjection.Apply(surfaceState, d)
		if err != nil {
			return contextState.(Context), surfaceState.(Surface), err
		}
		nextContext, err := ContextProjection.Apply(contextState, d)
		if err != nil {
			return contextState.(Context), nextSurface.(Surface), err
		}
		surfaceState, contextState = nextSurface, nextContext
	}
	return contextState.(Context), surfaceState.(Surface), nil
}

// Summary parts round-trip through the discriminated wire; foreign fields
// and unknown kinds are rejected (CHT-COD-1).
func TestPartsCodecRoundTripAndRejects(t *testing.T) {
	parts := Parts{TextPart{Text: "hello"}, ReferencePart{BindingID: "b1", Name: "file.txt"}}
	raw, err := parts.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var back Parts
	if err := back.UnmarshalJSON(raw); err != nil {
		t.Fatalf("%v\n%s", err, raw)
	}
	again, _ := back.MarshalJSON()
	if string(again) != string(raw) {
		t.Fatalf("round trip differs:\n%s\n%s", raw, again)
	}
	for name, wire := range map[string]string{
		"unknown kind":         `[{"kind":"twilight/chatlog/video"}]`,
		"retired tool call":    `[{"kind":"twilight/chatlog/tool_call","callId":"c","name":"n"}]`,
		"text with binding":    `[{"kind":"twilight/chatlog/text","text":"x","bindingId":"b"}]`,
		"reference without id": `[{"kind":"twilight/chatlog/reference","name":"f"}]`,
		"unknown field":        `[{"kind":"twilight/chatlog/text","text":"x","extra":1}]`,
	} {
		var p Parts
		if err := p.UnmarshalJSON([]byte(wire)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if ids, _ := PartsExtractor.BindingIDs(SummaryPayload{Summary: Summary{Parts: Parts{ReferencePart{BindingID: "b2"}, TextPart{}, ReferencePart{BindingID: "b1"}}}}); len(ids) != 2 || ids[0] != "b2" {
		t.Fatalf("extractor = %v", ids)
	}
}

// CHT-ENT-1/2: Run facts project into structural entries. The assistant
// names the frozen result and the calls its ToolStepOpened assigned; each
// call's terminal fact projects one tool_result; a verified supersession
// replaces the unknown result in place.
func TestRunFactsProjectEntries(t *testing.T) {
	ctxState, surf, err := foldSteps(t, []step{
		{TypeInputSubmitted, InputSubmittedPayload{InputID: "in-1", Content: jsonstable.MustParse(`{"text":"hi"}`), SubmittedAtUnixMilli: 1}},
		{TypeInputDelivered, InputDeliveredPayload{InputID: "in-1", TurnID: "t1"}},
		step{attempt.TypeStarted, attempt.StartedPayload{TurnID: "t1", RunID: "r1", Attempt: 1}}, runStep("r1", run.RunCreated{RunID: "r1"}),
		completed("r1", "s1", "sha256:res"),
		opened("r1", "s1", "c1", "c2"),
		runStep("r1", run.ToolCallCompleted{StepID: "s1/tools", CallID: "c1", OutputDigest: "sha256:out"}),
		runStep("r1", run.ToolCallFailed{StepID: "s1/tools", CallID: "c2", Failure: run.ToolFailure{Class: run.FailureEffectUnknown, Message: "lost"}, Outcome: run.ToolOutcomeUnknown}),
		{TypeToolResultSuperseded, ToolResultSupersededPayload{ToolResultID: "c2", Status: ToolSuccess, OutputDigest: "sha256:verified"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	entries := ctxState.Entries
	if len(entries) != 4 {
		t.Fatalf("entries = %+v", entries)
	}
	a := entries[1].Assistant
	if entries[1].Kind != EntryAssistant || a.ID != "s1" || a.TurnID != "t1" || a.RunID != "r1" || a.ResultDigest != "sha256:res" ||
		len(a.CallIDs) != 2 || a.CallIDs[0] != "c1" || a.CallIDs[1] != "c2" || entries[1].Digest != a.Digest {
		t.Fatalf("assistant = %+v", a)
	}
	if want, _ := DigestAssistant(a); want != a.Digest {
		t.Fatalf("assistant digest %s, want %s", a.Digest, want)
	}
	r1 := entries[2].ToolResult
	if r1.ID != "c1" || r1.TurnID != "t1" || r1.Status != ToolSuccess || r1.Source != SourceToolOutput || r1.OutputDigest != "sha256:out" || r1.Failure != nil {
		t.Fatalf("tool result 1 = %+v", r1)
	}
	r2 := entries[3].ToolResult
	if r2.ID != SupersedingToolResultID("c2") || r2.CallID != "c2" || r2.Status != ToolSuccess || r2.OutputDigest != "sha256:verified" {
		t.Fatalf("superseded result = %+v", r2)
	}
	if ctxState.Superseded["c2"] != r2.ID {
		t.Fatalf("superseded map = %+v", ctxState.Superseded)
	}
	// The Surface keeps the full history: the unknown result stays visible
	// and the replacement is appended.
	if surf.EntryOrder[len(surf.EntryOrder)-1].ID != string(r2.ID) || surf.ToolResults.Len() != 3 {
		t.Fatalf("surface = %+v", surf.EntryOrder)
	}
	if unknown, _ := surf.ToolResults.Get("c2"); unknown.Status != ToolUnknown || unknown.FailureText() != "effect_unknown: lost" {
		t.Fatalf("unknown result = %+v", unknown)
	}
	if owner, _ := surf.Runs.Get("r1"); owner.TurnID != "t1" {
		t.Fatalf("run owner = %+v", owner)
	}
	// Superseding twice, or a result outside the context, is a fold error.
	if _, _, err := foldSteps(t, []step{step{attempt.TypeStarted, attempt.StartedPayload{TurnID: "t1", RunID: "r1", Attempt: 1}}, runStep("r1", run.RunCreated{RunID: "r1"}), completed("r1", "s1", "sha256:res"), opened("r1", "s1", "c1"),
		runStep("r1", run.ToolCallFailed{StepID: "s1/tools", CallID: "c1", Failure: run.ToolFailure{Class: "x"}}),
		{TypeToolResultSuperseded, ToolResultSupersededPayload{ToolResultID: "c1", Status: ToolError}},
		{TypeToolResultSuperseded, ToolResultSupersededPayload{ToolResultID: "c1", Status: ToolError}}}); err == nil {
		t.Fatal("second supersession accepted")
	}
	if _, _, err := foldSteps(t, []step{{TypeToolResultSuperseded, ToolResultSupersededPayload{ToolResultID: "ghost", Status: ToolError}}}); err == nil {
		t.Fatal("supersession of an unknown result accepted")
	}
	// A tool step for an unknown assistant, and a result folded twice, are
	// fold errors.
	if _, _, err := foldSteps(t, []step{opened("r1", "nope", "c1")}); err == nil {
		t.Fatal("tool step for an unknown assistant accepted")
	}
	if _, _, err := foldSteps(t, []step{completed("r1", "s1", "sha256:a"), completed("r1", "s1", "sha256:a")}); err == nil {
		t.Fatal("assistant folded twice")
	}
}

// Surface and Context agree on the input lifecycle: only delivered inputs
// enter the context, in fold order; withdrawn and rejected inputs leave the
// queue; a second delivery is a reducer error (CHT-EVT-2).
func TestSurfaceAndContextFold(t *testing.T) {
	content := jsonstable.MustParse(`{"text":"hi"}`)
	steps := []step{
		{TypeInputSubmitted, InputSubmittedPayload{InputID: "in-1", Content: content, SubmittedAtUnixMilli: 1}},
		{TypeInputSubmitted, InputSubmittedPayload{InputID: "in-2", Content: content, SubmittedAtUnixMilli: 2}},
		{TypeInputSubmitted, InputSubmittedPayload{InputID: "in-3", Content: content, SubmittedAtUnixMilli: 3}},
		{TypeInputDelivered, InputDeliveredPayload{InputID: "in-1", TurnID: "t1"}},
		{TypeInputWithdrawn, InputWithdrawnPayload{InputID: "in-3", Reason: "user"}},
		step{attempt.TypeStarted, attempt.StartedPayload{TurnID: "t1", RunID: "r1", Attempt: 1}}, runStep("r1", run.RunCreated{RunID: "r1"}),
		completed("r1", "s1", "sha256:res"),
	}
	ctxState, surface, err := foldSteps(t, steps)
	if err != nil {
		t.Fatal(err)
	}
	in1, _ := surface.Inputs.Get("in-1")
	in2, _ := surface.Inputs.Get("in-2")
	in3, _ := surface.Inputs.Get("in-3")
	if in1.Status != InputDelivered || in2.Status != InputSubmitted || in3.Status != InputWithdrawn {
		t.Fatalf("inputs = %+v", surface.Inputs.Map())
	}
	if pending := surface.SubmittedInputs(); len(pending) != 1 || pending[0].ID != "in-2" {
		t.Fatalf("submitted = %+v", pending)
	}
	entries := ctxState.Entries
	if len(entries) != 2 || entries[0].Kind != EntryInput || entries[0].Input.TurnID != "t1" || entries[1].Kind != EntryAssistant {
		t.Fatalf("context = %+v", entries)
	}
	if _, pending := ctxState.Pending["in-2"]; !pending || len(ctxState.Pending) != 1 {
		t.Fatalf("pending = %+v", ctxState.Pending)
	}
	if _, _, err := foldSteps(t, append(steps, step{TypeInputDelivered, InputDeliveredPayload{InputID: "in-1", TurnID: "t1"}})); err == nil {
		t.Fatal("second delivery accepted")
	}
}

// EXT-COD-1: every registered event type's current codec is canonical
// round-trip stable — Encode, Decode, Encode reproduces the bytes.
func TestEventCodecCanonicalRoundTrip(t *testing.T) {
	summary := Summary{ID: "sum1", Parts: Parts{TextPart{Text: "so far"}, ReferencePart{BindingID: "b1", Name: "f"}}}
	var err error
	if summary.Digest, err = DigestSummary(&summary); err != nil {
		t.Fatal(err)
	}
	compaction := CompactionCreatedPayload{CompactionID: "ck1", CoveredThrough: session.Position{Commit: 3}, BaseContextDigest: "sha256:base",
		SummaryID: summary.ID, SummaryDigest: summary.Digest,
		Retained: []EntryDigestPair{{Kind: EntryAssistant, ID: "a1", Digest: "sha256:a1"}}}
	if compaction.Digest, err = DigestCompaction(&compaction); err != nil {
		t.Fatal(err)
	}
	samples := map[session.EventType]any{
		TypeInputSubmitted:        InputSubmittedPayload{InputID: "in-1", Content: jsonstable.MustParse(`{"text":"hi"}`), SubmittedAtUnixMilli: 1},
		TypeInputDelivered:        InputDeliveredPayload{InputID: "in-1", TurnID: "t1"},
		TypeInputWithdrawn:        InputWithdrawnPayload{InputID: "in-1", Reason: "user"},
		TypeInputRejected:         InputRejectedPayload{InputID: "in-1"},
		TypeToolResultSuperseded:  ToolResultSupersededPayload{ToolResultID: "tr1", Status: ToolSuccess, OutputDigest: "sha256:out"},
		TypeSummary:               SummaryPayload{Summary: summary},
		TypeCompactionCreated:     compaction,
		TypeCompactionInvalidated: CompactionInvalidatedPayload{CompactionID: "ck1", Reason: "host"},
	}
	for _, def := range Module.Events {
		val, ok := samples[def.Type]
		if !ok {
			t.Fatalf("no sample for %s", def.Type)
		}
		if len(def.Codecs) != 1 {
			t.Fatalf("%s: %d codecs, want one per schema this module writes", def.Type, len(def.Codecs))
		}
		codec := def.Codecs[Version]
		first, err := codec.Encode(val)
		if err != nil {
			t.Fatalf("%s: encode: %v", def.Type, err)
		}
		back, err := codec.Decode(first)
		if err != nil {
			t.Fatalf("%s: decode: %v", def.Type, err)
		}
		again, err := codec.Encode(back)
		if err != nil || !again.Equal(first) {
			t.Fatalf("%s: round trip changed bytes: %s vs %s (%v)", def.Type, first, again, err)
		}
	}
	// A success supersession names its body; an error one does not.
	r := registry(t)
	for name, p := range map[string]ToolResultSupersededPayload{
		"success without digest": {ToolResultID: "x", Status: ToolSuccess},
		"error with digest":      {ToolResultID: "x", Status: ToolError, OutputDigest: "sha256:o"},
		"unknown status":         {ToolResultID: "x", Status: ToolUnknown},
	} {
		if _, err := r.Encode(TypeToolResultSuperseded, p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if ids, _ := supersededExtractor.BindingIDs(ToolResultSupersededPayload{ToolResultID: "x", Status: ToolSuccess, OutputDigest: "sha256:o"}); len(ids) != 1 || ids[0] != runmod.FrozenBindingID("sha256:o") {
		t.Fatalf("superseded extractor = %v", ids)
	}
}

// --- materialization (CHT-MAT-1) ---------------------------------------------

type fakeContent struct {
	results map[es.Digest]model.ModelResult
	outputs map[es.Digest]run.CanonicalJSON
	reads   int
}

func (c *fakeContent) ModelResult(_ context.Context, d es.Digest) (model.ModelResult, error) {
	c.reads++
	r, ok := c.results[d]
	if !ok {
		return model.ModelResult{}, frozen.ErrMissing
	}
	return r, nil
}

func (c *fakeContent) ToolOutput(_ context.Context, d es.Digest) (run.CanonicalJSON, error) {
	c.reads++
	o, ok := c.outputs[d]
	if !ok {
		return run.CanonicalJSON{}, frozen.ErrMissing
	}
	return o, nil
}

func (c *fakeContent) ToolResponse(ctx context.Context, d es.Digest) (run.CanonicalJSON, error) {
	return c.ToolOutput(ctx, d)
}

// The materializer pairs the Run's CallIDs with the frozen result's tool
// calls, resolves successful outputs, renders failures from the entry, reads
// each digest once, and reports a lost body without touching the projection.
func TestMaterialize(t *testing.T) {
	ctxState, _, err := foldSteps(t, []step{
		step{attempt.TypeStarted, attempt.StartedPayload{TurnID: "t1", RunID: "r1", Attempt: 1}}, runStep("r1", run.RunCreated{RunID: "r1"}),
		completed("r1", "s1", "sha256:res"),
		opened("r1", "s1", "c1", "c2"),
		runStep("r1", run.ToolCallCompleted{StepID: "s1/tools", CallID: "c1", OutputDigest: "sha256:out"}),
		runStep("r1", run.ToolCallAnswered{StepID: "s1/tools", CallID: "c2", ResponseID: "resp", ResponseDigest: "sha256:ans"}),
		completed("r1", "s2", "sha256:res"),
	})
	if err != nil {
		t.Fatal(err)
	}
	content := &fakeContent{
		results: map[es.Digest]model.ModelResult{"sha256:res": {Text: "calling", ToolCalls: []model.ModelToolCall{
			{ToolCallID: "p1", ToolName: "echo", Input: model.ToolArguments{JSON: jsonstable.MustParse(`{"a":1}`)}},
			{ToolCallID: "p2", ToolName: "ask", Input: model.ToolArguments{JSON: jsonstable.MustParse(`{}`)}},
		}}},
		outputs: map[es.Digest]run.CanonicalJSON{"sha256:out": jsonstable.MustParse(`{"ok":true}`), "sha256:ans": jsonstable.MustParse(`"yes"`)},
	}
	entries, err := NewMaterializer(content).Entries(context.Background(), ctxState.Entries)
	if err != nil {
		t.Fatal(err)
	}
	first := entries[0].Calls
	if len(entries) != 4 || entries[0].Text() != "calling" || len(first) != 2 ||
		first[0].CallID != "c1" || first[0].ProviderCallID != "p1" || first[0].Name != "echo" || first[0].Input.String() != `{"a":1}` ||
		first[1].CallID != "c2" {
		t.Fatalf("assistant = %+v", entries[0])
	}
	if entries[1].Text() != `{"ok":true}` || entries[2].Text() != `"yes"` {
		t.Fatalf("outputs = %q %q", entries[1].Text(), entries[2].Text())
	}
	// The second assistant names the same result: one read serves both, and
	// its CallIDs are derived when no ToolStepOpened followed it.
	if content.reads != 3 || len(entries[3].Calls) != 2 || entries[3].Calls[0].CallID != CallID(schema.Identity().DeriveCallID("s2", 0)) {
		t.Fatalf("reads = %d, second assistant = %+v", content.reads, entries[3])
	}
	failed := Entry{Kind: EntryToolResult, ToolResult: &ToolResult{ID: "c9", CallID: "c9", Status: ToolError, Failure: &run.ToolFailure{Class: "boom", Message: "x"}}}
	if m, err := NewMaterializer(content).Entry(context.Background(), &failed); err != nil || m.Output != nil || m.Text() != "boom: x" {
		t.Fatalf("failed result = %+v %v", m, err)
	}
	lost := Entry{Kind: EntryAssistant, ID: "s9", Assistant: &Assistant{ID: "s9", StepID: "s9", ResultDigest: "sha256:gone"}}
	if _, err := NewMaterializer(content).Entry(context.Background(), &lost); !errors.Is(err, frozen.ErrMissing) {
		t.Fatalf("lost body: err = %v", err)
	}
}

// --- compaction fold (CHT-EVT-3, CHT-CTX-2, CHT-SUR-1) --------------------------

func mustSummary(t *testing.T, id SummaryID, text string) Summary {
	t.Helper()
	s := Summary{ID: id, Parts: Parts{TextPart{text}}}
	var err error
	if s.Digest, err = DigestSummary(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

// entryDigest is the digest the fold assigns to the assistant of one
// model_step_completed under Turn t1 in Run r1, with no tool step.
func entryDigest(t *testing.T, stepID run.StepID, result es.Digest) es.Digest {
	t.Helper()
	a, err := assistantOf(RunOwner{TurnID: "t1"}, "r1", &run.ModelStepCompleted{StepID: stepID, FinishReason: model.FinishReasonStop, ResultDigest: result})
	if err != nil {
		t.Fatal(err)
	}
	return a.Digest
}

// at is the ledger Position of foldSteps' step i.
func at(i int) session.Position { return session.Position{Commit: session.CommitSeq(i)} }

func mustCompaction(t *testing.T, id CompactionID, covered session.Position, base []EntryDigestPair, sum Summary, retained []EntryDigestPair) CompactionCreatedPayload {
	t.Helper()
	baseDigest, err := DigestBaseContext(base)
	if err != nil {
		t.Fatal(err)
	}
	p := CompactionCreatedPayload{CompactionID: id, CoveredThrough: covered, BaseContextDigest: baseDigest,
		SummaryID: sum.ID, SummaryDigest: sum.Digest, Retained: retained}
	if p.Digest, err = DigestCompaction(&p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCompactionFold(t *testing.T) {
	content := jsonstable.MustParse(`{"text":"hi"}`)
	inDigest, err := DigestInput("in-1", content)
	if err != nil {
		t.Fatal(err)
	}
	sum := mustSummary(t, "sum1", "so far")
	base := []EntryDigestPair{{Kind: EntryInput, ID: "in-1", Digest: inDigest}, {Kind: EntryAssistant, ID: "s1", Digest: entryDigest(t, "s1", "sha256:one")}}
	// The prefix folds to entries at steps 1 (delivered input), 3 (assistant),
	// 5 (summary), with a queued input that must survive compaction; the
	// compaction itself lands at step 7.
	prefix := []step{
		{TypeInputSubmitted, InputSubmittedPayload{InputID: "in-1", Content: content, SubmittedAtUnixMilli: 1}},
		{TypeInputDelivered, InputDeliveredPayload{InputID: "in-1", TurnID: "t1"}},
		step{attempt.TypeStarted, attempt.StartedPayload{TurnID: "t1", RunID: "r1", Attempt: 1}}, runStep("r1", run.RunCreated{RunID: "r1"}),
		completed("r1", "s1", "sha256:one"),
		{TypeInputSubmitted, InputSubmittedPayload{InputID: "in-q", Content: content, SubmittedAtUnixMilli: 2}},
		{TypeSummary, SummaryPayload{Summary: sum}},
	}
	valid := mustCompaction(t, "ck1", at(4), base, sum, base[1:])

	t.Run("valid compaction replaces the base and keeps the queue", func(t *testing.T) {
		ctxState, surf, err := foldSteps(t, append(prefix, step{TypeCompactionCreated, valid}, completed("r1", "s2", "sha256:after")))
		if err != nil {
			t.Fatal(err)
		}
		entries := ctxState.Entries
		if len(entries) != 3 || entries[0].Kind != EntrySummary || entries[1].ID != "s1" || entries[2].ID != "s2" {
			t.Fatalf("entries = %+v", entries)
		}
		if _, pending := ctxState.Pending["in-q"]; !pending {
			t.Fatal("queued input compacted away")
		}
		if got := surf.SubmittedInputs(); len(got) != 1 || got[0].ID != "in-q" {
			t.Fatalf("surface queue = %+v", got)
		}
		if v, _ := surf.Compactions.Get("ck1"); v.Status != CompactionActive {
			t.Fatalf("surface compaction = %+v", v)
		}
		if len(surf.EntryOrder) != 4 { // full history stays visible
			t.Fatalf("entry order = %+v", surf.EntryOrder)
		}
	})

	t.Run("invalidating the latest compaction restores base plus tail", func(t *testing.T) {
		ctxState, surf, err := foldSteps(t, append(prefix,
			step{TypeCompactionCreated, valid},
			completed("r1", "s2", "sha256:after"),
			step{TypeCompactionInvalidated, CompactionInvalidatedPayload{CompactionID: "ck1", Reason: "host"}}))
		if err != nil {
			t.Fatal(err)
		}
		entries := ctxState.Entries
		if len(entries) != 3 || entries[0].ID != "in-1" || entries[1].ID != "s1" || entries[2].ID != "s2" {
			t.Fatalf("restored entries = %+v", entries)
		}
		if len(ctxState.Compactions) != 0 {
			t.Fatalf("compaction stack = %+v", ctxState.Compactions)
		}
		if v, _ := surf.Compactions.Get("ck1"); v.Status != CompactionInvalidated || v.Reason != "host" {
			t.Fatalf("surface compaction = %+v", v)
		}
	})

	rejects := []struct {
		name  string
		steps []step
	}{
		{"covered through at or past the compaction position",
			append(prefix, step{TypeCompactionCreated, mustCompaction(t, "ck2", at(6), base, sum, nil)})},
		{"base context digest mismatch",
			append(prefix, step{TypeCompactionCreated, mustCompaction(t, "ck3", at(3), base[:1], sum, nil)})},
		{"retained outside the base",
			append(prefix, step{TypeCompactionCreated, mustCompaction(t, "ck4", at(3), base,
				sum, []EntryDigestPair{{Kind: EntryAssistant, ID: "s1", Digest: "sha256:wrong"}})})},
		{"gap holds more than the summary",
			append(append([]step{}, prefix...), completed("r1", "s9", "sha256:x"),
				step{TypeCompactionCreated, mustCompaction(t, "ck5", at(5),
					append(base, EntryDigestPair{Kind: EntryAssistant, ID: "s9"}), sum, nil)})},
		{"invalidating an unknown compaction",
			append(prefix, step{TypeCompactionInvalidated, CompactionInvalidatedPayload{CompactionID: "nope"}})},
	}
	for _, tc := range rejects {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := foldSteps(t, tc.steps); err == nil {
				t.Fatal("fold accepted")
			}
		})
	}

	t.Run("superseding a compacted result is rejected", func(t *testing.T) {
		// The assistant's digest changes when its tool step opens, so the base
		// is read back from a fold of the same prefix.
		steps := []step{step{attempt.TypeStarted, attempt.StartedPayload{TurnID: "t1", RunID: "r1", Attempt: 1}}, runStep("r1", run.RunCreated{RunID: "r1"}), completed("r1", "ac", "sha256:call"), opened("r1", "ac", "c1"),
			runStep("r1", run.ToolCallCompleted{StepID: "ac/tools", CallID: "c1", OutputDigest: "sha256:out"})}
		folded, _, err := foldSteps(t, steps)
		if err != nil {
			t.Fatal(err)
		}
		toolBase := []EntryDigestPair{folded.Entries[0].Pair(), folded.Entries[1].Pair()}
		sum2 := mustSummary(t, "sum2", "tools done")
		steps = append(steps,
			step{TypeSummary, SummaryPayload{Summary: sum2}},
			step{TypeCompactionCreated, mustCompaction(t, "ck6", at(4), toolBase, sum2, nil)},
			step{TypeToolResultSuperseded, ToolResultSupersededPayload{ToolResultID: "c1", Status: ToolError}})
		if _, _, err := foldSteps(t, steps); err == nil {
			t.Fatal("supersede of a compacted result accepted")
		}
	})
}
