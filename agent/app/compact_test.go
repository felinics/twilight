package app_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/artifact"
	"sync"
	"testing"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/context/compaction"
	executorlocal "github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agent/store/sqlite/sqlitetest"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
)

// compactAwareModel answers turns with numbered replies and compactor
// requests (compaction.CompactorSystemPrompt) with a fixed summary, recording every
// request. The compactor request reaches it through the Executor like any
// other model effect.
type compactAwareModel struct {
	mu      sync.Mutex
	seen    []sdk.Request
	replies int
}

func (m *compactAwareModel) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen = append(m.seen, req)
	if len(req.Messages) > 0 && req.Messages[0].Role == sdk.MessageRoleSystem && messageText(req.Messages[0]) == compaction.CompactorSystemPrompt {
		return sdk.ModelResult{Text: "summary-of-the-past", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}
	m.replies++
	return sdk.ModelResult{Text: fmt.Sprintf("reply-%d", m.replies), FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
}

func (m *compactAwareModel) requests() []sdk.Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]sdk.Request(nil), m.seen...)
}

func messageTexts(req sdk.Request) []string {
	out := make([]string, 0, len(req.Messages))
	for _, msg := range req.Messages {
		out = append(out, string(msg.Role)+": "+messageText(msg))
	}
	return out
}

// openCompactSession opens one process over the store and the content store:
// a restart shares both, since the ledger names the frozen bodies by digest.
func openCompactSession(t *testing.T, store session.Store, content artifact.ContentStore, model *compactAwareModel, opts app.SessionOptions) (*app.Application, *app.Session) {
	t.Helper()
	h := newHost(t, app.Config{Store: store, Content: content, Ownership: session.OpenOptions{Takeover: true}}, map[run.ModelRef]loop.ModelInvoker{"m-1": model})
	preset, err := h.RegisterPreset("b1", mustPreset("m-1", nil))
	if err != nil {
		t.Fatal(err)
	}
	opts.Preset = preset
	s, err := h.OpenSession(context.Background(), "s-ckpt", opts)
	if err != nil {
		t.Fatal(err)
	}
	return h, s
}

// An explicit Compact shrinks the next model request to the summary plus the
// retained suffix, and a restarted process assembles exactly the same context
// from the compacted log (CHT-EVT-3, APP-CKP-1).
func TestCompactShrinksContextAndReplaysAcrossRestart(t *testing.T) {
	ctx := context.Background()
	store, content := filestoretest.Store(t), durableContent(t)
	model := &compactAwareModel{}
	h, s := openCompactSession(t, store, content, model, app.SessionOptions{CompactRetainEntries: 1})

	for _, text := range []string{"one", "two"} {
		if _, err := s.Send(ctx, text); err != nil {
			t.Fatal(err)
		}
	}
	before := model.requests()
	grown := before[len(before)-1] // user one, assistant reply-1, user two
	if len(grown.Messages) != 3 {
		t.Fatalf("pre-compact request = %v", messageTexts(grown))
	}

	id, ok, err := s.Compact(ctx)
	if err != nil || !ok {
		t.Fatalf("compact = %s %v %v", id, ok, err)
	}
	chat, err := h.ChatlogSurface(ctx, "s-ckpt")
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := chat.Compactions.Get(id); v.Status != chatlog.CompactionActive {
		t.Fatalf("compaction = %+v", v)
	}

	if _, err := s.Send(ctx, "three"); err != nil {
		t.Fatal(err)
	}
	reqs := model.requests()
	compacted := reqs[len(reqs)-1]
	want := []string{"assistant: summary-of-the-past", "assistant: reply-2", "user: three"}
	if got := messageTexts(compacted); !equalStrings(got, want) {
		t.Fatalf("post-compact request = %v, want %v", got, want)
	}

	// "Restart": a second Host over the same store must assemble the next
	// request as exactly the settled continuation of the first process's view.
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	model2 := &compactAwareModel{}
	model2.replies = 3 // keep reply numbering aligned for readability only
	_, s2 := openCompactSession(t, store, content, model2, app.SessionOptions{CompactRetainEntries: 1})
	if _, err := s2.Send(ctx, "four"); err != nil {
		t.Fatal(err)
	}
	reqs2 := model2.requests()
	next := messageTexts(reqs2[len(reqs2)-1])
	wantNext := append(messageTexts(compacted), "assistant: reply-3", "user: four")
	if !equalStrings(next, wantNext) {
		t.Fatalf("restarted request = %v, want %v", next, wantNext)
	}
}

// The automatic policy compacts after settlement once the context passes the
// threshold; failures reach CompactWarn only (APP-CKP-1).
func TestAutoCompactAfterSettlement(t *testing.T) {
	ctx := context.Background()
	var warned []error
	model := &compactAwareModel{}
	h, s := openCompactSession(t, filestoretest.Store(t), durableContent(t), model, app.SessionOptions{
		CompactAfterEntries: 3, CompactRetainEntries: 1,
		CompactWarn: func(err error) { warned = append(warned, err) },
	})
	if _, err := s.Send(ctx, "one"); err != nil { // 2 entries, below threshold
		t.Fatal(err)
	}
	if _, err := s.Send(ctx, "two"); err != nil { // 4 entries, compacts
		t.Fatal(err)
	}
	if len(warned) != 0 {
		t.Fatalf("warnings = %v", warned)
	}
	chat, err := h.ChatlogSurface(ctx, "s-ckpt")
	if err != nil {
		t.Fatal(err)
	}
	if chat.Compactions.Len() != 1 {
		t.Fatalf("compactions = %+v", chat.Compactions.Map())
	}
	state, _, err := h.Projection(ctx, "s-ckpt", chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
	if err != nil {
		t.Fatal(err)
	}
	if entries := state.(chatlog.Context).Entries; len(entries) != 2 || entries[0].Kind != chatlog.EntrySummary {
		t.Fatalf("entries = %+v", entries)
	}
}

// Compact refuses while a Turn is active: compaction is a between-turns
// policy (APP-CKP-1).
func TestCompactRefusesWhileTurnActive(t *testing.T) {
	ctx := context.Background()
	tool := &gateTool{started: make(chan struct{}, 1), release: make(chan struct{})}
	model := &scriptedRequests{answers: []sdk.ModelResult{toolCallAnswer()}}
	_, _, _, s := setup(t, model, tool, app.SessionOptions{CompactRetainEntries: 1})
	done := make(chan error, 1)
	go func() {
		_, err := s.Send(ctx, "one")
		done <- err
	}()
	<-tool.started
	if _, _, err := s.Compact(ctx); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("compact mid-turn = %v, want conflict", err)
	}
	close(tool.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// The compactor's model Assignment must carry the frozen request inline: the
// durable Worker (the remote executor shape) cannot read the Owner's
// frozen store and rejects digest-only model assignments (APP-CKP-1,
// RUN-EXE-1, RUN-EXE-3).
func TestCompactDispatchServesDurableWorker(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	content := durableContent(t)
	model := &compactAwareModel{}
	cat, err := executorlocal.NewCatalog(map[run.ModelRef]loop.ModelInvoker{"m-1": model})
	if err != nil {
		t.Fatal(err)
	}
	local, err := executorlocal.NewLocalExecutor(cat, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := executor.NewWorker(ctx, sqlitetest.Open(t).Executions(), []executor.Route{executorlocal.Route(local)})
	if err != nil {
		t.Fatal(err)
	}
	h, err := app.Build(durablePorts(t, app.Config{Store: store, Content: content, Executor: app.ExecutorConfig{Port: worker},
		Ownership: session.OpenOptions{Takeover: true}}))
	if err != nil {
		t.Fatal(err)
	}
	ref, err := h.RegisterPreset("b1", mustPreset("m-1", nil))
	if err != nil {
		t.Fatal(err)
	}
	s, err := h.OpenSession(ctx, "s-ckpt-remote", app.SessionOptions{Preset: ref, CompactRetainEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"one", "two"} {
		if _, err := s.Send(ctx, text); err != nil {
			t.Fatal(err)
		}
	}
	id, ok, err := s.Compact(ctx)
	if err != nil || !ok {
		t.Fatalf("compact through durable worker = %s %v %v", id, ok, err)
	}
	chat, err := h.ChatlogSurface(ctx, "s-ckpt-remote")
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := chat.Compactions.Get(id); v.Status != chatlog.CompactionActive {
		t.Fatalf("compaction = %+v", v)
	}
	if _, err := s.Send(ctx, "three"); err != nil {
		t.Fatal(err)
	}
	reqs := model.requests()
	if got := messageTexts(reqs[len(reqs)-1]); !equalStrings(got, []string{"assistant: summary-of-the-past", "assistant: reply-2", "user: three"}) {
		t.Fatalf("post-compact request = %v", got)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
