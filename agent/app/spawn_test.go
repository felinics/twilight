package app_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/spawn"
	"github.com/felinics/twilight/agent/store/sqlite"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/filestore"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
)

func spawnCall(args string) sdk.ModelResult {
	return sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 1},
		ToolCalls: []sdk.ToolCall{{ToolCallID: "c1", ToolName: string(spawn.DefaultTool), Input: sdk.ParseToolArguments(args)}}}
}

func text(s string) sdk.ModelResult {
	return sdk.ModelResult{Text: s, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}
}

// spawnOutput decodes the spawn tool's result from the parent's chatlog and
// returns it with the recorded ToolResult (whose CallID is the run fact's
// derived call identity, not the SDK call id the model sent).
func spawnOutput(t *testing.T, h *app.Application, sid session.SessionID) (spawn.Result, chatlog.ToolResult) {
	t.Helper()
	ctx := context.Background()
	chat, err := h.ChatlogSurface(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	for i := len(chat.EntryOrder) - 1; i >= 0; i-- {
		e := chat.EntryOrder[i]
		if e.Kind != chatlog.EntryToolResult {
			continue
		}
		tr, _ := chat.ToolResults.Get(chatlog.ToolResultID(e.ID))
		entry := chatlog.Entry{Kind: chatlog.EntryToolResult, ID: e.ID, Digest: tr.Digest, ToolResult: &tr}
		m, err := chatlog.NewMaterializer(h.Content()).Entry(ctx, &entry)
		if err != nil {
			t.Fatal(err)
		}
		if m.Output == nil {
			t.Fatalf("tool result is a failure: %s", m.Text())
		}
		var out spawn.Result
		if err := json.Unmarshal(m.Output.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out, tr
	}
	t.Fatal("no tool result in the parent's chatlog")
	return spawn.Result{}, chatlog.ToolResult{}
}

// SPN-1/2/3: a spawn call runs a child Session under the parent's
// preset, the child's reply becomes the tool result, the child's identity is
// derived from the call, its creation metadata records the provenance, and
// fork mode gives it the parent's history before the calling Turn.
func TestSpawnRunsChildSessionAndReturnsReply(t *testing.T) {
	ctx := context.Background()
	model := &scriptedRequests{answers: []sdk.ModelResult{
		text("hi there"),
		spawnCall(`{"task":"summarize the conversation","mode":"fork"}`),
		text("child reply"),
		text("parent done"),
	}}
	store, content := filestoretest.Store(t), durableContent(t)
	h := newHost(t, app.Config{Store: store, Content: content, Spawn: &spawn.Options{}}, map[run.ModelRef]loop.ModelInvoker{"m-1": model})
	preset, err := h.RegisterPreset("b1", mustPreset("m-1", []loop.ExecutableTool{spawn.Options{}.ExecutableTool()}))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	parent, err := h.OpenSession(ctx, "parent", app.SessionOptions{Preset: preset, NewTurnID: func() turn.TurnID { n++; return turn.TurnID("p" + string(rune('0'+n))) }})
	if err != nil {
		t.Fatal(err)
	}
	if results, err := parent.Send(ctx, "hello"); err != nil || results[0].Reply != "hi there" {
		t.Fatalf("first turn = %+v %v", results, err)
	}
	results, err := parent.Send(ctx, "delegate this")
	if err != nil || len(results) != 1 || results[0].Status != turn.TurnCompleted || results[0].Reply != "parent done" {
		t.Fatalf("spawning turn = %+v %v", results, err)
	}

	out, tr := spawnOutput(t, h, "parent")
	surface, _ := h.TurnSurface(ctx, "parent")
	calling := surface.Turns["p2"]
	runID := calling.Attempts[len(calling.Attempts)-1].RunID
	if want := spawn.ChildID("parent", runID, run.CallID(tr.CallID)); out.ChildSession != want || out.Status != turn.TurnCompleted || out.Reply != "child reply" {
		t.Fatalf("spawn result = %+v, want child %s completed with the child's reply", out, want)
	}
	// Provenance in the child's creation record.
	header, err := store.Header(ctx, out.ChildSession)
	if err != nil {
		t.Fatal(err)
	}
	prov, ok, err := spawn.ProvenanceFromHeader(header)
	if err != nil || !ok {
		t.Fatalf("provenance = %v %v", ok, err)
	}
	if prov.ParentSession != "parent" || prov.ParentRun != runID || prov.CallID != run.CallID(tr.CallID) || prov.Depth != 1 || prov.Arguments.Mode != spawn.Fork || prov.Arguments.Task != "summarize the conversation" {
		t.Fatalf("provenance = %+v", prov)
	}
	// Fork mode: the child inherits the parent's history before the calling
	// Turn, so its model saw the first exchange and then the task.
	parentPage, _ := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "parent"})
	childPage, _ := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: out.ChildSession})
	if header.Parent == nil || childPage.Commits[0].CommitID != parentPage.Commits[0].CommitID {
		t.Fatalf("child does not share the parent's prefix: parent=%+v", header.Parent)
	}
	seen := model.requests()
	if len(seen) != 4 {
		t.Fatalf("model saw %d requests, want 4", len(seen))
	}
	childReq := seen[2]
	if got := roles(childReq.Messages); len(got) != 3 || messageText(childReq.Messages[2]) != "summarize the conversation" {
		t.Fatalf("child request messages = %v (%q)", got, messageText(childReq.Messages[len(childReq.Messages)-1]))
	}
	if err := parent.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// gatedModel answers from a script and then blocks every further call until
// released, so a process can "crash" while a child is mid-model.
type gatedModel struct {
	mu      sync.Mutex
	answers []sdk.ModelResult
	started chan struct{}
	release chan struct{}
}

func (m *gatedModel) Generate(_ context.Context, _ sdk.Request) (sdk.ModelResult, error) {
	m.mu.Lock()
	if len(m.answers) > 0 {
		next := m.answers[0]
		m.answers = m.answers[1:]
		m.mu.Unlock()
		return next, nil
	}
	m.mu.Unlock()
	m.started <- struct{}{}
	<-m.release
	return text("late"), nil
}

// SPN-4: after the parent's process dies while the child is executing,
// the new owner finds the parent's call Executing, derives the child from it
// and continues the same child Session; the parent's Turn then completes with
// the child's reply. No second child is created.
func TestSpawnSurvivesOwnerRestart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const sid session.SessionID = "parent"
	// Both processes share the Session store on disk. The second process
	// takes the parent over, finds its spawn call waiting for the
	// Responder's answer, and continues the same child from its durable
	// state (SPN-4); no execution record is involved.
	open := func(model loop.ModelInvoker, takeover bool) (*app.Application, *app.Session, turn.PresetRef) {
		t.Helper()
		store, err := filestore.New(root)
		if err != nil {
			t.Fatal(err)
		}
		content, err := filestore.NewContentStore(root, runmod.FrozenAuthority, filestore.ContentStoreOptions{})
		if err != nil {
			t.Fatal(err)
		}
		cfg := durablePorts(t, app.Config{Store: store, Content: content, Spawn: &spawn.Options{}, Ownership: session.OpenOptions{Takeover: takeover}})
		// The child's own model execution belongs to process 1's Worker; process
		// 2's clock runs an hour ahead so that record reads as orphaned, and
		// RecoverInterrupted asks process 2's Worker (effect.Recoverer) to
		// take it back and restart it (RUN-CMT-7, RUN-EXE-6).
		var clock func() time.Time
		if takeover {
			clock = func() time.Time { return time.Now().Add(time.Hour) }
			cfg.Worker = executor.WorkerOptions{Clock: clock}
		}
		records, err := sqlite.Open(filepath.Join(root, "executions.db"), sqlite.Options{Now: clock})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = records.Close() })
		cfg.Executions = records.Executions()
		h := newHost(t, cfg, map[run.ModelRef]loop.ModelInvoker{"m-1": model})
		preset, err := h.RegisterPreset("b1", mustPreset("m-1", []loop.ExecutableTool{spawn.Options{}.ExecutableTool()}))
		if err != nil {
			t.Fatal(err)
		}
		s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: preset, NewTurnID: func() turn.TurnID { return "p1" }})
		if err != nil {
			t.Fatal(err)
		}
		return h, s, preset
	}

	gate := &gatedModel{answers: []sdk.ModelResult{spawnCall(`{"task":"dig deeper"}`)}, started: make(chan struct{}, 1), release: make(chan struct{})}
	_, s1, _ := open(gate, false)
	sendErr := make(chan error, 1)
	go func() {
		_, err := s1.Send(ctx, "go")
		sendErr <- err
	}()
	select {
	case <-gate.started: // the child's model call is in flight
	case err := <-sendErr:
		t.Fatalf("send settled before the child executed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("child never reached its model call")
	}

	// Process 2 takes over. The parent's spawn call is Waiting for the
	// Responder; the child exists with an active Turn whose model step
	// process 1 was running, and process 2's records know nothing of it.
	model2 := &scriptedRequests{answers: []sdk.ModelResult{text("child reply"), text("parent done")}}
	h2, _, _ := open(model2, true)
	waitUntil(func() bool {
		surface, err := h2.TurnSurface(ctx, sid)
		return err == nil && surface.Turns["p1"].Status == turn.TurnCompleted
	})
	if reply := lastReply(t, h2, sid); reply != "parent done" {
		t.Fatalf("parent reply after takeover = %q", reply)
	}
	out, _ := spawnOutput(t, h2, sid)
	if out.Reply != "child reply" || out.Status != turn.TurnCompleted {
		t.Fatalf("spawn result after takeover = %+v", out)
	}
	// One child, continued: its Turn count is one and it holds exactly the
	// task input.
	childChat, err := h2.ChatlogSurface(ctx, out.ChildSession)
	if err != nil {
		t.Fatal(err)
	}
	childTurns, _ := h2.TurnSurface(ctx, out.ChildSession)
	if childChat.Inputs.Len() != 1 || len(childTurns.Order) != 1 {
		t.Fatalf("child after takeover: inputs=%d turns=%d, want one of each", childChat.Inputs.Len(), len(childTurns.Order))
	}
	if err := h2.Close(ctx); err != nil {
		t.Fatal(err)
	}
	close(gate.release)
	if err := <-sendErr; err == nil {
		t.Fatal("the superseded process's Send settled without an ownership error")
	}
}

// The Responder rejects malformed arguments, unknown named presets and calls
// from a Session at the depth limit before any child exists: the parent
// records a Known response_rejected failure and no child Session is created.
func TestSpawnValidation(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name     string
		args     string
		maxDepth int
		spawned  bool // the calling Session itself was spawned (depth 1)
		want     string
	}{
		{"missing task", `{"mode":"spawn"}`, 0, false, run.FailureResponseRejected},
		{"unknown mode", `{"task":"x","mode":"clone"}`, 0, false, run.FailureResponseRejected},
		{"unknown preset", `{"task":"x","preset":"ghost"}`, 0, false, run.FailureResponseRejected},
		{"depth limit", `{"task":"x"}`, 1, true, run.FailureResponseRejected},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model := &scriptedRequests{answers: []sdk.ModelResult{spawnCall(tc.args), text("recovered")}}
			store := filestoretest.Store(t)
			opts := spawn.Options{MaxDepth: tc.maxDepth}
			h := newHost(t, app.Config{Store: store, Content: durableContent(t), Spawn: &opts}, map[run.ModelRef]loop.ModelInvoker{"m-1": model})
			preset, err := h.RegisterPreset("b1", mustPreset("m-1", []loop.ExecutableTool{opts.ExecutableTool()}))
			if err != nil {
				t.Fatal(err)
			}
			sid := session.SessionID("s")
			if tc.spawned {
				sid = spawn.ChildID("root", "r1", "c0")
				ext, err := spawn.Extension(spawn.Provenance{
					ParentSession: "root", ParentRun: "r1", CallID: "c0", Depth: 1, Arguments: spawn.Arguments{Task: "t", Mode: spawn.Empty}})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.Create(ctx, session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: 1, Ext: ext}); err != nil {
					t.Fatal(err)
				}
			}
			s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: preset})
			if err != nil {
				t.Fatal(err)
			}
			results, err := s.Send(ctx, "go")
			if err != nil || len(results) != 1 || results[0].Status != turn.TurnCompleted || results[0].Reply != "recovered" {
				t.Fatalf("send = %+v %v", results, err)
			}
			chat, _ := h.ChatlogSurface(ctx, sid)
			var failure string
			for _, e := range chat.EntryOrder {
				if e.Kind == chatlog.EntryToolResult {
					tr, _ := chat.ToolResults.Get(chatlog.ToolResultID(e.ID))
					if tr.Failure != nil {
						failure = tr.Failure.Class
					}
				}
			}
			if failure != tc.want {
				t.Fatalf("tool failure class = %q, want %q", failure, tc.want)
			}
			surface, _ := h.TurnSurface(ctx, sid)
			for runID := range surface.RunOwner {
				if _, err := store.Header(ctx, spawn.ChildID(sid, runID, "c1")); !session.IsCode(err, session.ErrNotFound) {
					t.Fatalf("a rejected call left a child session: %v", err)
				}
			}
		})
	}
}
