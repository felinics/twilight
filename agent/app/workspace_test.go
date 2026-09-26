package app_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/environment/local"
	"github.com/felinics/twilight/agent/store/sqlite/sqlitetest"
	"github.com/felinics/twilight/agent/tools"
	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agent/workspace/workspacetest"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
)

// shellCall is a model answer that calls the shell tool.
func shellCall(command string) sdk.ModelResult {
	return sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 1},
		ToolCalls: []sdk.ToolCall{{ToolCallID: "c1", ToolName: string(tools.ShellRef), Input: sdk.ParseToolArguments(fmt.Sprintf(`{"command":%q}`, command))}}}
}

type workspaceHost struct {
	h      *app.Application
	preset turn.PresetRef
	store  *workspacetest.Map
	root   string
}

// newWorkspaceHost builds an application with the workspace layer over the
// local provider, and a preset whose tools are the workspace tools.
func newWorkspaceHost(t *testing.T, model loop.ModelInvoker, cfg app.Config) *workspaceHost {
	return newWorkspaceHostWith(t, model, cfg, false)
}

func newWorkspaceHostWith(t *testing.T, model loop.ModelInvoker, cfg app.Config, snapshotAfterTurn bool) *workspaceHost {
	t.Helper()
	root := filepath.Join(t.TempDir(), "envs")
	provider, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	store := &workspacetest.Map{}
	cfg.Workspaces = &app.WorkspaceConfig{Store: store, Provider: provider, Backend: local.Backend, SnapshotAfterTurn: snapshotAfterTurn}
	h := newHost(t, cfg, map[run.ModelRef]loop.ModelInvoker{"m-1": model})
	defs, err := app.WorkspaceTools(nil)
	if err != nil {
		t.Fatal(err)
	}
	preset, err := h.RegisterPreset("ws", mustPreset("m-1", nil, app.WithSystemPrompt("be brief"), app.WithPublicTools(defs...)))
	if err != nil {
		t.Fatal(err)
	}
	return &workspaceHost{h: h, preset: preset, store: store, root: root}
}

func (w *workspaceHost) open(t *testing.T, sid session.SessionID) *app.Session {
	t.Helper()
	ctx := context.Background()
	if err := w.h.EnsureSession(ctx, sid); err != nil {
		t.Fatal(err)
	}
	s, err := w.h.OpenSession(ctx, sid, app.SessionOptions{Preset: w.preset, InboxPoll: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// A bound Session's shell call runs inside the workspace's environment: the
// model is told which workspace it works in, the command's file lands in the
// environment's directory, and the workspace records the materialization
// (APP-WSP-2, APP-WSP-3, APP-WSP-4).
func TestWorkspaceToolsRunInTheBoundWorkspace(t *testing.T) {
	ctx := context.Background()
	model := &scriptedRequests{answers: []sdk.ModelResult{shellCall("printf hi > out.txt && cat out.txt")}}
	w := newWorkspaceHost(t, model, app.Config{})
	ws, err := w.h.AllocateWorkspace(ctx, "repo", "")
	if err != nil {
		t.Fatal(err)
	}
	s := w.open(t, "s-bound")
	if err := s.BindWorkspace(ctx, ws.ID); err != nil {
		t.Fatal(err)
	}
	if b, err := s.Workspace(ctx); err != nil || !b.Bound || b.Workspace != ws.ID || b.InheritedBy("s-bound") {
		t.Fatalf("binding = %+v %v", b, err)
	}
	results, err := s.Send(ctx, "write hi")
	if err != nil || len(results) != 1 || results[0].Status != turn.TurnCompleted {
		t.Fatalf("send = %+v %v", results, err)
	}
	stored, err := w.store.Get(ctx, ws.ID)
	if err != nil || stored.Runtime == nil || stored.Runtime.Backend != local.Backend || stored.Runtime.Generation != 1 {
		t.Fatalf("workspace after the call = %+v %v", stored, err)
	}
	data, err := os.ReadFile(filepath.Join(w.root, string(stored.Runtime.EnvironmentRef), "out.txt"))
	if err != nil || string(data) != "hi" {
		t.Fatalf("file in the environment = %q %v", data, err)
	}
	seen := model.requests()
	if len(seen) != 2 || !strings.Contains(messageText(seen[0].Messages[0]), string(ws.ID)) {
		t.Fatalf("system prompt = %q, want the workspace preface", messageText(seen[0].Messages[0]))
	}
	if last := fmt.Sprint(seen[1].Messages[len(seen[1].Messages)-1]); !strings.Contains(last, `hi`) || !strings.Contains(last, `exitCode`) {
		t.Fatalf("tool result seen by the model = %s", last)
	}
	// Binding again to the same workspace writes nothing; a workspace the
	// store does not know is refused.
	if err := s.BindWorkspace(ctx, ws.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.BindWorkspace(ctx, "ws-absent"); err == nil {
		t.Fatal("bound to an unknown workspace")
	}
}

// A Session bound to no workspace still completes: the call is refused
// before any effect as a Known failure the model reads, and the system
// prompt carries no workspace (APP-WSP-3).
func TestUnboundSessionToolCallIsAKnownFailure(t *testing.T) {
	ctx := context.Background()
	model := &scriptedRequests{answers: []sdk.ModelResult{shellCall("true")}}
	w := newWorkspaceHost(t, model, app.Config{})
	s := w.open(t, "s-unbound")
	results, err := s.Send(ctx, "run")
	if err != nil || len(results) != 1 || results[0].Status != turn.TurnCompleted {
		t.Fatalf("send = %+v %v", results, err)
	}
	seen := model.requests()
	if strings.Contains(messageText(seen[0].Messages[0]), "workspace") {
		t.Fatalf("system prompt of an unbound session = %q", messageText(seen[0].Messages[0]))
	}
	if last := fmt.Sprint(seen[1].Messages[len(seen[1].Messages)-1]); !strings.Contains(last, "bound to none") {
		t.Fatalf("tool result seen by the model = %s", last)
	}
	if _, err := os.ReadDir(w.root); err != nil {
		t.Fatal(err)
	} else if entries, _ := os.ReadDir(w.root); len(entries) != 0 {
		t.Fatalf("an environment was materialized for an unbound session: %v", entries)
	}
}

// A fork's child reads the parent's binding as inherited (Share); Unbind
// ends it for the child alone, and Bind gives the child its own (APP-WSP-5).
func TestForkChildInheritsTheBindingUntilItDecides(t *testing.T) {
	ctx := context.Background()
	done := sdk.ModelResult{Text: "done", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}
	model := &scriptedRequests{answers: []sdk.ModelResult{done}}
	w := newWorkspaceHost(t, model, app.Config{})
	parentWS, err := w.h.AllocateWorkspace(ctx, "repo", "")
	if err != nil {
		t.Fatal(err)
	}
	parent := w.open(t, "parent")
	if err := parent.BindWorkspace(ctx, parentWS.ID); err != nil {
		t.Fatal(err)
	}
	results, err := parent.Send(ctx, "hello")
	if err != nil || len(results) != 1 {
		t.Fatalf("send = %+v %v", results, err)
	}
	if _, err := w.h.ForkBeforeTurn(ctx, "parent", results[0].TurnID, "child"); err != nil {
		t.Fatal(err)
	}
	b, err := w.h.Workspace(ctx, "child")
	if err != nil || !b.Bound || b.Workspace != parentWS.ID || !b.InheritedBy("child") || b.InheritedBy("parent") {
		t.Fatalf("child binding = %+v %v, want the parent's, inherited", b, err)
	}
	child, err := w.h.OpenSession(ctx, "child", app.SessionOptions{Preset: w.preset, InboxPoll: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close(ctx)
	if err := child.UnbindWorkspace(ctx, "fork policy: allocate"); err != nil {
		t.Fatal(err)
	}
	if b, err := child.Workspace(ctx); err != nil || b.Bound {
		t.Fatalf("child after unbind = %+v %v", b, err)
	}
	if b, err := parent.Workspace(ctx); err != nil || !b.Bound || b.Workspace != parentWS.ID {
		t.Fatalf("parent after the child's unbind = %+v %v", b, err)
	}
	own, err := w.h.AllocateWorkspace(ctx, "repo", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := child.BindWorkspace(ctx, own.ID); err != nil {
		t.Fatal(err)
	}
	if b, err := child.Workspace(ctx); err != nil || !b.Bound || b.Workspace != own.ID || b.InheritedBy("child") {
		t.Fatalf("child after bind = %+v %v", b, err)
	}
	if err := child.UnbindWorkspace(ctx, "twice"); err != nil {
		t.Fatal(err)
	}
	if err := child.UnbindWorkspace(ctx, "no binding left"); err != nil {
		t.Fatalf("unbind of an unbound session = %v, want a no-op", err)
	}
}

// The binding is a command anyone can leave in the inbox: a bind_workspace
// enqueued before the Session is opened is applied on open; an unknown
// workspace resolves rejected (APP-INB-2, APP-WSP-2).
func TestBindWorkspaceThroughTheInbox(t *testing.T) {
	ctx := context.Background()
	inboxStore := sqlitetest.Open(t).Inbox()
	w := newWorkspaceHost(t, &scriptedRequests{}, app.Config{Inbox: inboxStore})
	ws, err := w.h.AllocateWorkspace(ctx, "repo", "")
	if err != nil {
		t.Fatal(err)
	}
	const sid session.SessionID = "s-inbox-ws"
	if err := w.h.EnsureSession(ctx, sid); err != nil {
		t.Fatal(err)
	}
	enqueue(t, w.h, sid, "bind", app.CommandBindWorkspace, app.BindWorkspaceCommand{WorkspaceID: ws.ID})
	enqueue(t, w.h, sid, "bind-absent", app.CommandBindWorkspace, app.BindWorkspaceCommand{WorkspaceID: "ws-absent"})
	s := w.open(t, sid)
	if r := await(t, w.h, sid, "bind"); r.Status != "applied" {
		t.Fatalf("bind = %+v", r)
	}
	if r := await(t, w.h, sid, "bind-absent"); r.Status != "rejected" || !strings.Contains(r.Reason, "not found") {
		t.Fatalf("bind to an unknown workspace = %+v", r)
	}
	if b, err := s.Workspace(ctx); err != nil || !b.Bound || b.Workspace != ws.ID {
		t.Fatalf("binding after the inbox = %+v %v", b, err)
	}
}

// toolResultText is the text of the last message of the n-th request the
// model saw: the tool result of the call it made in the request before.
func toolResultText(t *testing.T, model *scriptedRequests, n int) string {
	t.Helper()
	seen := model.requests()
	if len(seen) <= n {
		t.Fatalf("model saw %d requests, want more than %d", len(seen), n)
	}
	msgs := seen[n].Messages
	return fmt.Sprint(msgs[len(msgs)-1])
}

// With SnapshotAfterTurn every settled Turn records a Snapshot on the
// Session; a fork's child settles the inherited binding by its policy on
// open: Restore forks the workspace from the snapshot at the fork point,
// Clone from the latest, Allocate gives an empty one, None unbinds
// (APP-WSP-5, APP-WSP-7).
func TestForkPoliciesRestoreCloneAllocateNone(t *testing.T) {
	ctx := context.Background()
	done := sdk.ModelResult{Text: "done", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}
	model := &scriptedRequests{answers: []sdk.ModelResult{
		shellCall("printf v1 > f.txt"), done, // parent turn 1
		shellCall("printf v2 > f.txt"), done, // parent turn 2
		shellCall("cat f.txt"), done, // restore child
		shellCall("cat f.txt"), done, // clone child
	}}
	w := newWorkspaceHostWith(t, model, app.Config{}, true)
	ws, err := w.h.AllocateWorkspace(ctx, "repo", "")
	if err != nil {
		t.Fatal(err)
	}
	parent := w.open(t, "parent")
	if err := parent.BindWorkspace(ctx, ws.ID); err != nil {
		t.Fatal(err)
	}
	first, err := parent.Send(ctx, "one")
	if err != nil || len(first) != 1 || first[0].Status != turn.TurnCompleted {
		t.Fatalf("turn 1 = %+v %v", first, err)
	}
	if err := parent.Wait(ctx); err != nil { // the snapshot runs in the background
		t.Fatal(err)
	}
	afterOne, err := parent.Workspace(ctx)
	if err != nil || afterOne.Snapshot == "" {
		t.Fatalf("binding after turn 1 = %+v %v, want a snapshot", afterOne, err)
	}
	second, err := parent.Send(ctx, "two")
	if err != nil || len(second) != 1 {
		t.Fatalf("turn 2 = %+v %v", second, err)
	}
	if err := parent.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	afterTwo, err := parent.Workspace(ctx)
	if err != nil || afterTwo.Snapshot == "" || afterTwo.Snapshot == afterOne.Snapshot {
		t.Fatalf("binding after turn 2 = %+v %v, want a newer snapshot", afterTwo, err)
	}
	if stored, _ := w.store.Get(ctx, ws.ID); stored.Snapshot == nil || *stored.Snapshot != afterTwo.Snapshot {
		t.Fatalf("workspace latest snapshot = %v, want %s", stored.Snapshot, afterTwo.Snapshot)
	}
	openChild := func(name session.SessionID, policy workspace.InheritedPolicy) *app.Session {
		t.Helper()
		if _, err := w.h.ForkBeforeTurn(ctx, "parent", second[0].TurnID, name); err != nil {
			t.Fatal(err)
		}
		s, err := w.h.OpenSession(ctx, name, app.SessionOptions{Preset: w.preset, InboxPoll: time.Hour, InheritedWorkspace: policy})
		if err != nil {
			t.Fatalf("open %s with policy %s: %v", name, policy, err)
		}
		t.Cleanup(func() { _ = s.Close(context.Background()) })
		// The fork point precedes turn 2, so the child inherits its still
		// submitted input; the edit flow withdraws it (OWN-FRK-2).
		chat, err := w.h.ChatlogSurface(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		for _, in := range chat.SubmittedInputs() {
			if err := w.h.Owner.Chatlog.Withdraw(ctx, s.Handle().Writer(), run.InputID(in.ID), "fork"); err != nil {
				t.Fatal(err)
			}
		}
		return s
	}
	restore := openChild("restore", workspace.InheritRestore)
	rb, err := restore.Workspace(ctx)
	if err != nil || !rb.Bound || rb.Workspace == ws.ID || rb.InheritedBy("restore") {
		t.Fatalf("restore child binding = %+v %v, want its own workspace", rb, err)
	}
	if forked, _ := w.store.Get(ctx, rb.Workspace); forked.Snapshot == nil || *forked.Snapshot != afterOne.Snapshot {
		t.Fatalf("restore child workspace = %+v, want forked from the fork-point snapshot %s", forked, afterOne.Snapshot)
	}
	if _, err := restore.Send(ctx, "read"); err != nil {
		t.Fatal(err)
	}
	if got := toolResultText(t, model, 5); !strings.Contains(got, `v1`) || strings.Contains(got, `v2`) {
		t.Fatalf("restore child read = %s, want v1", got)
	}
	clone := openChild("clone", workspace.InheritClone)
	cb, _ := clone.Workspace(ctx)
	if forked, _ := w.store.Get(ctx, cb.Workspace); cb.Workspace == ws.ID || forked.Snapshot == nil || *forked.Snapshot != afterTwo.Snapshot {
		t.Fatalf("clone child workspace = %+v %+v, want forked from the latest snapshot", cb, forked)
	}
	if _, err := clone.Send(ctx, "read"); err != nil {
		t.Fatal(err)
	}
	if got := toolResultText(t, model, 7); !strings.Contains(got, `v2`) {
		t.Fatalf("clone child read = %s, want v2", got)
	}
	allocate := openChild("allocate", workspace.InheritAllocate)
	ab, _ := allocate.Workspace(ctx)
	if fresh, _ := w.store.Get(ctx, ab.Workspace); !ab.Bound || ab.Workspace == ws.ID || fresh.Snapshot != nil || fresh.Runtime != nil || fresh.Project != "repo" {
		t.Fatalf("allocate child workspace = %+v %+v, want a fresh one with the parent's project", ab, fresh)
	}
	none := openChild("none", workspace.InheritNone)
	if nb, _ := none.Workspace(ctx); nb.Bound {
		t.Fatalf("none child binding = %+v, want unbound", nb)
	}
	// The parent's binding and latest snapshot are untouched by its children.
	if pb, _ := parent.Workspace(ctx); pb != afterTwo {
		t.Fatalf("parent binding after the forks = %+v, want %+v", pb, afterTwo)
	}
}

// Without the workspace layer the workspace surface is unavailable, not a
// silent no-op.
func TestWorkspaceSurfaceRequiresTheLayer(t *testing.T) {
	ctx := context.Background()
	h, _, _, s := setup(t, &scriptedRequests{}, nil, app.SessionOptions{})
	if _, err := h.AllocateWorkspace(ctx, "repo", ""); !errors.Is(err, app.ErrNoWorkspaces) {
		t.Fatalf("allocate without the layer = %v", err)
	}
	if err := s.BindWorkspace(ctx, "ws"); !errors.Is(err, app.ErrNoWorkspaces) {
		t.Fatalf("bind without the layer = %v", err)
	}
	_ = workspace.NewID()
}
