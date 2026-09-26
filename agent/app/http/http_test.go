package http_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	ownerhttp "github.com/felinics/twilight/agent/app/http"
	"github.com/felinics/twilight/agent/environment/local"
	"github.com/felinics/twilight/agent/workspace/workspacetest"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/inbox/inboxtest"
	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
)

// echoModel answers every request with the text of its last user message.
type echoModel struct {
	mu   sync.Mutex
	seen int
}

func (m *echoModel) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	m.mu.Lock()
	m.seen++
	m.mu.Unlock()
	text := "done"
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == sdk.MessageRoleUser {
			for _, part := range req.Messages[i].Content {
				if t, ok := part.(sdk.TextPart); ok {
					text = "echo: " + t.Text
				}
			}
			break
		}
	}
	return sdk.ModelResult{Text: text, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
}

func newOwner(t *testing.T) (*app.Application, *ownerhttp.Client) {
	t.Helper()
	bindings, ledger := artifacttest.Stores(t)
	provider, err := local.New(filepath.Join(t.TempDir(), "envs"))
	if err != nil {
		t.Fatal(err)
	}
	a, err := app.Build(app.Config{
		Store:      filestoretest.Store(t),
		Content:    filestoretest.Content(t, runmod.FrozenAuthority),
		Artifacts:  owner.Artifacts{Bindings: bindings, Ledger: ledger},
		Executions: storetest.NewMap(nil),
		Inbox:      &inboxtest.Map{},
		Workspaces: &app.WorkspaceConfig{Store: &workspacetest.Map{}, Provider: provider, Backend: local.Backend},
		Ownership:  session.OpenOptions{Owner: "owner-a", LeaseDuration: time.Minute},
		Executor:   app.ExecutorConfig{Models: map[run.ModelRef]loop.ModelInvoker{"m-1": &echoModel{}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(context.Background()) })
	preset, err := app.NewPreset("m-1", nil, app.WithSystemPrompt("be brief"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.RegisterPreset("p1", preset); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&ownerhttp.Server{App: a, Options: app.SessionOptions{InboxPoll: time.Hour}}).Handler())
	t.Cleanup(server.Close)
	return a, &ownerhttp.Client{BaseURL: server.URL}
}

// A conversation through the face: ensure, open, a submit command that is
// applied and answered, the Turn and its reply read back, the events
// stream carrying the commits, the lease naming this owner, and close.
func TestCommandFaceDrivesASession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	a, c := newOwner(t)
	const sid session.SessionID = "s-1"
	if err := c.Ensure(ctx, sid); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Open(ctx, sid, ownerhttp.OpenRequest{Preset: "missing"}); !ownerhttp.IsCode(err, ownerhttp.CodeNotFound) {
		t.Fatalf("open with an unknown preset = %v", err)
	}
	opened, err := c.Open(ctx, sid, ownerhttp.OpenRequest{Preset: "p1"})
	if err != nil || opened.AlreadyOpen || opened.Active != "" {
		t.Fatalf("open = %+v %v", opened, err)
	}
	if again, err := c.Open(ctx, sid, ownerhttp.OpenRequest{Preset: "p1"}); err != nil || !again.AlreadyOpen {
		t.Fatalf("second open = %+v %v, want already open", again, err)
	}
	lease, err := c.Lease(ctx, sid)
	if err != nil || !lease.Held || lease.Lease == nil || lease.Lease.Owner != "owner-a" {
		t.Fatalf("lease = %+v %v", lease, err)
	}
	// Events from the start, collected in the background.
	var (
		mu    sync.Mutex
		types []session.EventType
	)
	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()
	streamDone := make(chan error, 1)
	go func() {
		streamDone <- c.Events(streamCtx, sid, 0, func(e ownerhttp.Event) bool {
			mu.Lock()
			types = append(types, e.Type)
			mu.Unlock()
			return true
		})
	}()
	cmd, err := app.NewCommand("c1", app.CommandSubmit, app.SubmitCommand{InputID: "in-1", Text: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := c.Enqueue(ctx, sid, cmd)
	if err != nil || !entry.Pending() || entry.Command.ID != "c1" {
		t.Fatalf("enqueue = %+v %v", entry, err)
	}
	if _, err := c.Enqueue(ctx, sid, inbox.Command{ID: "c1", Kind: app.CommandStop}); !ownerhttp.IsCode(err, ownerhttp.CodeCommandConflict) {
		t.Fatalf("reused command id = %v", err)
	}
	result, err := c.Await(ctx, sid, "c1")
	if err != nil || result.Status != inbox.StatusApplied {
		t.Fatalf("await = %+v %v", result, err)
	}
	if _, err := c.Command(ctx, sid, "absent", 0); !ownerhttp.IsCode(err, ownerhttp.CodeNotFound) {
		t.Fatalf("unknown command = %v", err)
	}
	opened2, _ := a.Opened(sid)
	if err := opened2.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	surface, err := c.Turns(ctx, sid)
	if err != nil || len(surface.Order) != 1 {
		t.Fatalf("turns = %+v %v", surface, err)
	}
	view, err := c.Turn(ctx, sid, surface.Order[0])
	if err != nil || view.Turn.Status != turn.TurnCompleted || view.Reply != "echo: hello" {
		t.Fatalf("turn = %+v %v", view, err)
	}
	if _, err := c.Turn(ctx, sid, "t-absent"); !ownerhttp.IsCode(err, ownerhttp.CodeNotFound) {
		t.Fatalf("unknown turn = %v", err)
	}
	if open, err := c.Wake(ctx, sid); err != nil || !open {
		t.Fatalf("wake = %v %v", open, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(types)
		mu.Unlock()
		if n >= 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopStream()
	<-streamDone
	mu.Lock()
	joined := ""
	for _, ty := range types {
		joined += string(ty) + " "
	}
	mu.Unlock()
	if !strings.Contains(joined, "twilight/chatlog/input_submitted") || !strings.Contains(joined, "twilight/turn/") {
		t.Fatalf("events seen = %s", joined)
	}
	if err := c.Close(ctx, sid); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(ctx, sid); !ownerhttp.IsCode(err, ownerhttp.CodeNotOpen) {
		t.Fatalf("second close = %v", err)
	}
	if open, err := c.Wake(ctx, sid); err != nil || open {
		t.Fatalf("wake after close = %v %v", open, err)
	}
	if lease, err := c.Lease(ctx, sid); err != nil || lease.Held {
		t.Fatalf("lease after close = %+v %v", lease, err)
	}
	if _, err := c.Lease(ctx, "s-absent"); !ownerhttp.IsCode(err, ownerhttp.CodeNotFound) {
		t.Fatalf("lease of an unknown session = %v", err)
	}
}

// Workspace allocation and binding through the face, and a fork before a
// Turn whose child opens with the restore policy refused for want of a
// snapshot (APP-WSP-5).
func TestCommandFaceWorkspacesAndFork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	a, c := newOwner(t)
	const sid session.SessionID = "s-ws"
	if err := c.Ensure(ctx, sid); err != nil {
		t.Fatal(err)
	}
	ws, err := c.AllocateWorkspace(ctx, ownerhttp.AllocateWorkspaceRequest{Project: "repo"})
	if err != nil || ws.ID == "" || ws.Project != "repo" {
		t.Fatalf("allocate = %+v %v", ws, err)
	}
	if _, err := c.Open(ctx, sid, ownerhttp.OpenRequest{Preset: "p1"}); err != nil {
		t.Fatal(err)
	}
	bind, _ := app.NewCommand("bind", app.CommandBindWorkspace, app.BindWorkspaceCommand{WorkspaceID: ws.ID})
	if _, err := c.Enqueue(ctx, sid, bind); err != nil {
		t.Fatal(err)
	}
	if r, err := c.Await(ctx, sid, "bind"); err != nil || r.Status != inbox.StatusApplied {
		t.Fatalf("bind = %+v %v", r, err)
	}
	if b, err := c.Workspace(ctx, sid); err != nil || !b.Bound || b.Workspace != ws.ID {
		t.Fatalf("workspace = %+v %v", b, err)
	}
	submit, _ := app.NewCommand("c1", app.CommandSubmit, app.SubmitCommand{InputID: "in-1", Text: "hi"})
	if _, err := c.Enqueue(ctx, sid, submit); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Await(ctx, sid, "c1"); err != nil {
		t.Fatal(err)
	}
	s, _ := a.Opened(sid)
	if err := s.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	surface, _ := c.Turns(ctx, sid)
	header, err := c.Fork(ctx, sid, ownerhttp.ForkRequest{Child: "child", BeforeTurn: surface.Order[0]})
	if err != nil || header.ID == "" || header.Parent == nil {
		t.Fatalf("fork = %+v %v", header, err)
	}
	if b, err := c.Workspace(ctx, "child"); err != nil || !b.Bound || !b.InheritedBy("child") {
		t.Fatalf("child binding = %+v %v, want inherited", b, err)
	}
	if _, err := c.Open(ctx, "child", ownerhttp.OpenRequest{Preset: "p1", InheritedWorkspace: "restore"}); err == nil {
		t.Fatal("restore without a snapshot succeeded")
	}
	if _, err := c.Open(ctx, "child", ownerhttp.OpenRequest{Preset: "p1", InheritedWorkspace: "dance"}); !ownerhttp.IsCode(err, ownerhttp.CodeInvalid) {
		t.Fatalf("unknown policy = %v", err)
	}
	if _, err := c.Open(ctx, "child", ownerhttp.OpenRequest{Preset: "p1", InheritedWorkspace: "allocate"}); err != nil {
		t.Fatal(err)
	}
	if b, err := c.Workspace(ctx, "child"); err != nil || !b.Bound || b.Workspace == ws.ID || b.InheritedBy("child") {
		t.Fatalf("child binding after allocate = %+v %v", b, err)
	}
	if _, err := c.Fork(ctx, sid, ownerhttp.ForkRequest{Child: ""}); !ownerhttp.IsCode(err, ownerhttp.CodeInvalid) {
		t.Fatalf("fork without a child = %v", err)
	}
}
