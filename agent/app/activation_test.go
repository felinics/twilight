package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/component/localagent"
	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agent/store/sqlite/sqlitetest"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
)

func awaitCondition(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: condition never held", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func awaitModel(t *testing.T, started <-chan sdk.Request) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the model call never started")
	}
}

func completedTurns(t *testing.T, h *localagent.Agent, sid session.SessionID) int {
	t.Helper()
	surface, err := h.TurnSurface(context.Background(), sid)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, id := range surface.Order {
		if surface.Turns[id].Status == turn.TurnCompleted {
			n++
		}
	}
	return n
}

// Two processes over one store, neither of which opens the Session: each
// Turn runs on the process its command reaches, ownership is held for the
// work and released once the Session is quiescent, a command reaching the
// process that does not hold the Session is applied by the one that does,
// and a command nobody woke an owner for is picked up by the scan
// (APP-ACT-1, APP-ACT-2, APP-ACT-3).
func TestTurnsOfOneSessionRunOnDifferentProcesses(t *testing.T) {
	ctx := context.Background()
	shared := sqlitetest.Open(t)
	inboxStore := shared.Inbox()
	root := t.TempDir()
	preset := mustPreset("m-1", nil, app.WithSystemPrompt("be brief"))
	build := func(name string, model local.ModelInvoker, scan time.Duration) *localagent.Agent {
		cfg := exampleStores(root, name)
		cfg.Inbox = inboxStore
		cfg.Presets = []app.Preset{{ID: "b1", Value: preset}}
		cfg.Kernel.Ownership = session.OpenOptions{Owner: name, LeaseDuration: time.Minute}
		cfg.Activation = &app.Activation{Preset: "b1", IdleRelease: 50 * time.Millisecond, Scan: scan,
			Options: app.SessionOptions{InboxPoll: 50 * time.Millisecond}}
		cfg.Warn = func(err error) { t.Logf("%s: warn: %v", name, err) }
		return newLocalHost(t, cfg, map[run.ModelRef]local.ModelInvoker{"m-1": model})
	}
	// Only a scans, and slowly: a command enqueued through b must be b's
	// activation, not a's scan racing it (both are legal; the test pins one).
	gateA := &gateModel{started: make(chan sdk.Request, 4), release: make(chan struct{})}
	gateB := &gateModel{started: make(chan sdk.Request, 4), release: make(chan struct{})}
	a := build("a", gateA, 2*time.Second)
	b := build("b", gateB, 0)
	defer func() { _ = a.Close(ctx); _ = b.Close(ctx) }()
	const sid session.SessionID = "s-1"
	released := func(h *localagent.Agent) func() bool {
		return func() bool {
			_, held, err := h.Lease(ctx, sid)
			if err != nil {
				t.Fatal(err)
			}
			_, open := h.Opened(sid)
			return !held && !open
		}
	}

	// Turn 1: the command reaches a, which acquires the Session.
	enqueue(t, a, sid, "c1", app.CommandSubmit, app.SubmitCommand{InputID: "in-1", Text: "first"})
	awaitModel(t, gateA.started)
	if lease, held, err := a.Lease(ctx, sid); err != nil || !held || lease.Owner != "a" {
		t.Fatalf("lease during turn 1 = %+v held:%v %v, want a", lease, held, err)
	}
	if _, open := b.Opened(sid); open {
		t.Fatal("b opened the session a holds")
	}
	// A command reaching b while a holds the Session: b's activation is
	// refused by the live lease and a applies it on its poll.
	enqueue(t, b, sid, "c2", app.CommandWithdraw, app.WithdrawCommand{InputID: "in-x"})
	if r := await(t, b, sid, "c2"); r.Status != inbox.StatusRejected {
		t.Fatalf("withdraw of an unknown input = %+v, want rejected by the holder", r)
	}
	close(gateA.release)
	awaitCondition(t, "turn 1", func() bool { return completedTurns(t, a, sid) == 1 })
	awaitCondition(t, "release after turn 1", released(a))

	// Turn 2: the command reaches b, which acquires the Session in turn.
	enqueue(t, b, sid, "c3", app.CommandSubmit, app.SubmitCommand{InputID: "in-2", Text: "second"})
	awaitModel(t, gateB.started)
	if lease, held, err := b.Lease(ctx, sid); err != nil || !held || lease.Owner != "b" || lease.Epoch != 2 {
		t.Fatalf("lease during turn 2 = %+v held:%v %v, want b at epoch 2", lease, held, err)
	}
	if _, open := a.Opened(sid); open {
		t.Fatal("a opened the session b holds")
	}
	close(gateB.release)
	awaitCondition(t, "turn 2", func() bool { return completedTurns(t, b, sid) == 2 })
	awaitCondition(t, "release after turn 2", released(b))

	// Turn 3: the command is written straight into the shared inbox with no
	// wake; a's scan (the only one running) activates the Session.
	cmd, err := app.NewCommand("c4", app.CommandSubmit, app.SubmitCommand{InputID: "in-3", Text: "third"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inboxStore.Enqueue(ctx, sid, cmd); err != nil {
		t.Fatal(err)
	}
	if r := await(t, a, sid, "c4"); r.Status != inbox.StatusApplied {
		t.Fatalf("scanned submit = %+v", r)
	}
	awaitCondition(t, "turn 3", func() bool { return completedTurns(t, a, sid) == 3 })
	awaitCondition(t, "release after turn 3", released(a))
}

func TestActivationConfiguration(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		cfg     app.Config
		build   bool
		wantErr error
	}{
		{name: "activation without an inbox", cfg: app.Config{Activation: &app.Activation{Preset: "b1"}}},
		{name: "activate without activation", cfg: app.Config{Inbox: sqlitetest.Open(t).Inbox()}, build: true, wantErr: app.ErrNoActivation},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := durablePorts(t, tc.cfg)
			cfg.Models = map[run.ModelRef]local.ModelInvoker{"m-1": &scriptedRequests{}}
			cfg.Executions = sqlitetest.Open(t).Executions()
			h, err := localagent.Compose(cfg)
			if (err == nil) != tc.build {
				t.Fatalf("build = %v, want ok=%v", err, tc.build)
			}
			if !tc.build {
				return
			}
			defer h.Close(ctx)
			if _, err := h.Activate(ctx, "s-1"); err != tc.wantErr { //nolint:errorlint // sentinel returned as is
				t.Fatalf("activate = %v, want %v", err, tc.wantErr)
			}
		})
	}
}
