package app_test

import (
	"context"
	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
	"strings"
	"testing"
)

// The example application module: source "example", module "audit". It records
// audit notes as its own durable events and folds a trail projection over its
// notes plus the chatlog inputs it Requires.
const (
	auditSource module.SourceID     = "example"
	auditID     module.ModuleID     = "audit"
	auditTrail  module.ProjectionID = "example/audit/trail"
)

var auditNoteType = module.ModulePrefix(auditSource, auditID) + "note"

type auditNote struct {
	InputID string `json:"inputId"`
	Text    string `json:"text"`
}

type auditState struct {
	Inputs []string `json:"inputs"`
	Notes  []string `json:"notes"`
}

var auditModule = module.ModuleDescriptor{
	Source: auditSource,
	ID:     auditID,
	Requires: []module.ModuleRequirement{{
		Source: module.SourceTwilight, Module: chatlog.ModuleID,
		Events: []ledger.EventType{chatlog.TypeInputSubmitted},
	}},
	Streams: []module.StreamDefinition{{Domain: "audit", Inheritance: module.Inherited}},
	Events: []module.EventDefinition{{
		Type: auditNoteType, Domain: "audit",
		Codecs: map[module.PayloadVersion]module.PayloadCodec{module.Pre(1): module.JSONCodec[auditNote]{}},
	}},
	Projections: []module.ProjectionDefinition{{
		ID: auditTrail, Version: 1,
		Consumes: []ledger.EventType{auditNoteType, chatlog.TypeInputSubmitted},
		Initial:  func() (any, error) { return auditState{}, nil },
		Apply: func(state any, e module.DecodedEvent) (any, error) {
			s := state.(auditState)
			switch v := e.Value.(type) {
			case auditNote:
				s.Notes = append(append([]string(nil), s.Notes...), v.Text)
			case chatlog.InputSubmittedPayload:
				s.Inputs = append(append([]string(nil), s.Inputs...), string(v.InputID))
			}
			return s, nil
		},
		StateCodec: module.JSONStateCodec[auditState]{},
	}},
}

// An application module registered through Ports.Modules writes its own
// events into the stream domain it declares and folds its own projection,
// while the first-party projections skip its rows as out-of-scope (EXT-REG-1,
// EXT-PRJ-2).
func TestAppModuleWritesItsOwnStream(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	h := newHost(t, app.Config{Sessions: app.SessionPorts{Store: store, Modules: []module.ModuleDescriptor{auditModule}}}, map[run.ModelRef]local.ModelInvoker{"m-1": &scriptedRequests{}})
	const sid session.SessionID = "s-app"
	if err := h.EnsureSession(ctx, sid); err != nil {
		t.Fatal(err)
	}
	preset, err := h.RegisterPreset("b1", mustPreset("m-1", nil))
	if err != nil {
		t.Fatal(err)
	}

	// The app module commits its own event through the Session's Writer,
	// held through Acquire; the conversation then runs over the same ledger.
	owned, err := h.Acquire(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	res, err := owned.Handle.Writer().Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) {
		return &writer.SemanticGroup{CommitID: "audit/n1",
			Batches: []writer.TypedBatch{{Domain: ledger.Domain{Name: "audit"}, Events: []writer.TypedEvent{{
				Type: auditNoteType, RecordedAtUnixMilli: 1, Value: auditNote{InputID: "in-1", Text: "flagged"},
			}}}}}, nil
	})
	if err != nil || res.Outcome != writer.CommitApplied {
		t.Fatalf("audit commit = %+v %v", res, err)
	}
	if err := owned.Close(ctx); err != nil {
		t.Fatal(err)
	}
	s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: preset, NewTurnID: func() turn.TurnID { return "t1" }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitInput(ctx, "in-1", "hello"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Settle(ctx, "t1"); err != nil {
		t.Fatal(err)
	}

	// The app projection folded both its own event and the chatlog input.
	state, _, err := h.Projection(ctx, sid, auditTrail, 1)
	if err != nil {
		t.Fatal(err)
	}
	trail := state.(auditState)
	if len(trail.Inputs) != 1 || trail.Inputs[0] != "in-1" || len(trail.Notes) != 1 || trail.Notes[0] != "flagged" {
		t.Fatalf("audit trail = %+v", trail)
	}

	// First-party projections fold across the app rows untouched.
	chat, err := h.ChatlogSurface(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if in, _ := chat.Inputs.Get("in-1"); in.Status != chatlog.InputDelivered {
		t.Fatalf("input status = %s", in.Status)
	}
	tsurf, err := h.TurnSurface(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if tsurf.Turns["t1"].Status != turn.TurnCompleted {
		t.Fatalf("turn status = %s", tsurf.Turns["t1"].Status)
	}

	// Both sources coexist in one commit ledger.
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
	if err != nil {
		t.Fatal(err)
	}
	var app, core int
	for _, c := range page.Commits {
		for _, b := range c.Batches {
			for _, e := range b.Events {
				switch {
				case strings.HasPrefix(string(e.Type), string(module.ModulePrefix(auditSource, auditID))):
					app++
				case strings.HasPrefix(string(e.Type), "twilight/"):
					core++
				default:
					t.Fatalf("unexpected type %s", e.Type)
				}
			}
		}
	}
	if app != 1 || core < 3 {
		t.Fatalf("stream mix: app=%d core=%d", app, core)
	}
}
