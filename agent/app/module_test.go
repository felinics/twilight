package app_test

import (
	"context"
	"strings"
	"testing"

	"github.com/felinics/twilight/agent/app"
	agentinput "github.com/felinics/twilight/agent/input"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// The example application module: source "example", module "audit". It records
// audit notes as its own durable events and folds a trail projection over its
// notes plus the chatlog inputs it Requires.
const (
	auditSource extension.SourceID     = "example"
	auditID     extension.ModuleID     = "audit"
	auditTrail  extension.ProjectionID = "example/audit/trail"
)

var auditNoteType = extension.ModulePrefix(auditSource, auditID) + "note"

type auditNote struct {
	InputID string `json:"inputId"`
	Text    string `json:"text"`
}

type auditState struct {
	Inputs []string `json:"inputs"`
	Notes  []string `json:"notes"`
}

var auditModule = extension.ModuleDescriptor{
	Source: auditSource,
	ID:     auditID,
	Requires: []extension.ModuleRequirement{{
		Source: extension.SourceTwilight, Module: chatlog.ModuleID,
		Events: []session.EventType{chatlog.TypeInputSubmitted},
	}},
	Streams: []extension.StreamDefinition{{Domain: "audit", Lineage: session.LineageSession}},
	Events: []extension.EventDefinition{{
		Type: auditNoteType, Stream: "audit",
		Codecs: map[extension.PayloadVersion]extension.PayloadCodec{1: extension.JSONCodec[auditNote]{}},
	}},
	Projections: []extension.ProjectionDefinition{{
		ID: auditTrail, Version: 1,
		Consumes: []session.EventType{auditNoteType, chatlog.TypeInputSubmitted},
		Initial:  func() (any, error) { return auditState{}, nil },
		Apply: func(state any, e extension.DecodedEvent) (any, error) {
			s := state.(auditState)
			switch v := e.Value.(type) {
			case auditNote:
				s.Notes = append(append([]string(nil), s.Notes...), v.Text)
			case chatlog.InputSubmittedPayload:
				s.Inputs = append(append([]string(nil), s.Inputs...), string(v.InputID))
			}
			return s, nil
		},
		StateCodec: extension.JSONStateCodec[auditState]{},
	}},
}

// An application module registered through Ports.Modules writes its own
// events into the stream domain it declares and folds its own projection,
// while the first-party projections skip its rows as out-of-scope (EXT-REG-1,
// EXT-PRJ-2).
func TestAppModuleWritesItsOwnStream(t *testing.T) {
	ctx := context.Background()
	h := newHost(t, app.Config{Modules: []extension.ModuleDescriptor{auditModule}}, map[run.ModelRef]loop.ModelInvoker{"m-1": &scriptedRequests{}})
	const sid session.SessionID = "s-app"
	if err := h.EnsureSession(ctx, sid); err != nil {
		t.Fatal(err)
	}
	preset, err := h.RegisterPreset("b1", mustPreset("m-1", nil))
	if err != nil {
		t.Fatal(err)
	}

	owned, err := h.Owner.Open(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	w := owned.Writer()
	in, err := h.Owner.Chatlog.Submit(ctx, w, "in-1", agentinput.Text("hello"))
	if err != nil {
		t.Fatal(err)
	}
	// The app module commits its own event through the same Writer.
	res, err := w.Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) {
		return &writer.SemanticGroup{CommitID: "audit/n1",
			Batches: []writer.TypedBatch{{Stream: session.StreamRef{Domain: "audit"}, Events: []writer.TypedEvent{{
				Type: auditNoteType, RecordedAtUnixMilli: 1, Value: auditNote{InputID: "in-1", Text: "flagged"},
			}}}}}, nil
	})
	if err != nil || res.Outcome != writer.CommitApplied {
		t.Fatalf("audit commit = %+v %v", res, err)
	}
	if _, err := h.Owner.Turns.Start(ctx, w, turn.StartRequest{Ref: turn.TurnRef{SessionID: sid, TurnID: "t1"},
		Inputs: []run.AgentInput{in}, Preset: preset}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Owner.Driver.Drive(ctx, w, "t1"); err != nil {
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
	page, err := h.Owner.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
	if err != nil {
		t.Fatal(err)
	}
	var app, core int
	for _, c := range page.Commits {
		for _, b := range c.Batches {
			for _, e := range b.Events {
				switch {
				case strings.HasPrefix(string(e.Type), string(extension.ModulePrefix(auditSource, auditID))):
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
