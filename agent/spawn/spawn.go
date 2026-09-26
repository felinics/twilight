// Package spawn is the reference agent's subagent capability (SPN), built on
// the core's Waiting(ExternalResponse) and Driver.Responders seam: the tool
// a model calls to delegate a task, the argument and result shapes, the
// deterministic child Session identity, and the provenance record that makes
// a spawn call recoverable after a crash.
//
// A subagent is an ordinary agent Session started by a ToolCall. The ToolCall
// is the invocation identity, the child Session is the durable history, the
// child's Turn and Run are its execution, and the Host drives it. Nothing new
// enters the Run fact ontology: the parent sees one tool call that completes
// with the child's reply.
//
// The binding from the call to the child is derived, not stored: the child's
// SessionID is a digest of (parent Session, parent Run, CallID), and the
// child segment's creation record records the provenance and the full
// arguments. That record is what a takeover needs to continue the same
// invocation after a crash; when the effect runs through a durable Worker,
// the Worker persists it as ExecutionRef{twilight/session, child} (RUN-EXE-9).
//
// This package is the protocol core. The orchestration that creates, drives
// and settles child Sessions lives in the Host (Ports.Spawn).
package spawn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// DefaultTool is the ToolRef the model calls to start a subagent.
const DefaultTool run.ToolRef = "agent_spawn"

// DefaultDepth bounds how deep subagents may nest.
const DefaultDepth = 3

// Module is the extension slot the provenance lives under in the child
// segment's creation record (SES-WIR-5).
var Module = session.ModuleKey{Source: extension.SourceTwilight, ID: "spawn"}

// Mode selects where the child's history starts.
type Mode string

const (
	// Empty starts the child from an empty Session.
	Empty Mode = "spawn"
	// Fork starts the child from the parent's history before the Turn that
	// made the call (OWN-FRK-2): the conversation so far, without the Turn
	// that is still executing.
	Fork Mode = "fork"
)

// Arguments is the shape of the spawn tool's arguments.
type Arguments struct {
	Task   string `json:"task"`
	Mode   Mode   `json:"mode,omitempty"`
	Preset string `json:"preset,omitempty"`
}

// Result is the spawn tool's output: the child and its settled Turn.
type Result struct {
	ChildSession session.SessionID `json:"childSession"`
	TurnID       turn.TurnID       `json:"turnId"`
	Status       turn.TurnStatus   `json:"status"`
	Reply        string            `json:"reply"`
}

// Provenance is the child segment's creation record under Module: who
// spawned it, with what, and how deep it sits.
type Provenance struct {
	ParentSession session.SessionID `json:"parentSession"`
	ParentRun     run.RunID         `json:"parentRun"`
	CallID        run.CallID        `json:"callId"`
	Depth         int               `json:"depth"`
	Arguments     Arguments         `json:"arguments"`
}

// ChildID derives the child Session of one spawn call. The identity is a
// function of the invocation alone, so a takeover recomputes it from the
// Executing call it finds (RUN-CMT-7).
func ChildID(parent session.SessionID, runID run.RunID, callID run.CallID) session.SessionID {
	raw, err := es.EncodeTypedPayload(1, "twilight/spawn/child", struct {
		Parent session.SessionID `json:"parent"`
		Run    run.RunID         `json:"run"`
		Call   run.CallID        `json:"call"`
	}{parent, runID, callID})
	if err != nil {
		panic(err) // three strings always encode
	}
	return session.SessionID("spawn-" + string(es.DigestBytes(raw))[len("sha256:"):])
}

// DecodeArguments decodes and validates the tool arguments.
func DecodeArguments(args run.CanonicalJSON) (Arguments, error) {
	var a Arguments
	if err := args.Decode(&a); err != nil {
		return a, err
	}
	if a.Task == "" {
		return a, errors.New("task is required")
	}
	switch a.Mode {
	case "":
		a.Mode = Empty
	case Empty, Fork:
	default:
		return a, fmt.Errorf("unknown mode %q", a.Mode)
	}
	return a, nil
}

// Extension builds the segment extension slot that records prov.
func Extension(prov Provenance) (session.Extensions, error) {
	raw, err := json.Marshal(prov)
	if err != nil {
		return nil, err
	}
	return session.Extensions{Module: session.RawValue(raw)}, nil
}

// ProvenanceFromHeader decodes the spawn record from a segment's creation
// record; ok is false when the segment carries none.
func ProvenanceFromHeader(header session.SegmentHeader) (prov Provenance, ok bool, err error) {
	raw, ok := header.Ext[Module]
	if !ok {
		return Provenance{}, false, nil
	}
	if err := json.Unmarshal(raw, &prov); err != nil {
		return Provenance{}, false, fmt.Errorf("spawn: provenance: %w", err)
	}
	return prov, true, nil
}

// DepthExceeded reports whether a Session at depth has reached the nesting
// limit and may no longer spawn (SPN-3).
func DepthExceeded(depth, limit int) bool {
	return depth >= limit
}

// ArgumentsConflict reports whether a recorded child was created for
// different arguments than the call carries now (SPN-2, RUN-EXE-3).
func ArgumentsConflict(prov Provenance, args Arguments) bool {
	return prov.Arguments != args
}

// Tool is the model-facing definition of the spawn tool for preset
// catalogs. Its ResponsePolicy is ExternalResponse: a call waits, and the
// Responder the Driver holds for the tool answers it (SPN-1, DRV-4). Execute
// never runs.
func Tool(ref run.ToolRef) loop.ExecutableTool { return tool{ref: ref} }

type tool struct{ ref run.ToolRef }

func (t tool) Ref() run.ToolRef { return t.ref }

func (t tool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{
		Name:        string(t.ref),
		Description: "Delegate a task to a subagent that runs in its own session and returns its final reply. mode \"fork\" starts it from this conversation's history before the current turn; \"spawn\" (default) starts it empty.",
		Parameters: &jsonschema.Schema{
			Type: "object",
			Properties: map[string]*jsonschema.Schema{
				"task":   {Type: "string", Description: "What the subagent should do."},
				"mode":   {Type: "string", Enum: []any{"spawn", "fork"}},
				"preset": {Type: "string", Description: "Optional named preset the subagent runs under."},
			},
			Required:             []string{"task"},
			AdditionalProperties: &jsonschema.Schema{Not: &jsonschema.Schema{}},
		},
	}
}

func (tool) ResponsePolicy() run.ResponsePolicy { return run.ExternalResponse }

// Replay is allowed: the child Session's identity derives from the call and
// answering an existing child again continues it.
func (tool) Replay() run.ReplayPolicy { return run.ReplayAllowed }

// Placement is in the executor process: spawning a child Session touches
// no workspace.
func (tool) Placement() run.ToolPlacement { return run.PlacementProcess }

func (tool) ValidateArguments(args run.CanonicalJSON) error {
	_, err := DecodeArguments(args)
	return err
}

func (t tool) Execute(context.Context, loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureExecution,
		Message: fmt.Sprintf("%s is answered by the spawn Responder (app.Config.Spawn), never executed", t.ref)}}
}
