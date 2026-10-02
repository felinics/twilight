package app

import (
	"context"
	"errors"
	"github.com/felinics/twilight/agent/contextprompt"
	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agent/executor/sandbox"
	"github.com/felinics/twilight/agent/sdkconv"
	"github.com/felinics/twilight/agent/tools"
	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
)

// --- session lifecycle, forwarded from the Authority ---------------------------------

// Fork creates a child session from a parent's ledger prefix (OWN-FRK-1).
// The prefix must end at a quiescent point: a fork inside a Turn would hand
// the child a Turn whose Run is the parent's execution.
func (app *Application) Fork(ctx context.Context, req ForkRequest) (session.SegmentHeader, error) {
	if err := app.history.RequireNoActiveTurnAt(ctx, req.Parent, req.At); err != nil {
		return session.SegmentHeader{}, err
	}
	return app.svc.Lifecycle.Fork(ctx, req)
}

// ForkBeforeTurn forks a session at the commit before the named turn started
// (OWN-FRK-2), so the turn's inputs can be regenerated or edited in the child.
func (app *Application) ForkBeforeTurn(ctx context.Context, parent session.SessionID, turnID turn.TurnID, child session.SessionID) (session.SegmentHeader, error) {
	at, err := app.history.BeforeStart(ctx, parent, turnID)
	if err != nil {
		return session.SegmentHeader{}, err
	}
	return app.Fork(ctx, ForkRequest{Parent: parent, At: at, Child: child})
}

// DeleteSession tombstones a session and reclaims along its path (OWN-FRK-3).
// A Session this process holds open is closed first; one it is opening or
// closing is owner.ErrSessionOpen; one owned by another process is the
// store's ErrOwned.
func (app *Application) DeleteSession(ctx context.Context, sid session.SessionID) error {
	if s, ok := app.Opened(sid); ok {
		if err := s.Close(ctx); err != nil {
			return err
		}
	}
	// Acquiring proves no generation is in transition and closes any Writer
	// still held for the Session, so the delete meets a released one.
	owned, err := app.Acquire(ctx, sid)
	if err != nil {
		return err
	}
	if err := owned.Close(ctx); err != nil {
		return err
	}
	return app.svc.Lifecycle.Delete(ctx, sid)
}

// Collect reclaims segments no live path still names, and truncates the rest to the greatest remaining span (SES-GC-2).
func (app *Application) Collect(ctx context.Context) (session.CollectReport, error) {
	return app.svc.Lifecycle.Collect(ctx)
}

// ChatlogSurface reads the chatlog surface of a Session.
func (app *Application) ChatlogSurface(ctx context.Context, sid session.SessionID) (chatlog.Surface, error) {
	return chatlog.ReadSurface(ctx, app.svc.Projections, sid)
}

// TurnSurface reads the turn surface of a Session.
func (app *Application) TurnSurface(ctx context.Context, sid session.SessionID) (turn.TurnSurface, error) {
	return turn.ReadSurface(ctx, app.svc.Projections, sid)
}

// TurnStatus reads one Turn's status and where its Run stands; it needs no
// ownership.
func (app *Application) TurnStatus(ctx context.Context, ref turn.TurnRef) (turn.TurnResult, error) {
	return app.svc.Turns.Status(ctx, ref)
}

// RunRecord is one verified read of a Run by SessionID: every fact of the
// Run in stream order, folded and compared with the projection. It needs
// no ownership.
func (app *Application) RunRecord(ctx context.Context, sid session.SessionID, runID run.RunID) (sessionstore.Record, error) {
	return app.svc.Runs.Record(ctx, sid, runID)
}

// Projection reads any registered projection of a Session (APP-MEM-1).
func (app *Application) Projection(ctx context.Context, sid session.SessionID, id module.ProjectionID, v module.ProjectionVersion) (any, ledger.Head, error) {
	return app.svc.Projections.Load(ctx, sid, id, v)
}

// CreateSession creates the Session.
func (app *Application) CreateSession(ctx context.Context, sid session.SessionID) error {
	return app.svc.Lifecycle.Create(ctx, sid, nil)
}

// EnsureSession creates the stream when it does not exist yet.
func (app *Application) EnsureSession(ctx context.Context, sid session.SessionID) error {
	return app.svc.Lifecycle.Ensure(ctx, sid)
}

// Reply is the settled Turn's last assistant text (CHT-MAT-1): the
// conversation's reply, empty when the Turn produced none. That a reply is
// the last assistant text is this agent's convention, not a kernel fact.
func (app *Application) Reply(ctx context.Context, ref turn.TurnRef) (string, error) {
	return chatlog.LastAssistantText(ctx, app.svc.Projections, app.svc.Content, ref.SessionID, chatlog.TurnID(ref.TurnID))
}

// Materialize resolves the frozen body a chatlog entry names.
func (app *Application) Materialize(ctx context.Context, entry *chatlog.Entry) (chatlog.Materialized, error) {
	return chatlog.NewMaterializer(app.svc.Content).Entry(ctx, entry)
}

// --- workspaces ---------------------------------------------------------------------

// ErrNoWorkspaces reports a workspace operation on an application built
// without Config.Workspaces.
var ErrNoWorkspaces = errors.New("app: no workspace layer is configured")

// AllocateWorkspace creates a new Workspace record (APP-WSP-2); binding a
// Session to it is a separate command (Session.BindWorkspace or the
// bind_workspace inbox command).
func (app *Application) AllocateWorkspace(ctx context.Context, project string, base workspace.RevisionRef) (workspace.Workspace, error) {
	if app.workspaces == nil {
		return workspace.Workspace{}, ErrNoWorkspaces
	}
	ws := workspace.Workspace{ID: workspace.NewID(), Project: project, Base: base}
	if err := app.workspaces.Store.Create(ctx, ws); err != nil {
		return workspace.Workspace{}, err
	}
	return ws, nil
}

// Workspace is a Session's current binding, read without ownership.
func (app *Application) Workspace(ctx context.Context, sid session.SessionID) (workspace.Binding, error) {
	if app.workspaces == nil {
		return workspace.Binding{}, ErrNoWorkspaces
	}
	return workspace.Read(ctx, app.svc.Projections, sid)
}

// --- presets ------------------------------------------------------------------------

// PresetOption tunes NewPreset and NewPresetFromDefinitions.
type PresetOption func(*preset.AgentPreset)

// WithTools adds frozen tool contracts to the preset: the way
// tools that are not implemented in this process (the workspace tools the
// sandbox backend serves) enter a preset. See WorkspaceTools.
func WithTools(defs ...preset.ToolContract) PresetOption {
	return func(p *preset.AgentPreset) { p.Tools = append(p.Tools, defs...) }
}

// WorkspaceTools freezes the workspace tools' definitions for a preset with
// their workspace placement (APP-WSP-3); nil selects tools.Default().
func WorkspaceTools(ts []tools.Tool) ([]preset.ToolContract, error) {
	if ts == nil {
		ts = tools.Default()
	}
	return sandbox.ToolContracts(ts)
}

// WithSystemPrompt sets the instruction included in the preset digest.
func WithSystemPrompt(s string) PresetOption {
	return func(p *preset.AgentPreset) { p.SystemPrompt = s }
}

// WithPrompt selects the prompt builder the preset names.
func WithPrompt(ref preset.PromptBuilderRef) PresetOption {
	return func(p *preset.AgentPreset) { p.PromptBuilder = ref }
}

// WithScheduling selects tool scheduling.
func WithScheduling(s run.ToolScheduling) PresetOption {
	return func(p *preset.AgentPreset) { p.Scheduling = s }
}

// WithMalformedRetries sets malformed model response retries.
func WithMalformedRetries(n uint8) PresetOption {
	return func(p *preset.AgentPreset) { p.MalformedRetries = n }
}

// NewPreset constructs the common preset shape. Tool implementations are
// used only to freeze their public definitions; they are not stored in the
// preset.
func NewPreset(model run.ModelRef, impls []local.ExecutableTool, opts ...PresetOption) (preset.AgentPreset, error) {
	defs := make([]preset.ToolContract, 0, len(impls))
	seen := make(map[run.ToolRef]struct{}, len(impls))
	for _, tool := range impls {
		if tool == nil {
			return preset.AgentPreset{}, errNilTool
		}
		if _, ok := seen[tool.Ref()]; ok {
			return preset.AgentPreset{}, &duplicateToolError{tool.Ref()}
		}
		seen[tool.Ref()] = struct{}{}
		definition, err := sdkconv.FreezeToolDefinition(tool.Definition())
		if err != nil {
			return preset.AgentPreset{}, err
		}
		defs = append(defs, preset.ToolContract{Ref: tool.Ref(), Definition: definition, Policy: tool.ResponsePolicy(), Replay: tool.Replay(), Placement: tool.Placement()})
	}
	return NewPresetFromDefinitions(model, defs, opts...)
}

// NewPresetFromDefinitions constructs a preset from already frozen public
// tool definitions, for an Owner without local tool implementations.
func NewPresetFromDefinitions(model run.ModelRef, defs []preset.ToolContract, opts ...PresetOption) (preset.AgentPreset, error) {
	if model == "" {
		return preset.AgentPreset{}, errNoModel
	}
	p := preset.AgentPreset{Model: model, PromptBuilder: contextprompt.V1, Tools: append([]preset.ToolContract(nil), defs...)}
	for _, opt := range opts {
		opt(&p)
	}
	if err := preset.ValidatePreset(&p); err != nil {
		return preset.AgentPreset{}, err
	}
	return p, nil
}
