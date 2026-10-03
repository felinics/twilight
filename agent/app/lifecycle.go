package app

import (
	"context"
	"errors"
	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agent/executor/sandbox"
	"github.com/felinics/twilight/agent/prompt"
	"github.com/felinics/twilight/agent/sdkconv"
	"github.com/felinics/twilight/agent/tools"
	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
)

// --- session lifecycle, forwarded from the Authority ---------------------------------

// Fork creates a child session from a parent's ledger prefix (OWN-FRK-1).
func (app *Application) Fork(ctx context.Context, req ForkRequest) (session.SegmentHeader, error) {
	return app.Kernel.Fork(ctx, req)
}

// ForkBeforeTurn forks a session at the commit before the named turn started
// (OWN-FRK-2), so the turn's inputs can be regenerated or edited in the child.
func (app *Application) ForkBeforeTurn(ctx context.Context, parent session.SessionID, turnID turn.TurnID, child session.SessionID) (session.SegmentHeader, error) {
	return app.Kernel.ForkBeforeTurn(ctx, parent, turnID, child)
}

// DeleteSession tombstones a session and reclaims along its path (OWN-FRK-3).
func (app *Application) DeleteSession(ctx context.Context, sid session.SessionID) error {
	return app.Owner.DeleteSession(ctx, sid)
}

// Collect reclaims segments no live path still names, and truncates the rest to the greatest remaining span.
func (app *Application) Collect(ctx context.Context) (session.CollectReport, error) {
	return app.Kernel.Collect(ctx)
}

// ChatlogSurface reads the chatlog surface of a Session.
func (app *Application) ChatlogSurface(ctx context.Context, sid session.SessionID) (chatlog.Surface, error) {
	return chatlog.ReadSurface(ctx, app.Kernel.Projections, sid)
}

// TurnSurface reads the turn surface of a Session.
func (app *Application) TurnSurface(ctx context.Context, sid session.SessionID) (turn.TurnSurface, error) {
	return turn.ReadSurface(ctx, app.Kernel.Projections, sid)
}

// Projection reads any registered projection of a Session (APP-MEM-1).
func (app *Application) Projection(ctx context.Context, sid session.SessionID, id module.ProjectionID, v module.ProjectionVersion) (any, ledger.Head, error) {
	return app.Kernel.Projection(ctx, sid, id, v)
}

// Content materializes the frozen bodies projections name (CHT-MAT-1).
func (app *Application) Content() chatlog.ContentResolver { return app.Kernel.Content }

// CreateSession creates the Session.
func (app *Application) CreateSession(ctx context.Context, sid session.SessionID) error {
	return app.Kernel.CreateSession(ctx, sid, nil)
}

// EnsureSession creates the stream when it does not exist yet.
func (app *Application) EnsureSession(ctx context.Context, sid session.SessionID) error {
	return app.Kernel.EnsureSession(ctx, sid)
}

// Reply is the settled Turn's last assistant text (CHT-MAT-1): the
// conversation's reply, empty when the Turn produced none. That a reply is
// the last assistant text is this agent's convention, not a kernel fact.
func (app *Application) Reply(ctx context.Context, ref turn.TurnRef) (string, error) {
	return chatlog.LastAssistantText(ctx, app.Kernel.Projections, app.Kernel.Content, ref.SessionID, chatlog.TurnID(ref.TurnID))
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
	return workspace.Read(ctx, app.Kernel.Projections, sid)
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
// their workspace placement (APP-WSP-3); nil selects tools.Default.
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

// WithPrompt selects the decision component used by the preset.
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
	p := preset.AgentPreset{Model: model, PromptBuilder: prompt.PromptContextV1, Tools: append([]preset.ToolContract(nil), defs...)}
	for _, opt := range opts {
		opt(&p)
	}
	if err := preset.ValidatePreset(&p); err != nil {
		return preset.AgentPreset{}, err
	}
	return p, nil
}
