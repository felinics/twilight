// Package contextprompt is the first-party prompt builder of the reference
// agent: it folds the chatlog context projection into one provider request.
// The agent core only knows the Builder seam and the Catalog that resolves a
// PromptBuilderRef; which builder a preset names, and how it assembles
// context, is this agent's strategy.
package contextprompt

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/sdkconv"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/sdk"

	"github.com/felinics/twilight/agent/input"
	"github.com/felinics/twilight/agent/workspace"
)

// V1 names the context prompt builder: the chatlog context projection
// folded into one provider request.
const V1 preset.PromptBuilderRef = "twilight/contextprompt/v1"

// Builder is the V1 prompt builder: it reads the chatlog context
// projection, materializes its entries and assembles the next sdk.Request.
// Every assistant and tool_result of the Session is in the fold already,
// including those of earlier attempts of the same Turn.
type Builder struct {
	Sources prompt.Sources
	Preset  preset.AgentPreset
	// InputText extracts the user text of one input payload; nil selects the
	// v1 shape {"text": ...} of the agent's input package.
	InputText func(jsonstable.Value) (string, error)
	// Preface, when set, contributes text the builder appends to the system
	// prompt of every request, read from the Session's projections: the
	// application's standing facts the model must know, such as the
	// workspace it works in (APP-WSP-4). An empty string adds nothing.
	Preface Preface
}

// Preface reads the application's standing context of a Session.
type Preface func(ctx context.Context, sources prompt.Sources, sid session.SessionID) (string, error)

// New is the BuilderFactory of V1.
func New(ap preset.AgentPreset, sources prompt.Sources) prompt.Builder {
	return &Builder{Sources: sources, Preset: ap}
}

func (p *Builder) Build(ctx context.Context, hint run.PromptInput) (prompt.Prompt, error) {
	if p.Sources.Projections == nil || p.Preset.Model == "" {
		return prompt.Prompt{}, errors.New("contextprompt: builder requires projections and a model")
	}
	if hint.Scope == "" {
		return prompt.Prompt{}, errors.New("contextprompt: builder hint has no session")
	}
	state, head, err := p.Sources.Projections.Load(ctx, session.SessionID(hint.Scope), chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
	if err != nil {
		return prompt.Prompt{}, err
	}
	cctx, ok := state.(chatlog.Context)
	if !ok {
		return prompt.Prompt{}, fmt.Errorf("contextprompt: context projection is %T", state)
	}
	entries, err := chatlog.NewMaterializer(p.Sources.Content).Entries(ctx, cctx.Entries)
	if err != nil {
		return prompt.Prompt{}, err
	}
	system := p.Preset.SystemPrompt
	if p.Preface != nil {
		preface, err := p.Preface(ctx, p.Sources, session.SessionID(hint.Scope))
		if err != nil {
			return prompt.Prompt{}, err
		}
		if preface != "" {
			if system != "" {
				system += "\n\n"
			}
			system += preface
		}
	}
	sdkMsgs, err := p.messages(system, entries)
	if err != nil {
		return prompt.Prompt{}, err
	}
	msgs := make([]model.Message, len(sdkMsgs))
	for i := range sdkMsgs {
		if msgs[i], err = sdkconv.FreezeMessage(sdkMsgs[i]); err != nil {
			return prompt.Prompt{}, fmt.Errorf("contextprompt: message %d: %w", i, err)
		}
	}
	specs, defs, err := prompt.ToolSpecs(p.Preset.Tools)
	if err != nil {
		return prompt.Prompt{}, err
	}
	ids := make([]run.InputID, 0, len(hint.Inputs))
	for _, in := range hint.Inputs {
		ids = append(ids, in.ID)
	}
	return prompt.Prompt{
		Model:    p.Preset.Model,
		Request:  model.ModelRequest{Model: string(p.Preset.Model), Messages: msgs, Tools: defs},
		InputIDs: ids,
		Token:    run.PromptToken(fmt.Sprintf("%d", head.Next)),
		Tools:    specs,
		Policy:   run.StepPolicy{Scheduling: p.Preset.Scheduling, MalformedRetries: p.Preset.MalformedRetries},
	}, nil
}

// messages renders the materialized entries as provider messages: the
// system prompt first, then the context in order, with each tool result
// paired to the assistant call that issued it.
func (p *Builder) messages(system string, entries []chatlog.Materialized) ([]sdk.Message, error) {
	var msgs []sdk.Message
	if system != "" {
		msgs = append(msgs, sdk.SystemMessage(system))
	}
	inputText := p.InputText
	if inputText == nil {
		inputText = input.TextOf
	}
	// ProviderCallID and tool name per CallID, from the assistant that issued
	// the call, for pairing tool results.
	type callInfo struct{ provider, name string }
	calls := map[chatlog.CallID]callInfo{}
	// Inputs delivered mid-turn are committed while tool calls are still open
	// (TRN-DLV-2); providers require tool results to follow their assistant
	// message directly, so such inputs are held until the open calls resolve.
	open := map[chatlog.CallID]struct{}{}
	var deferred []sdk.Message
	flushDeferred := func() {
		if len(open) == 0 && len(deferred) > 0 {
			msgs = append(msgs, deferred...)
			deferred = nil
		}
	}
	for i := range entries {
		m := &entries[i]
		e := m.Entry
		switch e.Kind {
		case chatlog.EntryInput:
			text, err := inputText(e.Input.Content)
			if err != nil {
				return nil, err
			}
			if len(open) > 0 {
				deferred = append(deferred, sdk.UserMessage(text))
			} else {
				msgs = append(msgs, sdk.UserMessage(text))
			}
		case chatlog.EntryAssistant:
			if len(open) > 0 {
				return nil, errors.New("contextprompt: assistant follows unresolved tool calls")
			}
			flushDeferred()
			if m.Result == nil {
				return nil, fmt.Errorf("contextprompt: assistant %s is not materialized", e.ID)
			}
			var parts []sdk.MessagePart
			for _, rp := range m.Result.ReasoningParts {
				if rp.Text != "" {
					parts = append(parts, sdk.ReasoningPart{Text: rp.Text})
				}
			}
			if m.Result.Text != "" {
				parts = append(parts, sdk.TextPart{Text: m.Result.Text})
			}
			for _, call := range m.Calls {
				calls[call.CallID] = callInfo{provider: call.ProviderCallID, name: call.Name}
				open[call.CallID] = struct{}{}
				parts = append(parts, sdk.ToolCallPart{ToolCallID: call.ProviderCallID, ToolName: call.Name, Input: sdk.ToolArguments{JSON: call.Input.RawMessage()}})
			}
			if len(parts) > 0 {
				msgs = append(msgs, sdk.Message{Role: sdk.MessageRoleAssistant, Content: parts})
			}
		case chatlog.EntryToolResult:
			r := e.ToolResult
			if _, ok := open[r.CallID]; !ok {
				return nil, fmt.Errorf("contextprompt: tool result %s has no open call", r.CallID)
			}
			info := calls[r.CallID]
			part := sdk.ToolResultPart{ToolCallID: info.provider, ToolName: info.name}
			text := m.Text()
			switch r.Status {
			case chatlog.ToolSuccess:
				part.Result = sdk.TextOutput(text)
			case chatlog.ToolError:
				part.Result, part.IsError = sdk.TextOutput(text), true
			case chatlog.ToolUnknown:
				part.Result, part.IsError = sdk.TextOutput("tool outcome unknown: "+text), true
			}
			msgs = append(msgs, sdk.ToolMessage(part))
			delete(open, r.CallID)
			flushDeferred()
		case chatlog.EntrySummary:
			if len(open) > 0 {
				return nil, errors.New("contextprompt: summary follows unresolved tool calls")
			}
			flushDeferred()
			msgs = append(msgs, sdk.AssistantMessage(chatlog.PartsText(e.Summary.Parts)))
		}
	}
	if len(open) > 0 {
		return nil, errors.New("contextprompt: context has unresolved tool calls")
	}
	return msgs, nil
}

// DefaultCatalog is the reference agent's catalog: the context builder
// under V1.
func DefaultCatalog() *prompt.Catalog { return CatalogWith(nil) }

// CatalogWith is the catalog whose context builder carries preface.
func CatalogWith(preface Preface) *prompt.Catalog {
	factory := func(preset preset.AgentPreset, sources prompt.Sources) prompt.Builder {
		return &Builder{Sources: sources, Preset: preset, Preface: preface}
	}
	catalog, _ := prompt.NewCatalog(map[prompt.BuilderRef]prompt.BuilderFactory{V1: factory})
	return catalog
}

// WorkspacePreface tells the model which workspace the Session works in
// (APP-WSP-4): the binding projection's current Workspace, own or inherited,
// and nothing when the Session is bound to none.
func WorkspacePreface(ctx context.Context, sources prompt.Sources, sid session.SessionID) (string, error) {
	b, err := workspace.Read(ctx, sources.Projections, sid)
	if err != nil {
		return "", err
	}
	if !b.Bound {
		return "", nil
	}
	return fmt.Sprintf("You work in workspace %s. Shell commands and file tools run inside it; paths are relative to its root.", b.Workspace), nil
}
