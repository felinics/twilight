// Package prompt is the first-party prompt builder of the reference agent: it
// folds the chatlog context projection into one sdk.Request (DEC-PMT). The
// agent core only knows the PromptBuilder seam and the catalog that resolves a
// PromptBuilderRef (agentcore/decision); which builder a preset names, and
// how it assembles context, is this agent's strategy.
package prompt

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/decision"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"

	"github.com/felinics/twilight/agent/input"
	"github.com/felinics/twilight/agent/workspace"
)

// PromptContextV1 names the context prompt builder: the chatlog context projection
// folded into one provider request (DEC-PMT-1).
const PromptContextV1 turn.PromptBuilderRef = "twilight/decision/prompt/context-v1"

// ContextPromptBuilder is the context-v1 PromptBuilder (DEC-PMT): it reads the
// chatlog context projection, materializes its entries and assembles the next
// sdk.Request. Every assistant and tool_result of the Session is in the fold
// already, including those of earlier attempts of the same Turn (DEC-PMT-6).
type ContextPromptBuilder struct {
	Sources decision.Sources
	Preset  turn.AgentPreset
	// InputText extracts the user text of one input payload; nil selects the
	// v1 shape {"text": ...} (DEC-INP-1).
	InputText func(run.CanonicalJSON) (string, error)
	// Preface, when set, contributes text the builder appends to the system
	// prompt of every request, read from the Session's projections: the
	// application's standing facts the model must know, such as the
	// workspace it works in (APP-WSP-4). An empty string adds nothing.
	Preface Preface
}

// Preface reads the application's standing context of a Session.
type Preface func(ctx context.Context, sources decision.Sources, sid session.SessionID) (string, error)

// NewContextPromptBuilder is the PromptBuilderFactory of PromptContextV1.
func NewContextPromptBuilder(preset turn.AgentPreset, sources decision.Sources) loop.PromptBuilder {
	return &ContextPromptBuilder{Sources: sources, Preset: preset}
}

func (p *ContextPromptBuilder) Build(ctx context.Context, hint plan.PromptInput) (loop.Prompt, error) {
	if p.Sources.Projections == nil || p.Preset.Model == "" {
		return loop.Prompt{}, errors.New("decision: builder requires projections and a model")
	}
	if hint.Scope == "" {
		return loop.Prompt{}, errors.New("decision: builder hint has no session")
	}
	state, head, err := p.Sources.Projections.Load(ctx, session.SessionID(hint.Scope), chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
	if err != nil {
		return loop.Prompt{}, err
	}
	cctx, ok := state.(chatlog.Context)
	if !ok {
		return loop.Prompt{}, fmt.Errorf("decision: context projection is %T", state)
	}
	entries, err := chatlog.NewMaterializer(p.Sources.Content).Entries(ctx, cctx.Entries)
	if err != nil {
		return loop.Prompt{}, err
	}
	system := p.Preset.SystemPrompt
	if p.Preface != nil {
		preface, err := p.Preface(ctx, p.Sources, session.SessionID(hint.Scope))
		if err != nil {
			return loop.Prompt{}, err
		}
		if preface != "" {
			if system != "" {
				system += "\n\n"
			}
			system += preface
		}
	}
	msgs, err := p.messages(system, entries)
	if err != nil {
		return loop.Prompt{}, err
	}
	specs, defs, err := p.Preset.ToolSpecs()
	if err != nil {
		return loop.Prompt{}, err
	}
	ids := make([]run.InputID, 0, len(hint.Inputs))
	for _, in := range hint.Inputs {
		ids = append(ids, in.ID)
	}
	return loop.Prompt{
		Model:    p.Preset.Model,
		Request:  sdk.Request{Model: string(p.Preset.Model), Messages: msgs, Tools: defs},
		InputIDs: ids,
		Token:    run.PromptToken(fmt.Sprintf("%d", head.Next)),
		Tools:    specs,
	}, nil
}

// messages is DEC-PMT-2.
func (p *ContextPromptBuilder) messages(system string, entries []chatlog.Materialized) ([]sdk.Message, error) {
	var msgs []sdk.Message
	if system != "" {
		msgs = append(msgs, sdk.SystemMessage(system))
	}
	inputText := p.InputText
	if inputText == nil {
		inputText = input.TextOf
	}
	// ProviderCallID and tool name per CallID, from the assistant that issued
	// the call, for pairing tool results (DEC-PMT-2 step 2).
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
				return nil, errors.New("decision: assistant follows unresolved tool calls")
			}
			flushDeferred()
			if m.Result == nil {
				return nil, fmt.Errorf("decision: assistant %s is not materialized", e.ID)
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
				return nil, fmt.Errorf("decision: tool result %s has no open call", r.CallID)
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
				return nil, errors.New("decision: summary follows unresolved tool calls")
			}
			flushDeferred()
			msgs = append(msgs, sdk.AssistantMessage(chatlog.PartsText(e.Summary.Parts)))
		}
	}
	if len(open) > 0 {
		return nil, errors.New("decision: context has unresolved tool calls")
	}
	return msgs, nil
}

// DefaultPromptBuilders is the reference agent's catalog: the context builder
// under PromptContextV1.
func DefaultPromptBuilders() *decision.PromptBuilders { return PromptBuildersWith(nil) }

// PromptBuildersWith is the catalog whose context builder carries preface.
func PromptBuildersWith(preface Preface) *decision.PromptBuilders {
	factory := func(preset turn.AgentPreset, sources decision.Sources) loop.PromptBuilder {
		return &ContextPromptBuilder{Sources: sources, Preset: preset, Preface: preface}
	}
	builders, _ := decision.NewPromptBuilders(map[turn.PromptBuilderRef]decision.PromptBuilderFactory{PromptContextV1: factory})
	return builders
}

// WorkspacePreface tells the model which workspace the Session works in
// (APP-WSP-4): the binding projection's current Workspace, own or inherited,
// and nothing when the Session is bound to none.
func WorkspacePreface(ctx context.Context, sources decision.Sources, sid session.SessionID) (string, error) {
	b, err := workspace.Read(ctx, sources.Projections, sid)
	if err != nil {
		return "", err
	}
	if !b.Bound {
		return "", nil
	}
	return fmt.Sprintf("You work in workspace %s. Shell commands and file tools run inside it; paths are relative to its root.", b.Workspace), nil
}
