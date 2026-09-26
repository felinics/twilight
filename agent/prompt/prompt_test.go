package prompt_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/felinics/twilight/agent/input"
	"github.com/felinics/twilight/agent/prompt"
	"github.com/felinics/twilight/agentcore/decision"
	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/turn"
)

// fixedSource serves one Context state at one head, the way an Owner or
// an observer would for the same stream position.
type fixedSource struct {
	state chatlog.Context
	head  session.Head
}

func (s fixedSource) Load(_ context.Context, _ session.SessionID, id extension.ProjectionID, _ extension.ProjectionVersion) (any, session.Head, error) {
	if id != chatlog.ContextProjectionID {
		return nil, session.Head{}, errors.New("unexpected projection")
	}
	return s.state, s.head, nil
}

// fixedContent is the frozen store as the prompt builder sees it: bodies by
// digest.
type fixedContent struct {
	results map[es.Digest]model.ModelResult
	outputs map[es.Digest]run.CanonicalJSON
}

func (c fixedContent) ModelResult(_ context.Context, d es.Digest) (model.ModelResult, error) {
	r, ok := c.results[d]
	if !ok {
		return model.ModelResult{}, fmt.Errorf("%w: %s", frozen.ErrMissing, d)
	}
	return r, nil
}

func (c fixedContent) ToolOutput(_ context.Context, d es.Digest) (run.CanonicalJSON, error) {
	o, ok := c.outputs[d]
	if !ok {
		return run.CanonicalJSON{}, fmt.Errorf("%w: %s", frozen.ErrMissing, d)
	}
	return o, nil
}

func (c fixedContent) ToolResponse(ctx context.Context, d es.Digest) (run.CanonicalJSON, error) {
	return c.ToolOutput(ctx, d)
}

func sources(state chatlog.Context, head session.Head, content fixedContent) decision.Sources {
	return decision.Sources{Projections: fixedSource{state: state, head: head}, Content: content}
}

func preset() turn.AgentPreset {
	return turn.AgentPreset{SchemaVersion: 1, Model: "m-1", Prompt: prompt.PromptContextV1, SystemPrompt: "be brief"}
}

func entries() (chatlog.Context, fixedContent) {
	in := chatlog.Input{ID: "in-1", TurnID: "t1", Content: input.Text("hello")}
	as := chatlog.Assistant{ID: "a-1", TurnID: "t1", StepID: "a-1", ResultDigest: "sha256:r1"}
	content := fixedContent{results: map[es.Digest]model.ModelResult{"sha256:r1": {Text: "hi", FinishReason: model.FinishReasonStop}}}
	return chatlog.Context{Entries: []chatlog.Entry{
		{Kind: chatlog.EntryInput, ID: "in-1", Position: session.Position{Commit: 1}, Input: &in},
		{Kind: chatlog.EntryAssistant, ID: "a-1", Position: session.Position{Commit: 2}, Assistant: &as},
	}}, content
}

// DEC-CAT-2 / DEC-PMT-1: two authorities resolving the same AgentPreset
// against the same projection state and frozen bodies build the same prompt;
// the registry refuses refs it does not hold.
func TestPromptBuildersResolveDeterministically(t *testing.T) {
	state, content := entries()
	src := sources(state, session.Head{Next: 3}, content)
	input := plan.PromptInput{Scope: "s", Inputs: []run.AgentInput{{ID: "in-1", Digest: "sha256:in-1"}}}
	var prompts []loop.Prompt
	for i := 0; i < 2; i++ {
		builders := prompt.DefaultPromptBuilders() // a fresh process builds its own registry
		builder, err := builders.Resolve(preset(), src)
		if err != nil {
			t.Fatal(err)
		}
		prompt, err := builder.Build(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		prompts = append(prompts, prompt)
	}
	if !reflect.DeepEqual(prompts[0], prompts[1]) {
		t.Fatalf("prompts differ across processes:\n%+v\n%+v", prompts[0], prompts[1])
	}
	if prompts[0].Token != "3" || len(prompts[0].Request.Messages) != 3 || prompts[0].Model != "m-1" {
		t.Fatalf("prompt = %+v", prompts[0])
	}

	p := preset()
	p.Prompt = "x/builder"
	if _, err := prompt.DefaultPromptBuilders().Resolve(p, src); !errors.Is(err, decision.ErrUnknownPromptBuilder) {
		t.Fatalf("unknown builder: err = %v, want %v", err, decision.ErrUnknownPromptBuilder)
	}
	var none *decision.PromptBuilders
	if _, err := none.Resolve(preset(), src); err == nil {
		t.Fatal("nil registry resolved")
	}
	// A body the frozen store lost fails the build; the projection itself is
	// unaffected (CHT-MAT-1).
	if _, err := prompt.NewContextPromptBuilder(preset(), sources(state, session.Head{}, fixedContent{})).Build(context.Background(), input); !errors.Is(err, frozen.ErrMissing) {
		t.Fatalf("missing body: err = %v", err)
	}
}

// DEC-CAT-1: registration rejects empty refs, nil factories and duplicates.
func TestPromptBuilderRegistration(t *testing.T) {
	builders, err := decision.NewPromptBuilders(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := builders.Register("", prompt.NewContextPromptBuilder); err == nil {
		t.Fatal("empty builder ref accepted")
	}
	if err := builders.Register("p", nil); err == nil {
		t.Fatal("nil factory accepted")
	}
	if err := builders.Register("p", prompt.NewContextPromptBuilder); err != nil {
		t.Fatal(err)
	}
	if err := builders.Register("p", prompt.NewContextPromptBuilder); err == nil {
		t.Fatal("duplicate builder accepted")
	}
}

// DEC-INP-1: the v1 input shape round-trips.
func TestInputContentRoundTrip(t *testing.T) {
	for _, text := range []string{"hello", "", `quote " and \ slash`, "多字节"} {
		got, err := input.TextOf(input.Text(text))
		if err != nil || got != text {
			t.Fatalf("round trip %q: got %q err %v", text, got, err)
		}
	}
}

func TestPromptRejectsUnpairedToolHistory(t *testing.T) {
	content := fixedContent{results: map[es.Digest]model.ModelResult{
		"sha256:call": {FinishReason: model.FinishReasonToolCalls, ToolCalls: []model.ModelToolCall{{ToolCallID: "provider-call", ToolName: "tool", Input: model.ToolArguments{JSON: run.MustParseCanonicalJSON(`{}`)}}}},
		"sha256:none": {Text: "plain", FinishReason: model.FinishReasonStop},
	}}
	call := chatlog.Entry{Kind: chatlog.EntryAssistant, ID: "s1", Assistant: &chatlog.Assistant{ID: "s1", StepID: "s1", ResultDigest: "sha256:call", CallIDs: []chatlog.CallID{"call"}}}
	plain := chatlog.Entry{Kind: chatlog.EntryAssistant, ID: "s2", Assistant: &chatlog.Assistant{ID: "s2", StepID: "s2", ResultDigest: "sha256:none"}}
	result := chatlog.Entry{Kind: chatlog.EntryToolResult, ID: "call", ToolResult: &chatlog.ToolResult{ID: "call", CallID: "call", Status: chatlog.ToolError, Failure: &run.ToolFailure{Class: "tool_error"}}}
	input := chatlog.Entry{Kind: chatlog.EntryInput, Input: &chatlog.Input{Content: input.Text("next")}}
	summary := chatlog.Entry{Kind: chatlog.EntrySummary, Summary: &chatlog.Summary{Parts: chatlog.Parts{chatlog.TextPart{Text: "so far"}}}}
	for _, tc := range []struct {
		name    string
		entries []chatlog.Entry
	}{
		{"unfinished call", []chatlog.Entry{call, input}},
		{"orphan result", []chatlog.Entry{result}},
		{"duplicate result", []chatlog.Entry{call, result, result}},
		{"interleaved assistant", []chatlog.Entry{call, plain, result}},
		{"interleaved summary", []chatlog.Entry{call, summary, result}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := prompt.NewContextPromptBuilder(preset(), sources(chatlog.Context{Entries: tc.entries}, session.Head{}, content))
			if _, err := builder.Build(context.Background(), plan.PromptInput{Scope: "s"}); err == nil {
				t.Fatal("unpaired history produced a provider request")
			}
		})
	}
	builder := prompt.NewContextPromptBuilder(preset(), sources(chatlog.Context{Entries: []chatlog.Entry{call, input, result}}, session.Head{}, content))
	prompt, err := builder.Build(context.Background(), plan.PromptInput{Scope: "s"})
	if err != nil {
		t.Fatal(err)
	}
	msgs := prompt.Request.Messages
	if len(msgs) != 4 || msgs[2].Role != "tool" || msgs[3].Role != "user" {
		t.Fatalf("paired context = %+v", msgs)
	}
}
