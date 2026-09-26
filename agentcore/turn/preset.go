package turn

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/sdk"
)

type (
	// PromptBuilderRef names the decision component that builds the model
	// prompt for a Turn (DEC-PMT). It is part of the preset digest.
	PromptBuilderRef string
)

// PublicTool is one tool of an AgentPreset: its ref, frozen definition and
// response policy. ToolSpecs and the provider-facing tool list both derive
// from it.
type PublicTool struct {
	Ref        run.ToolRef          `json:"ref"`
	Definition model.ToolDefinition `json:"definition"`
	Policy     run.ResponsePolicy   `json:"policy"`
	// Replay is the tool's declared replay policy (RUN-EXE-9); it enters
	// the preset digest and the frozen ToolSpec. Omitted when unknown.
	Replay run.ReplayPolicy `json:"replay,omitempty"`
	// Placement is the tool's declared placement (RUN-LOP-9); it enters the
	// preset digest and the frozen ToolSpec. Omitted for process placement.
	Placement run.ToolPlacement `json:"placement,omitempty"`
}

// AgentPreset is the decision identity a Turn is started under (TRN-SCP-6):
// every input to the decision layer that must be the same when another
// process resumes the Turn. The Session records PresetRef{ID, Digest};
// credentials, clients and tool implementations never enter it.
type AgentPreset struct {
	SchemaVersion uint16       `json:"schemaVersion"`
	Model         run.ModelRef `json:"model"`
	Tools         []PublicTool `json:"tools,omitempty"`
	Streaming     bool         `json:"streaming,omitempty"`
	// Prompt names the decision component resolved on the Owner side.
	Prompt PromptBuilderRef `json:"prompt"`
	// Scheduling is how the tool calls of one step run: parallel (default) or
	// sequential, with an optional bound on concurrent workers. It is frozen
	// onto each ToolStep (RUN-MCH).
	Scheduling run.ToolScheduling `json:"scheduling,omitempty"`
	// MalformedRetries is how many times one model step is retried after a
	// malformed result before the Run fails; zero fails on the first.
	MalformedRetries uint8 `json:"malformedRetries,omitempty"`
	// SystemPrompt is the conversation instruction frozen by the preset digest.
	SystemPrompt string `json:"systemPrompt,omitempty"`
}

// PresetDigestDomain is the digest domain of DigestPreset.
const PresetDigestDomain = "twilight/turn/preset"

// DigestPreset covers the fields that change what the decision layer does
// for a Turn: SchemaVersion, Model, Tools, Streaming, Prompt, Scheduling and
// MalformedRetries and SystemPrompt (TRN-PST-1).
func DigestPreset(p *AgentPreset) (es.Digest, error) {
	body := struct {
		SchemaVersion    uint16             `json:"schemaVersion"`
		Model            run.ModelRef       `json:"model"`
		Tools            []PublicTool       `json:"tools,omitempty"`
		Streaming        bool               `json:"streaming,omitempty"`
		Prompt           PromptBuilderRef   `json:"prompt"`
		Scheduling       run.ToolScheduling `json:"scheduling,omitempty"`
		MalformedRetries uint8              `json:"malformedRetries,omitempty"`
		SystemPrompt     string             `json:"systemPrompt,omitempty"`
	}{p.SchemaVersion, p.Model, p.Tools, p.Streaming, p.Prompt, p.Scheduling, p.MalformedRetries, p.SystemPrompt}
	raw, err := es.EncodeTypedPayload(1, PresetDigestDomain, body)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(raw), nil
}

// ValidatePreset checks the identity fields a registry must refuse to record
// without (TRN-PST-2): schema version, model and prompt builder, plus a
// well-formed Scheduling.
func ValidatePreset(p *AgentPreset) error {
	switch {
	case p.SchemaVersion == 0:
		return errors.New("turn: preset requires schemaVersion")
	case p.Model == "":
		return errors.New("turn: preset requires a model")
	case p.Prompt == "":
		return errors.New("turn: preset requires a prompt builder ref")
	}
	if m := p.Scheduling.Mode; m != "" && m != run.ToolScheduleParallel && m != run.ToolScheduleSequential {
		return fmt.Errorf("turn: preset scheduling mode %q is unknown", m)
	}
	if p.Scheduling.MaxParallel < 0 {
		return errors.New("turn: preset scheduling MaxParallel is negative")
	}
	return nil
}

// ToolSpecs derives the frozen ToolSpecs and provider definitions of p, in
// order (DEC-PMT-4).
func (p *AgentPreset) ToolSpecs() ([]run.ToolSpec, []sdk.ToolDefinition, error) {
	specs := make([]run.ToolSpec, 0, len(p.Tools))
	defs := make([]sdk.ToolDefinition, 0, len(p.Tools))
	for _, t := range p.Tools {
		d, err := schema.Canonical().DigestToolDefinition(t.Definition)
		if err != nil {
			return nil, nil, err
		}
		specs = append(specs, run.ToolSpec{Ref: t.Ref, Name: t.Definition.Name, DefinitionDigest: d, Policy: t.Policy, Replay: t.Replay, Placement: t.Placement})
		def_, err := sdkconv.ToolDefinition(t.Definition)
		if err != nil {
			return nil, nil, err
		}
		defs = append(defs, def_)
	}
	return specs, defs, nil
}
