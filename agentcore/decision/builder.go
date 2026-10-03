package decision

import (
	"context"

	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/schema"
)

// BuilderRef names a Builder in the catalog; it is part of the preset
// digest.
type BuilderRef = preset.PromptBuilderRef

// Builder assembles the next model input from the surrounding
// conversation, which it reads by the Input's Scope. The Prompt it returns
// is already the frozen ModelRequest the Run persists; a builder that
// composes its request in a provider SDK's types converts before returning.
type Builder interface {
	Build(context.Context, Input) (Prompt, error)
}

// Prompt is one built model input: the model to call, the frozen request
// (messages and tool definitions), the inputs it consumed, the freshness
// token of the context it was built from, and the frozen tool specs.
type Prompt struct {
	Model    run.ModelRef
	Request  model.ModelRequest
	InputIDs []run.InputID
	Token    run.PromptToken
	Tools    []run.ToolSpec
}

// Input is what the execution hands the Builder: the Run boundary facts
// only. Conversation content (previous assistant output, tool results) is
// read from the Session by the Builder itself.
type Input struct {
	Scope      run.Scope // filled by the caller; planning does not know it
	RunID      run.RunID
	SourceStep run.StepID
	Inputs     []run.AgentInput
}

// ToolSpecs splits the preset's tool contracts into what each side of the
// boundary needs: the run.ToolSpec the Run persists (definition digest,
// policy, replay and placement declarations) and the definitions the model
// request carries, in contract order.
func ToolSpecs(tools []preset.ToolContract) ([]run.ToolSpec, []model.ToolDefinition, error) {
	specs := make([]run.ToolSpec, 0, len(tools))
	defs := make([]model.ToolDefinition, 0, len(tools))
	for _, t := range tools {
		d, err := schema.Canonical().DigestToolDefinition(t.Definition)
		if err != nil {
			return nil, nil, err
		}
		specs = append(specs, run.ToolSpec{Ref: t.Ref, Name: t.Definition.Name, DefinitionDigest: d, Policy: t.Policy, Replay: t.Replay, Placement: t.Placement})
		defs = append(defs, t.Definition)
	}
	return specs, defs, nil
}
