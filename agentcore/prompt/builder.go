package prompt

import (
	"context"

	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/model"
	"github.com/felinics/twilight/agentcore/turn/schema"
)

// BuilderRef names a Builder in the catalog; it is part of the preset
// digest.
type BuilderRef = preset.PromptBuilderRef

// Builder assembles the next model input from the surrounding
// conversation, which it reads by the Input's Scope. The Prompt it returns
// is already the frozen ModelRequest the Turn persists; a builder that
// composes its request in a provider SDK's types converts before returning.
type Builder interface {
	Build(context.Context, Input) (Prompt, error)
}

// Prompt is one built model input: the model to call, the frozen request
// (messages and tool definitions), the inputs it consumed, the freshness
// token of the context it was built from, and the frozen tool specs.
type Prompt struct {
	Model    turn.ModelRef
	Request  model.ModelRequest
	InputIDs []turn.InputID
	Token    turn.PromptToken
	Tools    []turn.ToolSpec
}

// Input is what the execution hands the Builder: the Turn boundary facts
// only. Conversation content (previous assistant output, tool results) is
// read from the Session by the Builder itself.
type Input struct {
	Scope      turn.Scope // filled by the caller; planning does not know it
	TurnID     turn.TurnID
	SourceStep turn.StepID
	Inputs     []turn.AgentInput
}

// ToolSpecs splits the preset's tool contracts into what each side of the
// boundary needs: the turn.ToolSpec the Turn persists (definition digest,
// policy, replay and placement declarations) and the definitions the model
// request carries, in contract order.
func ToolSpecs(tools []preset.ToolContract) ([]turn.ToolSpec, []model.ToolDefinition, error) {
	specs := make([]turn.ToolSpec, 0, len(tools))
	defs := make([]model.ToolDefinition, 0, len(tools))
	for _, t := range tools {
		d, err := schema.Canonical().DigestToolDefinition(t.Definition)
		if err != nil {
			return nil, nil, err
		}
		specs = append(specs, turn.ToolSpec{Ref: t.Ref, Name: t.Definition.Name, DefinitionDigest: d, Policy: t.Policy, Replay: t.Replay, Placement: t.Placement})
		defs = append(defs, t.Definition)
	}
	return specs, defs, nil
}
