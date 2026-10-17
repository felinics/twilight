package wire

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/turn"
)

// The sealed fact and command variants of one schema version are registered
// once, in that version's variantRegistry. The wire discriminator, the
// decoder and the Go type of a variant come from the same entry, so adding a
// variant is one line: a discriminator that decodes cannot lack a name, and a
// named variant cannot lack a decoder. Each Codec holds its own
// registry, so a later version may rename or reshape a variant without
// touching how v1 decodes. FactTypes exposes the closed union of names to
// modules that register wire types.

type factVariant struct {
	name   string
	goType reflect.Type
	decode func([]byte) (turn.Fact, error)
}

type commandVariant struct {
	name   string
	goType reflect.Type
	decode func([]byte) (turn.AgentCommand, error)
}

func factOf[T turn.Fact](name string) factVariant {
	var zero T
	return factVariant{name: name, goType: reflect.TypeOf(zero), decode: func(raw []byte) (turn.Fact, error) {
		var f T
		err := jsonstable.DecodeStrict(raw, &f)
		return f, err
	}}
}

func commandOf[T turn.AgentCommand](name string) commandVariant {
	var zero T
	return commandVariant{name: name, goType: reflect.TypeOf(zero), decode: func(raw []byte) (turn.AgentCommand, error) {
		var c T
		err := jsonstable.DecodeStrict(raw, &c)
		return c, err
	}}
}

// factVariants is the closed list of facts in wire-registration order.
var factVariants = []factVariant{
	factOf[turn.TurnStarted]("turn_started"),
	factOf[turn.ModelStepPrepared]("model_step_prepared"),
	factOf[turn.ModelStepWithdrawn]("model_step_withdrawn"),
	factOf[turn.ModelStepStarted]("model_step_started"),
	factOf[turn.ModelStepRecovered]("model_step_recovered"),
	factOf[turn.ModelStepRejected]("model_step_rejected"),
	factOf[turn.ModelStepFailed]("model_step_failed"),
	factOf[turn.ModelStepCompleted]("model_step_completed"),
	factOf[turn.ToolStepOpened]("tool_step_opened"),
	factOf[turn.ToolCallStarted]("tool_call_started"),
	factOf[turn.ToolCallApproved]("tool_call_approved"),
	factOf[turn.ToolCallCompleted]("tool_call_completed"),
	factOf[turn.ToolCallAnswered]("tool_call_answered"),
	factOf[turn.ToolCallFailed]("tool_call_failed"),
	factOf[turn.InputAccepted]("input_accepted"),
	factOf[turn.TurnEnded]("turn_ended"),
}

// commandVariants is the closed list of commands.
var commandVariants = []commandVariant{
	commandOf[turn.PrepareModelRequest]("prepare_model_request"),
	commandOf[turn.WithdrawPreparedStep]("withdraw_prepared_step"),
	commandOf[turn.StartModelExecution]("start_model_execution"),
	commandOf[turn.RecoverModelExecution]("recover_model_execution"),
	commandOf[turn.SubmitModelResult]("submit_model_result"),
	commandOf[turn.SubmitModelFailure]("submit_model_failure"),
	commandOf[turn.RejectModelResult]("reject_model_result"),
	commandOf[turn.StartToolCall]("start_tool_call"),
	commandOf[turn.SubmitToolResult]("submit_tool_result"),
	commandOf[turn.SubmitToolFailure]("submit_tool_failure"),
	commandOf[turn.DeclineToolCall]("decline_tool_call"),
	commandOf[turn.ApproveToolCall]("approve_tool_call"),
	commandOf[turn.RejectToolCall]("reject_tool_call"),
	commandOf[turn.SubmitToolResponse]("submit_tool_response"),
	commandOf[turn.CancelTurn]("cancel_turn"),
	commandOf[turn.AcceptInput]("accept_input"),
}

// variantRegistry is one schema version's closed variant table.
type variantRegistry struct {
	facts         []factVariant
	factByName    map[string]factVariant
	factByType    map[reflect.Type]string
	commandByName map[string]commandVariant
	commandByType map[reflect.Type]string
}

func newVariantRegistry(facts []factVariant, commands []commandVariant) *variantRegistry {
	r := &variantRegistry{facts: facts, factByName: map[string]factVariant{}, factByType: map[reflect.Type]string{},
		commandByName: map[string]commandVariant{}, commandByType: map[reflect.Type]string{}}
	for _, v := range facts {
		if _, dup := r.factByName[v.name]; dup {
			panic("agent: duplicate fact variant " + v.name)
		}
		r.factByName[v.name] = v
		r.factByType[v.goType] = v.name
	}
	for _, v := range commands {
		if _, dup := r.commandByName[v.name]; dup {
			panic("agent: duplicate command variant " + v.name)
		}
		r.commandByName[v.name] = v
		r.commandByType[v.goType] = v.name
	}
	return r
}

// variants is the variant registry; Facts speaks through it.
var variants = newVariantRegistry(factVariants, commandVariants)

func (r *variantRegistry) factType(f turn.Fact) string {
	if f == nil {
		return ""
	}
	return r.factByType[reflect.TypeOf(f)]
}

func (r *variantRegistry) commandType(c turn.AgentCommand) string {
	if c == nil {
		return ""
	}
	return r.commandByType[reflect.TypeOf(c)]
}

func (r *variantRegistry) factTypes() []string {
	out := make([]string, len(r.facts))
	for i, v := range r.facts {
		out[i] = v.name
	}
	return out
}

func emptyBody(raw []byte) bool {
	return len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func (r *variantRegistry) decodeFact(typ string, raw []byte) (turn.Fact, error) {
	if emptyBody(raw) {
		return nil, fmt.Errorf("agent: codec: fact %q has empty body", typ)
	}
	v, ok := r.factByName[typ]
	if !ok {
		return nil, fmt.Errorf("agent: codec: unknown fact type %q", typ)
	}
	return v.decode(raw)
}

func (r *variantRegistry) decodeCommand(typ string, raw []byte) (turn.AgentCommand, error) {
	if emptyBody(raw) {
		return nil, fmt.Errorf("agent: codec: command %q has empty body", typ)
	}
	v, ok := r.commandByName[typ]
	if !ok {
		return nil, fmt.Errorf("agent: codec: unknown command type %q", typ)
	}
	return v.decode(raw)
}

// allVariants lists every schema version's registry, oldest first. FactTypes
// is their union; a later version appends its own registry here.
var allVariants = []*variantRegistry{variants}

// FactTypes lists every fact discriminator any schema version registers, in
// registration order and without duplicates: the closed set of
// twilight/turn/ wire names a Session module registers.
func FactTypes() []string {
	var out []string
	seen := map[string]bool{}
	for _, r := range allVariants {
		for _, name := range r.factTypes() {
			if !seen[name] {
				seen[name] = true
				out = append(out, name)
			}
		}
	}
	return out
}

// FactType returns the local event name of a fact (the part of the EventType
// after twilight/turn/): the first schema version that knows the variant
// names it. Callers with a Turn's schema in hand use Schema.Wire.FactType.
func FactType(f turn.Fact) string {
	for _, r := range allVariants {
		if name := r.factType(f); name != "" {
			return name
		}
	}
	return ""
}
