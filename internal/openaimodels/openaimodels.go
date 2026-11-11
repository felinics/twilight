// Package openaimodels decodes the model list of an OpenAI-compatible
// GET /models endpoint, shared by the Chat Completions and Responses
// providers.
//
// The OpenAI API reports only id, object, created and owned_by. OpenRouter
// adds model metadata to the same items: name, description, context_length,
// architecture, top_provider, supported_parameters and reasoning. Model maps
// what an item carries onto sdk.Model and leaves the rest at its zero value,
// so an OpenAI item produces a model with only its ID.
package openaimodels

import (
	"encoding/json"
	"slices"

	"github.com/felinics/twilight/sdk"
)

// List is a GET /models response.
type List struct {
	Data []Object `json:"data"`
}

// Object is one item of a model list, or the GET /models/{id} response.
type Object struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`

	// Extension holds the OpenRouter fields. It is zero when the host sends
	// none, or sends them in a shape that does not decode.
	Extension Extension `json:"-"`
}

// Extension is the model metadata OpenRouter adds to a model list item.
type Extension struct {
	Name                string        `json:"name"`
	Description         string        `json:"description"`
	ContextLength       int           `json:"context_length"`
	Architecture        *Architecture `json:"architecture"`
	TopProvider         *TopProvider  `json:"top_provider"`
	SupportedParameters []string      `json:"supported_parameters"`
	Reasoning           *Reasoning    `json:"reasoning"`
}

// Architecture lists the modalities a model takes and returns: text, image,
// audio, video, file.
type Architecture struct {
	InputModalities  []string `json:"input_modalities"`
	OutputModalities []string `json:"output_modalities"`
}

// TopProvider describes the upstream provider OpenRouter routes to first.
type TopProvider struct {
	ContextLength       int `json:"context_length"`
	MaxCompletionTokens int `json:"max_completion_tokens"`
}

// Reasoning is OpenRouter's description of a model's reasoning controls.
type Reasoning struct {
	// Mandatory reports that reasoning cannot be turned off.
	Mandatory *bool `json:"mandatory"`
	// DefaultEnabled reports whether the model reasons when the request sets
	// no reasoning control.
	DefaultEnabled *bool `json:"default_enabled"`
	// SupportedEfforts lists the accepted reasoning effort values; "none"
	// turns reasoning off.
	SupportedEfforts []string `json:"supported_efforts"`
	DefaultEffort    string   `json:"default_effort"`
	// SupportsMaxTokens reports that a reasoning token budget is accepted.
	SupportsMaxTokens bool `json:"supports_max_tokens"`
}

// UnmarshalJSON decodes the OpenAI fields as before and the OpenRouter fields
// on a best-effort basis: an extension that does not decode is dropped rather
// than failing the whole list, so a compatible host whose extra fields differ
// in shape still lists its models.
func (o *Object) UnmarshalJSON(data []byte) error {
	type core Object
	var c core
	if err := json.Unmarshal(data, &c); err != nil {
		return err
	}
	*o = Object(c)
	var ext Extension
	if err := json.Unmarshal(data, &ext); err == nil {
		o.Extension = ext
	}
	return nil
}

// Models binds every item of the list to provider.
func (l *List) Models(provider sdk.Provider) []sdk.Model {
	models := make([]sdk.Model, 0, len(l.Data))
	for i := range l.Data {
		models = append(models, l.Data[i].Model(provider))
	}
	return models
}

// effortNone is the OpenRouter effort value that turns reasoning off.
const effortNone = "none"

// Model maps the item onto an sdk.Model bound to provider.
//
//   - DisplayName and Description come from name and description.
//   - ContextWindow is context_length (top_provider.context_length when it is
//     absent); OpenRouter reports the whole window, input and output together.
//   - MaxOutputTokens is top_provider.max_completion_tokens; null leaves it 0.
//   - Modalities come from architecture; "file" is ModalityPDF, which is what
//     OpenRouter accepts as a file input, and other values pass through.
//   - ToolCall is supported_parameters containing "tools".
//   - StructuredOutput is supported_parameters containing
//     "structured_outputs". "response_format" alone may mean JSON mode
//     without a schema, so it does not count.
//   - Reasoning is set when supported_parameters contains "reasoning" or
//     "include_reasoning", or the reasoning object states that the model
//     always reasons or names its effort levels.
func (o *Object) Model(provider sdk.Provider) sdk.Model {
	ext := &o.Extension
	model := sdk.Model{
		ID:               o.ID,
		DisplayName:      ext.Name,
		Provider:         provider,
		Description:      ext.Description,
		ContextWindow:    max(ext.ContextLength, 0),
		ToolCall:         slices.Contains(ext.SupportedParameters, "tools"),
		StructuredOutput: slices.Contains(ext.SupportedParameters, "structured_outputs"),
		Reasoning:        ext.reasoning(),
	}
	if top := ext.TopProvider; top != nil {
		if model.ContextWindow == 0 {
			model.ContextWindow = max(top.ContextLength, 0)
		}
		model.MaxOutputTokens = max(top.MaxCompletionTokens, 0)
	}
	if arch := ext.Architecture; arch != nil {
		model.Modalities = sdk.Modalities{
			Input:  modalities(arch.InputModalities),
			Output: modalities(arch.OutputModalities),
		}
	}
	return model
}

func modalities(values []string) []sdk.Modality {
	if len(values) == 0 {
		return nil
	}
	out := make([]sdk.Modality, 0, len(values))
	for _, value := range values {
		modality := sdk.Modality(value)
		if value == "file" {
			modality = sdk.ModalityPDF
		}
		if value != "" && !slices.Contains(out, modality) {
			out = append(out, modality)
		}
	}
	return out
}

// reasoning maps OpenRouter's reasoning description. Efforts are the stated
// levels without "none", weakest first, with Dialect tier; a stated budget is
// a ReasoningBudget without bounds. Off is rejected when reasoning is
// mandatory and accepted when it is not, or when "none" is an effort. The
// request toggle and the replay field are not stated by the list.
func (e *Extension) reasoning() *sdk.ModelReasoning {
	r := e.Reasoning
	stated := r != nil && ((r.Mandatory != nil && *r.Mandatory) || len(r.SupportedEfforts) > 0)
	if !stated && !slices.Contains(e.SupportedParameters, "reasoning") &&
		!slices.Contains(e.SupportedParameters, "include_reasoning") {
		return nil
	}
	reasoning := &sdk.ModelReasoning{}
	if r == nil {
		return reasoning
	}

	var offEffort bool
	var efforts []string
	for _, effort := range r.SupportedEfforts {
		switch {
		case effort == effortNone:
			offEffort = true
		case effort != "" && !slices.Contains(efforts, effort):
			efforts = append(efforts, effort)
		}
	}
	reasoning.Efforts = sortEfforts(efforts)
	if r.SupportsMaxTokens {
		reasoning.Budget = &sdk.ReasoningBudget{}
	}
	switch {
	case len(reasoning.Efforts) > 0:
		reasoning.Dialect = sdk.ReasoningDialectTier
	case reasoning.Budget != nil:
		reasoning.Dialect = sdk.ReasoningDialectBudget
	}

	switch {
	case r.Mandatory != nil && *r.Mandatory:
		reasoning.Off = sdk.ReasoningOffRejected
	case r.Mandatory != nil || offEffort:
		reasoning.Off = sdk.ReasoningOffAccepted
	}

	def := sdk.ReasoningDefault{Enabled: r.DefaultEnabled}
	if def.Enabled == nil && r.Mandatory != nil && *r.Mandatory {
		def.Enabled = boolPtr(true)
	}
	switch r.DefaultEffort {
	case "":
	case effortNone:
		if def.Enabled == nil {
			def.Enabled = boolPtr(false)
		}
	default:
		def.Effort = r.DefaultEffort
	}
	if def.Enabled != nil || def.Effort != "" {
		reasoning.Default = &def
	}
	return reasoning
}

// effortOrder ranks the sdk effort levels, weakest first.
var effortOrder = []string{
	sdk.ReasoningEffortMinimal,
	sdk.ReasoningEffortLow,
	sdk.ReasoningEffortMedium,
	sdk.ReasoningEffortHigh,
	sdk.ReasoningEffortXHigh,
	sdk.ReasoningEffortMax,
}

// sortEfforts orders the known levels weakest first and keeps any other value
// after them in the order given.
func sortEfforts(efforts []string) []string {
	if len(efforts) == 0 {
		return nil
	}
	sorted := make([]string, 0, len(efforts))
	for _, level := range effortOrder {
		if slices.Contains(efforts, level) {
			sorted = append(sorted, level)
		}
	}
	for _, effort := range efforts {
		if !slices.Contains(effortOrder, effort) {
			sorted = append(sorted, effort)
		}
	}
	return sorted
}

func boolPtr(v bool) *bool { return &v }
