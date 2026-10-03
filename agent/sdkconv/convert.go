package sdkconv

import (
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

func freezeRawJSON(raw json.RawMessage) (jsonstable.Value, error) {
	return jsonstable.Parse(raw)
}

// FreezeProviderMetadata copies the SDK's string tokens into the persisted
// map; nil stays nil.
func FreezeProviderMetadata(meta sdk.ProviderMetadata) model.ProviderMetadata {
	return model.ProviderMetadata(meta.Clone())
}

// ProviderMetadata copies the persisted tokens back into an SDK map; nil
// stays nil.
func ProviderMetadata(m model.ProviderMetadata) sdk.ProviderMetadata {
	return sdk.ProviderMetadata(m).Clone()
}

// FreezeToolArguments freezes the arguments of a tool call. A JSON document
// is canonicalized; invalid argument text is kept verbatim, except that text
// which is not valid UTF-8 cannot be persisted as the model wrote it (JSON
// would rewrite it) and is rejected.
func FreezeToolArguments(a sdk.ToolArguments) (model.ToolArguments, error) {
	if !a.Valid() {
		if !utf8.ValidString(a.Text) {
			return model.ToolArguments{}, fmt.Errorf("tool arguments are not valid UTF-8")
		}
		return model.ToolArguments{Text: a.Text}, nil
	}
	doc, err := freezeRawJSON(a.Object())
	if err != nil {
		return model.ToolArguments{}, err
	}
	return model.ToolArguments{JSON: doc}, nil
}

// ToolArguments converts persisted arguments back to the SDK value.
func ToolArguments(a model.ToolArguments) sdk.ToolArguments {
	if !a.Valid() {
		return sdk.ToolArguments{Text: a.Text}
	}
	return sdk.ToolArguments{JSON: a.Canonical().RawMessage()}
}

// FreezeToolOutput freezes what a tool returned: a JSON document is
// canonicalized, text is kept verbatim and must be valid UTF-8.
func FreezeToolOutput(o sdk.ToolOutput) (model.ToolOutput, error) {
	if o.IsJSON() {
		doc, err := freezeRawJSON(o.JSON)
		if err != nil {
			return model.ToolOutput{}, err
		}
		return model.ToolOutput{JSON: doc}, nil
	}
	if !utf8.ValidString(o.Text) {
		return model.ToolOutput{}, fmt.Errorf("tool output is not valid UTF-8")
	}
	return model.ToolOutput{Text: o.Text}, nil
}

// ToolOutput converts a persisted tool output back to the SDK value.
func ToolOutput(o model.ToolOutput) sdk.ToolOutput {
	if !o.JSON.IsZero() {
		// The persisted document is already canonical (jsonstable.Value), so
		// the literal carries it as is rather than re-encoding it.
		return sdk.ToolOutput{JSON: o.JSON.RawMessage()}
	}
	return sdk.TextOutput(o.Text)
}

func FreezeCacheControl(c *sdk.CacheControl) *model.CacheControl {
	if c == nil {
		return nil
	}
	return &model.CacheControl{Type: c.Type, TTL: c.TTL}
}

func CacheControl(c *model.CacheControl) *sdk.CacheControl {
	if c == nil {
		return nil
	}
	return &sdk.CacheControl{Type: c.Type, TTL: c.TTL}
}

// FreezeToolDefinition renders the definition's schema as canonical JSON; a
// definition without a schema freezes with zero Parameters.
func FreezeToolDefinition(def sdk.ToolDefinition) (model.ToolDefinition, error) {
	var params jsonstable.Value
	if def.Parameters != nil {
		raw, err := json.Marshal(def.Parameters)
		if err != nil {
			return model.ToolDefinition{}, fmt.Errorf("tool definition parameters: %w", err)
		}
		params, err = freezeRawJSON(raw)
		if err != nil {
			return model.ToolDefinition{}, fmt.Errorf("tool definition parameters: %w", err)
		}
	}
	return model.ToolDefinition{
		Name:         def.Name,
		Description:  def.Description,
		Parameters:   params,
		CacheControl: FreezeCacheControl(def.CacheControl),
	}, nil
}

// ToolDefinition decodes the persisted schema back into the SDK's schema
// type; a persisted schema always decodes, since it was encoded from one.
func ToolDefinition(d model.ToolDefinition) (sdk.ToolDefinition, error) {
	var schema *jsonschema.Schema
	if !d.Parameters.IsZero() {
		schema = new(jsonschema.Schema)
		if err := json.Unmarshal(d.Parameters.Bytes(), schema); err != nil {
			return sdk.ToolDefinition{}, fmt.Errorf("tool definition %q parameters: %w", d.Name, err)
		}
	}
	return sdk.ToolDefinition{
		Name:         d.Name,
		Description:  d.Description,
		Parameters:   schema,
		CacheControl: CacheControl(d.CacheControl),
	}, nil
}

func FreezeResponseFormat(f *sdk.ResponseFormat) (*model.ResponseFormat, error) {
	if f == nil {
		return nil, nil
	}
	out := &model.ResponseFormat{Type: model.ResponseFormatType(f.Type)}
	if f.JSONSchema != nil {
		raw, err := json.Marshal(f.JSONSchema)
		if err != nil {
			return nil, err
		}
		out.JSONSchema, err = freezeRawJSON(raw)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func ResponseFormat(f *model.ResponseFormat) (*sdk.ResponseFormat, error) {
	if f == nil {
		return nil, nil
	}
	out := &sdk.ResponseFormat{Type: sdk.ResponseFormatType(f.Type)}
	if !f.JSONSchema.IsZero() {
		var schema jsonschema.Schema
		if err := json.Unmarshal(f.JSONSchema.Bytes(), &schema); err != nil {
			return nil, err
		}
		out.JSONSchema = &schema
	}
	return out, nil
}

func FreezeToolChoice(choice sdk.ToolChoice) model.ToolChoice {
	return model.ToolChoice{Mode: model.ToolChoiceMode(choice.Mode), Tool: choice.Tool}
}

func ToolChoice(c model.ToolChoice) sdk.ToolChoice {
	return sdk.ToolChoice{Mode: sdk.ToolChoiceMode(c.Mode), Tool: c.Tool}
}

func FreezeMessagePart(p sdk.MessagePart) (model.MessagePart, error) {
	switch part := p.(type) {
	case sdk.TextPart:
		return model.MessagePart{Type: model.MessagePartTypeText, Text: part.Text, CacheControl: FreezeCacheControl(part.CacheControl), ProviderMetadata: FreezeProviderMetadata(part.ProviderMetadata)}, nil
	case *sdk.TextPart:
		if part == nil {
			return model.MessagePart{}, fmt.Errorf("nil *sdk.TextPart")
		}
		return FreezeMessagePart(*part)
	case sdk.ReasoningPart:
		return model.MessagePart{Type: model.MessagePartTypeReasoning, ID: part.ID, Text: part.Text, Format: model.ReasoningFormat(part.Format), Model: part.Model, ProviderMetadata: FreezeProviderMetadata(part.ProviderMetadata)}, nil
	case *sdk.ReasoningPart:
		if part == nil {
			return model.MessagePart{}, fmt.Errorf("nil *sdk.ReasoningPart")
		}
		return FreezeMessagePart(*part)
	case sdk.ImagePart:
		return model.MessagePart{Type: model.MessagePartTypeImage, Image: part.Image, MediaType: part.MediaType, CacheControl: FreezeCacheControl(part.CacheControl)}, nil
	case *sdk.ImagePart:
		if part == nil {
			return model.MessagePart{}, fmt.Errorf("nil *sdk.ImagePart")
		}
		return FreezeMessagePart(*part)
	case sdk.FilePart:
		return model.MessagePart{Type: model.MessagePartTypeFile, Data: part.Data, MediaType: part.MediaType, Filename: part.Filename, CacheControl: FreezeCacheControl(part.CacheControl)}, nil
	case *sdk.FilePart:
		if part == nil {
			return model.MessagePart{}, fmt.Errorf("nil *sdk.FilePart")
		}
		return FreezeMessagePart(*part)
	case sdk.ToolCallPart:
		input, err := FreezeToolArguments(part.Input)
		if err != nil {
			return model.MessagePart{}, err
		}
		return model.MessagePart{Type: model.MessagePartTypeToolCall, ToolCallID: part.ToolCallID, ToolName: part.ToolName, Input: input, CacheControl: FreezeCacheControl(part.CacheControl), ProviderMetadata: FreezeProviderMetadata(part.ProviderMetadata)}, nil
	case *sdk.ToolCallPart:
		if part == nil {
			return model.MessagePart{}, fmt.Errorf("nil *sdk.ToolCallPart")
		}
		return FreezeMessagePart(*part)
	case sdk.ToolResultPart:
		result, err := FreezeToolOutput(part.Result)
		if err != nil {
			return model.MessagePart{}, err
		}
		return model.MessagePart{Type: model.MessagePartTypeToolResult, ToolCallID: part.ToolCallID, ToolName: part.ToolName, Result: result, IsError: part.IsError, CacheControl: FreezeCacheControl(part.CacheControl)}, nil
	case *sdk.ToolResultPart:
		if part == nil {
			return model.MessagePart{}, fmt.Errorf("nil *sdk.ToolResultPart")
		}
		return FreezeMessagePart(*part)
	default:
		return model.MessagePart{}, fmt.Errorf("unsupported sdk.MessagePart %T", p)
	}
}

//nolint:gocritic // hugeParam: MessagePart is an agent-owned value DTO converted back to SDK at the boundary.
func MessagePart(p model.MessagePart) (sdk.MessagePart, error) {
	switch p.Type {
	case model.MessagePartTypeText:
		return sdk.TextPart{Text: p.Text, CacheControl: CacheControl(p.CacheControl), ProviderMetadata: ProviderMetadata(p.ProviderMetadata)}, nil
	case model.MessagePartTypeReasoning:
		return sdk.ReasoningPart{ID: p.ID, Text: p.Text, Format: sdk.ReasoningFormat(p.Format), Model: p.Model, ProviderMetadata: ProviderMetadata(p.ProviderMetadata)}, nil
	case model.MessagePartTypeImage:
		return sdk.ImagePart{Image: p.Image, MediaType: p.MediaType, CacheControl: CacheControl(p.CacheControl)}, nil
	case model.MessagePartTypeFile:
		return sdk.FilePart{Data: p.Data, MediaType: p.MediaType, Filename: p.Filename, CacheControl: CacheControl(p.CacheControl)}, nil
	case model.MessagePartTypeToolCall:
		return sdk.ToolCallPart{ToolCallID: p.ToolCallID, ToolName: p.ToolName, Input: ToolArguments(p.Input), CacheControl: CacheControl(p.CacheControl), ProviderMetadata: ProviderMetadata(p.ProviderMetadata)}, nil
	case model.MessagePartTypeToolResult:
		return sdk.ToolResultPart{ToolCallID: p.ToolCallID, ToolName: p.ToolName, Result: ToolOutput(p.Result), IsError: p.IsError, CacheControl: CacheControl(p.CacheControl)}, nil
	default:
		return nil, fmt.Errorf("unknown message part type %q", p.Type)
	}
}

func FreezeMessage(m sdk.Message) (model.Message, error) {
	parts := make([]model.MessagePart, len(m.Content))
	for i, p := range m.Content {
		frozen, err := FreezeMessagePart(p)
		if err != nil {
			return model.Message{}, fmt.Errorf("message part %d: %w", i, err)
		}
		parts[i] = frozen
	}
	var usage *model.Usage
	if m.Usage != nil {
		u := FreezeUsage(*m.Usage)
		usage = &u
	}
	return model.Message{Role: model.MessageRole(m.Role), Content: parts, Usage: usage}, nil
}

func Message(m model.Message) (sdk.Message, error) {
	parts := make([]sdk.MessagePart, len(m.Content))
	for i := range m.Content {
		part, err := MessagePart(m.Content[i])
		if err != nil {
			return sdk.Message{}, fmt.Errorf("message part %d: %w", i, err)
		}
		parts[i] = part
	}
	var usage *sdk.Usage
	if m.Usage != nil {
		u := Usage(*m.Usage)
		usage = &u
	}
	return sdk.Message{Role: sdk.MessageRole(m.Role), Content: parts, Usage: usage}, nil
}

//nolint:gocritic // hugeParam: freezes a caller-owned SDK Request value into an agent-owned protocol value.
func FreezeModelRequest(req sdk.Request) (model.ModelRequest, error) {
	messages := make([]model.Message, len(req.Messages))
	for i, m := range req.Messages {
		msg, err := FreezeMessage(m)
		if err != nil {
			return model.ModelRequest{}, fmt.Errorf("message %d: %w", i, err)
		}
		messages[i] = msg
	}
	tools := make([]model.ToolDefinition, len(req.Tools))
	for i, t := range req.Tools {
		tool, err := FreezeToolDefinition(t)
		if err != nil {
			return model.ModelRequest{}, fmt.Errorf("tool %d: %w", i, err)
		}
		tools[i] = tool
	}
	format, err := FreezeResponseFormat(req.ResponseFormat)
	if err != nil {
		return model.ModelRequest{}, fmt.Errorf("response format: %w", err)
	}
	options := make(map[string]jsonstable.Value, len(req.ProviderOptions))
	if req.ProviderOptions != nil {
		for k, v := range req.ProviderOptions {
			frozen, err := freezeRawJSON(v)
			if err != nil {
				return model.ModelRequest{}, fmt.Errorf("provider option %q: %w", k, err)
			}
			options[k] = frozen
		}
	} else {
		options = nil
	}
	return model.ModelRequest{
		Model:            req.Model,
		System:           req.System,
		Messages:         messages,
		Tools:            tools,
		ToolChoice:       FreezeToolChoice(req.ToolChoice),
		ResponseFormat:   format,
		Temperature:      clonePtr(req.Temperature),
		TopP:             clonePtr(req.TopP),
		MaxTokens:        clonePtr(req.MaxTokens),
		StopSequences:    append([]string(nil), req.StopSequences...),
		FrequencyPenalty: clonePtr(req.FrequencyPenalty),
		PresencePenalty:  clonePtr(req.PresencePenalty),
		Seed:             clonePtr(req.Seed),
		ReasoningEffort:  clonePtr(req.ReasoningEffort),
		ReasoningSummary: clonePtr(req.ReasoningSummary),
		PromptCacheKey:   clonePtr(req.PromptCacheKey),
		ProviderOptions:  options,
	}, nil
}

//nolint:gocritic // hugeParam: ModelRequest converts the persisted value DTO into a detached SDK Request.
func ModelRequest(r model.ModelRequest) (sdk.Request, error) {
	messages := make([]sdk.Message, len(r.Messages))
	for i, m := range r.Messages {
		msg, err := Message(m)
		if err != nil {
			return sdk.Request{}, fmt.Errorf("message %d: %w", i, err)
		}
		messages[i] = msg
	}
	tools := make([]sdk.ToolDefinition, len(r.Tools))
	for i, t := range r.Tools {
		tool, err := ToolDefinition(t)
		if err != nil {
			return sdk.Request{}, fmt.Errorf("tool %d: %w", i, err)
		}
		tools[i] = tool
	}
	format, err := ResponseFormat(r.ResponseFormat)
	if err != nil {
		return sdk.Request{}, fmt.Errorf("response format: %w", err)
	}
	options := make(map[string]json.RawMessage, len(r.ProviderOptions))
	if r.ProviderOptions != nil {
		for k, v := range r.ProviderOptions {
			options[k] = v.RawMessage()
		}
	} else {
		options = nil
	}
	return sdk.Request{
		Model:            r.Model,
		System:           r.System,
		Messages:         messages,
		Tools:            tools,
		ToolChoice:       ToolChoice(r.ToolChoice),
		ResponseFormat:   format,
		Temperature:      clonePtr(r.Temperature),
		TopP:             clonePtr(r.TopP),
		MaxTokens:        clonePtr(r.MaxTokens),
		StopSequences:    append([]string(nil), r.StopSequences...),
		FrequencyPenalty: clonePtr(r.FrequencyPenalty),
		PresencePenalty:  clonePtr(r.PresencePenalty),
		Seed:             clonePtr(r.Seed),
		ReasoningEffort:  clonePtr(r.ReasoningEffort),
		ReasoningSummary: clonePtr(r.ReasoningSummary),
		PromptCacheKey:   clonePtr(r.PromptCacheKey),
		ProviderOptions:  options,
	}, nil
}

//nolint:gocritic // hugeParam: SDK Usage is copied into an agent-owned Usage value.
func FreezeUsage(u sdk.Usage) model.Usage {
	return model.Usage{
		InputTokens:       u.InputTokens,
		OutputTokens:      u.OutputTokens,
		TotalTokens:       u.TotalTokens,
		ReasoningTokens:   u.ReasoningTokens,
		CachedInputTokens: u.CachedInputTokens,
		InputTokenDetails: model.InputTokenDetail{
			NoCacheTokens:      u.InputTokenDetails.NoCacheTokens,
			CacheReadTokens:    u.InputTokenDetails.CacheReadTokens,
			CacheWriteTokens:   u.InputTokenDetails.CacheWriteTokens,
			CacheWrite5mTokens: u.InputTokenDetails.CacheWrite5mTokens,
			CacheWrite1hTokens: u.InputTokenDetails.CacheWrite1hTokens,
		},
		OutputTokenDetails: model.OutputTokenDetail{
			TextTokens:      u.OutputTokenDetails.TextTokens,
			ReasoningTokens: u.OutputTokenDetails.ReasoningTokens,
		},
	}
}

//nolint:gocritic // hugeParam: Usage conversion is pure and returns a detached SDK value.
func Usage(u model.Usage) sdk.Usage {
	return sdk.Usage{
		InputTokens:       u.InputTokens,
		OutputTokens:      u.OutputTokens,
		TotalTokens:       u.TotalTokens,
		ReasoningTokens:   u.ReasoningTokens,
		CachedInputTokens: u.CachedInputTokens,
		InputTokenDetails: sdk.InputTokenDetail{
			NoCacheTokens:      u.InputTokenDetails.NoCacheTokens,
			CacheReadTokens:    u.InputTokenDetails.CacheReadTokens,
			CacheWriteTokens:   u.InputTokenDetails.CacheWriteTokens,
			CacheWrite5mTokens: u.InputTokenDetails.CacheWrite5mTokens,
			CacheWrite1hTokens: u.InputTokenDetails.CacheWrite1hTokens,
		},
		OutputTokenDetails: sdk.OutputTokenDetail{
			TextTokens:      u.OutputTokenDetails.TextTokens,
			ReasoningTokens: u.OutputTokenDetails.ReasoningTokens,
		},
	}
}

func FreezeReasoningPart(p sdk.ReasoningPart) model.ReasoningPart {
	return model.ReasoningPart{ID: p.ID, Text: p.Text, Format: model.ReasoningFormat(p.Format), Model: p.Model, ProviderMetadata: FreezeProviderMetadata(p.ProviderMetadata)}
}

func ReasoningPart(p model.ReasoningPart) sdk.ReasoningPart {
	return sdk.ReasoningPart{ID: p.ID, Text: p.Text, Format: sdk.ReasoningFormat(p.Format), Model: p.Model, ProviderMetadata: ProviderMetadata(p.ProviderMetadata)}
}

func FreezeSource(s sdk.Source) model.Source {
	return model.Source{SourceType: s.SourceType, ID: s.ID, URL: s.URL, Title: s.Title, ProviderMetadata: FreezeProviderMetadata(s.ProviderMetadata)}
}

func Source(s model.Source) sdk.Source {
	return sdk.Source{SourceType: s.SourceType, ID: s.ID, URL: s.URL, Title: s.Title, ProviderMetadata: ProviderMetadata(s.ProviderMetadata)}
}

func FreezeGeneratedFile(f sdk.GeneratedFile) model.GeneratedFile {
	return model.GeneratedFile{Data: f.Data, MediaType: f.MediaType}
}

func GeneratedFile(f model.GeneratedFile) sdk.GeneratedFile {
	return sdk.GeneratedFile{Data: f.Data, MediaType: f.MediaType}
}

func FreezeModelToolCall(c sdk.ToolCall) (model.ModelToolCall, error) {
	input, err := FreezeToolArguments(c.Input)
	if err != nil {
		return model.ModelToolCall{}, fmt.Errorf("tool call input: %w", err)
	}
	return model.ModelToolCall{ToolCallID: c.ToolCallID, ToolName: c.ToolName, Input: input, ProviderMetadata: FreezeProviderMetadata(c.ProviderMetadata)}, nil
}

func ModelToolCall(c model.ModelToolCall) sdk.ToolCall {
	return sdk.ToolCall{ToolCallID: c.ToolCallID, ToolName: c.ToolName, Input: ToolArguments(c.Input), ProviderMetadata: ProviderMetadata(c.ProviderMetadata)}
}

//nolint:gocritic // hugeParam: ResponseMetadata is copied into the persisted mirror value.
func FreezeResponseMetadata(r sdk.ResponseMetadata) model.ResponseMetadata {
	out := model.ResponseMetadata{ID: r.ID, ModelID: r.ModelID}
	if !r.Timestamp.IsZero() {
		out.Timestamp = r.Timestamp.UTC().Format(time.RFC3339Nano)
	}
	if r.Headers != nil {
		out.Headers = make(map[string]string, len(r.Headers))
		for k, v := range r.Headers {
			out.Headers[k] = v
		}
	}
	return out
}

//nolint:gocritic // hugeParam: the persisted mirror value is converted into a detached SDK value.
func ResponseMetadata(r model.ResponseMetadata) (sdk.ResponseMetadata, error) {
	out := sdk.ResponseMetadata{ID: r.ID, ModelID: r.ModelID}
	if r.Timestamp != "" {
		t, err := time.Parse(time.RFC3339Nano, r.Timestamp)
		if err != nil {
			return sdk.ResponseMetadata{}, err
		}
		out.Timestamp = t
	}
	if r.Headers != nil {
		out.Headers = make(map[string]string, len(r.Headers))
		for k, v := range r.Headers {
			out.Headers[k] = v
		}
	}
	return out, nil
}

//nolint:gocritic // hugeParam: freezes a caller-owned SDK ModelResult value into an agent-owned protocol value.
func FreezeModelResult(r sdk.ModelResult) (model.ModelResult, error) {
	reasoning := make([]model.ReasoningPart, len(r.ReasoningParts))
	for i, p := range r.ReasoningParts {
		reasoning[i] = FreezeReasoningPart(p)
	}
	sources := make([]model.Source, len(r.Sources))
	for i, s := range r.Sources {
		sources[i] = FreezeSource(s)
	}
	files := make([]model.GeneratedFile, len(r.Files))
	for i, f := range r.Files {
		files[i] = FreezeGeneratedFile(f)
	}
	calls := make([]model.ModelToolCall, len(r.ToolCalls))
	for i, c := range r.ToolCalls {
		call, err := FreezeModelToolCall(c)
		if err != nil {
			return model.ModelResult{}, fmt.Errorf("tool call %d: %w", i, err)
		}
		calls[i] = call
	}
	return model.ModelResult{
		Text:                 r.Text,
		Reasoning:            r.Reasoning,
		ReasoningParts:       reasoning,
		TextProviderMetadata: FreezeProviderMetadata(r.TextProviderMetadata),
		FinishReason:         model.FinishReason(r.FinishReason),
		RawFinishReason:      r.RawFinishReason,
		Usage:                FreezeUsage(r.Usage),
		Sources:              sources,
		Files:                files,
		ToolCalls:            calls,
		Response:             FreezeResponseMetadata(r.Response),
	}, nil
}

//nolint:gocritic // hugeParam: ModelResult converts the persisted value DTO into a detached SDK result.
func ModelResult(r model.ModelResult) (sdk.ModelResult, error) {
	reasoning := make([]sdk.ReasoningPart, len(r.ReasoningParts))
	for i, p := range r.ReasoningParts {
		reasoning[i] = ReasoningPart(p)
	}
	sources := make([]sdk.Source, len(r.Sources))
	for i, s := range r.Sources {
		sources[i] = Source(s)
	}
	files := make([]sdk.GeneratedFile, len(r.Files))
	for i, f := range r.Files {
		files[i] = GeneratedFile(f)
	}
	calls := make([]sdk.ToolCall, len(r.ToolCalls))
	for i, c := range r.ToolCalls {
		calls[i] = ModelToolCall(c)
	}
	response, err := ResponseMetadata(r.Response)
	if err != nil {
		return sdk.ModelResult{}, fmt.Errorf("response metadata: %w", err)
	}
	return sdk.ModelResult{
		Text:                 r.Text,
		Reasoning:            r.Reasoning,
		ReasoningParts:       reasoning,
		TextProviderMetadata: ProviderMetadata(r.TextProviderMetadata),
		FinishReason:         sdk.FinishReason(r.FinishReason),
		RawFinishReason:      r.RawFinishReason,
		Usage:                Usage(r.Usage),
		Sources:              sources,
		Files:                files,
		ToolCalls:            calls,
		Response:             response,
	}, nil
}
