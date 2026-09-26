# Tool Calling

Tool calling in Twilight AI: you describe tools on the `sdk.Request`, the model answers with typed `ToolCall`s, and you put the results back into the next request. A tool that lives behind another protocol, such as an MCP server, is described to the model the same way: turn its input schema into a `*jsonschema.Schema` and put the definition on the request.

## Defining a Tool

A tool the model can see is a `ToolDefinition`: a name, a description and a JSON Schema for its arguments.

```go
type ToolDefinition struct {
    Name         string
    Description  string
    Parameters   *jsonschema.Schema
    CacheControl *CacheControl // optional prompt caching of the definition (Anthropic)
}
```

### Inferring the schema from a Go struct

```go
type WeatherParams struct {
    City  string `json:"city" jsonschema:"City name"`
    Units string `json:"units,omitempty" jsonschema:"celsius or fahrenheit"`
}

weather, err := sdk.NewToolDefinition[WeatherParams]("get_weather", "Get current weather for a city")
```

`NewToolDefinition[T]` infers the schema of `T` with `github.com/google/jsonschema-go`; struct tags name the properties and describe them.

### Writing the schema directly

```go
weather := sdk.ToolDefinition{
    Name:        "get_weather",
    Description: "Get current weather for a city",
    Parameters: &jsonschema.Schema{
        Type: "object",
        Properties: map[string]*jsonschema.Schema{
            "city": {Type: "string", Description: "City name"},
        },
        Required: []string{"city"},
    },
}
```

## Arguments and outputs

The model's arguments arrive as `sdk.ToolArguments`, and a tool result goes back as `sdk.ToolOutput`:

```go
type ToolArguments struct {
    JSON json.RawMessage // the arguments as a JSON document; nil when the model's text was not one
    Text string          // the model's argument text when it is not a JSON document
}

type ToolOutput struct {
    Text string          // plain text the model reads
    JSON json.RawMessage // or a JSON document
}
```

`args.Unmarshal(&v)` decodes the document into your type; it returns `ErrInvalidToolArguments` when the model's text was not a JSON document. `sdk.TextOutput(s)`, `sdk.JSONOutput(v)` and `sdk.RawJSONOutput(raw)` build outputs.

The JSON the SDK keeps is canonical in the sense of RFC 8785 (JCS): `ParseToolArguments`, `ToolArgumentsJSON`, `JSONOutput` and `RawJSONOutput` re-encode the document with object members sorted, no insignificant whitespace and numbers in their IEEE-754 binary64 form (`sdk.CanonicalJSON` is the rule; `sdk.CanonicalProviderOptions` applies it to `Request.ProviderOptions`). Two spellings of the same document give the same bytes in every language that implements RFC 8785, and `json.Marshal` of a `Request` built this way is deterministic, so a digest over it needs no further normalization. Numbers have binary64 semantics: `2.0` becomes `2`, and an identifier that must stay exact belongs in a JSON string, not a number. An object with a repeated member is not accepted: `ParseToolArguments` keeps such text as invalid arguments and `RawJSONOutput` returns `ErrInvalidJSON`.

A model sometimes emits arguments that are not valid JSON (a truncated call, for example). Such a call is still reported, with the text in `Text` and `Valid()` false, so you can answer it with an error result and let the model correct itself instead of running the tool on guessed arguments. When such a call is replayed in a later request, every provider sends its arguments as `{}` (`Object()`); the text the model produced reaches it only through the error result you answered with, so a backend that parses historical arguments never sees a document it cannot parse.

## One Model Call

Describe the tools on the request. The reply carries the calls the model wants:

```go
result, err := model.Generate(ctx, sdk.Request{
    Messages: []sdk.Message{sdk.UserMessage("What's the weather in Tokyo?")},
    Tools:    []sdk.ToolDefinition{weather},
})
if err != nil {
    log.Fatal(err)
}
for _, call := range result.ToolCalls {
    fmt.Println(call.ToolCallID, call.ToolName, call.Input.String())
}
```

`result.FinishReason` is `FinishReasonToolCalls` when the model stopped to call tools.

## Replaying a Step

After running the calls, append two messages to the conversation: the assistant message that made the calls, and a tool message with the results. Providers need the assistant message to carry the model's reasoning parts (in order, including empty-text redacted blocks) and the `ProviderMetadata` of each part, or the next request is rejected.

```go
func stepMessages(r sdk.ModelResult, results []sdk.ToolResultPart) []sdk.Message {
    var parts []sdk.MessagePart
    for _, rp := range r.ReasoningParts {
        parts = append(parts, rp)
    }
    if r.Text != "" {
        parts = append(parts, sdk.TextPart{Text: r.Text, ProviderMetadata: r.TextProviderMetadata})
    }
    for _, c := range r.ToolCalls {
        parts = append(parts, sdk.ToolCallPart{ToolCallID: c.ToolCallID, ToolName: c.ToolName, Input: c.Input, ProviderMetadata: c.ProviderMetadata})
    }
    return []sdk.Message{{Role: sdk.MessageRoleAssistant, Content: parts}, sdk.ToolMessage(results...)}
}
```

A result is one `ToolResultPart` per call, matched by `ToolCallID`; set `IsError` when the tool failed or the arguments were invalid, and put the reason in `Result` so the model reads it.

```go
messages := []sdk.Message{sdk.UserMessage("What's the weather in Tokyo?")}
for step := 0; step < 8; step++ {
    result, err := model.Generate(ctx, sdk.Request{Messages: messages, Tools: defs})
    if err != nil {
        return err
    }
    if len(result.ToolCalls) == 0 {
        return handle(result.Text)
    }
    results := runTools(ctx, result.ToolCalls)
    messages = append(messages, stepMessages(result, results)...)
}
```

## Tool Choice

`Request.ToolChoice` steers whether the model may, must, or must not call a tool:

```go
sdk.ToolChoice{Mode: sdk.ToolChoiceAuto}                       // default: the model decides
sdk.ToolChoice{Mode: sdk.ToolChoiceRequired}                   // at least one call
sdk.ToolChoice{Mode: sdk.ToolChoiceNone}                       // text only
sdk.ToolChoice{Mode: sdk.ToolChoiceTool, Tool: "get_weather"}  // this tool
```

## Streaming with Tools

`Model.Stream` reports a call as a `*StreamToolCallPart` once its arguments are complete, after the `ToolInputStart` / `ToolInputDelta` / `ToolInputEnd` parts that carried the argument text. The assembled `ModelResult` from `stream.Result()` carries the same `ToolCalls` as `Generate` would. See [Streaming](streaming.md).

## Next Steps

- [Streaming](streaming.md) — `Model.Stream` and the `StreamPart` types
- [Providers](providers.md) — provider-specific tool behaviour, prompt caching of tool definitions
- [API Reference](api-reference.md) — complete type and function reference
