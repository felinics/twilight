# Streaming

Twilight AI uses Go channels for streaming, giving you type-safe, idiomatic control over real-time LLM output.

## Basic Streaming

`Model.Stream` takes the same `sdk.Request` as `Model.Generate` and returns a `sdk.ModelStream`:

```go
stream, err := model.Stream(ctx, sdk.Request{
    Messages: []sdk.Message{
        sdk.UserMessage("Count from 1 to 10."),
    },
})
if err != nil {
    log.Fatal(err)
}

for part := range stream.Parts {
    switch p := part.(type) {
    case *sdk.TextDeltaPart:
        fmt.Print(p.Text)
    case *sdk.ErrorPart:
        log.Fatal(p.Error)
    }
}
```

## ModelStream

```go
type ModelStream struct {
    Parts  <-chan StreamPart          // closed when the stream ends
    Result func() (*ModelResult, error) // valid once Parts is drained
}
```

`Parts` yields the provider's parts as they arrive. `Result` assembles them into the same `ModelResult` that `Generate` returns: the streamed and the non-streamed path cannot disagree about a response, because both go through one assembler. Call it only after the `for range` loop exits.

`sdk.CollectStream(ctx, parts)` drains a channel of parts and returns the `ModelResult` in one call, for callers that do not need the parts themselves.

A stream is one model call. When the model answers with tool calls, the result carries them in `ToolCalls` (see [Tool Calling](tools.md)).

## StreamPart Types

Every chunk from the stream implements the `StreamPart` interface:

```go
type StreamPart interface {
    Type() StreamPartType
}
```

Use a type switch to handle specific parts. Here is the complete list:

### Text Parts

| Type | Fields | Description |
|------|--------|-------------|
| `*TextStartPart` | `ID` | Text generation has started |
| `*TextDeltaPart` | `ID`, `Text` | A chunk of generated text |
| `*TextEndPart` | `ID` | Text generation has ended |

### Reasoning Parts

For models that emit reasoning content (e.g. o1, DeepSeek-R1):

| Type | Fields | Description |
|------|--------|-------------|
| `*ReasoningStartPart` | `ID` | Reasoning started |
| `*ReasoningDeltaPart` | `ID`, `Text` | A chunk of reasoning text |
| `*ReasoningEndPart` | `ID` | Reasoning ended |

### Tool Input Parts

Streamed as the LLM constructs tool call arguments:

| Type | Fields | Description |
|------|--------|-------------|
| `*ToolInputStartPart` | `ID`, `ToolName` | LLM started building tool arguments |
| `*ToolInputDeltaPart` | `ID`, `Delta` | A chunk of tool argument JSON |
| `*ToolInputEndPart` | `ID` | Tool argument construction complete |

### Tool Call Parts

`*StreamToolCallPart` is emitted once a call's arguments are complete.

| Type | Fields | Description |
|------|--------|-------------|
| `*StreamToolCallPart` | `ToolCallID`, `ToolName`, `Input ToolArguments`, `ProviderMetadata` | Complete tool call; `Input.Valid()` is false when the model's arguments were not a JSON document |

### Source & File Parts

| Type | Fields | Description |
|------|--------|-------------|
| `*StreamSourcePart` | `Source` | A source reference (RAG) |
| `*StreamFilePart` | `File` | A generated file |

### Lifecycle Parts

| Type | Fields | Description |
|------|--------|-------------|
| `*StartPart` | — | Stream started |
| `*FinishPart` | `FinishReason`, `RawFinishReason`, `TotalUsage` | Stream finished |
| `*StartStepPart` | — | A new step started |
| `*FinishStepPart` | `FinishReason`, `RawFinishReason`, `Usage`, `Response` | Step finished |
| `*ErrorPart` | `Error` | An error occurred |

## Stream Lifecycle

One model call produces parts in this order:

```
StartPart
  StartStepPart
    TextStartPart
    TextDeltaPart (repeated)
    TextEndPart
  FinishStepPart
FinishPart
```

A call the model answers with a tool call ends with the call instead of text:

```
StartPart
  StartStepPart
    ToolInputStartPart
    ToolInputDeltaPart (repeated)
    ToolInputEndPart
    StreamToolCallPart
  FinishStepPart
FinishPart
```

The next model call is a new stream.

## Handling Reasoning Content

Models like o1 or DeepSeek-R1 emit reasoning before the final answer:

```go
for part := range stream.Parts {
    switch p := part.(type) {
    case *sdk.ReasoningDeltaPart:
        fmt.Fprintf(os.Stderr, "[thinking] %s", p.Text)
    case *sdk.TextDeltaPart:
        fmt.Print(p.Text)
    }
}
```

## Full Example: Stream, Then Run the Calls

```go
stream, err := model.Stream(ctx, sdk.Request{Messages: msgs, Tools: defs})
if err != nil {
    log.Fatal(err)
}

for part := range stream.Parts {
    switch p := part.(type) {
    case *sdk.StartPart:
        fmt.Println("--- Stream started ---")
    case *sdk.TextDeltaPart:
        fmt.Print(p.Text)
    case *sdk.ReasoningDeltaPart:
        fmt.Fprintf(os.Stderr, "%s", p.Text)
    case *sdk.StreamToolCallPart:
        fmt.Printf("\n🔧 %s(%s)\n", p.ToolName, p.Input.String())
    case *sdk.FinishPart:
        fmt.Printf("\n--- Done (reason: %s, tokens: %d) ---\n",
            p.FinishReason, p.TotalUsage.TotalTokens)
    case *sdk.ErrorPart:
        log.Printf("Error: %v", p.Error)
    }
}

result, err := stream.Result()
if err != nil {
    log.Fatal(err)
}

// The assembled result carries the same ToolCalls Generate would return.
for _, call := range result.ToolCalls {
    fmt.Printf("🔧 %s(%s)\n", call.ToolName, call.Input.String())
}
```

## Next Steps

- [Tool Calling](tools.md) — tool definitions, typed arguments and replaying a step
- [API Reference](api-reference.md) — complete type and function reference
