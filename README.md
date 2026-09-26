# Twilight AI

A lightweight, idiomatic AI SDK for Go — inspired by [Vercel AI SDK](https://sdk.vercel.ai/).

[![Go Reference](https://pkg.go.dev/badge/github.com/felinics/twilight.svg)](https://pkg.go.dev/github.com/felinics/twilight)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## Features

- **One call, one result** — `Model.Generate` and `Model.Stream` take an `sdk.Request` and return a `ModelResult` or a stream of typed parts. `Embed`, `EmbedMany`, `GenerateImage`, `EditImage`, `GenerateVideo`, `GenerateSpeech` and `StreamSpeech` cover the other modalities
- **Provider-agnostic** — swap between OpenAI, Anthropic, Google, GitHub Copilot, Edge TTS, or any OpenAI-compatible endpoint
- **Model discovery** — `ListModels` fetches available models, `Test` checks provider connectivity and model support
- **Tool calling** — describe tools with `ToolDefinition` (or infer the schema from a Go struct with `NewToolDefinition[T]`); the model's calls come back as typed `ToolCall`s with `ToolArguments`
- **Streaming** — first-class channel-based streaming with fine-grained `StreamPart` types
- **Rich message types** — text, images, files, reasoning content, tool calls/results
- **Embeddings** — generate embeddings with `Embed` / `EmbedMany`, supports OpenAI and Google providers
- **Image generation** — generate and edit images with `GenerateImage` / `EditImage`, supports OpenAI (dall-e, gpt-image) and Alibaba Cloud DashScope (Qwen-Image, Wan) models
- **Video generation** — create, poll, and download video jobs with OpenRouter and Ark/ModelArk providers
- **Speech synthesis** — generate speech with `GenerateSpeech` / `StreamSpeech`, supports Edge TTS with an open provider model

## Installation

```bash
go get github.com/felinics/twilight
```

Requires **Go 1.25+**.

## Quick Start

### Generate Text (Chat Completions API)

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/felinics/twilight/provider/openai/completions"
    "github.com/felinics/twilight/sdk"
)

func main() {
    provider := completions.New(
        completions.WithAPIKey("sk-..."),
    )
    model := provider.ChatModel("gpt-4o-mini")

    result, err := model.Generate(context.Background(), sdk.Request{
        Messages: []sdk.Message{
            sdk.UserMessage("Explain Go channels in 3 sentences."),
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(result.Text)
}
```

### Generate Text (Responses API)

```go
import "github.com/felinics/twilight/provider/openai/responses"

provider := responses.New(
    responses.WithAPIKey("sk-..."),
)
model := provider.ChatModel("gpt-4o-mini")

result, err := model.Generate(context.Background(), sdk.Request{
    Messages: []sdk.Message{
        sdk.UserMessage("Explain Go channels in 3 sentences."),
    },
})
fmt.Println(result.Text)
```

The Responses API is OpenAI's newer API with first-class support for reasoning models (o3, o4-mini), URL citation annotations, and a flat input format. See [Providers](docs/providers.md) for details.

### OpenCode Go

```go
import opencodego "github.com/felinics/twilight/provider/opencode/go"

provider := opencodego.New(
    opencodego.WithAPIKey("your-opencode-go-key"),
    opencodego.WithHeaders(map[string]string{"User-Agent": "my-agent/1.0"}),
)
ctx := sdk.WithRequestHeaders(context.Background(), map[string]string{
    opencodego.SessionHeader: conversationID, // stable across turns and tool calls
})
result, err := provider.ChatModel("glm-5.2").Generate(ctx, sdk.Request{
    Messages: []sdk.Message{sdk.UserMessage("Explain this code")},
})
if err != nil {
    log.Fatal(err)
}
fmt.Println(result.Text)
```

Models route to Completions, Responses or Messages using an explicit catalog.
See [OpenCode Go](docs/providers.md#opencode-go-provider) for model discovery,
route overrides and session handling.

### Anthropic

```go
import "github.com/felinics/twilight/provider/anthropic/messages"

provider := messages.New(
    messages.WithAPIKey("sk-ant-..."),
)
model := provider.ChatModel("claude-sonnet-4-20250514")

maxTokens := 1024
result, err := model.Generate(context.Background(), sdk.Request{
    MaxTokens: &maxTokens,
    Messages: []sdk.Message{
        sdk.UserMessage("Explain Go channels in 3 sentences."),
    },
})
fmt.Println(result.Text)
```

For extended thinking (reasoning), configure the provider with `WithThinking`:

```go
provider := messages.New(
    messages.WithAPIKey("sk-ant-..."),
    messages.WithThinking(messages.ThinkingConfig{
        Type:         "enabled",
        BudgetTokens: 4000,
    }),
)
```

### Google Gemini

```go
import "github.com/felinics/twilight/provider/google/generativeai"

provider := generativeai.New(
    generativeai.WithAPIKey("AIza..."),
)
model := provider.ChatModel("gemini-2.5-flash")

result, err := model.Generate(context.Background(), sdk.Request{
    Messages: []sdk.Message{
        sdk.UserMessage("Explain Go channels in 3 sentences."),
    },
})
fmt.Println(result.Text)
```

### GitHub Copilot Agent

```go
import "github.com/felinics/twilight/provider/github/copilot"

provider := copilot.New(
    // Use the inbound X-GitHub-Token value from your Copilot agent request.
    copilot.WithGitHubToken("ghu_..."),
)
model := provider.ChatModel(copilot.AutoModel)

result, err := model.Generate(context.Background(), sdk.Request{
    Messages: []sdk.Message{
        sdk.UserMessage("Explain Go channels in 3 sentences."),
    },
})
fmt.Println(result.Text)
```

This provider targets GitHub Copilot agent / extension runtimes that can call `api.githubcopilot.com/chat/completions`. GitHub currently does not expose a public Copilot models discovery endpoint, so `copilot.AutoModel` tells the provider to let GitHub choose the backing model instead of inventing an undocumented model ID.

### Stream Text

```go
stream, err := model.Stream(ctx, sdk.Request{
    Messages: []sdk.Message{
        sdk.UserMessage("Write a haiku about concurrency."),
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

// Once Parts is drained, the assembled result is available: text, usage,
// finish reason and any tool calls, identical to what Generate returns.
result, err := stream.Result()
```

### Tool Calling

Describe the tool with a Go struct — the SDK infers the JSON Schema. The model asks for the call; the caller runs it and replays the step:

```go
type WeatherParams struct {
    City string `json:"city" jsonschema:"City name"`
}

weather, err := sdk.NewToolDefinition[WeatherParams]("get_weather", "Get current weather for a city")
if err != nil {
    log.Fatal(err)
}

messages := []sdk.Message{sdk.UserMessage("What's the weather in Tokyo?")}
for {
    result, err := model.Generate(ctx, sdk.Request{Messages: messages, Tools: []sdk.ToolDefinition{weather}})
    if err != nil {
        log.Fatal(err)
    }
    if len(result.ToolCalls) == 0 {
        fmt.Println(result.Text)
        break
    }
    var assistant []sdk.MessagePart
    for _, rp := range result.ReasoningParts {
        assistant = append(assistant, rp) // reasoning first, with the provider's tokens
    }
    if result.Text != "" {
        assistant = append(assistant, sdk.TextPart{Text: result.Text, ProviderMetadata: result.TextProviderMetadata})
    }
    var results []sdk.ToolResultPart
    for _, call := range result.ToolCalls {
        assistant = append(assistant, sdk.ToolCallPart{ToolCallID: call.ToolCallID, ToolName: call.ToolName, Input: call.Input, ProviderMetadata: call.ProviderMetadata})
        var params WeatherParams
        if err := call.Input.Unmarshal(&params); err != nil { // not a JSON document: tell the model
            results = append(results, sdk.ToolResultPart{ToolCallID: call.ToolCallID, ToolName: call.ToolName, Result: sdk.TextOutput(err.Error()), IsError: true})
            continue
        }
        out, _ := sdk.JSONOutput(map[string]any{"city": params.City, "temp": "22°C"})
        results = append(results, sdk.ToolResultPart{ToolCallID: call.ToolCallID, ToolName: call.ToolName, Result: out})
    }
    messages = append(messages, sdk.Message{Role: sdk.MessageRoleAssistant, Content: assistant}, sdk.ToolMessage(results...))
}
```

Each iteration is one model call. See [Tool Calling](docs/tools.md).

### Image Generation

Generate images from text prompts using OpenAI's image models:

```go
import "github.com/felinics/twilight/provider/openai/images"

provider := images.New(images.WithAPIKey("sk-..."))
model := provider.GenerationModel("gpt-image-1")

result, err := sdk.GenerateImage(ctx,
    sdk.WithImageGenerationModel(model),
    sdk.WithImagePrompt("A sunset over mountains, oil painting style"),
    sdk.WithImageSize("1024x1024"),
)
// result.Data[0].B64JSON contains the base64-encoded image
```

Edit existing images with inpainting or extensions:

```go
model := provider.EditModel("gpt-image-1")

result, err := sdk.EditImage(ctx,
    sdk.WithImageEditModel(model),
    sdk.WithEditPrompt("Add a rainbow in the sky"),
    sdk.WithEditImages(sdk.ImageInput{
        Data:     pngBytes,
        Filename: "photo.png",
    }),
)
```

Alibaba Cloud Model Studio (DashScope) image models work through the same API:

```go
import "github.com/felinics/twilight/provider/alibabacloud/images"

provider := images.New(images.WithAPIKey("sk-..."))
model := provider.GenerationModel("qwen-image-max")

result, err := sdk.GenerateImage(ctx,
    sdk.WithImageGenerationModel(model),
    sdk.WithImagePrompt("A sunset over mountains, oil painting style"),
    sdk.WithImageSize("1024x1024"),
)
// result.Data[0].URL contains the generated image URL
```

The DashScope provider routes Qwen-Image and Wan models to the right endpoint automatically and transparently polls async generation tasks. See [Images](docs/images.md) for details.

### Embeddings

Generate vector embeddings for text using OpenAI or Google:

```go
import "github.com/felinics/twilight/provider/openai/embedding"

provider := embedding.New(embedding.WithAPIKey("sk-..."))
model := provider.EmbeddingModel("text-embedding-3-small")

// Single value
vec, err := sdk.Embed(ctx, "Hello world", sdk.WithEmbeddingModel(model))
// vec is []float64

// Multiple values
result, err := sdk.EmbedMany(ctx, []string{"Hello", "World"},
    sdk.WithEmbeddingModel(model),
    sdk.WithDimensions(256),
)
// result.Embeddings is [][]float64
// result.Usage.Tokens reports token consumption
```

Google Gemini embeddings:

```go
import "github.com/felinics/twilight/provider/google/embedding"

provider := embedding.New(
    embedding.WithAPIKey("AIza..."),
    embedding.WithTaskType("RETRIEVAL_DOCUMENT"),
)
model := provider.EmbeddingModel("gemini-embedding-001")

vec, err := sdk.Embed(ctx, "Hello world", sdk.WithEmbeddingModel(model))
```

### Speech Synthesis

Generate speech audio from text using Edge TTS (free, no API key required):

```go
import "github.com/felinics/twilight/provider/edge/speech"

provider := speech.New()
model := provider.SpeechModel("edge-read-aloud")

// Generate complete audio
result, err := sdk.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("Hello, world!"),
    sdk.WithSpeechConfig(map[string]any{
        "voice": "en-US-EmmaMultilingualNeural",
        "speed": 1.0,
    }),
)
// result.Audio is []byte, result.ContentType is "audio/mpeg"
```

Stream audio chunks for low-latency playback:

```go
sr, err := sdk.StreamSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("你好，这是流式语音合成。"),
    sdk.WithSpeechConfig(map[string]any{
        "voice": "zh-CN-XiaoxiaoNeural",
    }),
)
for chunk := range sr.Stream {
    // write chunk to audio player or file
}
```

### Provider Health Check & Model Discovery

Test connectivity and discover available models before making generation requests:

```go
provider := completions.New(completions.WithAPIKey("sk-..."))

// Check provider connectivity
result := provider.Test(context.Background())
switch result.Status {
case sdk.ProviderStatusOK:
    fmt.Println("Provider is healthy")
case sdk.ProviderStatusUnhealthy:
    fmt.Println("Connected but unhealthy:", result.Message)
case sdk.ProviderStatusUnreachable:
    fmt.Println("Cannot connect:", result.Message)
}

// List all available models
models, err := provider.ListModels(context.Background())
for _, m := range models {
    fmt.Println(m.ID)
}

// Check if a specific model is supported
model := provider.ChatModel("gpt-4o")
testResult, err := model.Test(context.Background())
if testResult.Supported {
    fmt.Println("Model is supported")
}
```

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](docs/getting-started.md) | Installation, setup, and first request |
| [Providers](docs/providers.md) | Provider interface, OpenAI, Anthropic, and Google Gemini |
| [Images](docs/images.md) | Generate and edit images with OpenAI and Alibaba Cloud DashScope image models |
| [Embeddings](docs/embeddings.md) | Generate vector embeddings with OpenAI and Google |
| [Speech](docs/speech.md) | Speech synthesis with Edge TTS and custom providers |
| [Tool Calling](docs/tools.md) | Tool definitions, typed arguments and outputs, replaying a step |
| [Streaming](docs/streaming.md) | `Model.Stream`, the `ModelStream` and its StreamPart types |
| [API Reference](docs/api-reference.md) | Complete type and function reference |

## Supported Providers

| Provider | Constructor | API | Status |
|----------|-------------|-----|--------|
| OpenAI Chat Completions | `completions.New()` | `/chat/completions` | ✅ Stable |
| OpenAI Responses | `responses.New()` | `/responses` | ✅ Stable |
| OpenAI Codex | `codex.New()` | `/codex/responses` | ✅ Stable |
| OpenAI-compatible (DeepSeek, Groq, etc.) | `completions.New()` + `WithBaseURL` | `/chat/completions` | ✅ Stable |
| OpenRouter Responses | `responses.New()` + `WithBaseURL` | `/responses` | ✅ Stable |
| OpenCode Go | `opencodego.New()` | Per-model Completions / Responses / Messages | New |
| Anthropic | `messages.New()` | `/messages` | ✅ Stable |
| Google Gemini | `generativeai.New()` | Generative AI API | ✅ Stable |
| OpenAI Images | `images.New()` | `/images/generations`, `/images/edits` | ✅ Stable |
| Alibaba Cloud DashScope Images | `images.New()` | DashScope text2image / multimodal-generation | ✅ Stable |
| OpenAI Embeddings | `embedding.New()` | `/embeddings` | ✅ Stable |
| Google Embeddings | `embedding.New()` | `embedContent` / `batchEmbedContents` | ✅ Stable |
| Edge TTS | `speech.New()` | Bing WebSocket | ✅ Stable |
| OpenAI / compatible TTS | `speech.New()` | `/audio/speech` | ✅ Stable |
| Deepgram TTS | `speech.New()` | `/v1/speak` | ✅ Stable |
| ElevenLabs TTS | `speech.New()` | `/v1/text-to-speech/{voice_id}` | ✅ Stable |
| MiniMax TTS | `speech.New()` | `/v1/t2a_v2` | ✅ Stable |
| MiMo TTS | `speech.New()` | `/chat/completions` + audio output | ✅ Stable |
| Alibaba Cloud CosyVoice | `speech.New()` | DashScope WebSocket | ✅ Stable |
| Volcengine SAMI TTS | `speech.New()` | `/api/v1/invoke` | ✅ Stable |

## License

[Apache License 2.0](LICENSE)
