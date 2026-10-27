# Speech Synthesis

Speech synthesis converts text into audio. Twilight AI provides a unified speech API with an open-ended configuration model that lets each provider define its own parameters.

## Core Concepts

| Concept | Description |
|---------|-------------|
| `SpeechProvider` | Interface that speech backends implement |
| `SpeechModel` | A model bound to a provider, created via `provider.SpeechModel(id)` |
| `GenerateSpeech` | Synthesize complete audio in one call |
| `StreamSpeech` | Synthesize audio as a stream of chunks |

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"

    "github.com/felinics/twilight/provider/edge/speech"
    "github.com/felinics/twilight/sdk"
)

func main() {
    provider := speech.New()
    model := provider.SpeechModel("edge-read-aloud")

    result, err := sdk.GenerateSpeech(context.Background(),
        sdk.WithSpeechModel(model),
        sdk.WithText("Hello, this is a speech synthesis test."),
        sdk.WithSpeechConfig(map[string]any{
            "voice": "en-US-EmmaMultilingualNeural",
            "speed": 1.0,
        }),
    )
    if err != nil {
        log.Fatal(err)
    }

    if err := os.WriteFile("output.mp3", result.Audio, 0644); err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Saved %d bytes (%s)\n", len(result.Audio), result.ContentType)
}
```

## Generate Speech

`sdk.GenerateSpeech` synthesizes the entire audio and returns it as a byte slice:

```go
result, err := sdk.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("The quick brown fox jumps over the lazy dog."),
    sdk.WithSpeechConfig(map[string]any{
        "voice":  "en-US-JennyNeural",
        "format": "audio-24khz-96kbitrate-mono-mp3",
    }),
)
// result.Audio       — raw audio bytes
// result.ContentType — MIME type, e.g. "audio/mpeg"
```

## Stream Speech

`sdk.StreamSpeech` returns audio chunks via a channel for low-latency playback:

```go
sr, err := sdk.StreamSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("你好，这是流式语音合成测试。"),
    sdk.WithSpeechConfig(map[string]any{
        "voice": "zh-CN-XiaoxiaoNeural",
    }),
)
if err != nil {
    log.Fatal(err)
}

for chunk := range sr.Stream {
    // Write each chunk to an audio player, file, or HTTP response
    writer.Write(chunk)
}
if err := sr.Err(); err != nil {
    // The stream ended early. A provider failure is an *sdk.APIError;
    // a failure frame on a WebSocket stream has StatusCode 0.
    log.Fatal(err)
}
```

Or use the convenience method to collect all audio:

```go
sr, _ := sdk.StreamSpeech(ctx, ...)
audio, err := sr.Bytes()
```

## Options

| Option | Description |
|--------|-------------|
| `sdk.WithSpeechModel(model)` | **Required.** The speech model to use |
| `sdk.WithText(text)` | **Required.** The text to synthesize |
| `sdk.WithSpeechConfig(cfg)` | Provider-specific configuration map |

## Using a Client Instance

The package-level `sdk.GenerateSpeech` and `sdk.StreamSpeech` use a default client. You can also create your own:

```go
client := sdk.NewClient()
result, err := client.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("Hello"),
)
```

## Edge TTS Provider

The `provider/edge/speech` package provides free speech synthesis via Microsoft Edge's built-in TTS service. No API key is required.

### Basic Usage

```go
import "github.com/felinics/twilight/provider/edge/speech"

provider := speech.New()
model := provider.SpeechModel("edge-read-aloud")
```

### Configuration Keys

The Edge provider reads these keys from `WithSpeechConfig`:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `voice` | `string` | `en-US-EmmaMultilingualNeural` | Voice ID from the Edge TTS voice catalog |
| `language` | `string` | Auto-detected from voice | BCP-47 language tag (e.g. `en-US`, `zh-CN`) |
| `format` | `string` | `audio-24khz-48kbitrate-mono-mp3` | Output audio format string |
| `speed` | `float64` | `0` (server default) | Speech rate; `1.0` = normal, `2.0` = double speed |
| `pitch` | `float64` | `0` | Pitch adjustment in Hz |

### Available Formats

| Format | Content Type |
|--------|-------------|
| `audio-24khz-48kbitrate-mono-mp3` | `audio/mpeg` |
| `audio-24khz-96kbitrate-mono-mp3` | `audio/mpeg` |
| `webm-24khz-16bit-mono-opus` | `audio/opus` |

### Voices

Edge TTS supports 400+ voices across 100+ languages. Use `speech.Voices()` to browse the full catalog. It returns a copy, so modifying the result does not affect the package:

```go
import "github.com/felinics/twilight/provider/edge/speech"

// Map of language tag → voice IDs
for lang, voices := range speech.Voices() {
    fmt.Printf("%s: %v\n", lang, voices)
}

// Reverse lookup: voice ID → language
lang, ok := speech.LookupVoiceLang("zh-CN-XiaoxiaoNeural")
// lang = "zh-CN", ok = true
```

Popular voices:

| Language | Voice ID |
|----------|----------|
| English (US) | `en-US-EmmaMultilingualNeural`, `en-US-JennyNeural`, `en-US-AriaNeural` |
| English (GB) | `en-GB-SoniaNeural`, `en-GB-RyanNeural` |
| Chinese (CN) | `zh-CN-XiaoxiaoNeural`, `zh-CN-YunxiNeural` |
| Japanese | `ja-JP-NanamiNeural`, `ja-JP-KeitaNeural` |
| Korean | `ko-KR-SunHiNeural`, `ko-KR-InJoonNeural` |

### Provider Options

| Option | Description |
|--------|-------------|
| `speech.WithBaseURL(url)` | Override the WebSocket endpoint (for testing) |

## OpenAI TTS Provider

The `provider/openai/speech` package targets the OpenAI `/audio/speech` endpoint and is compatible with any proxy that mirrors this API.

```go
import "github.com/felinics/twilight/provider/openai/speech"

provider := speech.New(
    speech.WithAPIKey("sk-..."),
    // optional: override for proxies
    // speech.WithBaseURL("https://openrouter.ai/api/v1"),
)
model := provider.SpeechModel("tts-1")

result, err := sdk.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("Hello!"),
    sdk.WithSpeechConfig(map[string]any{
        "voice":           "alloy",
        "response_format": "mp3",
        "speed":           1.0,
    }),
)
```

`DoStream` reads the HTTP chunked body, delivering audio in real-time.

## ElevenLabs TTS Provider

The `provider/elevenlabs/speech` package calls `/v1/text-to-speech/{voice_id}` for full synthesis and `/v1/text-to-speech/{voice_id}/stream` for streaming.

```go
import "github.com/felinics/twilight/provider/elevenlabs/speech"

provider := speech.New(
    speech.WithAPIKey("your-elevenlabs-key"),
)
model := provider.SpeechModel("elevenlabs-tts")

sr, err := sdk.StreamSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("Hi there!"),
    sdk.WithSpeechConfig(map[string]any{
        "voice_id":         "21m00Tcm4TlvDq8ikWAM",
        "model_id":         "eleven_turbo_v2_5",
        "stability":        0.4,
        "similarity_boost": 0.8,
    }),
)
```

## Deepgram TTS Provider

The `provider/deepgram/speech` package calls `POST /v1/speak` using `Authorization: Token` authentication. The response is a chunked binary audio stream.

```go
import "github.com/felinics/twilight/provider/deepgram/speech"

provider := speech.New(
    speech.WithAPIKey("your-deepgram-key"),
)
model := provider.SpeechModel("deepgram-tts")

result, err := sdk.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("Greetings from Deepgram!"),
    sdk.WithSpeechConfig(map[string]any{
        "model":     "aura-2-asteria-en",
        "container": "wav",
    }),
)
```

## MiniMax TTS Provider

The `provider/minimax/speech` package calls `POST /v1/t2a_v2`. The response is a JSON object containing the audio as a hex-encoded string; the provider decodes it automatically.

```go
import "github.com/felinics/twilight/provider/minimax/speech"

provider := speech.New(
    speech.WithAPIKey("your-minimax-key"),
)
model := provider.SpeechModel("minimax-tts")

result, err := sdk.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("你好！"),
    sdk.WithSpeechConfig(map[string]any{
        "voice_id": "Chinese_narrator_female",
        "model":    "speech-2.8-hd",
    }),
)
```

## MiMo TTS Provider

The `provider/mimo/speech` package targets Xiaomi MiMo's `/chat/completions` audio output. Non-streaming responses return base64 audio directly, while streaming responses emit PCM chunks that the provider repackages as WAV for callers.

```go
import "github.com/felinics/twilight/provider/mimo/speech"

provider := speech.New(
    speech.WithAPIKey("your-mimo-api-key"),
)
model := provider.SpeechModel("mimo-v2.5-tts")

result, err := sdk.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("Hello from MiMo speech."),
    sdk.WithSpeechConfig(map[string]any{
        "voice":        "mimo_default",
        "format":       "wav",
        "style_prompt": "Warm, calm narration.",
    }),
)
```

Supported config keys:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `voice` | `string` | `mimo_default` | MiMo preset voice ID |
| `format` | `string` | `wav` | Output format for non-streaming synthesis (`wav` or `pcm16`) |
| `style_prompt` | `string` | `""` | Optional style prompt prepended to the request |

## OpenRouter TTS Provider

The `provider/openrouter/speech` package uses the audio output of OpenRouter's `/chat/completions` endpoint (`modalities: ["text", "audio"]`, `audio.format: "pcm16"`, `stream: true`); OpenRouter has no `/audio/speech` endpoint. The audio arrives as base64 PCM-16 (24 kHz, mono, little-endian) in the `delta.audio.data` field of the SSE chunks.

```go
import "github.com/felinics/twilight/provider/openrouter/speech"

provider := speech.New(
    speech.WithAPIKey("sk-or-..."),
)
model := provider.SpeechModel("openai/gpt-audio-mini")

sr, err := sdk.StreamSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("Hello from OpenRouter."),
    sdk.WithSpeechConfig(map[string]any{
        "voice": "coral",
    }),
)
```

Supported config keys:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `model` | `string` | `openai/gpt-audio-mini` | OpenRouter model ID, used when the `SpeechModel` has no ID |
| `voice` | `string` | `coral` | Voice name supported by the model |
| `speed` | `float64` | `0` (not sent) | Speaking rate, sent as an extra body field (model-dependent) |

`DoSynthesize` reads the whole SSE stream and returns one complete WAV file (`audio/wav`).

`DoStream` forwards audio as each SSE audio delta arrives, with `ContentType` `audio/wav`:

- The first chunk starts with a 44-byte WAV header whose RIFF and `data` sizes are `0xFFFFFFFF`, the usual marker for a WAV of unknown length, followed by the first PCM-16 samples.
- Every later chunk is raw PCM-16 continuing the same `data` chunk. Chunks hold whole 2-byte samples; only the last one can end on an odd byte, when the upstream sent an odd number of bytes in total.
- Concatenating every chunk gives a playable WAV file with the same PCM data as `DoSynthesize`; only the header sizes differ. Rewrite bytes 4–7 and 40–43 if a tool needs exact sizes.

How soon and how often deltas arrive is up to OpenRouter and the model it routes to; the provider adds no buffering of its own. A non-2xx response is returned by `StreamSpeech` before the stream starts. An OpenRouter mid-stream error event ends the stream, and `Err` returns it as an `*sdk.APIError` with `StatusCode` 0.

## Alibaba Cloud DashScope CosyVoice Provider

The `provider/alibabacloud/speech` package implements the WebSocket-based DashScope CosyVoice API. The provider connects to `wss://dashscope.aliyuncs.com/api-ws/v1/inference/` with Bearer auth and uses the `run-task` → `continue-task` → `finish-task` message sequence.

```go
import "github.com/felinics/twilight/provider/alibabacloud/speech"

provider := speech.New(
    speech.WithAPIKey("your-dashscope-api-key"),
)
model := provider.SpeechModel("cosyvoice-tts")

sr, err := sdk.StreamSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("你好，世界！"),
    sdk.WithSpeechConfig(map[string]any{
        "model":  "cosyvoice-v2",
        "voice":  "longanyang",
        "format": "mp3",
        "rate":   1.0,
    }),
)
```

`DoStream` opens a WebSocket connection and delivers audio binary frames as they arrive from the server.

## Volcengine SAMI TTS Provider

The `provider/volcengine/speech` package calls the Volcengine SAMI HTTP API at `https://sami.bytedance.com/api/v1/invoke`. It uses a two-step authentication flow:

1. Call `open.volcengineapi.com/GetToken` — authenticated with Volcengine V4 HMAC-SHA256 signing using `access_key` + `secret_key`.
2. Use the returned token as a URL parameter for every TTS invoke call.

Tokens are cached for their full validity window (1 hour).

```go
import "github.com/felinics/twilight/provider/volcengine/speech"

provider := speech.New(
    speech.WithAccessKey("your-access-key"),
    speech.WithSecretKey("your-secret-key"),
    speech.WithAppKey("your-app-key"),
)
model := provider.SpeechModel("sami-tts")

result, err := sdk.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("欢迎使用语音合成服务。"),
    sdk.WithSpeechConfig(map[string]any{
        "speaker":     "zh_female_qingxin",
        "encoding":    "mp3",
        "sample_rate": 24000,
    }),
)
```

## Implementing a Custom Speech Provider

To add support for a new speech backend, implement the `sdk.SpeechProvider` interface:

```go
package myprovider

import (
    "context"
    "github.com/felinics/twilight/sdk"
)

type MyProvider struct {
    apiKey string
}

func New(apiKey string) *MyProvider {
    return &MyProvider{apiKey: apiKey}
}

func (p *MyProvider) SpeechModel(id string) *sdk.SpeechModel {
    return &sdk.SpeechModel{ID: id, Provider: p}
}

func (p *MyProvider) DoSynthesize(ctx context.Context, params sdk.SpeechParams) (*sdk.SpeechResult, error) {
    // Read provider-specific config from params.Config
    voice, _ := params.Config["voice"].(string)
    format, _ := params.Config["response_format"].(string)

    // Call your TTS API...
    audio := synthesizeAudio(params.Text, voice, format)

    return &sdk.SpeechResult{
        Audio:       audio,
        ContentType: "audio/mpeg",
    }, nil
}

func (p *MyProvider) DoStream(ctx context.Context, params sdk.SpeechParams) (*sdk.SpeechStreamResult, error) {
    ch := make(chan []byte, 8)
    errCh := make(chan error, 1)

    go func() {
        defer close(ch)
        defer close(errCh)
        // Stream audio chunks into ch...
    }()

    return sdk.NewSpeechStreamResult(ch, "audio/mpeg", errCh), nil
}
```

Then use it like the built-in provider:

```go
provider := myprovider.New("my-api-key")
model := provider.SpeechModel("tts-1")

result, err := sdk.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("Hello"),
    sdk.WithSpeechConfig(map[string]any{
        "voice":           "alloy",
        "response_format": "mp3",
    }),
)
```

Each provider defines its own configuration keys in `Config`. This open-ended design allows providers with very different parameter sets (Edge TTS pitch/format vs. OpenAI voice/response_format vs. others) to coexist under the same interface.

## Common Patterns

### Save to File

```go
result, _ := sdk.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("Save this to a file."),
)
os.WriteFile("output.mp3", result.Audio, 0644)
```

### Stream to HTTP Response

```go
func handleTTS(w http.ResponseWriter, r *http.Request) {
    sr, err := sdk.StreamSpeech(r.Context(),
        sdk.WithSpeechModel(model),
        sdk.WithText(r.FormValue("text")),
    )
    if err != nil {
        http.Error(w, err.Error(), 500)
        return
    }
    w.Header().Set("Content-Type", sr.ContentType)
    for chunk := range sr.Stream {
        w.Write(chunk)
        if f, ok := w.(http.Flusher); ok {
            f.Flush()
        }
    }
    if err := sr.Err(); err != nil {
        log.Printf("tts stream: %v", err)
    }
}
```

## Next Steps

- [Providers](providers.md) — learn about chat and embedding providers
- [Streaming](streaming.md) — understand channel-based streaming for text
- [API Reference](api-reference.md) — complete type and function reference
