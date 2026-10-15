# Providers

A **Provider** is the abstraction that connects the SDK to an AI backend. It handles HTTP communication, request/response mapping, and streaming protocol details.

## The Provider Interface

```go
type Provider interface {
    Name() string
    ListModels(ctx context.Context) ([]Model, error)
    Test(ctx context.Context) error
    TestModel(ctx context.Context, modelID string) (*ModelTestResult, error)
    DoGenerate(ctx context.Context, req Request) (ModelResult, error)
    DoStream(ctx context.Context, req Request) (<-chan StreamPart, error)
}
```

| Method | Purpose |
|--------|---------|
| `Name()` | Returns a human-readable provider identifier (e.g. `"openai-completions"`) |
| `ListModels(ctx)` | Fetches available models from the backend API |
| `Test(ctx)` | Health check; `nil` means reachable with valid credentials (see below) |
| `TestModel(ctx, id)` | Checks whether a specific model ID is supported |
| `DoGenerate()` | Performs one non-streaming model call and returns the single result |
| `DoStream()` | Performs one streaming model call, returning a channel of `StreamPart` it must close |

The seam carries one model call and nothing else: `Request` in, `ModelResult` or
`StreamPart` out. Orchestration state — steps, output messages, tool execution —
never crosses it, and a provider never assembles `StreamPart`s into a
`ModelResult`: that folding happens once, in the SDK. A provider that only
streams answers `DoGenerate` with `sdk.CollectStream(ctx, parts)`, so its two
methods cannot disagree.

`DoStream` must close the channel it returns, respect `ctx` while sending, and
report a mid-stream failure as an `ErrorPart` rather than a truncated
success.

Callers never invoke a provider directly: `Model.Generate` and `Model.Stream` bind the request to the model, hand it to the provider and harden the result. The `Model` struct carries a reference to its provider:

```go
type Model struct {
    ID          string
    DisplayName string
    Provider    Provider

    // Optional metadata: modalities, context window, output limit, tool and
    // structured output support, reasoning controls. See the API reference.
}
```

`Model` also has a `Test(ctx)` method that delegates to `Provider.TestModel`.

### Provider Health Check

`Test(ctx)` returns an error:

| Result | Meaning |
|--------|---------|
| `nil` | The provider's check succeeded. It needs valid credentials, except where the provider checks a public endpoint (OpenCode Go), which only establishes reachability |
| `*sdk.APIError` in the chain | The provider answered and rejected the check. `Kind` is `KindAuthentication` or `KindPermissionDenied` for a rejected key |
| Any other error | The endpoint was not reached (DNS, TLS, connection refused, context ended) |

```go
if err := provider.Test(ctx); err != nil {
    if kind := sdk.KindOf(err); kind == sdk.KindAuthentication || kind == sdk.KindPermissionDenied {
        log.Fatalf("credentials rejected: %v", err)
    }
    log.Fatalf("provider issue: %v", err)
}
```

### Model Discovery

`ListModels(ctx)` returns all models available from the provider. Each returned `Model` is bound to the provider.

```go
models, err := provider.ListModels(ctx)
for _, m := range models {
    fmt.Printf("%-40s %s\n", m.ID, m.DisplayName)
}
```

`ListModels` never filters the list. It fills the optional `Model` metadata
from what the endpoint reports and leaves the rest at its zero value ("not
stated"):

| Provider | Endpoint reports | Filled |
|----------|------------------|--------|
| OpenAI Completions / Responses against OpenAI | `id`, `object`, `created`, `owned_by` | `ID` |
| OpenAI Completions / Responses against OpenRouter, or a host with the same fields | `name`, `description`, `context_length`, `top_provider`, `architecture`, `supported_parameters`, `reasoning` | `DisplayName`, `Description`, `ContextWindow`, `MaxOutputTokens`, `Modalities`, `ToolCall`, `StructuredOutput`, `Reasoning` |
| Anthropic Messages | `display_name`, `max_input_tokens`, `max_tokens`, `capabilities` | `DisplayName`, `ContextWindow`, `MaxOutputTokens`, and from `capabilities`: `Modalities`, `StructuredOutput`, `Reasoning` |

OpenRouter fields map as follows:

- `ContextWindow` is `context_length`; `MaxOutputTokens` is
  `top_provider.max_completion_tokens` (0 when null).
- `Modalities` come from `architecture.input_modalities` and
  `output_modalities`; `file` becomes `pdf` and other values pass through.
- `ToolCall` is `supported_parameters` containing `tools`. `StructuredOutput`
  is `supported_parameters` containing `structured_outputs`; `response_format`
  alone does not count, since it can mean JSON mode without a schema.
- `Reasoning` is set when `supported_parameters` contains `reasoning` or
  `include_reasoning`, or the `reasoning` object says the model always reasons
  or lists effort levels. `Efforts` are `supported_efforts` without `none`,
  weakest first, with `Dialect` tier; `supports_max_tokens` gives a `Budget`
  without bounds; `Off` is rejected when `mandatory` is true and accepted when
  it is false or `none` is an effort; `Default` comes from `default_enabled`
  and `default_effort`. `Toggle` and `Replay` are not stated.

Anthropic fields map as follows:

- `ContextWindow` is `max_input_tokens`. On Claude the response shares the
  context window with the input, so the input limit is the whole window. A
  compatible host may report a smaller input limit that leaves room for the
  response (OpenRouter does for some models), and `ContextWindow` is then that
  smaller value. `MaxOutputTokens` is `max_tokens`.
- With `capabilities`, `Modalities` are text in and out plus image and PDF
  input when `image_input` and `pdf_input` are supported, and
  `StructuredOutput` is `structured_outputs`. `Reasoning` is set when
  `thinking` is supported: `Efforts` are the supported `effort` levels;
  `thinking.types.enabled` gives a `Budget` without bounds; `Dialect` is
  adaptive when `thinking.types.adaptive` is supported, otherwise budget;
  `thinking.types.disabled` decides `Off` and, when accepted, `Toggle`.
  `Default` is not stated. Tool use is not among the capabilities, so
  `ToolCall` stays false.
- `ListModels` reads every page: `limit=1000`, then `after_id` set to the
  previous `last_id` while `has_more` is true. It stops with the models
  already read when a page brings no new model or no cursor. A compatible
  host that rejects the first page's `limit=1000` with 400 or 422 is listed
  again once without `limit`, at its default page size.

Fields in a shape that does not decode are dropped for that model rather than
failing the list.

To check a single model without listing all:

```go
model := provider.ChatModel("gpt-4o")
result, err := model.Test(ctx)
if err != nil {
    // the probe itself failed, for example on authentication
} else if result.Supported {
    // safe to use this model
}
```

## Custom HTTP Headers

Anthropic Messages, all OpenAI providers (including embeddings, images, speech
and transcription), Google Generative AI, GitHub Copilot and OpenCode Go support
`WithHeaders(map[string]string)` for provider defaults and
`sdk.WithRequestHeaders(ctx, headers)` for call-scoped values.

```go
provider := completions.New(
    completions.WithAPIKey("sk-..."),
    completions.WithHeaders(map[string]string{"User-Agent": "my-agent/1.0"}),
)
ctx := sdk.WithRequestHeaders(context.Background(), map[string]string{
    "X-Conversation-ID": conversationID,
})
result, err := provider.ChatModel("gpt-4o-mini").Generate(ctx, sdk.Request{
    Messages: []sdk.Message{sdk.UserMessage("Hello")},
})
if err != nil {
    log.Fatal(err)
}
fmt.Println(result.Text)
```

Header names are case-insensitive. Precedence is **defaults < provider headers <
request-context headers**. Both APIs copy their input maps when called; mutating
those maps later does not reconfigure a provider or context. Nested request
contexts inherit headers and override matching names. Multiple `WithHeaders`
provider options replace the provider header map; use one map to combine defaults.
SSE negotiation headers, the JSON body `Content-Type`, generated multipart
boundaries, and AWS signing are applied after custom headers to preserve their
transport requirements.

Use a context per conversation for session IDs, and reuse it for every call in
that conversation, including retries and the calls that replay tool results. The
same context can be passed to `ListModels`, `Test` and `TestModel`. This keeps
session data out of shared provider state. Attach headers only to the provider
calls that should receive them; child contexts inherit them, including contexts
derived for your own tool code. For that reason, keep credentials such as
`Authorization` in provider options rather than the context:
a context header overrides the API key of every provider that receives it. Other
provider packages may ignore request-context headers.

### Client Request ID

`sdk.WithClientRequestID(ctx, id)` sends an ID you choose with each provider
request made with the context. A provider that records it can find a request that
failed without a response, such as one that timed out; log the ID with the call
and quote it in support requests.

```go
id := uuid.NewString()
ctx := sdk.WithClientRequestID(conversationCtx, id)
result, err := model.Generate(ctx, req)
if err != nil {
    log.Printf("generate failed (client request id %s): %v", id, err)
}
```

| Provider | Header |
|---|---|
| OpenAI Completions, Responses, embeddings, images, speech and transcription | `X-Client-Request-Id` |
| OpenCode Go, for models routed to Completions or Responses | `X-Client-Request-Id` |
| Ark video | `X-Client-Request-Id` |
| GitHub Copilot | `X-Request-Id` |

Other providers, including Codex, do not send it. The OpenAI packages send it to
whichever endpoint they are configured for. It overrides a header of the same
name set with `sdk.WithRequestHeaders`, and an empty ID clears an inherited one.

OpenAI expects a unique ID per request, of at most 512 ASCII characters. Derive a
context with a new ID for every call; a context shared by a conversation would
send the same ID with every call made with it. `GenerateVideo` sends the ID only
with the request that creates the job, not with polling or the download.

## OpenAI Completions Provider

The `provider/openai/completions` package provides an implementation for the OpenAI Chat Completions API (`/chat/completions`).

### Basic Usage

```go
import "github.com/felinics/twilight/provider/openai/completions"

provider := completions.New(
    completions.WithAPIKey("sk-..."),
)
model := provider.ChatModel("gpt-4o-mini")
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key sent as `Authorization: Bearer <key>` |
| `WithBedrockRegion(region)` | disabled | Use AWS SigV4 with the default AWS credential chain for Bedrock's OpenAI-compatible endpoint |
| `WithBedrockCredentials(region, accessKeyID, secretAccessKey, sessionToken)` | disabled | Use AWS SigV4 with static AWS credentials for Bedrock |
| `WithBaseURL(url)` | `https://api.openai.com/v1` | Base URL for API requests |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client (for proxies, timeouts, etc.) |
| `WithHeaders(headers)` | none | Provider-wide headers; request-context headers take precedence |
| `WithMessageRoleCapabilities(capabilities)` | developer + mid-system enabled | Override instruction roles for a less-capable OpenAI-compatible endpoint |
| `WithReasoningReplay(replay)` | `reasoning_content`, not required | Field that carries assistant reasoning on replay, and whether every tool-call message must carry it; see [Replaying reasoning](#replaying-reasoning) |
| `WithDeepSeekChatCompletionsCompat()` | disabled | Map `WithReasoningEffort("none")` to DeepSeek's thinking disable toggle |
| `WithMiniMaxChatCompletionsCompat()` | disabled | Send `reasoning_split: true` and map reasoning effort to MiniMax's thinking toggle |
| `WithKimiChatCompletionsCompat()` | disabled | Rewrite tool schemas to Moonshot-flavored JSON Schema |

### API Endpoints for Discovery

| Method | API Endpoint |
|--------|-------------|
| `ListModels` | `GET /models` |
| `Test` | `GET /models?limit=1` |
| `TestModel` | `GET /models/{id}` |

`ListModels` fills model metadata from OpenRouter's `/models` fields when the
host sends them; see [Model Discovery](#model-discovery).

### OpenAI-Compatible Providers

Any service that implements the OpenAI Chat Completions API works out of the box.
DeepSeek uses the same endpoint, but disables thinking with a `thinking` toggle
instead of `reasoning_effort: "none"`, so enable the DeepSeek compatibility
option when you need that behavior:

```go
// DeepSeek
provider := completions.New(
    completions.WithAPIKey("your-deepseek-key"),
    completions.WithBaseURL("https://api.deepseek.com"),
    completions.WithDeepSeekChatCompletionsCompat(),
    completions.WithReasoningReplay(sdk.ReasoningReplay{Required: true}),
)
```

The compatibility option keeps the generic Chat Completions provider and only
adapts DeepSeek's thinking toggle:

| SDK option/value | DeepSeek request |
|------------------|------------------|
| `WithReasoningEffort("none")` | `thinking: {type: "disabled"}` |
| any other `WithReasoningEffort(...)` value | original `reasoning_effort` |

Note: `"none"` is the effort floor, which for DeepSeek means off. Sending
`reasoning_effort: "none"` alone does not stop it thinking, so the provider sends
`thinking: {type: "disabled"}` instead.

#### Replaying reasoning

Chat Completions has no standard field for a step's reasoning, and endpoints
that return it on a side channel read it back from the same field on later
requests. `WithReasoningReplay` sets the field and whether it is required:

| `sdk.ReasoningReplay.Field` | Sent on replayed assistant messages | Endpoints |
|-----------------------------|-------------------------------------|-----------|
| `sdk.ReasoningReplayContent` (default) | reasoning text as `reasoning_content` | DeepSeek, Kimi, GLM, Qwen |
| `sdk.ReasoningReplayDetails` | the `reasoning_details` array the endpoint returned, unchanged | OpenRouter, MiniMax |
| `sdk.ReasoningReplayReasoning` | reasoning text as `reasoning` | gateways that read `reasoning` |

Only the configured field is sent. `reasoning_details` is the only form that
carries signatures and encrypted reasoning, so it is what OpenRouter needs to
continue a hosted model's reasoning across tool calls. The provider keeps the
array a response or stream returned on the step's `ReasoningPart`, under the
`openai-chat` metadata namespace, merging streamed pieces by `index` with every
field kept. A step without preserved details sends no `reasoning_details`.

In thinking mode, DeepSeek validates replayed history when the request carries
tools: every assistant message with `tool_calls` must carry a
`reasoning_content` key, or the API answers
`400 The reasoning_content in the thinking mode must be passed back to the API`.
An empty string passes. Moonshot/Kimi enforces the same rule when thinking is
enabled. `Required: true` pads every assistant tool-call message that has no
reasoning with an empty value of the text field:

| Situation | Text field on assistant tool-call messages without a `ReasoningPart` |
|-----------|------------------------------------------------------|
| `Required: true` | `""` |
| Any message in the request already carries the field | `""` |
| Otherwise | omitted, since plain OpenAI rejects unknown message fields |

`Required` has no effect with `sdk.ReasoningReplayDetails`: an empty array
carries nothing.

On the response side, a `reasoning_content` key that is present but empty is
recorded as a `ReasoningPart` with empty text in the `openai-chat-v1` dialect.
DeepSeek returns `"reasoning_content": ""` for a thinking-mode step that
produced no reasoning and omits the key when thinking is disabled, so key
presence is the signal, and such a step replays with the key it needs.

A model that carries its metadata passes its replay setting as is:

```go
if r := model.Reasoning; r != nil && r.Replay != nil {
    opts = append(opts, completions.WithReasoningReplay(*r.Replay))
}
```

MiniMax also uses the OpenAI-compatible endpoint, but ignores `reasoning_effort`
and, by default, inlines reasoning into `content` as `<think>` tags. Enable the
MiniMax compatibility option to get clean, separated reasoning:

```go
// MiniMax
provider := completions.New(
    completions.WithAPIKey("your-minimax-key"),
    completions.WithBaseURL("https://api.minimax.io/v1"),
    completions.WithMiniMaxChatCompletionsCompat(),
    completions.WithReasoningReplay(sdk.ReasoningReplay{Field: sdk.ReasoningReplayDetails}),
)
```

Use `https://api.minimaxi.com/v1` instead for China-region MiniMax API keys.

The option always sends `reasoning_split: true` (so reasoning is returned in
`reasoning_details`) and maps reasoning effort to MiniMax's `thinking` toggle:

| SDK option/value | MiniMax request |
|------------------|-----------------|
| `WithReasoningEffort("none")` | `thinking: {type: "disabled"}` |
| any other `WithReasoningEffort(...)` value | `thinking: {type: "adaptive"}` |
| no reasoning effort | `thinking` omitted (MiniMax default) |

Reasoning is read from `reasoning_details` and preserved on the step's
`ReasoningPart`. MiniMax reads it back from `reasoning_details`, which is why the
example sets `sdk.ReasoningReplayDetails`; the compatibility option does not set a
replay policy.

Other OpenAI-compatible services use `completions.New()` with a base URL:

```go
// Groq
provider := completions.New(
    completions.WithAPIKey("your-groq-key"),
    completions.WithBaseURL("https://api.groq.com/openai/v1"),
)

// Azure OpenAI
provider := completions.New(
    completions.WithAPIKey("your-azure-key"),
    completions.WithBaseURL("https://your-resource.openai.azure.com/openai/deployments/gpt-4o"),
)

// Amazon Bedrock OpenAI-compatible endpoint (AWS credentials)
provider := completions.New(
    completions.WithBedrockRegion("us-east-1"),
    completions.WithBaseURL("https://bedrock-mantle.us-east-1.api.aws/v1"),
)

// Local (Ollama, vLLM, etc.)
provider := completions.New(
    completions.WithBaseURL("http://localhost:11434/v1"),
)
```

### Supported Features

| Feature | Supported |
|---------|-----------|
| Chat completions | ✅ |
| Streaming (SSE) | ✅ |
| Tool/function calling | ✅ |
| Vision (image inputs) | ✅ |
| Reasoning content (o1, DeepSeek-R1) | ✅ |
| JSON mode / JSON Schema | ✅ |
| Token usage reporting | ✅ |
| Cached token details | ✅ |
| ListModels / Test / TestModel | ✅ |

### Custom HTTP Client

Use `WithHTTPClient` for custom timeouts, proxies, or TLS settings:

```go
provider := completions.New(
    completions.WithAPIKey("sk-..."),
    completions.WithHTTPClient(&http.Client{
        Timeout: 120 * time.Second,
        Transport: &http.Transport{
            Proxy: http.ProxyFromEnvironment,
        },
    }),
)
```

## OpenAI Responses Provider

The `provider/openai/responses` package provides an implementation for the OpenAI Responses API (`/responses`). This is OpenAI's newer API that offers first-class reasoning support, URL citation annotations, and a flat input format.

### When to Use Responses vs Completions

| | Chat Completions | Responses |
|--|---|---|
| **Endpoint** | `/chat/completions` | `/responses` |
| **Reasoning models** | Basic support (`reasoning_content` field) | First-class (`reasoning` output items with summaries) |
| **Citations** | Not supported | URL citations via annotations |
| **Input format** | Nested `messages` array | Flat `input` array |
| **Compatibility** | Broad (DeepSeek, Groq, Ollama, Azure-style compatible endpoints, etc.) | OpenAI and OpenRouter |

Use **Completions** when you need broad compatibility with OpenAI-compatible endpoints. Add `WithDeepSeekChatCompletionsCompat()` when `WithReasoningEffort("none")` should disable DeepSeek thinking, and `WithReasoningReplay` for the endpoint's reasoning field. Use **Responses** when you want native reasoning model support (o3, o4-mini) or URL citation annotations.

### Basic Usage

```go
import "github.com/felinics/twilight/provider/openai/responses"

provider := responses.New(
    responses.WithAPIKey("sk-..."),
)
model := provider.ChatModel("gpt-4o-mini")
```

`Request.System` is sent through the Responses API's top-level
`instructions` field. System and developer messages inside `Messages` retain
their native roles and positions.

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key sent as `Authorization: Bearer <key>` |
| `WithBedrockRegion(region)` | disabled | Use AWS SigV4 with the default AWS credential chain for Bedrock's OpenAI-compatible endpoint |
| `WithBedrockCredentials(region, accessKeyID, secretAccessKey, sessionToken)` | disabled | Use AWS SigV4 with static AWS credentials for Bedrock |
| `WithBaseURL(url)` | `https://api.openai.com/v1` | Base URL for API requests |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |
| `WithHeaders(headers)` | none | Provider-wide headers; request-context headers take precedence |

### API Endpoints for Discovery

| Method | API Endpoint |
|--------|-------------|
| `ListModels` | `GET /models` |
| `Test` | `GET /models?limit=1` |
| `TestModel` | `GET /models/{id}` |

`ListModels` fills model metadata from OpenRouter's `/models` fields when the
host sends them; see [Model Discovery](#model-discovery).

### Using with OpenRouter

OpenRouter supports the Responses API as a beta feature:

```go
provider := responses.New(
    responses.WithAPIKey("sk-or-v1-..."),
    responses.WithBaseURL("https://openrouter.ai/api/v1"),
)

Amazon Bedrock's OpenAI-compatible Responses endpoint also works with the same provider when you configure a Bedrock Mantle base URL and either `WithBedrockRegion(...)` or `WithBedrockCredentials(...)`.
model := provider.ChatModel("openai/o4-mini")
```

### Reasoning Models

Reasoning models (o3, o4-mini) return both reasoning summaries and the final answer:

```go
effort, summary := "medium", "auto"
result, _ := provider.ChatModel("openai/o4-mini").Generate(ctx, sdk.Request{
    Messages: []sdk.Message{
        sdk.UserMessage("What is 15 * 37? Think step by step."),
    },
    ReasoningEffort:  &effort,
    ReasoningSummary: &summary,
})
fmt.Println(result.Reasoning)  // model's reasoning summary
fmt.Println(result.Text)       // final answer: "555"
```

`WithReasoningSummary` is an explicit opt-in. OpenAI Responses supports `"auto"`,
`"concise"`, and `"detailed"`; compatible endpoints may support a different
subset. In streaming mode, summaries arrive as `ReasoningStartPart` /
`ReasoningDeltaPart` / `ReasoningEndPart` before the text content.

### Supported Features

| Feature | Supported |
|---------|-----------|
| Text generation | ✅ |
| Streaming (SSE) | ✅ |
| Tool/function calling | ✅ |
| Vision (image inputs) | ✅ |
| Reasoning summaries (o3, o4-mini) | ✅ |
| URL citation annotations | ✅ |
| JSON mode / JSON Schema | ✅ |
| Token usage reporting | ✅ |
| Cached / reasoning token details | ✅ |
| ListModels / Test / TestModel | ✅ |

## OpenAI Codex Provider

The `provider/openai/codex` package provides an implementation for the [OpenAI Codex](https://openai.com/index/introducing-codex/) backend — a cloud-based coding agent powered by reasoning models. It communicates with the ChatGPT backend API via the Responses-style event stream at `/codex/responses`.

### When to Use Codex vs Completions / Responses

| | Chat Completions | Responses | Codex |
|--|---|---|---|
| **Endpoint** | `/chat/completions` | `/responses` | `/codex/responses` (ChatGPT backend) |
| **Authentication** | API key | API key | ChatGPT access token + account ID |
| **Reasoning models** | Basic | First-class | Native (encrypted reasoning content) |
| **Target use-case** | General chat | General + reasoning | Coding agents |
| **Compatibility** | Broad | OpenAI / OpenRouter | OpenAI Codex only |

Use **Codex** when you have a ChatGPT access token and want to leverage Codex-specific models (gpt-5.x-codex series) with encrypted reasoning support.

### Basic Usage

```go
import "github.com/felinics/twilight/provider/openai/codex"

provider := codex.New(
    codex.WithAccessToken("eyJhbGci..."),
    codex.WithAccountID("acct_123"),
)
model := provider.ChatModel("gpt-5.2-codex")
```

The account ID is optional — if omitted, the provider extracts it from the JWT access token's `https://api.openai.com/auth` claim automatically.

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAccessToken(token)` | `""` | ChatGPT access token (JWT) sent as `Authorization: Bearer <token>` |
| `WithAPIKey(token)` | `""` | Alias for `WithAccessToken` for migration convenience |
| `WithAccountID(id)` | Auto-extracted from token | ChatGPT account ID sent as `chatgpt-account-id` header |
| `WithOriginator(name)` | `"codex_cli_rs"` | Originator identifier sent in the `originator` header |
| `WithBaseURL(url)` | `https://chatgpt.com/backend-api` | Base URL for API requests |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |
| `WithHeaders(headers)` | none | Provider-wide headers; request-context headers take precedence |

### Available Models

The provider includes a static model catalog accessible via `codex.Catalog()`:

| Model | Reasoning Efforts |
|-------|-------------------|
| `gpt-5.2` | none, low, medium, high, xhigh |
| `gpt-5.2-codex` | low, medium, high, xhigh |
| `gpt-5.1-codex-max` | low, medium, high, xhigh |
| `gpt-5.1-codex` | low, medium, high |
| `gpt-5.1-codex-mini` | medium, high |
| `gpt-5.1` | none, low, medium, high |

### Reasoning

Codex models support reasoning with encrypted content preservation. When the model returns reasoning, the encrypted content is passed through via `ProviderMetadata` so it can be sent back in follow-up turns:

```go
effort := "high"
result, _ := provider.ChatModel("gpt-5.2-codex").Generate(ctx, sdk.Request{
    Messages: []sdk.Message{
        sdk.UserMessage("Refactor this function to use generics."),
    },
    ReasoningEffort: &effort,
})
fmt.Println(result.Reasoning) // reasoning summary
fmt.Println(result.Text)      // final answer
```

In streaming mode, reasoning arrives as `ReasoningStartPart` / `ReasoningDeltaPart` / `ReasoningEndPart` with the encrypted content in `ProviderMetadata.Get("openai", "reasoningEncryptedContent")`.

### Message Mapping

The Codex API uses a flat input format (similar to Responses). The provider converts SDK messages automatically:

| SDK Message | Codex Input |
|-------------|-------------|
| System message / `System` param | `instructions` field (joined with `\n\n`) |
| User message (text) | `{role: "user", content: [{type: "input_text", ...}]}` |
| User message (image) | `{role: "user", content: [{type: "input_image", ...}]}` |
| Assistant message (text) | `{role: "assistant", content: [{type: "output_text", ...}]}` |
| Assistant reasoning | `{type: "reasoning", summary: [...], encrypted_content: "..."}` |
| Tool call | `{type: "function_call", call_id, name, arguments}` |
| Tool result | `{type: "function_call_output", call_id, output}` |

### Supported Features

| Feature | Supported |
|---------|-----------|
| Text generation | ✅ |
| Streaming (SSE) | ✅ |
| Tool/function calling | ✅ |
| Vision (image inputs) | ✅ |
| Reasoning (encrypted content) | ✅ |
| Reasoning effort control | ✅ |
| JSON mode / JSON Schema | ✅ |
| Token usage reporting | ✅ |
| Cached / reasoning token details | ✅ |
| ListModels (static catalog) | ✅ |
| Test / TestModel | ✅ |

## GitHub Copilot Provider

The `provider/github/copilot` package implements GitHub Copilot's documented chat completions endpoint for Copilot agents/extensions at `https://api.githubcopilot.com/chat/completions`.

### When to Use Copilot vs Completions

| | GitHub Copilot | OpenAI Completions |
|--|---|---|
| **Endpoint** | `api.githubcopilot.com/chat/completions` | `/chat/completions` |
| **Authentication** | GitHub token issued to your Copilot agent/extension user context | API key |
| **Model discovery** | Static local catalog (`copilot-auto`) | `GET /models` |
| **Target use-case** | Copilot agents / extensions running inside GitHub's ecosystem | General OpenAI-compatible backends |

Use **GitHub Copilot** when your code is running as a Copilot agent/extension and GitHub has already provided the user-scoped token you must pass through to the Copilot LLM endpoint. Use **OpenAI Completions** for direct OpenAI or OpenAI-compatible integrations.

This is not a generic PAT-based GitHub Models integration.

### Basic Usage

```go
import "github.com/felinics/twilight/provider/github/copilot"

provider := copilot.New(
    // Pass through the inbound X-GitHub-Token value from GitHub.
    copilot.WithGitHubToken(r.Header.Get("X-GitHub-Token")),
)
model := provider.ChatModel(copilot.AutoModel)
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithGitHubToken(token)` | `""` | User-scoped GitHub token sent as `Authorization: Bearer <token>` |
| `WithAPIKey(token)` | `""` | Alias for `WithGitHubToken` for migration convenience |
| `WithBaseURL(url)` | `https://api.githubcopilot.com` | Base URL for API requests |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |
| `WithHeaders(headers)` | none | Provider-wide headers; request-context headers take precedence |
| `WithMessageRoleCapabilities(capabilities)` | both disabled | Enable native developer or mid-conversation system roles only when the configured Copilot endpoint supports them |

### Model Catalog

GitHub's documented Copilot LLM endpoint for agents/extensions does not expose a public `/models` discovery API. The provider therefore exposes a single static catalog entry that means "let GitHub choose":

| Model | Meaning |
|-------|---------|
| `copilot-auto` (`copilot.AutoModel`) | Let GitHub choose the backing Copilot model |

If GitHub documents or injects an explicit upstream model ID in your runtime, you can still pass that value to `provider.ChatModel("...")` and `TestModel("...")`. `AutoModel` is the safe default.

### Request Mapping

The provider reuses the same OpenAI-compatible chat-completions mapping as Twilight AI's OpenAI provider:

| SDK Message | Copilot API |
|-------------|-------------|
| System message / `System` param | `{"role":"system","content":"..."}` |
| User message | OpenAI-compatible `messages` entry |
| Assistant reasoning | `reasoning_content` field |
| Tool call | `tool_calls` |
| Tool result | `{"role":"tool","tool_call_id":...}` |

### Supported Features

| Feature | Supported |
|---------|-----------|
| Text generation | ✅ |
| Streaming (SSE) | ✅ |
| Tool/function calling | ✅ |
| Vision (image inputs) | ✅ |
| Reasoning content passthrough | ✅ |
| JSON mode / JSON Schema | ✅ |
| Token usage reporting | ✅ |
| ListModels (static catalog) | ✅ |
| Test / TestModel | ✅ |

### Limitations

- This provider is for GitHub Copilot agent / extension runtimes, not for GitHub Models.
- The provider intentionally exposes one sentinel local model because GitHub does not document a public model discovery API for this endpoint.
- `Test` / `TestModel` are implemented via a minimal probe request because there is no documented public model metadata API.

---

## OpenCode Go Provider

`provider/opencode/go` (package `opencodego`) exposes one provider that delegates
to the existing Chat Completions, Responses and Anthropic Messages providers.
The base URL defaults to `https://opencode.ai/zen/go/v1`.

```go
import opencodego "github.com/felinics/twilight/provider/opencode/go"

provider := opencodego.New(
    opencodego.WithAPIKey("your-opencode-go-key"),
    opencodego.WithHeaders(map[string]string{"User-Agent": "my-agent/1.0"}),
)
ctx := sdk.WithRequestHeaders(context.Background(), map[string]string{
    opencodego.SessionHeader: conversationID,
})
result, err := provider.ChatModel("glm-5.2").Generate(ctx, sdk.Request{
    Messages: []sdk.Message{sdk.UserMessage("Explain this code")},
})
if err != nil {
    log.Fatal(err)
}
fmt.Println(result.Text)
```

OpenCode Go asks clients to identify their application with a User-Agent and
send a stable `x-opencode-session` for each conversation. The caller owns that
ID: keep it stable for every call in the conversation, including tool-result
replays and retries, and use
a different context for a different conversation. The SDK neither generates a
new session ID per request nor checks for one before sending; custom HTTP
transports may also supply headers. See the [official client requirements](https://opencode.ai/docs/go/#where-can-i-use-it).

### Routing and discovery

| Protocol | Path relative to the base URL | Example documented model |
|----------|------------------------------|--------------------------|
| `ProtocolCompletions` | `/chat/completions` | `glm-5.2` |
| `ProtocolResponses` | `/responses` | `gpt-5.6-luna` |
| `ProtocolMessages` | `/messages` | `minimax-m2.7` |

Like OpenCode itself, the provider sends every model to Completions except
those that the [official endpoint table](https://opencode.ai/docs/go/#endpoints)
routes to Responses or Messages; the SDK keeps only that exception table,
checked on 2026-09-28. The model list changes about twice a week and most new
models use Completions, so a full directory would go stale.
`provider.ProtocolForModel(id)` exposes the routing decision for applications
that need protocol-specific reasoning or cache settings. Generic generation
parameters, tools, reasoning metadata and stream events retain the selected
protocol provider's behavior. The Go wrapper does not infer model-family
compatibility flags or thinking modes: OpenCode Go is its own service, and a
model's name says nothing about which upstream serves it.

`Request.ProviderOptions` for this provider are keyed by its name,
`"opencode-go"`, and are applied to the selected protocol's wire request as that
protocol provider would apply its own. Options keyed by the protocol providers'
namespaces (`"openai-completions"`, `"openai-responses"`,
`"anthropic-messages"`) are not applied.

Two adjustments apply to every Completions route. Both come from the service's
observed behavior, verified on 2026-09-20, not from the model vendor's API:

- Developer messages are sent as system messages. The routes accept the
  developer role, but several models silently ignore its content, while every
  model honors system messages.
- A replayed assistant tool call always carries `reasoning_content`, empty when
  the history has none. Some routes reject the request otherwise, and every
  route accepts the empty value.

Other upstream behavior passes through unchanged. `ReasoningEffort` is sent as
`reasoning_effort`; the service maps `"none"` to disabled thinking on most
routes, while `deepseek-v4-flash` ignores every thinking control and always
reasons. `glm-5.3-flash` rejects a request that sets a reasoning effort with
`MaxTokens` of 1024 or less.

`ListModels(ctx)` fetches the live `/models` list, including new models. The
upstream list does not include protocol metadata, so a new model uses
Completions. Model-name prefixes are not used to guess a protocol. When the
endpoint table documents a new Responses or Messages model before the SDK
lists it, register the route (or override an existing one) explicitly:

```go
provider := opencodego.New(
    opencodego.WithAPIKey("your-opencode-go-key"),
    opencodego.WithModelProtocols(map[string]opencodego.Protocol{
        "new-model-id": opencodego.ProtocolMessages,
    }),
)
```

Use raw API model IDs, such as `glm-5.2`. The `opencode-go/` prefix belongs to
OpenCode's application config and is not part of the API model ID.

### Options and probes

| Option | Purpose |
|--------|---------|
| `WithAPIKey(key)` | Bearer authentication for OpenAI endpoints; `x-api-key` for Messages |
| `WithBaseURL(url)` | Override the base URL, including `/v1` |
| `WithHTTPClient(client)` | Custom HTTP client |
| `WithHeaders(headers)` | Snapshot provider-wide HTTP headers |
| `WithModelProtocols(routes)` | Snapshot additional/overridden per-model routes |

`Test(ctx)` only checks reachability through the public models endpoint; its
success does not validate credentials or generation access. `TestModel(ctx, id)`
sends a small real generation request through the model's selected protocol.
Supply a session context and your application's User-Agent for the probe too.
A rejected request is returned as an error, including a missing session header;
400/422/429 responses are not treated as successful probes. Generation probes
can incur usage charges.

## Anthropic Provider

The `provider/anthropic/messages` package implements the [Anthropic Messages API](https://docs.anthropic.com/en/api/messages) for Claude models.

### Basic Usage

```go
import "github.com/felinics/twilight/provider/anthropic/messages"

provider := messages.New(
    messages.WithAPIKey("sk-ant-..."),
)
model := provider.ChatModel("claude-sonnet-4-20250514")
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key sent as `x-api-key` header |
| `WithAuthToken(token)` | `""` | OAuth token sent as `Authorization: Bearer <token>` |
| `WithBaseURL(url)` | `https://api.anthropic.com` | Base URL for API requests |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |
| `WithHeaders(headers)` | none | Provider-wide headers; request-context headers take precedence |
| `WithThinking(config)` | `nil` | Thinking configuration as a flat struct; prefer the options below |
| `WithThinkingEnabled(budget)` | none | `thinking: {type: "enabled", budget_tokens}`, for Claude 4.5 and earlier |
| `WithThinkingAdaptive()` | none | `thinking: {type: "adaptive"}`, for Claude 4.6 and later |
| `WithThinkingDisabled()` | none | `thinking: {type: "disabled"}`, sent explicitly |
| `WithThinkingBetweenTools()` | none | `thinking: {type: "between_tools"}`, Claude Sonnet 5.5 |
| `WithThinkingDisplay(display)` | `""` | `thinking.display`: `summarized` or `omitted` |
| `WithInterleavedThinking()` | off | Adds the `interleaved-thinking-2025-05-14` beta to `anthropic-beta` |
| `WithMidConversationSystemMessages(enabled)` | `false` | Preserve interleaved system messages for models that document native support |

By default, developer messages become user messages and mid-conversation
system messages become XML-escaped `<system>` user messages. Leading system
messages continue to use Anthropic's top-level `system` field.

### Extended Thinking

Claude supports [extended thinking](https://docs.anthropic.com/en/docs/build-with-claude/extended-thinking) (chain-of-thought reasoning):

```go
provider := messages.New(
    messages.WithAPIKey("sk-ant-..."),
    messages.WithThinking(messages.ThinkingConfig{
        Type:         "enabled",
        BudgetTokens: 10000,
    }),
)
```

When enabled, the model's internal reasoning appears in `result.Reasoning` (non-streaming) or as `ReasoningStartPart` / `ReasoningDeltaPart` / `ReasoningEndPart` events (streaming).

The SDK does not infer the model generation; pick the mode the target model accepts:

- **Claude 4.5 and earlier** take `WithThinkingEnabled(budget)`. To think between tool calls in this mode, add `WithInterleavedThinking()`. The Claude API ignores the beta on models without it, but other Anthropic-compatible endpoints may reject it, so it is opt-in.
- **Claude 4.6 and later** take `WithThinkingAdaptive()` with depth set per request through `ReasoningEffort` (`output_config.effort`). Adaptive thinking interleaves without the beta.
- **Turning thinking off** sends `thinking: {type: "disabled"}`. It is explicit because Claude 5 models think when the block is absent; on Claude 4.x it equals omitting it. Some models reject it (Claude Opus 5.5, Fable 5.x) or accept it only at effort `high` or below (Claude Opus 5, Haiku 5.5). Claude Sonnet 5.5 turns up-front thinking off with `WithThinkingBetweenTools()` instead.
- **Showing thinking text**: Claude Opus 4.7 and later return thinking blocks with empty text by default. `WithThinkingDisplay(messages.ThinkingDisplaySummarized)` returns a readable summary. The signature carries the full thinking either way and is replayed unchanged. Display is rejected with `disabled` and `between_tools`.

```go
provider := messages.New(
    messages.WithAPIKey("sk-ant-..."),
    messages.WithThinkingAdaptive(),
    messages.WithThinkingDisplay(messages.ThinkingDisplaySummarized),
)
```

### Prompt Caching

Claude supports [prompt caching](https://docs.anthropic.com/en/docs/build-with-claude/prompt-caching): attach a `*sdk.CacheControl` breakpoint to a content block, and Anthropic caches the entire prefix up to and including it. Caching is a prefix match — a byte change anywhere before a breakpoint invalidates it — so place breakpoints at the end of stable content. Up to 4 breakpoints per request.

`CacheControl` is honored on system, user, assistant, and tool-result content blocks, and on tool definitions. Other providers ignore it.

```go
// Cache a large, stable system prompt and the tool list.
msgs := []sdk.Message{
    {Role: sdk.MessageRoleSystem, Content: []sdk.MessagePart{
        sdk.TextPart{Text: largeSystemPrompt, CacheControl: &sdk.CacheControl{Type: "ephemeral"}},
    }},
    sdk.UserMessage("..."),
}

tools := []sdk.ToolDefinition{
    {Name: "search", Parameters: searchSchema},
    // Breakpoint on the last tool caches all tool definitions above it.
    {Name: "calc", Parameters: calcSchema, CacheControl: &sdk.CacheControl{Type: "ephemeral"}},
}
```

For multi-turn and agentic flows, move the breakpoint forward to the last block of the most recently settled turn so the growing prefix is reused on the next request — a trailing tool result, or the assistant's text / tool call:

```go
sdk.ToolResultPart{ToolCallID: id, Result: out, CacheControl: &sdk.CacheControl{Type: "ephemeral"}}
```

Set `TTL: "1h"` for a 1-hour cache (higher write rate; the default empty TTL is 5 minutes). Cache activity is reported on `result.Usage`: `CachedInputTokens` (read), and under `InputTokenDetails`, `CacheReadTokens` / `CacheWriteTokens` with the per-TTL split in `CacheWrite5mTokens` / `CacheWrite1hTokens`.

### Supported Features

| Feature | Supported |
|---------|-----------|
| Text generation | ✅ |
| Streaming (SSE) | ✅ |
| Tool/function calling | ✅ |
| Vision (image inputs) | ✅ |
| Extended thinking | ✅ |
| Prompt caching (`cache_control`) | ✅ |
| Token usage reporting | ✅ |
| Cached token details | ✅ |
| ListModels / Test / TestModel | ✅ |

### API Endpoints for Discovery

| Method | API Endpoint |
|--------|-------------|
| `ListModels` | `GET /v1/models?limit=1000`, then `&after_id={last_id}` while `has_more` |
| `Test` | `GET /v1/models?limit=1` |
| `TestModel` | `GET /v1/models/{id}` |

`ListModels` fills limits and capabilities into model metadata; see
[Model Discovery](#model-discovery).

---

## Google Gemini Provider

The `provider/google/generativeai` package implements the [Google Generative AI API](https://ai.google.dev/api) for Gemini models.

### Basic Usage

```go
import "github.com/felinics/twilight/provider/google/generativeai"

provider := generativeai.New(
    generativeai.WithAPIKey("AIza..."),
)
model := provider.ChatModel("gemini-2.5-flash")
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key sent as `x-goog-api-key` header |
| `WithBaseURL(url)` | `https://generativelanguage.googleapis.com/v1beta` | Base URL |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |
| `WithHeaders(headers)` | none | Provider-wide headers; request-context headers take precedence |

### Model ID

The model ID can be a simple name or a full resource path:

```go
// Simple name — resolved to "models/gemini-2.5-flash"
model := provider.ChatModel("gemini-2.5-flash")

// Full path — used as-is
model := provider.ChatModel("publishers/google/models/gemini-2.5-flash")
```

### API Endpoints

| Operation | Endpoint |
|-----------|----------|
| Non-streaming | `POST {baseURL}/models/{modelId}:generateContent` |
| Streaming | `POST {baseURL}/models/{modelId}:streamGenerateContent?alt=sse` |

### How Messages Are Mapped

The provider automatically converts SDK messages to Google's format:

| SDK | Google API |
|-----|-----------|
| `System` param | `systemInstruction` field (separate from `contents`) |
| User message | `{role: "user", parts: [{text: "..."}, ...]}` |
| Assistant message | `{role: "model", parts: [{text: "..."}, {functionCall: ...}]}` |
| Tool result message | `{role: "user", parts: [{functionResponse: {name, response}}]}` |

### Tool Choice Mapping

| SDK `ToolChoice` | Google `functionCallingConfig.mode` |
|------------------|-------------------------------------|
| `"auto"` | `AUTO` |
| `"none"` | `NONE` |
| `"required"` | `ANY` |

### Thinking / Reasoning

Gemini 2.5+ models support thinking (reasoning). The model returns parts with `thought: true` which the provider maps to `Reasoning` in the result. The API returns thought parts only when `includeThoughts` is true:

```go
provider := generativeai.New(generativeai.WithAPIKey("AIza..."))
model := provider.ChatModel("gemini-3-pro-preview")

effort := "high"
result, _ := model.Generate(ctx, sdk.Request{
    Messages: []sdk.Message{
        sdk.UserMessage("What is 15 * 37? Think step by step."),
    },
    ReasoningEffort: &effort, // thinkingLevel HIGH, includeThoughts true
})
fmt.Println(result.Reasoning) // model's thinking process
fmt.Println(result.Text)      // final answer
```

`ReasoningEffort` maps to `thinkingLevel` (upper-cased) and sets `includeThoughts`. Tiers outside Google's enum, such as `xhigh`, are sent as-is and surface as a 400. The off spellings `none`, `disable` and `disabled` send no `thinkingConfig`, because `thinkingLevel` has no off member. To turn thinking off or to set a token budget on 2.5-generation models, use `WithThinking`:

```go
off := generativeai.ThinkingBudgetDisabled // 0; Flash/Lite only, 2.5 Pro rejects it
provider := generativeai.New(
    generativeai.WithAPIKey("AIza..."),
    generativeai.WithThinking(generativeai.ThinkingConfig{ThinkingBudget: &off}),
)
```

`WithThinking` takes precedence over `ReasoningEffort` and leaves `includeThoughts` to the caller.

Thought signatures are kept per block, including on parts that carry a signature with empty text, and are replayed on the next request. In a stream, a signed thought part closes its reasoning block, matching the one-block-per-part shape of a non-streamed response.

### Supported Features

| Feature | Supported |
|---------|-----------|
| Text generation | ✅ |
| Streaming (SSE) | ✅ |
| Tool/function calling | ✅ |
| Vision (image inputs) | ✅ |
| Thinking / Reasoning (Gemini 2.5+) | ✅ |
| JSON mode | ✅ |
| Token usage reporting | ✅ |
| Cached content token details | ✅ |
| ListModels / Test / TestModel | ✅ |

### API Endpoints for Discovery

| Method | API Endpoint |
|--------|-------------|
| `ListModels` | `GET /v1beta/models` |
| `Test` | `GET /v1beta/models?pageSize=1` |
| `TestModel` | `GET /v1beta/models/{id}` |

---

## Image Providers

Image providers implement the `sdk.ImageGenerationProvider` and/or `sdk.ImageEditProvider` interfaces and are separate from chat, embedding, and speech providers.

```go
type ImageGenerationProvider interface {
    DoGenerate(ctx context.Context, params *ImageGenerationParams) (*ImageResult, error)
}

type ImageEditProvider interface {
    DoEdit(ctx context.Context, params *ImageEditParams) (*ImageResult, error)
}
```

### OpenAI Images Provider

The `provider/openai/images` package provides image generation and editing via the OpenAI Images API (`/images/generations` and `/images/edits`).

#### Basic Usage

```go
import (
    "github.com/felinics/twilight/provider/openai/images"
    "github.com/felinics/twilight/sdk"
)

provider := images.New(
    images.WithAPIKey("sk-..."),
)

// Generation
genModel := provider.GenerationModel("gpt-image-1")
result, err := sdk.GenerateImage(ctx,
    sdk.WithImageGenerationModel(genModel),
    sdk.WithImagePrompt("A sunset over mountains"),
    sdk.WithImageSize("1024x1024"),
)

// Editing
editModel := provider.EditModel("gpt-image-1")
result, err := sdk.EditImage(ctx,
    sdk.WithImageEditModel(editModel),
    sdk.WithEditPrompt("Add a rainbow"),
    sdk.WithEditImages(sdk.ImageInput{
        Data:     pngBytes,
        Filename: "photo.png",
    }),
)
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key sent as `Authorization: Bearer <key>` |
| `WithBaseURL(url)` | `https://api.openai.com/v1` | Base URL for API requests |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |
| `WithHeaders(headers)` | none | Provider-wide headers; request-context headers take precedence |

#### Supported Models

| Model | Generation | Editing | Notes |
|-------|-----------|---------|-------|
| `dall-e-2` | Yes | Yes | Legacy; `size`: 256/512/1024; `n`: 1-10 |
| `dall-e-3` | Yes | No | `n` must be 1; supports `style` (vivid/natural) |
| `gpt-image-1` | Yes | Yes | GPT Image; supports `background`, `output_format`, `moderation` |
| `gpt-image-1-mini` | Yes | Yes | Smaller GPT Image variant |
| `gpt-image-1.5` | Yes | Yes | Latest GPT Image |

#### Generation Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `Prompt` | string | **Required.** Text description of the desired image |
| `N` | *int | Number of images (1-10; dall-e-3 only supports 1) |
| `Size` | string | Image size (e.g. `"1024x1024"`, `"1536x1024"`) |
| `Quality` | string | `"auto"`, `"low"`, `"medium"`, `"high"`, `"standard"`, `"hd"` |
| `Style` | string | dall-e-3 only: `"vivid"`, `"natural"` |
| `ResponseFormat` | string | dall-e-2/3: `"url"`, `"b64_json"` |
| `Background` | string | GPT Image: `"transparent"`, `"opaque"`, `"auto"` |
| `OutputFormat` | string | GPT Image: `"png"`, `"jpeg"`, `"webp"` |
| `OutputCompression` | *int | GPT Image, jpeg/webp: 0-100 |
| `Moderation` | string | GPT Image: `"low"`, `"auto"` |

#### Edit Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `Images` | []ImageInput | Source images (up to 16 for GPT Image) |
| `Prompt` | string | **Required.** Edit description |
| `Mask` | *ImageInput | Mask image (transparent regions = edit area) |
| `InputFidelity` | string | GPT Image: `"high"`, `"low"` |
| Other params | — | Same as generation: `N`, `Size`, `Quality`, `Background`, `OutputFormat`, `OutputCompression`, `Moderation` |

#### Edit Input Modes

The provider automatically selects the request format based on the `ImageInput` fields:

| `ImageInput` field set | Request format | Use case |
|----------------------|----------------|----------|
| `Data` ([]byte) | `multipart/form-data` | File upload (local images) |
| `URL` or `FileID` | JSON body | URL/file-ID reference (GPT Image) |

#### OpenAI-Compatible Endpoints

Any service implementing the OpenAI Images API works with `WithBaseURL`:

```go
provider := images.New(
    images.WithAPIKey("your-key"),
    images.WithBaseURL("https://your-compatible-api.com/v1"),
)
```

See [Images](images.md) for complete documentation.

### Alibaba Cloud DashScope Images Provider

The `provider/alibabacloud/images` package provides image generation via Alibaba Cloud Model Studio (DashScope), supporting Qwen-Image and Wan image models.

#### Basic Usage

```go
import (
    "github.com/felinics/twilight/provider/alibabacloud/images"
    "github.com/felinics/twilight/sdk"
)

provider := images.New(
    images.WithAPIKey("sk-..."),
)

model := provider.GenerationModel("qwen-image-max")
result, err := sdk.GenerateImage(ctx,
    sdk.WithImageGenerationModel(model),
    sdk.WithImagePrompt("A sunset over mountains"),
    sdk.WithImageSize("1024x1024"),
)
// result.Data[0].URL contains the generated image URL
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | DashScope / Model Studio API key sent as `Authorization: Bearer <key>` |
| `WithBaseURL(url)` | `https://dashscope.aliyuncs.com/api/v1` | Base URL; also accepts Model Studio compatible-mode URLs, which are normalized to the native `/api/v1` base |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |
| `WithPollInterval(d)` | `3s` | Polling interval for async image tasks |
| `WithPollTimeout(d)` | `5m` | Maximum time to wait for an async image task |

#### Endpoint Routing

DashScope exposes several image APIs; the provider selects the endpoint from the model ID:

| Models | Endpoint | Mode |
|--------|----------|------|
| `qwen-image`, `qwen-image-plus` | `/services/aigc/text2image/image-synthesis` | Async task + polling |
| `wanx-*`, `wan2.0`–`wan2.5` | `/services/aigc/text2image/image-synthesis` | Async task + polling |
| `flux-*`, `stable-diffusion-*` | `/services/aigc/text2image/image-synthesis` | Async task + polling |
| Other `qwen-image*` (e.g. `qwen-image-max`, `qwen-image-2.0-pro`, dated snapshots) | `/services/aigc/multimodal-generation/generation` | Synchronous |
| `z-image*` (e.g. `z-image-turbo`) | `/services/aigc/multimodal-generation/generation` | Synchronous |
| Other `wan*` (e.g. `wan2.6-t2i` and newer) | `/services/aigc/image-generation/generation` | Async task + polling |

Async tasks are polled via `GET /tasks/{task_id}` until they succeed, fail, or the poll timeout is reached.

#### Parameters

DashScope image models honor `Prompt`, `N`, and `Size` (converted from `"1024x1024"` to DashScope's `"1024*1024"` form). Other `ImageGenerationParams` fields are OpenAI-specific and ignored. Results are returned as image URLs, which expire after 24 hours — download them promptly.

---

## Embedding Providers

Embedding providers implement the `sdk.EmbeddingProvider` interface and are separate from chat providers. They generate vector representations of text for use in search, retrieval, clustering, and other similarity-based tasks.

```go
type EmbeddingProvider interface {
    DoEmbed(ctx context.Context, params EmbedParams) (*EmbedResult, error)
}
```

### OpenAI Embedding Provider

The `provider/openai/embedding` package provides text embeddings via the OpenAI `/embeddings` endpoint.

#### Basic Usage

```go
import (
    "github.com/felinics/twilight/provider/openai/embedding"
    "github.com/felinics/twilight/sdk"
)

provider := embedding.New(
    embedding.WithAPIKey("sk-..."),
)
model := provider.EmbeddingModel("text-embedding-3-small")

// Single embedding
vec, err := sdk.Embed(ctx, "Hello world", sdk.WithEmbeddingModel(model))

// Batch embeddings
result, err := sdk.EmbedMany(ctx,
    []string{"Hello", "World"},
    sdk.WithEmbeddingModel(model),
)
```

#### Custom Dimensions

Models like `text-embedding-3-small` and `text-embedding-3-large` support custom output dimensions:

```go
vec, err := sdk.Embed(ctx, "Hello world",
    sdk.WithEmbeddingModel(model),
    sdk.WithDimensions(256),
)
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key sent as `Authorization: Bearer <key>` |
| `WithBedrockRegion(region)` | disabled | Use AWS SigV4 with the default AWS credential chain for Bedrock's OpenAI-compatible endpoint |
| `WithBedrockCredentials(region, accessKeyID, secretAccessKey, sessionToken)` | disabled | Use AWS SigV4 with static AWS credentials for Bedrock |
| `WithBaseURL(url)` | `https://api.openai.com/v1` | Base URL for API requests |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |
| `WithHeaders(headers)` | none | Provider-wide headers; request-context headers take precedence |

#### OpenAI-Compatible Endpoints

Any service that implements the OpenAI Embeddings API works:

```go
// Ollama
provider := embedding.New(
    embedding.WithBaseURL("http://localhost:11434/v1"),
)
model := provider.EmbeddingModel("nomic-embed-text")
```

### Google Embedding Provider

The `provider/google/embedding` package provides text embeddings via the Google Generative AI API.

#### Basic Usage

```go
import (
    "github.com/felinics/twilight/provider/google/embedding"
    "github.com/felinics/twilight/sdk"
)

provider := embedding.New(
    embedding.WithAPIKey("AIza..."),
)
model := provider.EmbeddingModel("gemini-embedding-001")

vec, err := sdk.Embed(ctx, "Hello world", sdk.WithEmbeddingModel(model))
```

#### Task Types

Google embedding models support a `taskType` parameter to optimize the embedding for a specific use case:

```go
provider := embedding.New(
    embedding.WithAPIKey("AIza..."),
    embedding.WithTaskType("RETRIEVAL_DOCUMENT"),
)
```

| Task Type | Use Case |
|-----------|----------|
| `RETRIEVAL_QUERY` | Query text for search/retrieval |
| `RETRIEVAL_DOCUMENT` | Document text being indexed |
| `SEMANTIC_SIMILARITY` | Comparing text similarity |
| `CLASSIFICATION` | Text classification |
| `CLUSTERING` | Text clustering |
| `QUESTION_ANSWERING` | Question answering |
| `FACT_VERIFICATION` | Fact verification |
| `CODE_RETRIEVAL_QUERY` | Code search queries |

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key sent as `x-goog-api-key` header |
| `WithBaseURL(url)` | `https://generativelanguage.googleapis.com/v1beta` | Base URL |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |
| `WithTaskType(taskType)` | `""` | Default task type for all requests |

#### API Endpoints

| Scenario | Endpoint |
|----------|----------|
| Single value | `POST {baseURL}/models/{modelId}:embedContent` |
| Multiple values | `POST {baseURL}/models/{modelId}:batchEmbedContents` |

The provider automatically selects the optimal endpoint based on the number of input values.

---

## Speech Providers

Speech providers implement the `sdk.SpeechProvider` interface and are separate from chat and embedding providers. They convert text into audio.

```go
type SpeechProvider interface {
    DoSynthesize(ctx context.Context, params SpeechParams) (*SpeechResult, error)
    DoStream(ctx context.Context, params SpeechParams) (*SpeechStreamResult, error)
}
```

Speech uses an open-ended `Config map[string]any` in `SpeechParams`, allowing each provider to define its own configuration keys.

### Edge TTS Provider

The `provider/edge/speech` package provides free speech synthesis via Microsoft Edge's built-in TTS service. No API key required.

#### Basic Usage

```go
import (
    "github.com/felinics/twilight/provider/edge/speech"
    "github.com/felinics/twilight/sdk"
)

provider := speech.New()
model := provider.SpeechModel("edge-read-aloud")

result, err := sdk.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("Hello, world!"),
    sdk.WithSpeechConfig(map[string]any{
        "voice": "en-US-EmmaMultilingualNeural",
        "speed": 1.0,
    }),
)
```

#### Configuration Keys

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `voice` | `string` | `en-US-EmmaMultilingualNeural` | Edge TTS voice ID |
| `language` | `string` | Auto-detected from voice | BCP-47 language tag |
| `format` | `string` | `audio-24khz-48kbitrate-mono-mp3` | Output audio format |
| `speed` | `float64` | `0` (server default) | Speech rate (1.0 = normal) |
| `pitch` | `float64` | `0` | Pitch adjustment in Hz |

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithBaseURL(url)` | Bing WSS endpoint | Override WebSocket endpoint (for testing) |

---

### OpenAI TTS Provider

The `provider/openai/speech` package targets the `/audio/speech` endpoint. It works with the official OpenAI TTS API and any OpenAI-compatible proxy (OpenRouter, CometAPI, Player2, Index-TTS vLLM, unspeech, etc.).

#### Basic Usage

```go
import "github.com/felinics/twilight/provider/openai/speech"

provider := speech.New(
    speech.WithAPIKey("sk-..."),
)
model := provider.SpeechModel("tts-1")

result, err := sdk.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("Hello, world!"),
    sdk.WithSpeechConfig(map[string]any{
        "voice": "alloy",
    }),
)
```

#### Configuration Keys

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `voice` | `string` | `alloy` | Voice ID: alloy/ash/ballad/coral/echo/fable/onyx/nova/shimmer |
| `response_format` | `string` | `mp3` | Output format: mp3/opus/aac/flac/wav/pcm |
| `speed` | `float64` | `0` (server default) | Speech rate 0.25–4.0 |
| `instructions` | `string` | `""` | Style instructions (gpt-4o-mini-tts only) |

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key for `Authorization: Bearer` |
| `WithBaseURL(url)` | `https://api.openai.com/v1` | Override for proxies or testing |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |
| `WithHeaders(headers)` | none | Provider-wide headers; request-context headers take precedence |

#### OpenAI-Compatible Endpoints

```go
// OpenRouter
provider := speech.New(
    speech.WithAPIKey("sk-or-v1-..."),
    speech.WithBaseURL("https://openrouter.ai/api/v1"),
)

// Local Index-TTS via vLLM
provider := speech.New(
    speech.WithBaseURL("http://localhost:8000/v1"),
)
```

---

### ElevenLabs TTS Provider

The `provider/elevenlabs/speech` package targets the `/v1/text-to-speech/{voice_id}` (full) and `/v1/text-to-speech/{voice_id}/stream` (streaming) endpoints.

#### Basic Usage

```go
import "github.com/felinics/twilight/provider/elevenlabs/speech"

provider := speech.New(
    speech.WithAPIKey("your-elevenlabs-key"),
)
model := provider.SpeechModel("elevenlabs-tts")

result, err := sdk.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("Hello!"),
    sdk.WithSpeechConfig(map[string]any{
        "voice_id": "21m00Tcm4TlvDq8ikWAM",
        "model_id": "eleven_multilingual_v2",
    }),
)
```

#### Configuration Keys

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `voice_id` | `string` | `""` | **Required.** ElevenLabs voice ID |
| `model_id` | `string` | `eleven_multilingual_v2` | Model: eleven_turbo_v2_5/eleven_v3/… |
| `stability` | `float64` | `0.5` | Voice stability 0–1 |
| `similarity_boost` | `float64` | `0.75` | Voice similarity boost 0–1 |
| `output_format` | `string` | `mp3_44100_128` | Output format: mp3_44100_128/pcm_16000/… |
| `speed` | `float64` | `0` (server default) | Speech rate 0.25–4.0 |

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key sent as `xi-api-key` header |
| `WithBaseURL(url)` | `https://api.elevenlabs.io` | Override base URL |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |

---

### Deepgram TTS Provider

The `provider/deepgram/speech` package targets the `POST /v1/speak` endpoint using `Authorization: Token` authentication.

#### Basic Usage

```go
import "github.com/felinics/twilight/provider/deepgram/speech"

provider := speech.New(
    speech.WithAPIKey("your-deepgram-key"),
)
model := provider.SpeechModel("deepgram-tts")

result, err := sdk.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("Hello!"),
    sdk.WithSpeechConfig(map[string]any{
        "model": "aura-2-asteria-en",
    }),
)
```

#### Configuration Keys

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `model` | `string` | `aura-2-asteria-en` | Voice model: aura-2-*/aura-* series |
| `encoding` | `string` | `""` | Audio encoding: linear16/mulaw/alaw |
| `sample_rate` | `int` | `0` (server default) | Sample rate in Hz |
| `container` | `string` | `""` | Container format: wav/none |

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key sent as `Authorization: Token` |
| `WithBaseURL(url)` | `https://api.deepgram.com` | Override base URL |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |

---

### MiniMax TTS Provider

The `provider/minimax/speech` package targets the `POST /v1/t2a_v2` endpoint. The API returns audio as a hex-encoded string inside a JSON response; this provider decodes it automatically.

#### Basic Usage

```go
import "github.com/felinics/twilight/provider/minimax/speech"

provider := speech.New(
    speech.WithAPIKey("your-minimax-key"),
)
model := provider.SpeechModel("minimax-tts")

result, err := sdk.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("你好，世界！"),
    sdk.WithSpeechConfig(map[string]any{
        "voice_id": "Chinese_narrator_female",
        "model":    "speech-2.8-hd",
    }),
)
```

#### Configuration Keys

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `voice_id` | `string` | `English_expressive_narrator` | Voice ID |
| `model` | `string` | `speech-2.8-hd` | Model version |
| `speed` | `float64` | `0` (server default) | Speech speed |
| `vol` | `float64` | `0` (server default) | Volume |
| `pitch` | `int` | `0` | Pitch adjustment |
| `output_format` | `string` | `mp3` | Output format: mp3/pcm/flac/wav |
| `sample_rate` | `int` | `32000` | Sample rate in Hz |

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key for `Authorization: Bearer` |
| `WithBaseURL(url)` | `https://api.minimax.io` | Override base URL |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |

> **Note:** MiniMax streaming TTS is a paid-tier API feature. `DoStream` returns the fully synthesized audio as a single chunk (equivalent to `DoSynthesize`).

---

### Alibaba Cloud DashScope CosyVoice Provider

The `provider/alibabacloud/speech` package implements the DashScope CosyVoice WebSocket protocol (`wss://dashscope.aliyuncs.com/api-ws/v1/inference/`). It uses the `run-task` / `continue-task` / `finish-task` message flow with Bearer authentication during the WebSocket handshake.

#### Basic Usage

```go
import "github.com/felinics/twilight/provider/alibabacloud/speech"

provider := speech.New(
    speech.WithAPIKey("your-dashscope-api-key"),
)
model := provider.SpeechModel("cosyvoice-tts")

result, err := sdk.GenerateSpeech(ctx,
    sdk.WithSpeechModel(model),
    sdk.WithText("你好，世界！"),
    sdk.WithSpeechConfig(map[string]any{
        "model": "cosyvoice-v1",
        "voice": "longanyang",
    }),
)
```

#### Configuration Keys

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `model` | `string` | `cosyvoice-v1` | CosyVoice model: cosyvoice-v1/cosyvoice-v2/cosyvoice-v3-flash/… |
| `voice` | `string` | `""` | Voice ID (system voice or custom clone ID) |
| `format` | `string` | `mp3` | Output format: mp3/wav/pcm/opus |
| `sample_rate` | `int` | `22050` | Sample rate in Hz |
| `volume` | `int` | `0` (server default) | Volume 0–100 |
| `rate` | `float64` | `0` (server default) | Speech rate 0.5–2.0 |
| `pitch` | `float64` | `0` (server default) | Pitch multiplier 0.5–2.0 |

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | DashScope API key (Bearer auth during WS handshake) |
| `WithBaseURL(url)` | `wss://dashscope.aliyuncs.com/api-ws/v1/inference/` | Override WebSocket endpoint |

---

### Volcengine SAMI TTS Provider

The `provider/volcengine/speech` package implements the Volcengine SAMI (Speech Audio Machine Intelligence) TTS API. Authentication uses a two-step process: first obtain a bearer token via `open.volcengineapi.com/GetToken` (signed with Volcengine V4 HMAC-SHA256), then call `POST https://sami.bytedance.com/api/v1/invoke`.

#### Basic Usage

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
    sdk.WithText("你好，世界！"),
    sdk.WithSpeechConfig(map[string]any{
        "speaker": "zh_female_qingxin",
    }),
)
```

Tokens obtained via `GetToken` are cached for their full validity period (1 hour) and reused automatically.

#### Configuration Keys

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `speaker` | `string` | `""` | **Required.** SAMI voice speaker ID |
| `encoding` | `string` | `mp3` | Output format: mp3/wav/aac |
| `sample_rate` | `int` | `24000` | Sample rate in Hz |
| `speech_rate` | `int` | `0` | Speech rate [-50, 100] (100 = 2× speed) |
| `pitch_rate` | `int` | `0` | Pitch adjustment [-12, 12] |

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAccessKey(key)` | `""` | Volcengine AccessKeyID |
| `WithSecretKey(key)` | `""` | Volcengine SecretAccessKey |
| `WithAppKey(key)` | `""` | SAMI Application AppKey |
| `WithBaseURL(url)` | `https://sami.bytedance.com` | Override SAMI base URL |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |
| `WithToken(token)` | `""` | Inject a static pre-obtained token (skips GetToken; useful for testing) |

> **Note:** SAMI streaming is not exposed via a public non-SDK endpoint. `DoStream` wraps `DoSynthesize` and returns the fully synthesized audio as a single chunk.

See [Speech](speech.md) for complete documentation including voices, streaming, and custom provider implementation.

---

## Implementing a Custom Provider

To add support for a new AI backend, implement the `sdk.Provider` interface:

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

func (p *MyProvider) Name() string {
    return "my-provider"
}

func (p *MyProvider) ListModels(ctx context.Context) ([]sdk.Model, error) {
    // Fetch models from your backend's API
    return []sdk.Model{
        {ID: "my-model-v1", Provider: p},
    }, nil
}

func (p *MyProvider) Test(ctx context.Context) error {
    // Try a lightweight authenticated call. Return the backend's rejection as
    // a *sdk.APIError (with Kind set) and wrap transport errors with %w.
    _, err := p.ListModels(ctx)
    return err
}

func (p *MyProvider) TestModel(ctx context.Context, modelID string) (*sdk.ModelTestResult, error) {
    // Check if a specific model exists
    models, err := p.ListModels(ctx)
    if err != nil {
        return nil, err
    }
    for _, m := range models {
        if m.ID == modelID {
            return &sdk.ModelTestResult{Supported: true, Message: "supported"}, nil
        }
    }
    return &sdk.ModelTestResult{Supported: false, Message: "model not found"}, nil
}

func (p *MyProvider) ChatModel(id string) *sdk.Model {
    return &sdk.Model{ID: id, Provider: p}
}

func (p *MyProvider) DoGenerate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) {
    // Make HTTP request to your backend, reading req.Model, req.Messages,
    // req.Tools and the generation settings...
    return sdk.ModelResult{
        Text:         "response text",
        FinishReason: sdk.FinishReasonStop,
    }, nil
}

func (p *MyProvider) DoStream(ctx context.Context, req sdk.Request) (<-chan sdk.StreamPart, error) {
    ch := make(chan sdk.StreamPart, 64)

    go func() {
        defer close(ch)
        send := func(part sdk.StreamPart) bool {
            select {
            case ch <- part:
                return true
            case <-ctx.Done():
                return false
            }
        }
        if !send(&sdk.StartPart{}) {
            return
        }
        send(&sdk.StartStepPart{})
        send(&sdk.TextStartPart{})
        send(&sdk.TextDeltaPart{Text: "Hello"})
        send(&sdk.TextEndPart{})
        send(&sdk.FinishStepPart{FinishReason: sdk.FinishReasonStop})
        send(&sdk.FinishPart{FinishReason: sdk.FinishReasonStop})
    }()

    return ch, nil
}
```

Then use it exactly like the built-in provider:

```go
provider := myprovider.New("my-key")
model := provider.ChatModel("my-model-v1")

result, err := model.Generate(ctx, sdk.Request{
    Messages: []sdk.Message{sdk.UserMessage("Hello")},
})
```

## Next Steps

- [Images](images.md) — generate and edit images with OpenAI and Alibaba Cloud DashScope image models
- [Embeddings](embeddings.md) — generate vector embeddings with OpenAI and Google
- [Speech](speech.md) — speech synthesis with Edge TTS and custom providers
- [Tool Calling](tools.md) — define tools and run the calls the model makes
- [Streaming](streaming.md) — understand StreamPart types
- [API Reference](api-reference.md) — complete type and function reference
