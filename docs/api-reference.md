# API Reference

Complete reference for all exported types and functions in the Twilight AI SDK.

## Package `sdk`

### Model Calls

```go
func (m *Model) Generate(ctx context.Context, req Request) (ModelResult, error)
func (m *Model) Stream(ctx context.Context, req Request) (ModelStream, error)
```

`Generate` and `Stream` are one model call each: the `Request` is the complete input, the `ModelResult` (or the assembled result of the `ModelStream`) is the complete output. They are the chat entry points.

### Client

```go
type Client struct{}

func NewClient() *Client
```

The `Client` carries the embedding, image, speech, transcription and video calls documented below; each also exists as a package-level function.

---

### Provider

```go
type Provider interface {
    Name() string
    ListModels(ctx context.Context) ([]Model, error)
    Test(ctx context.Context) *ProviderTestResult
    TestModel(ctx context.Context, modelID string) (*ModelTestResult, error)
    DoGenerate(ctx context.Context, req Request) (ModelResult, error)
    DoStream(ctx context.Context, req Request) (<-chan StreamPart, error)
}
```

| Method | Purpose |
|--------|---------|
| `Name()` | Returns a provider identifier (e.g. `"openai-completions"`) |
| `ListModels(ctx)` | Fetches available models from the backend API |
| `Test(ctx)` | Health check: returns OK, Unhealthy, or Unreachable |
| `TestModel(ctx, id)` | Checks if a specific model ID is supported |
| `DoGenerate(ctx, req)` | Performs one non-streaming model call |
| `DoStream(ctx, req)` | Performs one streaming model call |

#### ProviderStatus

```go
type ProviderStatus string

const (
    ProviderStatusOK          ProviderStatus = "ok"          // Connected and healthy
    ProviderStatusUnhealthy   ProviderStatus = "unhealthy"   // Connected but health check failed
    ProviderStatusUnreachable ProviderStatus = "unreachable" // Cannot connect
)
```

#### ProviderTestResult

```go
type ProviderTestResult struct {
    Status  ProviderStatus
    Message string
    Error   error
}
```

#### ModelTestResult

```go
type ModelTestResult struct {
    Supported bool
    Message   string
}
```

### Model

```go
type Model struct {
    ID          string
    DisplayName string
    Provider    Provider
    Type        ModelType
    MaxTokens   int
}

type ModelType string
const ModelTypeChat      ModelType = "chat"
const ModelTypeEmbedding ModelType = "embedding"
```

#### Methods

```go
func (m *Model) Test(ctx context.Context) (*ModelTestResult, error)
```

Checks whether this model is supported by its provider. Delegates to `Provider.TestModel`.

---

### Messages

```go
type Message struct {
    Role    MessageRole
    Content []MessagePart
}
```

#### MessageRole

| Constant | Value |
|----------|-------|
| `MessageRoleUser` | `"user"` |
| `MessageRoleAssistant` | `"assistant"` |
| `MessageRoleSystem` | `"system"` |
| `MessageRoleTool` | `"tool"` |
| `MessageRoleDeveloper` | `"developer"` |

#### Message Constructors

```go
func UserMessage(text string, extra ...MessagePart) Message
func SystemMessage(text string) Message
func DeveloperMessage(text string) Message
func AssistantMessage(text string) Message
func ToolMessage(results ...ToolResultPart) Message
```

`UserMessage` accepts optional extra parts (e.g. `ImagePart`) after the text.

`Request.System` is the stable root instruction placed before the
conversation. Use `SystemMessage` when an instruction belongs at a specific
point in the message timeline. Providers preserve native instruction roles
when supported; otherwise developer messages become user messages and
mid-conversation system messages become XML-escaped `<system>` user messages.

```go
type MessageRoleCapabilities struct {
    Developer             bool
    MidConversationSystem bool
}
```

#### MessagePart Interface

```go
type MessagePart interface {
    PartType() MessagePartType
}
```

#### Part Types

```go
type TextPart struct {
    Text             string
    CacheControl     *CacheControl     // optional, Anthropic only
    ProviderMetadata ProviderMetadata  // opaque tokens a provider bound to the text
}

type ReasoningPart struct {
    ID               string
    Text             string
    Format           ReasoningFormat
    Model            string
    ProviderMetadata ProviderMetadata  // signature, encrypted payload, item id
}

type ImagePart struct {
    Image        string  // URL or base64
    MediaType    string  // optional, e.g. "image/png"
    CacheControl *CacheControl  // optional, Anthropic only
}

type FilePart struct {
    Data         string
    MediaType    string  // optional
    Filename     string  // optional
    CacheControl *CacheControl  // optional, Anthropic only
}

type ToolCallPart struct {
    ToolCallID       string
    ToolName         string
    Input            ToolArguments
    CacheControl     *CacheControl  // optional, Anthropic only
    ProviderMetadata ProviderMetadata
}

type ToolResultPart struct {
    ToolCallID   string
    ToolName     string
    Result       ToolOutput
    IsError      bool   // optional
    CacheControl *CacheControl  // optional, Anthropic only
}
```

`Message` supports full JSON serialization with automatic type discrimination.

#### CacheControl

`CacheControl` marks a content block as a prompt-caching breakpoint. It is
honored only by the Anthropic provider; other providers ignore it. See
[Prompt Caching](./providers.md#prompt-caching) for placement guidance.

```go
type CacheControl struct {
    Type string  // "ephemeral"
    TTL  string  // "" (5-minute default) | "1h"
}
```

---

### Generation

#### Request

The complete input of one model call — the seam a provider sees.

```go
type Request struct {
    Model            string
    System           string
    Messages         []Message
    Tools            []ToolDefinition
    ToolChoice       ToolChoice
    ResponseFormat   *ResponseFormat
    Temperature      *float64
    TopP             *float64
    MaxTokens        *int
    StopSequences    []string
    FrequencyPenalty *float64
    PresencePenalty  *float64
    Seed             *int
    ReasoningEffort  *string
    ReasoningSummary *string
    PromptCacheKey   *string
    ProviderOptions  map[string]json.RawMessage  // keyed by Provider.Name(); each provider decodes its own entry; CanonicalProviderOptions puts the values in canonical form
}
```

`Model` is filled from the `*Model` the call is made on; a `Request` that names a different model is an error.

#### ModelResult

What one model call produced. It carries no orchestration state — no steps, no output messages, no tool execution — because none of that crosses the provider seam.

```go
type ModelResult struct {
    Text                 string
    Reasoning            string           // the reasoning parts' text joined, for display
    ReasoningParts       []ReasoningPart  // one per block, in provider order; replay these
    TextProviderMetadata ProviderMetadata // a token the provider bound to the answer text
    FinishReason         FinishReason
    RawFinishReason      string
    Usage                Usage
    Sources              []Source
    Files                []GeneratedFile
    ToolCalls            []ToolCall
    Response             *ResponseMetadata
}
```

#### ModelStream

```go
type ModelStream struct {
    Parts  <-chan StreamPart            // closed when the stream ends
    Result func() (*ModelResult, error) // valid once Parts is drained
}

func CollectStream(ctx context.Context, parts <-chan StreamPart) (ModelResult, error)
```

One assembler turns parts into the `ModelResult`, so the streamed and the non-streamed path agree about a response.

#### ToolArguments, ToolOutput, ProviderMetadata

```go
type ToolArguments struct {
    JSON json.RawMessage // the model's arguments as a JSON document
    Text string          // the model's text when it was not a JSON document
}

func ParseToolArguments(text string) ToolArguments        // classifies provider text; "" is the empty object; a document is kept canonical
func ToolArgumentsJSON(v any) (ToolArguments, error)      // encodes v in canonical form
func (a ToolArguments) Valid() bool                        // a document (the zero value counts as {})
func (a ToolArguments) Unmarshal(v any) error              // ErrInvalidToolArguments when not Valid
func (a ToolArguments) String() string                     // the text as the model produced it, for display and logs
func (a ToolArguments) Object() json.RawMessage            // the document, or {} when invalid; what every wire field carries

type ToolOutput struct {
    Text string
    JSON json.RawMessage
}

func TextOutput(text string) ToolOutput
func JSONOutput(v any) (ToolOutput, error)                 // encodes v in canonical form
func RawJSONOutput(raw json.RawMessage) (ToolOutput, error) // re-encodes raw in canonical form; ErrInvalidJSON otherwise
func (o ToolOutput) String() string
func (o ToolOutput) IsJSON() bool

// Canonical form: the JSON the SDK carries through (tool arguments, tool
// outputs, provider options) is re-encoded as RFC 8785 (JCS), so equal
// documents are equal bytes and json.Marshal(Request) is deterministic.
func CanonicalJSON(raw []byte) (json.RawMessage, error)   // RFC 8785; numbers in binary64 form; ErrInvalidJSON for anything but one document (a repeated member or an escaped lone surrogate included)
func CanonicalProviderOptions(options map[string]json.RawMessage) (map[string]json.RawMessage, error)
var ErrInvalidJSON error

type ProviderMetadata map[string]map[string]string  // namespace → key → opaque token

func NewProviderMetadata(namespace string, values map[string]string) ProviderMetadata
func (m ProviderMetadata) Get(namespace, key string) string
func (m ProviderMetadata) Merge(other ProviderMetadata) ProviderMetadata
func (m ProviderMetadata) Clone() ProviderMetadata
```

Every value a model or a provider produces has a closed type here: arguments and outputs are JSON documents or text, and provider tokens are strings under the provider's namespace. Nothing on a `Request`, a `ModelResult` or a `Message` is an `any`.

#### FinishReason

| Constant | Value | Description |
|----------|-------|-------------|
| `FinishReasonStop` | `"stop"` | Normal completion |
| `FinishReasonLength` | `"length"` | Max tokens reached |
| `FinishReasonContentFilter` | `"content-filter"` | Content filter triggered |
| `FinishReasonToolCalls` | `"tool-calls"` | Model wants to call tools |
| `FinishReasonError` | `"error"` | An error occurred |
| `FinishReasonOther` | `"other"` | Provider-specific reason |
| `FinishReasonUnknown` | `"unknown"` | Unknown reason |

#### ToolDefinition & ToolChoice

What the model sees of a tool.

```go
type ToolDefinition struct {
    Name         string
    Description  string
    Parameters   *jsonschema.Schema
    CacheControl *CacheControl // optional, Anthropic only — caches tool definitions
}

func NewToolDefinition[T any](name, description string) (ToolDefinition, error) // Parameters inferred from T

type ToolChoiceMode string

const (
    ToolChoiceAuto     ToolChoiceMode = "auto"
    ToolChoiceNone     ToolChoiceMode = "none"
    ToolChoiceRequired ToolChoiceMode = "required"
    ToolChoiceTool     ToolChoiceMode = "tool"
)

type ToolChoice struct {
    Mode ToolChoiceMode
    Tool string // the tool to call when Mode is ToolChoiceTool
}
```

#### ToolCall

```go
type ToolCall struct {
    ToolCallID       string
    ToolName         string
    Input            ToolArguments
    ProviderMetadata ProviderMetadata
}
```

A caller answers each call with a `ToolResultPart` of the same `ToolCallID` in a tool message, after an assistant message that carries the step's reasoning parts, text and `ToolCallPart`s with their `ProviderMetadata`.

### Streaming

#### StreamPart Interface

```go
type StreamPart interface {
    Type() StreamPartType
}
```

#### All StreamPart Types

**Text:**

| Type | Key Fields |
|------|-----------|
| `*TextStartPart` | `ID` |
| `*TextDeltaPart` | `ID`, `Text` |
| `*TextEndPart` | `ID` |

**Reasoning:**

| Type | Key Fields |
|------|-----------|
| `*ReasoningStartPart` | `ID` |
| `*ReasoningDeltaPart` | `ID`, `Text` |
| `*ReasoningEndPart` | `ID` |

**Tool Input:**

| Type | Key Fields |
|------|-----------|
| `*ToolInputStartPart` | `ID`, `ToolName` |
| `*ToolInputDeltaPart` | `ID`, `Delta` |
| `*ToolInputEndPart` | `ID` |

**Tool Calls:**

| Type | Key Fields |
|------|-----------|
| `*StreamToolCallPart` | `ToolCallID`, `ToolName`, `Input ToolArguments`, `ProviderMetadata` |

**Sources & Files:**

| Type | Key Fields |
|------|-----------|
| `*StreamSourcePart` | `Source` |
| `*StreamFilePart` | `File` |

**Lifecycle:**

| Type | Key Fields |
|------|-----------|
| `*StartPart` | — |
| `*FinishPart` | `FinishReason`, `RawFinishReason`, `TotalUsage` |
| `*StartStepPart` | — |
| `*FinishStepPart` | `FinishReason`, `RawFinishReason`, `Usage`, `Response` |
| `*ErrorPart` | `Error` |

---

### Usage

```go
type Usage struct {
    InputTokens         int
    OutputTokens        int
    TotalTokens         int
    ReasoningTokens     int
    CachedInputTokens   int
    InputTokenDetails   InputTokenDetail
    OutputTokenDetails  OutputTokenDetail
}

type InputTokenDetail struct {
    NoCacheTokens      int
    CacheReadTokens    int  // tokens served from cache
    CacheWriteTokens   int  // tokens written to cache
    CacheWrite5mTokens int  // Anthropic: written to the 5-minute cache
    CacheWrite1hTokens int  // Anthropic: written to the 1-hour cache (ttl="1h")
}

type OutputTokenDetail struct {
    TextTokens      int
    ReasoningTokens int
    AudioTokens     int
}
```

### Source

```go
type Source struct {
    SourceType       string
    ID               string
    URL              string
    Title            string
    ProviderMetadata ProviderMetadata
}
```

### GeneratedFile

```go
type GeneratedFile struct {
    Data      string
    MediaType string
}
```

### ResponseMetadata

```go
type ResponseMetadata struct {
    ID        string
    ModelID   string
    Timestamp time.Time
    Headers   map[string]string
}
```

---

### Image Generation & Editing

#### ImageGenerationProvider

```go
type ImageGenerationProvider interface {
    DoGenerate(ctx context.Context, params *ImageGenerationParams) (*ImageResult, error)
}
```

The interface that image generation backends must implement.

#### ImageEditProvider

```go
type ImageEditProvider interface {
    DoEdit(ctx context.Context, params *ImageEditParams) (*ImageResult, error)
}
```

The interface that image editing backends must implement.

#### ImageGenerationModel

```go
type ImageGenerationModel struct {
    ID       string
    Provider ImageGenerationProvider
}
```

Represents an image generation model bound to an `ImageGenerationProvider`.

#### ImageEditModel

```go
type ImageEditModel struct {
    ID       string
    Provider ImageEditProvider
}
```

Represents an image edit model bound to an `ImageEditProvider`.

#### ImageGenerationParams

```go
type ImageGenerationParams struct {
    Model             *ImageGenerationModel
    Prompt            string
    N                 *int
    Size              string
    Quality           string
    Style             string
    ResponseFormat    string
    Background        string
    OutputFormat      string
    OutputCompression *int
    Moderation        string
    User              string
}
```

| Field | Description |
|-------|-------------|
| `Model` | **Required.** The image generation model to use |
| `Prompt` | **Required.** Text description of the desired image |
| `N` | Number of images (1-10; dall-e-3 only supports 1) |
| `Size` | Image size (e.g. `"1024x1024"`, `"1536x1024"`) |
| `Quality` | `"auto"`, `"low"`, `"medium"`, `"high"`, `"standard"`, `"hd"` |
| `Style` | dall-e-3 only: `"vivid"`, `"natural"` |
| `ResponseFormat` | dall-e-2/3: `"url"`, `"b64_json"` |
| `Background` | GPT Image: `"transparent"`, `"opaque"`, `"auto"` |
| `OutputFormat` | GPT Image: `"png"`, `"jpeg"`, `"webp"` |
| `OutputCompression` | GPT Image, jpeg/webp: 0-100 |
| `Moderation` | GPT Image: `"low"`, `"auto"` |
| `User` | End-user identifier |

#### ImageEditParams

```go
type ImageEditParams struct {
    Model             *ImageEditModel
    Images            []ImageInput
    Prompt            string
    Mask              *ImageInput
    N                 *int
    Size              string
    Quality           string
    Background        string
    OutputFormat      string
    OutputCompression *int
    InputFidelity     string
    Moderation        string
    ResponseFormat    string
    User              string
}
```

| Field | Description |
|-------|-------------|
| `Model` | **Required.** The image edit model to use |
| `Images` | Source images (up to 16 for GPT Image models) |
| `Prompt` | **Required.** Description of the edit |
| `Mask` | Mask image (transparent regions = edit area) |
| `InputFidelity` | GPT Image: `"high"`, `"low"` |
| Other fields | Same semantics as `ImageGenerationParams` |

#### ImageInput

```go
type ImageInput struct {
    Data      []byte
    MediaType string
    Filename  string
    URL       string
    FileID    string
}
```

Exactly one of `Data`, `URL`, or `FileID` should be set. When `Data` is set, the provider uses `multipart/form-data`; otherwise JSON.

#### ImageResult

```go
type ImageResult struct {
    Created int64
    Data    []ImageData
    Usage   ImageUsage
}
```

#### ImageData

```go
type ImageData struct {
    B64JSON       string
    URL           string
    RevisedPrompt string
}
```

| Field | Description |
|-------|-------------|
| `B64JSON` | Base64-encoded image (GPT Image default; dall-e with `b64_json` format) |
| `URL` | Temporary URL (dall-e-2/3 with `url` format; valid ~60 minutes) |
| `RevisedPrompt` | dall-e-3 only: the model's revised prompt |

#### ImageUsage

```go
type ImageUsage struct {
    TotalTokens       int
    InputTokens       int
    OutputTokens      int
    InputTokenDetails *ImageInputTokenDetails
}

type ImageInputTokenDetails struct {
    TextTokens  int
    ImageTokens int
}
```

#### Image Generate Options

All options are of type `ImageGenerateOption` (`func(*imageGenerateConfig)`).

| Function | Description |
|----------|-------------|
| `WithImageGenerationModel(model)` | **Required.** The image generation model |
| `WithImagePrompt(prompt)` | **Required.** Text description |
| `WithImageN(n)` | Number of images |
| `WithImageSize(size)` | Image dimensions |
| `WithImageQuality(quality)` | Quality level |
| `WithImageStyle(style)` | dall-e-3 style |
| `WithImageResponseFormat(format)` | dall-e-2/3 response format |
| `WithImageBackground(bg)` | GPT Image background |
| `WithImageOutputFormat(format)` | GPT Image output format |
| `WithImageOutputCompression(n)` | Compression level |
| `WithImageModeration(mod)` | GPT Image moderation |
| `WithImageUser(user)` | End-user identifier |

#### Image Edit Options

All options are of type `ImageEditOption` (`func(*imageEditConfig)`).

| Function | Description |
|----------|-------------|
| `WithImageEditModel(model)` | **Required.** The image edit model |
| `WithEditPrompt(prompt)` | **Required.** Edit description |
| `WithEditImages(images...)` | Source images |
| `WithEditMask(mask)` | Mask image |
| `WithEditN(n)` | Number of images |
| `WithEditSize(size)` | Output size |
| `WithEditQuality(quality)` | Quality level |
| `WithEditBackground(bg)` | Background transparency |
| `WithEditOutputFormat(format)` | Output format |
| `WithEditOutputCompression(n)` | Compression level |
| `WithEditInputFidelity(fidelity)` | Input fidelity |
| `WithEditModeration(mod)` | Moderation level |
| `WithEditResponseFormat(format)` | dall-e-2 response format |
| `WithEditUser(user)` | End-user identifier |

#### Client Methods

```go
func (c *Client) GenerateImage(ctx context.Context, options ...ImageGenerateOption) (*ImageResult, error)
func (c *Client) EditImage(ctx context.Context, options ...ImageEditOption) (*ImageResult, error)
```

| Method | Description |
|--------|-------------|
| `GenerateImage` | Generates images from a text prompt |
| `EditImage` | Edits or extends images given a prompt |

#### Package-Level Functions

```go
func GenerateImage(ctx context.Context, options ...ImageGenerateOption) (*ImageResult, error)
func EditImage(ctx context.Context, options ...ImageEditOption) (*ImageResult, error)
```

These use the default client instance.

---

### Embedding

#### EmbeddingProvider

```go
type EmbeddingProvider interface {
    DoEmbed(ctx context.Context, params EmbedParams) (*EmbedResult, error)
}
```

The interface that embedding backends must implement.

#### EmbeddingModel

```go
type EmbeddingModel struct {
    ID                   string
    Provider             EmbeddingProvider
    MaxEmbeddingsPerCall int
}
```

Represents an embedding model bound to an `EmbeddingProvider`. `MaxEmbeddingsPerCall` indicates the maximum number of input values per single API call (typically 2048).

#### EmbedParams

```go
type EmbedParams struct {
    Model      *EmbeddingModel
    Values     []string
    Dimensions *int
}
```

| Field | Description |
|-------|-------------|
| `Model` | **Required.** The embedding model to use |
| `Values` | Input texts to embed |
| `Dimensions` | Optional output dimensionality (not all models support this) |

#### EmbedResult

```go
type EmbedResult struct {
    Embeddings [][]float64
    Usage      EmbeddingUsage
}
```

| Field | Description |
|-------|-------------|
| `Embeddings` | One `[]float64` vector per input value |
| `Usage` | Token usage for the request |

#### EmbeddingUsage

```go
type EmbeddingUsage struct {
    Tokens int
}
```

#### Embed Options

All options are of type `EmbedOption` (`func(*embedConfig)`).

| Function | Description |
|----------|-------------|
| `WithEmbeddingModel(model *EmbeddingModel)` | **Required.** The embedding model to use |
| `WithDimensions(d int)` | Output dimensionality (model-dependent) |

#### Client Methods

```go
func (c *Client) Embed(ctx context.Context, value string, options ...EmbedOption) ([]float64, error)
func (c *Client) EmbedMany(ctx context.Context, values []string, options ...EmbedOption) (*EmbedResult, error)
```

| Method | Description |
|--------|-------------|
| `Embed` | Generates an embedding for a single string; returns the vector |
| `EmbedMany` | Generates embeddings for multiple strings; returns the full result |

#### Package-Level Functions

```go
func Embed(ctx context.Context, value string, options ...EmbedOption) ([]float64, error)
func EmbedMany(ctx context.Context, values []string, options ...EmbedOption) (*EmbedResult, error)
```

These use the default client instance, equivalent to `client.Embed` and `client.EmbedMany`.

---

### Speech

#### SpeechProvider

```go
type SpeechProvider interface {
    DoSynthesize(ctx context.Context, params SpeechParams) (*SpeechResult, error)
    DoStream(ctx context.Context, params SpeechParams) (*SpeechStreamResult, error)
}
```

The interface that speech synthesis backends must implement.

#### SpeechModel

```go
type SpeechModel struct {
    ID       string
    Provider SpeechProvider
}
```

Represents a speech model bound to a `SpeechProvider`.

#### SpeechParams

```go
type SpeechParams struct {
    Model  *SpeechModel
    Text   string
    Config map[string]any
}
```

| Field | Description |
|-------|-------------|
| `Model` | **Required.** The speech model to use |
| `Text` | **Required.** The text to synthesize |
| `Config` | Provider-specific configuration (e.g. voice, format, speed) |

#### SpeechResult

```go
type SpeechResult struct {
    Audio       []byte
    ContentType string
}
```

| Field | Description |
|-------|-------------|
| `Audio` | Raw audio bytes |
| `ContentType` | MIME type (e.g. `audio/mpeg`) |

#### SpeechStreamResult

```go
type SpeechStreamResult struct {
    Stream      <-chan []byte
    ContentType string
}

func (r *SpeechStreamResult) Bytes() ([]byte, error)
```

| Field/Method | Description |
|-------|-------------|
| `Stream` | Channel that yields raw audio chunks; closed when done |
| `ContentType` | MIME type (e.g. `audio/mpeg`) |
| `Bytes()` | Consumes the stream and returns concatenated audio data |

#### Speech Options

All options are of type `SpeechOption` (`func(*speechConfig)`).

| Function | Description |
|----------|-------------|
| `WithSpeechModel(model *SpeechModel)` | **Required.** The speech model to use |
| `WithText(text string)` | **Required.** The text to synthesize |
| `WithSpeechConfig(cfg map[string]any)` | Provider-specific configuration |

#### Client Methods

```go
func (c *Client) GenerateSpeech(ctx context.Context, options ...SpeechOption) (*SpeechResult, error)
func (c *Client) StreamSpeech(ctx context.Context, options ...SpeechOption) (*SpeechStreamResult, error)
```

| Method | Description |
|--------|-------------|
| `GenerateSpeech` | Synthesizes speech; returns complete audio |
| `StreamSpeech` | Synthesizes speech; returns streaming audio chunks |

#### Package-Level Functions

```go
func GenerateSpeech(ctx context.Context, options ...SpeechOption) (*SpeechResult, error)
func StreamSpeech(ctx context.Context, options ...SpeechOption) (*SpeechStreamResult, error)
```

These use the default client instance.

---

## Package `provider/edge/speech`

### Provider

```go
type Provider struct { /* unexported */ }

func New(options ...Option) *Provider
```

Implements `sdk.SpeechProvider`. Uses Microsoft Edge's built-in TTS via WebSocket. No API key required.

#### Options

```go
type Option func(*Provider)

func WithBaseURL(url string) Option
```

| Option | Default | Description |
|--------|---------|-------------|
| `WithBaseURL(url)` | Bing WSS endpoint | Override the WebSocket endpoint (for testing) |

#### Methods

```go
func (p *Provider) SpeechModel(id string) *sdk.SpeechModel
func (p *Provider) DoSynthesize(ctx context.Context, params sdk.SpeechParams) (*sdk.SpeechResult, error)
func (p *Provider) DoStream(ctx context.Context, params sdk.SpeechParams) (*sdk.SpeechStreamResult, error)
```

| Method | Description |
|--------|-------------|
| `SpeechModel(id)` | Creates a `SpeechModel` bound to this provider. Default model: `edge-read-aloud` |
| `DoSynthesize` | Synthesizes complete audio via WebSocket |
| `DoStream` | Synthesizes streaming audio chunks via WebSocket |

#### Configuration Keys

The Edge provider reads these keys from `SpeechParams.Config`:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `voice` | `string` | `en-US-EmmaMultilingualNeural` | Voice ID |
| `language` | `string` | Auto-detected | BCP-47 language tag |
| `format` | `string` | `audio-24khz-48kbitrate-mono-mp3` | Output format |
| `speed` | `float64` | `0` | Speech rate (1.0 = normal) |
| `pitch` | `float64` | `0` | Pitch in Hz |

#### Package-Level Variables

```go
var EdgeTTSVoices map[string][]string  // language tag → voice IDs
```

#### Helper Functions

```go
func LookupVoiceLang(voiceID string) (string, bool)
```

Returns the language tag for a voice ID, or `("", false)` if unknown.

---

## Package `provider/openai/images`

### Provider

```go
type Provider struct { /* unexported */ }

func New(options ...Option) *Provider
```

Implements `sdk.ImageGenerationProvider` and `sdk.ImageEditProvider`. Uses the OpenAI Images API (`/images/generations` and `/images/edits`).

#### Options

```go
type Option func(*Provider)

func WithAPIKey(apiKey string) Option
func WithBaseURL(baseURL string) Option
func WithHTTPClient(client *http.Client) Option
```

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key sent as `Authorization: Bearer <key>` |
| `WithBaseURL(url)` | `https://api.openai.com/v1` | Base URL for API requests |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |

#### Methods

```go
func (p *Provider) GenerationModel(id string) *sdk.ImageGenerationModel
func (p *Provider) EditModel(id string) *sdk.ImageEditModel
func (p *Provider) DoGenerate(ctx context.Context, params *sdk.ImageGenerationParams) (*sdk.ImageResult, error)
func (p *Provider) DoEdit(ctx context.Context, params *sdk.ImageEditParams) (*sdk.ImageResult, error)
```

| Method | Description |
|--------|-------------|
| `GenerationModel(id)` | Creates an `ImageGenerationModel` bound to this provider |
| `EditModel(id)` | Creates an `ImageEditModel` bound to this provider |
| `DoGenerate` | Sends `POST /images/generations` (JSON) |
| `DoEdit` | Sends `POST /images/edits` (multipart when `Data` bytes present, JSON otherwise) |

#### Supported Models

- Generation: `dall-e-2`, `dall-e-3`, `gpt-image-1`, `gpt-image-1-mini`, `gpt-image-1.5`
- Editing: `gpt-image-1`, `gpt-image-1-mini`, `gpt-image-1.5`, `dall-e-2`

---

## Package `provider/openai/embedding`

### Provider

```go
type Provider struct { /* unexported */ }

func New(options ...Option) *Provider
```

Implements `sdk.EmbeddingProvider`. Uses the OpenAI Embeddings API (`/embeddings`).

#### Options

```go
type Option func(*Provider)

func WithAPIKey(apiKey string) Option
func WithBaseURL(baseURL string) Option
func WithHTTPClient(client *http.Client) Option
```

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key sent as `Authorization: Bearer <key>` |
| `WithBaseURL(url)` | `https://api.openai.com/v1` | Base URL for API requests |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |

#### Methods

```go
func (p *Provider) EmbeddingModel(id string) *sdk.EmbeddingModel
func (p *Provider) DoEmbed(ctx context.Context, params sdk.EmbedParams) (*sdk.EmbedResult, error)
```

| Method | Description |
|--------|-------------|
| `EmbeddingModel(id)` | Creates an `EmbeddingModel` bound to this provider (MaxEmbeddingsPerCall: 2048) |
| `DoEmbed(ctx, params)` | Sends a `POST /embeddings` request with `encoding_format: "float"` |

#### Supported Models

Any model available via the OpenAI `/embeddings` endpoint, including:
- `text-embedding-3-small`
- `text-embedding-3-large`
- `text-embedding-ada-002`

---

## Package `provider/google/embedding`

### Provider

```go
type Provider struct { /* unexported */ }

func New(options ...Option) *Provider
```

Implements `sdk.EmbeddingProvider`. Uses the Google Generative AI Embedding API.

#### Options

```go
type Option func(*Provider)

func WithAPIKey(apiKey string) Option
func WithBaseURL(baseURL string) Option
func WithHTTPClient(client *http.Client) Option
func WithTaskType(taskType string) Option
```

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key sent as `x-goog-api-key` header |
| `WithBaseURL(url)` | `https://generativelanguage.googleapis.com/v1beta` | Base URL |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |
| `WithTaskType(taskType)` | `""` | Default task type for all requests |

#### Task Types

| Value | Use Case |
|-------|----------|
| `RETRIEVAL_QUERY` | Query text for search/retrieval |
| `RETRIEVAL_DOCUMENT` | Document text being indexed |
| `SEMANTIC_SIMILARITY` | Comparing text similarity |
| `CLASSIFICATION` | Text classification |
| `CLUSTERING` | Text clustering |
| `QUESTION_ANSWERING` | Question answering |
| `FACT_VERIFICATION` | Fact verification |
| `CODE_RETRIEVAL_QUERY` | Code search queries |

#### Methods

```go
func (p *Provider) EmbeddingModel(id string) *sdk.EmbeddingModel
func (p *Provider) DoEmbed(ctx context.Context, params sdk.EmbedParams) (*sdk.EmbedResult, error)
```

| Method | Description |
|--------|-------------|
| `EmbeddingModel(id)` | Creates an `EmbeddingModel` bound to this provider (MaxEmbeddingsPerCall: 2048) |
| `DoEmbed(ctx, params)` | Single value: `embedContent`; multiple values: `batchEmbedContents` |

#### Supported Models

- `gemini-embedding-001`
- `text-embedding-004`

---

## Package `provider/openai/completions`

### Provider

```go
type Provider struct { /* unexported */ }

func New(options ...Option) *Provider
```

Implements `sdk.Provider`. Uses the OpenAI Chat Completions API (`/chat/completions`).

#### Options

```go
type Option func(*Provider)

func WithAPIKey(apiKey string) Option
func WithBaseURL(baseURL string) Option
func WithHTTPClient(client *http.Client) Option
func WithMessageRoleCapabilities(capabilities sdk.MessageRoleCapabilities) Option
func WithDeepSeekChatCompletionsCompat() Option
func WithMiniMaxChatCompletionsCompat() Option
```

OpenAI's defaults enable native developer and mid-conversation system roles.
Use `WithMessageRoleCapabilities` to disable either role for a compatible
endpoint that does not implement the full OpenAI message-role contract.

`WithDeepSeekChatCompletionsCompat` keeps the generic `/chat/completions`
transport while adapting DeepSeek's thinking toggle: `WithReasoningEffort("none")`
sends `thinking: {type: "disabled"}` and omits `reasoning_effort`.

`WithMiniMaxChatCompletionsCompat` keeps the generic `/chat/completions`
transport while adapting MiniMax: it sends `reasoning_split: true` (so reasoning
is returned in the `reasoning_details` field instead of inline `<think>` tags)
and maps reasoning effort onto MiniMax's `thinking` toggle —
`WithReasoningEffort("none")` sends `thinking: {type: "disabled"}`, any other
effort sends `thinking: {type: "adaptive"}`, and `reasoning_effort` is omitted.
The provider reads `reasoning_details` as reasoning text and preserves it in
assistant history for later tool-call turns.

#### Methods

```go
func (p *Provider) Name() string                  // "openai-completions"
func (p *Provider) ChatModel(id string) *sdk.Model
func (p *Provider) ListModels(ctx context.Context) ([]sdk.Model, error)
func (p *Provider) Test(ctx context.Context) *sdk.ProviderTestResult
func (p *Provider) TestModel(ctx context.Context, modelID string) (*sdk.ModelTestResult, error)
func (p *Provider) DoGenerate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error)
func (p *Provider) DoStream(ctx context.Context, req sdk.Request) (<-chan sdk.StreamPart, error)
```

| Method | API Endpoint |
|--------|-------------|
| `ListModels` | `GET /models` |
| `Test` | `GET /models?limit=1` |
| `TestModel` | `GET /models/{id}` |

---

## Package `provider/openai/responses`

### Provider

```go
type Provider struct { /* unexported */ }

func New(options ...Option) *Provider
```

Implements `sdk.Provider`. Uses the OpenAI Responses API (`/responses`). Supports reasoning models (o3, o4-mini) with first-class reasoning summaries, URL citation annotations, and a flat input format.

#### Options

```go
type Option func(*Provider)

func WithAPIKey(apiKey string) Option
func WithBaseURL(baseURL string) Option
func WithHTTPClient(client *http.Client) Option
```

#### Methods

```go
func (p *Provider) Name() string                  // "openai-responses"
func (p *Provider) ChatModel(id string) *sdk.Model
func (p *Provider) ListModels(ctx context.Context) ([]sdk.Model, error)
func (p *Provider) Test(ctx context.Context) *sdk.ProviderTestResult
func (p *Provider) TestModel(ctx context.Context, modelID string) (*sdk.ModelTestResult, error)
func (p *Provider) DoGenerate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error)
func (p *Provider) DoStream(ctx context.Context, req sdk.Request) (<-chan sdk.StreamPart, error)
```

| Method | API Endpoint |
|--------|-------------|
| `ListModels` | `GET /models` |
| `Test` | `GET /models?limit=1` |
| `TestModel` | `GET /models/{id}` |

#### Responses API-Specific Behavior

**Input Conversion**: The provider converts `sdk.Message` types into the Responses API's flat input format:

| SDK Message | Responses Input Type |
|-------------|---------------------|
| `Request.System` | Top-level `instructions` |
| System message | `{ "type": "message", "role": "system" }` |
| Developer message | `{ "type": "message", "role": "developer" }` |
| User message (text) | `{ "type": "message", "role": "user" }` |
| User message (image) | Content part with `{ "type": "input_image" }` |
| Assistant message | `{ "type": "message", "role": "assistant" }` |
| Assistant reasoning | `{ "type": "reasoning" }` item |
| Tool call | `{ "type": "function_call" }` |
| Tool result | `{ "type": "function_call_output" }` |

**Output Parsing**: Responses API output items are mapped to SDK types:

| Responses Output | SDK Result |
|-----------------|------------|
| `message` with text content | `ModelResult.Text` |
| `reasoning` | `ModelResult.ReasoningParts` |
| `function_call` | `ModelResult.ToolCalls` |
| URL citation annotations | `ModelResult.Sources` |

**Finish Reason Mapping**:

| API Condition | SDK FinishReason |
|--------------|-----------------|
| No `incomplete_details` | `stop` |
| `incomplete_details.reason == "max_output_tokens"` | `length` |
| `incomplete_details.reason == "content_filter"` | `content-filter` |
| Has function calls | `tool-calls` |

**Streaming Events**: The provider handles these SSE event types:

| SSE Event | SDK StreamPart |
|-----------|---------------|
| `response.output_text.delta` | `TextDeltaPart` |
| `response.reasoning_summary_text.delta` | `ReasoningDeltaPart` |
| `response.function_call_arguments.delta` | `ToolInputDeltaPart` |
| `response.output_item.done` (function_call) | `ToolInputEndPart` |
| `response.output_text.annotation.added` (url_citation) | `StreamSourcePart` |
| `response.completed` / `response.incomplete` | `FinishStepPart` + `FinishPart` |

---

## Package `provider/openai/codex`

### Provider

```go
type Provider struct { /* unexported */ }

func New(options ...Option) *Provider
```

Implements `sdk.Provider`. Uses the OpenAI Codex backend API (`/codex/responses`) with SSE streaming. Targets Codex-specific models (gpt-5.x-codex series) with encrypted reasoning content support.

### Model Catalog

```go
type ModelDescriptor struct {
    ID                string
    DisplayName       string
    SupportsToolCall  bool
    SupportsReasoning bool
    ReasoningEfforts  []string
}

func Catalog() []ModelDescriptor
```

Returns the static model catalog. `ListModels` delegates to `Catalog()` (no HTTP call).

#### Options

```go
type Option func(*Provider)

func WithAccessToken(token string) Option
func WithAPIKey(token string) Option          // alias for WithAccessToken
func WithAccountID(accountID string) Option
func WithOriginator(originator string) Option
func WithBaseURL(baseURL string) Option
func WithHTTPClient(client *http.Client) Option
```

| Option | Default |
|--------|---------|
| `WithBaseURL` | `https://chatgpt.com/backend-api` |
| `WithOriginator` | `"codex_cli_rs"` |
| `WithHTTPClient` | `&http.Client{}` |
| `WithAccountID` | Auto-extracted from JWT |

#### Methods

```go
func (p *Provider) Name() string                  // "openai-codex"
func (p *Provider) ChatModel(id string) *sdk.Model
func (p *Provider) ListModels(ctx context.Context) ([]sdk.Model, error)
func (p *Provider) Test(ctx context.Context) *sdk.ProviderTestResult
func (p *Provider) TestModel(ctx context.Context, modelID string) (*sdk.ModelTestResult, error)
func (p *Provider) DoGenerate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error)
func (p *Provider) DoStream(ctx context.Context, req sdk.Request) (<-chan sdk.StreamPart, error)
```

| Method | API Endpoint |
|--------|-------------|
| `ListModels` | Static catalog (no HTTP) |
| `Test` / `TestModel` | `POST /codex/responses` (probe) |

#### Codex-Specific Behavior

**Input Conversion**: The provider converts `sdk.Message` types into the Codex flat input format:

| SDK Message | Codex Input |
|-------------|-------------|
| System message / `System` param | `instructions` field (joined with `\n\n`) |
| User message (text) | `{type: "input_text"}` in user content |
| User message (image) | `{type: "input_image"}` in user content |
| Assistant message (text) | `{type: "output_text"}` in assistant content |
| Assistant reasoning | `{type: "reasoning", summary: [...], encrypted_content: "..."}` |
| Tool call | `{type: "function_call", call_id, name, arguments}` |
| Tool result | `{type: "function_call_output", call_id, output}` |

**Streaming Events**: Codex SSE events map to SDK `StreamPart` types:

| SSE Event | SDK StreamPart |
|-----------|---------------|
| `response.created` | Captures response ID, model, timestamp |
| `response.output_item.added` (message) | `TextStartPart` |
| `response.output_item.added` (reasoning) | `ReasoningStartPart` (with encrypted content metadata) |
| `response.output_item.added` (function_call) | `ToolInputStartPart` |
| `response.output_text.delta` | `TextDeltaPart` |
| `response.reasoning_summary_text.delta` | `ReasoningDeltaPart` |
| `response.function_call_arguments.delta` | `ToolInputDeltaPart` |
| `response.output_item.done` (function_call) | `ToolInputEndPart` + `StreamToolCallPart` |
| `response.completed` / `response.incomplete` | `FinishStepPart` + `FinishPart` |

**Encrypted Reasoning**: When the model returns reasoning with encrypted content, it is preserved in `ReasoningStartPart.ProviderMetadata["openai"]["reasoningEncryptedContent"]` and round-tripped back via `ReasoningPart.ProviderMetadata` in follow-up turns.
