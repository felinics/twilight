package transcription

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"path"
	"strconv"
	"strings"

	"github.com/felinics/twilight/internal/errorformat"
	"github.com/felinics/twilight/internal/utils"
	sdk "github.com/felinics/twilight/sdk"
)

// providerName identifies this package in APIError.Provider.
const providerName = "openrouter-transcription"

const (
	defaultModelID = "openai/gpt-4o-mini-transcribe"
	defaultBaseURL = "https://openrouter.ai/api/v1"
)

type Option func(*Provider)

func WithAPIKey(key string) Option { return func(p *Provider) { p.apiKey = key } }
func WithBaseURL(url string) Option {
	return func(p *Provider) { p.baseURL = strings.TrimRight(url, "/") }
}
func WithHTTPClient(hc *http.Client) Option { return func(p *Provider) { p.httpClient = hc } }

type Provider struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
	catalog    *modelCatalog
}

func New(opts ...Option) *Provider {
	p := &Provider{baseURL: defaultBaseURL, httpClient: &http.Client{}, catalog: newModelCatalog()}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *Provider) TranscriptionModel(id string) *sdk.TranscriptionModel {
	if id == "" {
		id = defaultModelID
	}
	return &sdk.TranscriptionModel{ID: id, Provider: p}
}

func (p *Provider) ListModels(ctx context.Context) ([]*sdk.TranscriptionModel, error) {
	rawModels, err := p.fetchModels(ctx)
	if err != nil {
		return nil, err
	}
	p.catalog.put(ctx, rawModels)

	models := make([]*sdk.TranscriptionModel, 0, len(rawModels))
	for _, m := range rawModels {
		if isAudioInputModel(m.ID, m.Architecture.InputModalities) {
			models = append(models, p.TranscriptionModel(m.ID))
		}
	}
	if len(models) == 0 {
		return nil, errors.New("openrouter transcription: no transcription-capable models returned by provider")
	}
	return models, nil
}

// fetchModels returns OpenRouter's full model catalog, including models whose
// only output is a transcription.
func (p *Provider) fetchModels(ctx context.Context) ([]openRouterModel, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/models?output_modalities=all", http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("openrouter transcription: build list models request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.apiKey)

	httpResp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openrouter transcription: list models request failed: %w", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("openrouter transcription: list models: %w", utils.NewHTTPError(providerName, httpResp, errorformat.DecodeOpenRouter))
	}
	rawModels, err := decodeOpenRouterModels(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("openrouter transcription: decode list models response: %w", err)
	}
	return rawModels, nil
}

type openRouterModel struct {
	ID           string `json:"id"`
	Architecture struct {
		InputModalities  []string `json:"input_modalities"`
		OutputModalities []string `json:"output_modalities"`
	} `json:"architecture"`
}

func decodeOpenRouterModels(r io.Reader) ([]openRouterModel, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var wrapped struct {
		Data []openRouterModel `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapped); err == nil && len(wrapped.Data) > 0 {
		return wrapped.Data, nil
	}

	var direct []openRouterModel
	if err := json.Unmarshal(body, &direct); err != nil {
		return nil, err
	}
	return direct, nil
}

func isAudioInputModel(id string, inputs []string) bool {
	for _, input := range inputs {
		if strings.EqualFold(input, "audio") {
			return true
		}
	}
	lowerID := strings.ToLower(id)
	return strings.Contains(lowerID, "transcribe")
}

// audioConfig holds the keys read from TranscriptionParams.Config.
//
// Prompt applies only to chat models (/chat/completions). The other keys apply
// only to transcription-only models (/audio/transcriptions), whose API has no
// prompt field.
type audioConfig struct {
	Prompt                 string
	Language               string
	Temperature            *float64
	ResponseFormat         string
	TimestampGranularities []string
	Keyterms               []string
	Diarize                *bool
}

func parseConfig(cfg map[string]any) audioConfig {
	ac := audioConfig{}
	if cfg == nil {
		return ac
	}
	if v, ok := cfg["prompt"].(string); ok && v != "" {
		ac.Prompt = v
	}
	if v, ok := cfg["language"].(string); ok && v != "" {
		ac.Language = v
	}
	if v, ok := utils.ToFloat64(cfg["temperature"]); ok {
		ac.Temperature = &v
	}
	if v, ok := cfg["response_format"].(string); ok && v != "" {
		ac.ResponseFormat = v
	}
	ac.TimestampGranularities = stringList(cfg["timestamp_granularities"])
	ac.Keyterms = stringList(cfg["keyterms"])
	if v, ok := cfg["diarize"].(bool); ok {
		ac.Diarize = &v
	}
	return ac
}

// stringList accepts a string, []string or []any of strings.
func stringList(v any) []string {
	switch list := v.(type) {
	case string:
		if list != "" {
			return []string{list}
		}
	case []string:
		out := make([]string, 0, len(list))
		for _, s := range list {
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(list))
		for _, item := range list {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// DoTranscribe sends transcription-only models (catalog modality
// audio->transcription) to /audio/transcriptions and every other model to
// /chat/completions with the audio as an input_audio part.
func (p *Provider) DoTranscribe(ctx context.Context, params sdk.TranscriptionParams) (*sdk.TranscriptionResult, error) {
	cfg := parseConfig(params.Config)
	modelID := defaultModelID
	if params.Model != nil && params.Model.ID != "" {
		modelID = params.Model.ID
	}
	if p.usesTranscriptionsEndpoint(ctx, modelID) {
		return p.transcribe(ctx, &params, modelID, cfg)
	}
	return p.chatTranscribe(ctx, &params, modelID, cfg)
}

func (p *Provider) transcribe(ctx context.Context, params *sdk.TranscriptionParams, modelID string, cfg audioConfig) (*sdk.TranscriptionResult, error) {
	body, contentType, err := buildMultipartBody(params, modelID, cfg)
	if err != nil {
		return nil, fmt.Errorf("openrouter transcription: build request body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/audio/transcriptions", body)
	if err != nil {
		return nil, fmt.Errorf("openrouter transcription: build request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openrouter transcription: request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("openrouter transcription: transcribe: %w", utils.NewHTTPError(providerName, resp, errorformat.DecodeOpenRouter))
	}

	var payload transcriptionResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("openrouter transcription: decode response: %w", err)
	}
	return payload.result(), nil
}

// buildMultipartBody encodes an OpenRouter /audio/transcriptions request.
// OpenRouter derives the audio format from the filename extension or the file
// part's content type, so both are always set.
func buildMultipartBody(params *sdk.TranscriptionParams, modelID string, cfg audioConfig) (*bytes.Buffer, string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	fields := [][2]string{{"model", modelID}}
	if cfg.Language != "" {
		fields = append(fields, [2]string{"language", cfg.Language})
	}
	if cfg.Temperature != nil {
		fields = append(fields, [2]string{"temperature", strconv.FormatFloat(*cfg.Temperature, 'f', -1, 64)})
	}
	if cfg.ResponseFormat != "" {
		fields = append(fields, [2]string{"response_format", cfg.ResponseFormat})
	}
	for _, g := range cfg.TimestampGranularities {
		fields = append(fields, [2]string{"timestamp_granularities[]", g})
	}
	for _, term := range cfg.Keyterms {
		fields = append(fields, [2]string{"keyterms", term})
	}
	if cfg.Diarize != nil {
		fields = append(fields, [2]string{"diarize", strconv.FormatBool(*cfg.Diarize)})
	}
	for _, f := range fields {
		if err := w.WriteField(f[0], f[1]); err != nil {
			return nil, "", err
		}
	}

	format := audioFormatFromContentType(params.ContentType, params.Filename)
	filename := params.Filename
	if filename == "" {
		filename = "audio"
	}
	if path.Ext(filename) == "" {
		filename += "." + format
	}
	partType := params.ContentType
	if partType == "" {
		partType = "application/octet-stream"
	}
	header := make(textproto.MIMEHeader)
	// Same quoting as multipart.Writer.CreateFormFile; OpenRouter rejects the
	// unquoted, reordered parameters mime.FormatMediaType would produce.
	header.Set("Content-Disposition", `form-data; name="file"; filename="`+quoteEscaper.Replace(filename)+`"`)
	header.Set("Content-Type", partType)
	part, err := w.CreatePart(header)
	if err != nil {
		return nil, "", err
	}
	if _, err := part.Write(params.Audio); err != nil {
		return nil, "", err
	}
	if err := w.Close(); err != nil {
		return nil, "", err
	}
	return &buf, w.FormDataContentType(), nil
}

var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

// transcriptionResponse is OpenRouter's STTResponse. language, duration and
// segments appear only with response_format verbose_json; words only when the
// provider returns word timestamps.
type transcriptionResponse struct {
	Text     string  `json:"text"`
	Language string  `json:"language"`
	Duration float64 `json:"duration"`
	Words    []struct {
		Word         string  `json:"word"`
		Start        float64 `json:"start"`
		End          float64 `json:"end"`
		Speaker      *int64  `json:"speaker"`
		SpeakerLabel string  `json:"speaker_label"`
	} `json:"words"`
	Usage struct {
		Seconds float64 `json:"seconds"`
	} `json:"usage"`
}

func (r *transcriptionResponse) result() *sdk.TranscriptionResult {
	out := &sdk.TranscriptionResult{
		Text:            r.Text,
		Language:        r.Language,
		DurationSeconds: r.Duration,
	}
	if out.DurationSeconds == 0 {
		// usage.seconds is the billed input audio duration, present in plain json too.
		out.DurationSeconds = r.Usage.Seconds
	}
	if len(r.Words) > 0 {
		out.Words = make([]sdk.TranscriptionWord, 0, len(r.Words))
		for _, w := range r.Words {
			speaker := w.SpeakerLabel
			if speaker == "" && w.Speaker != nil {
				speaker = strconv.FormatInt(*w.Speaker, 10)
			}
			out.Words = append(out.Words, sdk.TranscriptionWord{Text: w.Word, Start: w.Start, End: w.End, SpeakerID: speaker})
		}
	}
	return out
}

func (p *Provider) chatTranscribe(ctx context.Context, params *sdk.TranscriptionParams, modelID string, cfg audioConfig) (*sdk.TranscriptionResult, error) {
	format := audioFormatFromContentType(params.ContentType, params.Filename)
	prompt := cfg.Prompt
	if prompt == "" {
		prompt = "Transcribe this audio exactly. Return only the transcript text."
	}

	reqBody := map[string]any{
		"model": modelID,
		"messages": []map[string]any{
			{
				"role": "user",
				"content": []map[string]any{
					{"type": "text", "text": prompt},
					{
						"type": "input_audio",
						"input_audio": map[string]any{
							"data":   base64.StdEncoding.EncodeToString(params.Audio),
							"format": format,
						},
					},
				},
			},
		},
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("openrouter transcription: marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/chat/completions", bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("openrouter transcription: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openrouter transcription: request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("openrouter transcription: transcribe: %w", utils.NewHTTPError(providerName, resp, errorformat.DecodeOpenRouter))
	}

	var payload struct {
		Choices []struct {
			Message struct {
				Content any `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("openrouter transcription: decode response: %w", err)
	}
	if len(payload.Choices) == 0 {
		return nil, fmt.Errorf("openrouter transcription: empty response")
	}
	text := extractText(payload.Choices[0].Message.Content)
	return &sdk.TranscriptionResult{Text: text}, nil
}

func extractText(content any) string {
	switch v := content.(type) {
	case string:
		return v
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			obj, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if obj["type"] == "text" {
				if text, ok := obj["text"].(string); ok {
					parts = append(parts, text)
				}
			}
		}
		return strings.Join(parts, "")
	default:
		return ""
	}
}

func audioFormatFromContentType(contentType, filename string) string {
	lower := strings.ToLower(contentType)
	switch {
	case strings.Contains(lower, "wav"):
		return "wav"
	case strings.Contains(lower, "mpeg"), strings.Contains(lower, "mp3"):
		return "mp3"
	case strings.Contains(lower, "ogg"):
		return "ogg"
	case strings.Contains(lower, "flac"):
		return "flac"
	case strings.Contains(lower, "aac"):
		return "aac"
	case strings.Contains(lower, "m4a"), strings.Contains(lower, "mp4"):
		return "m4a"
	}
	lower = strings.ToLower(filename)
	switch {
	case strings.HasSuffix(lower, ".mp3"):
		return "mp3"
	case strings.HasSuffix(lower, ".ogg"):
		return "ogg"
	case strings.HasSuffix(lower, ".flac"):
		return "flac"
	case strings.HasSuffix(lower, ".aac"):
		return "aac"
	case strings.HasSuffix(lower, ".m4a"), strings.HasSuffix(lower, ".mp4"):
		return "m4a"
	default:
		return "wav"
	}
}
