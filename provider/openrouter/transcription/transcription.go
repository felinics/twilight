package transcription

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	sdk "github.com/felinics/twilight/sdk"
)

const (
	partTypeText = "text"
)

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
}

func New(opts ...Option) *Provider {
	p := &Provider{baseURL: defaultBaseURL, httpClient: &http.Client{}}
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
		body, _ := io.ReadAll(httpResp.Body)
		return nil, fmt.Errorf("openrouter transcription: unexpected status %d: %s", httpResp.StatusCode, string(body))
	}
	rawModels, err := decodeOpenRouterModels(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("openrouter transcription: decode list models response: %w", err)
	}

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

type audioConfig struct {
	Prompt string
}

func parseConfig(cfg map[string]any) audioConfig {
	ac := audioConfig{}
	if cfg == nil {
		return ac
	}
	if v, ok := cfg["prompt"].(string); ok && v != "" {
		ac.Prompt = v
	}
	return ac
}

func (p *Provider) DoTranscribe(ctx context.Context, params sdk.TranscriptionParams) (*sdk.TranscriptionResult, error) {
	cfg := parseConfig(params.Config)
	modelID := defaultModelID
	if params.Model != nil && params.Model.ID != "" {
		modelID = params.Model.ID
	}
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
					{"type": partTypeText, partTypeText: prompt},
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
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("openrouter transcription: unexpected status %d: %s", resp.StatusCode, string(body))
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
			if obj["type"] == partTypeText {
				if text, ok := obj[partTypeText].(string); ok {
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
