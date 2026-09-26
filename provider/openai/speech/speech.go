// Package speech provides an OpenAI-compatible TTS provider that targets the
// /audio/speech endpoint.  The same implementation works for the official
// OpenAI API as well as any drop-in proxy (OpenRouter, CometAPI, Player2,
// Index-TTS vLLM, unspeech, etc.) by pointing BaseURL at the proxy.
package speech

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	sdk "github.com/felinics/twilight/sdk"

	"github.com/felinics/twilight/internal/utils"
)

const (
	defaultModelID   = "gpt-4o-mini-tts"
	defaultBaseURL   = "https://api.openai.com/v1"
	defaultVoice     = "coral"
	defaultFormat    = "mp3"
	contentTypeAudio = "audio/mpeg"
)

// Option configures the OpenAI TTS provider.
type Option func(*Provider)

// WithHeaders sets provider-wide HTTP headers, overriding defaults. The map is
// copied when the option is created. Use sdk.WithRequestHeaders for call-scoped
// values such as session IDs; those take precedence over these headers.
func WithHeaders(headers map[string]string) Option {
	headers = utils.MergeHeaders(headers)
	return func(p *Provider) { p.headers = headers }
}

// WithAPIKey sets the API key used for Bearer authentication.
func WithAPIKey(key string) Option {
	return func(p *Provider) { p.apiKey = key }
}

// WithBaseURL overrides the API base URL (useful for proxies and testing).
func WithBaseURL(url string) Option {
	return func(p *Provider) { p.baseURL = url }
}

// WithHTTPClient replaces the default HTTP client.
func WithHTTPClient(hc *http.Client) Option {
	return func(p *Provider) { p.httpClient = hc }
}

// Provider implements sdk.SpeechProvider for the OpenAI /audio/speech API.
type Provider struct {
	headers    map[string]string
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

// New creates a new OpenAI TTS provider.
func New(opts ...Option) *Provider {
	p := &Provider{
		baseURL:    defaultBaseURL,
		httpClient: &http.Client{},
	}
	for _, o := range opts {
		o(p)
	}
	p.baseURL = strings.TrimRight(p.baseURL, "/")
	return p
}

// SpeechModel creates a SpeechModel bound to this provider.
func (p *Provider) SpeechModel(id string) *sdk.SpeechModel {
	if id == "" {
		id = defaultModelID
	}
	return &sdk.SpeechModel{ID: id, Provider: p}
}

// ListModels returns the speech models exposed by this provider.
func (p *Provider) ListModels(ctx context.Context) ([]*sdk.SpeechModel, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/models", http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("openai speech: build list models request: %w", err)
	}
	for key, value := range p.requestHeaders(ctx) {
		req.Header.Set(key, value)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai speech: list models request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("openai speech: unexpected status %d: %s", resp.StatusCode, string(body))
	}

	rawModels, err := decodeModelIDs(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("openai speech: decode response: %w", err)
	}

	models := make([]*sdk.SpeechModel, 0, len(rawModels))
	for _, id := range rawModels {
		m := struct{ ID string }{ID: id}
		if isOpenAITTSModel(m.ID) {
			models = append(models, p.SpeechModel(m.ID))
		}
	}
	if len(models) == 0 {
		return nil, errors.New("openai speech: no speech models returned by provider")
	}
	return models, nil
}

func isOpenAITTSModel(id string) bool {
	id = strings.ToLower(id)
	return strings.Contains(id, "tts") || strings.Contains(id, "audio")
}

func decodeModelIDs(r io.Reader) ([]string, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var wrapped struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapped); err == nil && len(wrapped.Data) > 0 {
		out := make([]string, 0, len(wrapped.Data))
		for _, m := range wrapped.Data {
			if m.ID != "" {
				out = append(out, m.ID)
			}
		}
		return out, nil
	}

	var direct []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &direct); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(direct))
	for _, m := range direct {
		if m.ID != "" {
			out = append(out, m.ID)
		}
	}
	return out, nil
}

// DoSynthesize synthesizes speech and returns the complete audio bytes.
func (p *Provider) DoSynthesize(ctx context.Context, params sdk.SpeechParams) (*sdk.SpeechResult, error) {
	cfg := parseConfig(params.Config)

	modelID := defaultModelID
	if params.Model != nil && params.Model.ID != "" {
		modelID = params.Model.ID
	}

	body, err := p.doRequest(ctx, modelID, params.Text, cfg)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	audio, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("openai speech: read response: %w", err)
	}
	return &sdk.SpeechResult{
		Audio:       audio,
		ContentType: contentTypeForFormat(cfg.ResponseFormat),
	}, nil
}

// DoStream synthesizes speech and returns a streaming result backed by chunked HTTP body.
func (p *Provider) DoStream(ctx context.Context, params sdk.SpeechParams) (*sdk.SpeechStreamResult, error) {
	cfg := parseConfig(params.Config)

	modelID := defaultModelID
	if params.Model != nil && params.Model.ID != "" {
		modelID = params.Model.ID
	}

	body, err := p.doRequest(ctx, modelID, params.Text, cfg)
	if err != nil {
		return nil, err
	}

	ch, errCh := utils.StreamHTTPBody(ctx, body, "openai speech")
	return sdk.NewSpeechStreamResult(ch, contentTypeForFormat(cfg.ResponseFormat), errCh), nil
}

// doRequest sends a POST /audio/speech request and returns the raw response body.
func (p *Provider) doRequest(ctx context.Context, model, text string, cfg audioConfig) (io.ReadCloser, error) {
	reqBody := map[string]any{
		"model":           model,
		"input":           text,
		"voice":           cfg.Voice,
		"response_format": cfg.ResponseFormat,
	}
	if cfg.Speed != 0 {
		reqBody["speed"] = cfg.Speed
	}
	// instructions is only supported by gpt-4o-mini-tts; ignore for other models.
	if cfg.Instructions != "" && strings.Contains(model, "gpt-4o-mini-tts") {
		reqBody["instructions"] = cfg.Instructions
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("openai speech: marshal request: %w", err)
	}

	url := p.baseURL + "/audio/speech"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("openai speech: build request: %w", err)
	}
	for key, value := range p.requestHeaders(ctx) {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai speech: request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("openai speech: unexpected status %d: %s", resp.StatusCode, string(body))
	}
	return resp.Body, nil
}

func (p *Provider) requestHeaders(ctx context.Context) map[string]string {
	return utils.RequestHeaders(ctx, utils.AuthHeader(p.apiKey), p.headers)
}
