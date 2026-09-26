package transcription

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"

	"github.com/felinics/twilight/internal/utils"
	sdk "github.com/felinics/twilight/sdk"
)

const (
	defaultModelID = "gpt-4o-mini-transcribe"
	defaultBaseURL = "https://api.openai.com/v1"
)

type Option func(*Provider)

// WithHeaders sets provider-wide HTTP headers, overriding defaults. The map is
// copied when the option is created. Use sdk.WithRequestHeaders for call-scoped
// values such as session IDs; those take precedence over these headers.
func WithHeaders(headers map[string]string) Option {
	headers = utils.MergeHeaders(headers)
	return func(p *Provider) { p.headers = headers }
}

func WithAPIKey(key string) Option { return func(p *Provider) { p.apiKey = key } }
func WithBaseURL(url string) Option {
	return func(p *Provider) { p.baseURL = strings.TrimRight(url, "/") }
}
func WithHTTPClient(hc *http.Client) Option { return func(p *Provider) { p.httpClient = hc } }

type Provider struct {
	headers    map[string]string
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/models", http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("openai transcription: build list models request: %w", err)
	}
	for key, value := range p.requestHeaders(ctx) {
		req.Header.Set(key, value)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai transcription: list models request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("openai transcription: unexpected status %d: %s", resp.StatusCode, string(body))
	}

	rawModels, err := decodeModelIDs(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("openai transcription: decode response: %w", err)
	}

	models := make([]*sdk.TranscriptionModel, 0, len(rawModels))
	for _, id := range rawModels {
		m := struct{ ID string }{ID: id}
		if isTranscriptionModel(m.ID) {
			models = append(models, p.TranscriptionModel(m.ID))
		}
	}
	if len(models) == 0 {
		return nil, errors.New("openai transcription: no transcription models returned by provider")
	}
	return models, nil
}

func isTranscriptionModel(id string) bool {
	id = strings.ToLower(id)
	return id == "whisper-1" || strings.Contains(id, "transcribe")
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

func (p *Provider) DoTranscribe(ctx context.Context, params sdk.TranscriptionParams) (*sdk.TranscriptionResult, error) {
	cfg := parseConfig(params.Config)
	modelID := defaultModelID
	if params.Model != nil && params.Model.ID != "" {
		modelID = params.Model.ID
	}

	body, contentType, err := buildMultipartBody(&params, modelID, cfg)
	if err != nil {
		return nil, fmt.Errorf("openai transcription: build request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/audio/transcriptions", body)
	if err != nil {
		return nil, fmt.Errorf("openai transcription: build request: %w", err)
	}
	for key, value := range p.requestHeaders(ctx) {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai transcription: request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("openai transcription: unexpected status %d: %s", resp.StatusCode, string(respBody))
	}

	return decodeResponse(resp.Body)
}

type audioConfig struct {
	Language       string
	Prompt         string
	Temperature    *float64
	ResponseFormat string
}

func parseConfig(cfg map[string]any) audioConfig {
	ac := audioConfig{ResponseFormat: "json"}
	if cfg == nil {
		return ac
	}
	if v, ok := cfg["language"].(string); ok && v != "" {
		ac.Language = v
	}
	if v, ok := cfg["prompt"].(string); ok && v != "" {
		ac.Prompt = v
	}
	if v, ok := utils.ToFloat64(cfg["temperature"]); ok {
		ac.Temperature = &v
	}
	if v, ok := cfg["response_format"].(string); ok && v != "" {
		ac.ResponseFormat = v
	}
	return ac
}

func buildMultipartBody(params *sdk.TranscriptionParams, modelID string, cfg audioConfig) (*bytes.Buffer, string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	if err := w.WriteField("model", modelID); err != nil {
		return nil, "", err
	}
	if cfg.Language != "" {
		if err := w.WriteField("language", cfg.Language); err != nil {
			return nil, "", err
		}
	}
	if cfg.Prompt != "" {
		if err := w.WriteField("prompt", cfg.Prompt); err != nil {
			return nil, "", err
		}
	}
	if cfg.Temperature != nil {
		if err := w.WriteField("temperature", strconv.FormatFloat(*cfg.Temperature, 'f', -1, 64)); err != nil {
			return nil, "", err
		}
	}
	if cfg.ResponseFormat != "" {
		if err := w.WriteField("response_format", cfg.ResponseFormat); err != nil {
			return nil, "", err
		}
	}

	part, err := w.CreateFormFile("file", params.Filename)
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

type simpleResponse struct {
	Text string `json:"text"`
}

type verboseResponse struct {
	Text     string  `json:"text"`
	Language string  `json:"language"`
	Duration float64 `json:"duration"`
	Words    []struct {
		Word  string  `json:"word"`
		Start float64 `json:"start"`
		End   float64 `json:"end"`
	} `json:"words"`
}

func decodeResponse(r io.Reader) (*sdk.TranscriptionResult, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("openai transcription: read response: %w", err)
	}

	var verbose verboseResponse
	if err := json.Unmarshal(body, &verbose); err == nil && verbose.Text != "" {
		out := &sdk.TranscriptionResult{
			Text:            verbose.Text,
			Language:        verbose.Language,
			DurationSeconds: verbose.Duration,
		}
		if len(verbose.Words) > 0 {
			out.Words = make([]sdk.TranscriptionWord, 0, len(verbose.Words))
			for _, w := range verbose.Words {
				out.Words = append(out.Words, sdk.TranscriptionWord{Text: w.Word, Start: w.Start, End: w.End})
			}
		}
		return out, nil
	}

	var simple simpleResponse
	if err := json.Unmarshal(body, &simple); err != nil {
		return nil, fmt.Errorf("openai transcription: decode response: %w", err)
	}
	return &sdk.TranscriptionResult{Text: simple.Text}, nil
}

func (p *Provider) requestHeaders(ctx context.Context) map[string]string {
	return utils.RequestHeaders(ctx, utils.AuthHeader(p.apiKey), p.headers)
}
