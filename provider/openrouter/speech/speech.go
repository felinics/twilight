// Package speech provides an OpenRouter TTS provider.
//
// OpenRouter does not expose a dedicated /audio/speech endpoint; instead it
// exposes audio generation through the chat/completions API with
//
//	"modalities": ["text", "audio"],
//	"audio":      {"voice": "...", "format": "pcm16"},
//	"stream":     true
//
// This provider translates sdk.SpeechParams into that shape, collects the
// base64-encoded PCM-16 chunks from the SSE stream, and wraps the result in
// a minimal WAV container so that the caller receives standard audio/wav bytes.
package speech

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"

	sdk "github.com/felinics/twilight/sdk"
)

const (
	modalityAudio = "audio"
)

const (
	defaultModelID   = "openai/gpt-audio-mini"
	defaultBaseURL   = "https://openrouter.ai/api/v1"
	defaultModel     = "openai/gpt-audio-mini"
	defaultVoice     = "coral"
	contentTypeAudio = "audio/wav"

	// pcmSampleRate is the sample rate used by OpenRouter's pcm16 format.
	pcmSampleRate uint32 = 24000
)

// Option configures the OpenRouter speech provider.
type Option func(*Provider)

// WithAPIKey sets the OpenRouter API key.
func WithAPIKey(key string) Option {
	return func(p *Provider) { p.apiKey = key }
}

// WithBaseURL overrides the API base URL (useful for testing).
func WithBaseURL(u string) Option {
	return func(p *Provider) { p.baseURL = strings.TrimRight(u, "/") }
}

// WithHTTPClient replaces the default HTTP client.
func WithHTTPClient(hc *http.Client) Option {
	return func(p *Provider) { p.httpClient = hc }
}

// Provider implements sdk.SpeechProvider via OpenRouter's audio modality.
type Provider struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

// New creates a new OpenRouter speech provider.
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
		id = defaultModel
	}
	return &sdk.SpeechModel{ID: id, Provider: p}
}

// ListModels returns the speech models exposed by this provider.
func (p *Provider) ListModels(ctx context.Context) ([]*sdk.SpeechModel, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/models?output_modalities=all", http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("openrouter speech: build list models request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openrouter speech: list models request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("openrouter speech: unexpected status %d: %s", resp.StatusCode, string(body))
	}

	rawModels, err := decodeOpenRouterModels(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("openrouter speech: decode list models response: %w", err)
	}

	models := make([]*sdk.SpeechModel, 0, len(rawModels))
	for _, m := range rawModels {
		if isOpenRouterSpeechModel(m.ID, m.Architecture.OutputModalities) {
			models = append(models, p.SpeechModel(m.ID))
		}
	}
	if len(models) == 0 {
		return nil, errors.New("openrouter speech: no speech models returned by provider")
	}
	return models, nil
}

func isOpenRouterSpeechModel(id string, outputs []string) bool {
	for _, output := range outputs {
		switch strings.ToLower(output) {
		case "speech", modalityAudio:
			return true
		}
	}
	lowerID := strings.ToLower(id)
	return strings.Contains(lowerID, "tts") || strings.Contains(lowerID, modalityAudio)
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

// DoSynthesize synthesizes speech and returns the complete WAV audio.
func (p *Provider) DoSynthesize(ctx context.Context, params sdk.SpeechParams) (*sdk.SpeechResult, error) {
	cfg := parseConfig(params.Config)
	if params.Model != nil && params.Model.ID != "" {
		cfg.Model = params.Model.ID
	}

	wav, err := p.synthesize(ctx, params.Text, cfg)
	if err != nil {
		return nil, err
	}
	return &sdk.SpeechResult{
		Audio:       wav,
		ContentType: contentTypeAudio,
	}, nil
}

// DoStream synthesizes speech and returns a single-chunk streaming result.
// OpenRouter returns all PCM data through SSE before any audio is available,
// so true incremental streaming is not possible; the completed WAV is sent as
// one chunk.
func (p *Provider) DoStream(ctx context.Context, params sdk.SpeechParams) (*sdk.SpeechStreamResult, error) {
	cfg := parseConfig(params.Config)
	if params.Model != nil && params.Model.ID != "" {
		cfg.Model = params.Model.ID
	}

	wav, err := p.synthesize(ctx, params.Text, cfg)
	if err != nil {
		return nil, err
	}

	ch := make(chan []byte, 1)
	errCh := make(chan error, 1)
	ch <- wav
	close(ch)
	close(errCh)
	return sdk.NewSpeechStreamResult(ch, contentTypeAudio, errCh), nil
}

// synthesize sends the chat/completions request and returns assembled WAV bytes.
func (p *Provider) synthesize(ctx context.Context, text string, cfg audioConfig) ([]byte, error) {
	reqBody := map[string]any{
		"model": cfg.Model,
		"messages": []map[string]any{
			{
				"role":    "user",
				"content": ttsPrompt(text),
			},
		},
		"modalities":  []string{"text", modalityAudio},
		modalityAudio: map[string]any{"voice": cfg.Voice, "format": "pcm16"},
		"stream":      true,
	}
	if cfg.Speed != 0 {
		reqBody["speed"] = cfg.Speed
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("openrouter speech: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.baseURL+"/chat/completions", bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("openrouter speech: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openrouter speech: request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("openrouter speech: unexpected status %d: %s", resp.StatusCode, string(body))
	}

	chunks, err := collectPCMChunks(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("openrouter speech: collect audio: %w", err)
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("openrouter speech: no audio data in response")
	}

	pcm, err := decodePCMChunks(chunks)
	if err != nil {
		return nil, fmt.Errorf("openrouter speech: decode pcm: %w", err)
	}
	return buildWAV(pcm, pcmSampleRate), nil
}

// ttsPrompt wraps the user text in a prompt that instructs the model to read
// it aloud without any additional commentary.
func ttsPrompt(text string) string {
	return "Read this text aloud exactly as written, without any commentary or extra words:\n\n" + text
}

// sseEvent holds parsed fields from a single SSE data line.
type sseEvent struct {
	Choices []struct {
		Delta struct {
			Audio *struct {
				Data string `json:"data"`
			} `json:"audio"`
		} `json:"delta"`
	} `json:"choices"`
}

// collectPCMChunks reads an SSE stream and returns all base64 PCM-16 chunks
// emitted by the audio delta events.
func collectPCMChunks(body io.Reader) ([]string, error) {
	var chunks []string
	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		if payload == "[DONE]" {
			break
		}
		var evt sseEvent
		if err := json.Unmarshal([]byte(payload), &evt); err != nil {
			// Skip malformed lines rather than aborting.
			continue
		}
		for _, choice := range evt.Choices {
			if choice.Delta.Audio != nil && choice.Delta.Audio.Data != "" {
				chunks = append(chunks, choice.Delta.Audio.Data)
			}
		}
	}
	return chunks, scanner.Err()
}

// decodePCMChunks decodes each base64 chunk independently and concatenates the
// raw PCM bytes.  Each SSE audio delta carries an independently valid base64
// string; joining them before decoding would embed padding characters in the
// middle and produce invalid base64.
func decodePCMChunks(chunks []string) ([]byte, error) {
	var pcm []byte
	for _, chunk := range chunks {
		decoded, err := base64.StdEncoding.DecodeString(chunk)
		if err != nil {
			// Fall back to raw (no-padding) encoding in case the server omits
			// padding on intermediate chunks.
			decoded, err = base64.RawStdEncoding.DecodeString(chunk)
			if err != nil {
				return nil, err
			}
		}
		pcm = append(pcm, decoded...)
	}
	return pcm, nil
}

// buildWAV wraps raw PCM-16 mono data in a standard 44-byte WAV container.
func buildWAV(pcm []byte, sampleRate uint32) []byte {
	const (
		numChannels   uint32 = 1
		bitsPerSample uint32 = 16
	)
	byteRate := sampleRate * numChannels * bitsPerSample / 8
	blockAlign := uint16(numChannels * bitsPerSample / 8)
	dataSize64 := uint64(len(pcm))
	dataSize := uint32(math.MaxUint32)
	if dataSize64 <= math.MaxUint32 {
		dataSize = uint32(dataSize64)
	}
	chunkSize := 36 + dataSize

	buf := new(bytes.Buffer)
	buf.WriteString("RIFF")
	_ = binary.Write(buf, binary.LittleEndian, chunkSize)
	buf.WriteString("WAVE")

	buf.WriteString("fmt ")
	_ = binary.Write(buf, binary.LittleEndian, uint32(16))
	_ = binary.Write(buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(buf, binary.LittleEndian, uint16(numChannels))
	_ = binary.Write(buf, binary.LittleEndian, sampleRate)
	_ = binary.Write(buf, binary.LittleEndian, byteRate)
	_ = binary.Write(buf, binary.LittleEndian, blockAlign)
	_ = binary.Write(buf, binary.LittleEndian, uint16(bitsPerSample))

	buf.WriteString("data")
	_ = binary.Write(buf, binary.LittleEndian, dataSize)
	buf.Write(pcm)

	return buf.Bytes()
}
