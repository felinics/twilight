package speech

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	sdk "github.com/felinics/twilight/sdk"
)

const (
	defaultModelID    = "mimo-v2.5-tts"
	defaultBaseURL    = "https://api.xiaomimimo.com/v1"
	defaultVoice      = "mimo_default"
	defaultFormat     = "wav"
	speechContentType = "audio/wav"
	pcmSampleRate     = uint32(24000)
)

type Option func(*Provider)

func WithAPIKey(key string) Option {
	return func(p *Provider) { p.apiKey = key }
}

func WithBaseURL(url string) Option {
	return func(p *Provider) { p.baseURL = strings.TrimRight(url, "/") }
}

func WithHTTPClient(hc *http.Client) Option {
	return func(p *Provider) {
		if hc != nil {
			p.httpClient = hc
		}
	}
}

type Provider struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

func New(opts ...Option) *Provider {
	p := &Provider{
		baseURL:    defaultBaseURL,
		httpClient: &http.Client{},
	}
	for _, opt := range opts {
		opt(p)
	}
	p.baseURL = strings.TrimRight(p.baseURL, "/")
	return p
}

func (p *Provider) SpeechModel(id string) *sdk.SpeechModel {
	if id == "" {
		id = defaultModelID
	}
	return &sdk.SpeechModel{ID: id, Provider: p}
}

func (p *Provider) ListModels(context.Context) ([]*sdk.SpeechModel, error) {
	return []*sdk.SpeechModel{p.SpeechModel(defaultModelID)}, nil
}

func (p *Provider) DoSynthesize(ctx context.Context, params sdk.SpeechParams) (*sdk.SpeechResult, error) {
	modelID := defaultModelID
	if params.Model != nil && strings.TrimSpace(params.Model.ID) != "" {
		modelID = strings.TrimSpace(params.Model.ID)
	}
	cfg := parseConfig(params.Config)

	resp, err := p.doRequest(ctx, p.buildRequest(modelID, params.Text, cfg, false))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	audio, err := decodeSpeechResponse(resp.Body)
	if err != nil {
		return nil, err
	}
	return &sdk.SpeechResult{
		Audio:       audio,
		ContentType: contentTypeForFormat(cfg.Format),
	}, nil
}

func (p *Provider) DoStream(ctx context.Context, params sdk.SpeechParams) (*sdk.SpeechStreamResult, error) {
	modelID := defaultModelID
	if params.Model != nil && strings.TrimSpace(params.Model.ID) != "" {
		modelID = strings.TrimSpace(params.Model.ID)
	}
	cfg := parseConfig(params.Config)
	cfg.Format = formatPCM16

	resp, err := p.doRequest(ctx, p.buildRequest(modelID, params.Text, cfg, true))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	chunks, err := collectAudioChunks(resp.Body)
	if err != nil {
		return nil, err
	}
	pcm, err := decodeAudioChunks(chunks)
	if err != nil {
		return nil, err
	}

	audio := buildWAV(pcm, pcmSampleRate)
	ch := make(chan []byte, 1)
	errCh := make(chan error, 1)
	ch <- audio
	close(ch)
	close(errCh)
	return sdk.NewSpeechStreamResult(ch, speechContentType, errCh), nil
}

func (p *Provider) buildRequest(modelID, text string, cfg audioConfig, stream bool) map[string]any {
	messages := make([]map[string]any, 0, 2)
	if cfg.StylePrompt != "" {
		messages = append(messages, map[string]any{
			"role":    "user",
			"content": cfg.StylePrompt,
		})
	}
	messages = append(messages, map[string]any{
		"role":    "assistant",
		"content": text,
	})

	reqBody := map[string]any{
		"model":    modelID,
		"messages": messages,
		"audio": map[string]any{
			"format": cfg.Format,
			"voice":  cfg.Voice,
		},
	}
	if stream {
		reqBody["stream"] = true
	}
	return reqBody
}

func (p *Provider) doRequest(ctx context.Context, reqBody map[string]any) (*http.Response, error) {
	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("mimo speech: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/chat/completions", bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("mimo speech: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("api-key", p.apiKey)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("mimo speech: request: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("mimo speech: unexpected status %d: %s", resp.StatusCode, string(body))
	}
	return resp, nil
}

type speechResponse struct {
	Choices []struct {
		Message struct {
			Audio *struct {
				Data string `json:"data"`
			} `json:"audio"`
		} `json:"message"`
	} `json:"choices"`
}

func decodeSpeechResponse(r io.Reader) ([]byte, error) {
	var payload speechResponse
	if err := json.NewDecoder(r).Decode(&payload); err != nil {
		return nil, fmt.Errorf("mimo speech: decode response: %w", err)
	}
	if len(payload.Choices) == 0 || payload.Choices[0].Message.Audio == nil || payload.Choices[0].Message.Audio.Data == "" {
		return nil, fmt.Errorf("mimo speech: response missing audio payload")
	}
	audio, err := decodeBase64Chunk(payload.Choices[0].Message.Audio.Data)
	if err != nil {
		return nil, fmt.Errorf("mimo speech: decode audio payload: %w", err)
	}
	return audio, nil
}

type speechStreamEvent struct {
	Choices []struct {
		Delta struct {
			Audio *struct {
				Data string `json:"data"`
			} `json:"audio"`
		} `json:"delta"`
	} `json:"choices"`
}

func collectAudioChunks(r io.Reader) ([]string, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	chunks := make([]string, 0)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" {
			continue
		}
		if payload == "[DONE]" {
			break
		}

		var evt speechStreamEvent
		if err := json.Unmarshal([]byte(payload), &evt); err != nil {
			continue
		}
		for _, choice := range evt.Choices {
			if choice.Delta.Audio != nil && choice.Delta.Audio.Data != "" {
				chunks = append(chunks, choice.Delta.Audio.Data)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("mimo speech: read stream: %w", err)
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("mimo speech: no audio chunks in stream")
	}
	return chunks, nil
}

func decodeAudioChunks(chunks []string) ([]byte, error) {
	var out []byte
	for _, chunk := range chunks {
		decoded, err := decodeBase64Chunk(chunk)
		if err != nil {
			return nil, err
		}
		out = append(out, decoded...)
	}
	return out, nil
}

func decodeBase64Chunk(chunk string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(chunk)
	if err == nil {
		return decoded, nil
	}
	return base64.RawStdEncoding.DecodeString(chunk)
}

func buildWAV(pcm []byte, sampleRate uint32) []byte {
	const (
		numChannels   = uint32(1)
		bitsPerSample = uint32(16)
	)
	const maxUint32 = ^uint32(0)

	byteRate := sampleRate * numChannels * bitsPerSample / 8
	blockAlign := uint16(numChannels * bitsPerSample / 8)
	dataSize64 := uint64(len(pcm))
	dataSize := maxUint32
	if dataSize64 <= uint64(maxUint32) {
		dataSize = uint32(dataSize64)
	}

	chunkSize64 := uint64(36) + dataSize64
	chunkSize := maxUint32
	if chunkSize64 <= uint64(maxUint32) {
		chunkSize = uint32(chunkSize64)
	}

	buf := new(bytes.Buffer)
	_, _ = buf.WriteString("RIFF")
	_ = binary.Write(buf, binary.LittleEndian, chunkSize)
	_, _ = buf.WriteString("WAVE")
	_, _ = buf.WriteString("fmt ")
	_ = binary.Write(buf, binary.LittleEndian, uint32(16))
	_ = binary.Write(buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(buf, binary.LittleEndian, uint16(numChannels))
	_ = binary.Write(buf, binary.LittleEndian, sampleRate)
	_ = binary.Write(buf, binary.LittleEndian, byteRate)
	_ = binary.Write(buf, binary.LittleEndian, blockAlign)
	_ = binary.Write(buf, binary.LittleEndian, uint16(bitsPerSample))
	_, _ = buf.WriteString("data")
	_ = binary.Write(buf, binary.LittleEndian, dataSize)
	_, _ = buf.Write(pcm)
	return buf.Bytes()
}
