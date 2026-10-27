// Package speech provides an OpenRouter TTS provider.
//
// OpenRouter does not expose a dedicated /audio/speech endpoint; instead it
// exposes audio generation through the chat/completions API with
//
//	"modalities": ["text", "audio"],
//	"audio":      {"voice": "...", "format": "pcm16"},
//	"stream":     true
//
// The audio arrives as base64-encoded PCM-16 (24 kHz, mono, little-endian)
// in the delta.audio.data field of the SSE chunks. DoSynthesize collects the
// whole stream and wraps it in a WAV container; DoStream forwards the audio
// as each delta arrives, behind a streaming WAV header.
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

	"github.com/felinics/twilight/internal/errorformat"
	"github.com/felinics/twilight/internal/utils"
	sdk "github.com/felinics/twilight/sdk"
)

// providerName identifies this package in APIError.Provider.
const providerName = "openrouter-speech"

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
		return nil, fmt.Errorf("openrouter speech: list models: %w", utils.NewHTTPError(providerName, resp, errorformat.DecodeOpenRouter))
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
		case "speech", "audio":
			return true
		}
	}
	lowerID := strings.ToLower(id)
	return strings.Contains(lowerID, "tts") || strings.Contains(lowerID, "audio")
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
	body, header, err := p.openStream(ctx, params)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	var pcm []byte
	err = readAudioStream(body, header, func(chunk []byte) error {
		pcm = append(pcm, chunk...)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("openrouter speech: %w", err)
	}
	return &sdk.SpeechResult{
		Audio:       buildWAV(pcm, pcmSampleRate),
		ContentType: contentTypeAudio,
	}, nil
}

// DoStream synthesizes speech and forwards the audio as each SSE audio delta
// arrives, without waiting for the whole response.
//
// The stream is audio/wav: the first chunk starts with a 44-byte WAV header
// whose RIFF and data sizes are 0xFFFFFFFF, the usual marker for a WAV of
// unknown length, followed by PCM-16 (24 kHz, mono, little-endian). Every
// later chunk is raw PCM-16 continuing the same data chunk. Each chunk holds
// whole samples; only the last one can end on an odd byte, when the upstream
// sent an odd number of bytes in total. Concatenating every chunk therefore
// gives the same PCM data as DoSynthesize, behind a header that leaves the
// sizes unset.
//
// How soon and how often deltas arrive is decided by OpenRouter and the model
// it routes to; this provider adds no buffering of its own. A non-2xx
// response is returned as an error before the stream starts. A failure after
// that (an OpenRouter mid-stream error event, a read or decode failure, a
// response without audio, or ctx being cancelled) closes Stream and is
// reported by Err.
func (p *Provider) DoStream(ctx context.Context, params sdk.SpeechParams) (*sdk.SpeechStreamResult, error) {
	body, header, err := p.openStream(ctx, params)
	if err != nil {
		return nil, err
	}

	ch := make(chan []byte, 8)
	errCh := make(chan error, 1)
	go func() {
		defer body.Close()
		defer close(ch)
		defer close(errCh)

		headerSent := false
		err := readAudioStream(body, header, func(pcm []byte) error {
			chunk := pcm
			if !headerSent {
				chunk = append(wavHeader(math.MaxUint32, pcmSampleRate), pcm...)
				headerSent = true
			}
			select {
			case ch <- chunk:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if err == nil {
			return
		}
		// A cancelled request surfaces from the body as a transport error;
		// report the context's own error so errors.Is(err, context.Canceled)
		// holds whichever way the cancellation was noticed.
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		}
		errCh <- fmt.Errorf("openrouter speech: stream: %w", err)
	}()
	return sdk.NewSpeechStreamResult(ch, contentTypeAudio, errCh), nil
}

// openStream sends the chat/completions request and, once OpenRouter has
// answered 200, returns the SSE body and the response header. The caller must
// close the body.
func (p *Provider) openStream(ctx context.Context, params sdk.SpeechParams) (io.ReadCloser, http.Header, error) {
	cfg := parseConfig(params.Config)
	if params.Model != nil && params.Model.ID != "" {
		cfg.Model = params.Model.ID
	}

	reqBody := map[string]any{
		"model": cfg.Model,
		"messages": []map[string]any{
			{
				"role":    "user",
				"content": ttsPrompt(params.Text),
			},
		},
		"modalities": []string{"text", "audio"},
		"audio":      map[string]any{"voice": cfg.Voice, "format": "pcm16"},
		"stream":     true,
	}
	if cfg.Speed != 0 {
		reqBody["speed"] = cfg.Speed
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, nil, fmt.Errorf("openrouter speech: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.baseURL+"/chat/completions", bytes.NewReader(data))
	if err != nil {
		return nil, nil, fmt.Errorf("openrouter speech: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("openrouter speech: request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, nil, fmt.Errorf("openrouter speech: synthesize: %w", utils.NewHTTPError(providerName, resp, errorformat.DecodeOpenRouter))
	}
	return resp.Body, resp.Header, nil
}

// ttsPrompt wraps the user text in a prompt that instructs the model to read
// it aloud without any additional commentary.
func ttsPrompt(text string) string {
	return "Read this text aloud exactly as written, without any commentary or extra words:\n\n" + text
}

// maxSSELineBytes bounds a single SSE line, which carries one base64 audio
// delta; it matches the limit utils.FetchSSE uses.
const maxSSELineBytes = 10 * 1024 * 1024

// errNoAudio reports a stream that ended without any audio data.
var errNoAudio = errors.New("no audio data in response")

// sseEvent holds parsed fields from a single SSE data line. Error is set on
// an OpenRouter mid-stream error event
// (https://openrouter.ai/docs/api-reference/errors), which arrives after the
// 200 status line and ends the stream.
type sseEvent struct {
	Error   json.RawMessage `json:"error"`
	Choices []struct {
		Delta struct {
			Audio *struct {
				Data string `json:"data"`
			} `json:"audio"`
		} `json:"delta"`
	} `json:"choices"`
}

// readAudioStream reads the chat/completions SSE stream from body and calls
// emit with the PCM-16 decoded from each audio delta as soon as the delta has
// been read. emit always receives a fresh, non-empty slice. Every slice but
// the last holds whole samples, and the slices concatenate to the full PCM.
//
// It returns the first error from emit, an *sdk.APIError for a mid-stream
// error event, errNoAudio when the stream carried no audio, or a read or
// decode error. Lines that are not JSON data are skipped, as is everything
// after [DONE].
func readAudioStream(body io.Reader, header http.Header, emit func(pcm []byte) error) error {
	var dec pcmDecoder
	total := 0
	send := func(pcm []byte) error {
		if len(pcm) == 0 {
			return nil
		}
		total += len(pcm)
		return emit(pcm)
	}

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), maxSSELineBytes)
	for scanner.Scan() {
		field, value, ok := strings.Cut(scanner.Text(), ":")
		if !ok || field != "data" {
			continue
		}
		payload := strings.TrimSpace(value)
		if payload == "[DONE]" {
			break
		}
		var evt sseEvent
		if err := json.Unmarshal([]byte(payload), &evt); err != nil {
			// Skip malformed lines rather than aborting.
			continue
		}
		if len(evt.Error) > 0 && string(evt.Error) != "null" {
			return utils.NewBodyError(providerName, header, []byte(payload), errorformat.DecodeOpenRouter)
		}
		for _, choice := range evt.Choices {
			if choice.Delta.Audio == nil || choice.Delta.Audio.Data == "" {
				continue
			}
			pcm, err := dec.write(choice.Delta.Audio.Data)
			if err != nil {
				return fmt.Errorf("decode audio: %w", err)
			}
			if err := send(pcm); err != nil {
				return err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read stream: %w", err)
	}

	tail, err := dec.flush()
	if err != nil {
		return fmt.Errorf("decode audio: %w", err)
	}
	if err := send(tail); err != nil {
		return err
	}
	if total == 0 {
		return errNoAudio
	}
	return nil
}

// pcmDecoder turns the base64 audio deltas of one stream into PCM-16 bytes
// that stay aligned to 2-byte samples across deltas.
//
// A delta that decodes on its own, with or without padding, is taken as is:
// that is how the deltas are encoded upstream. A delta that does not (its
// length leaves a single character over, so it cannot end a base64 string)
// is taken as part of one base64 text cut at an arbitrary character; it and
// the deltas after it are decoded quantum by quantum, carrying an incomplete
// quantum over to the next delta, until the text is aligned again.
type pcmDecoder struct {
	pending string // base64 characters short of a whole 4-character quantum
	odd     []byte // the first byte of a sample whose second byte is still due
}

// write decodes one delta and returns the whole samples it completes, in a
// fresh slice. The result may be empty.
func (d *pcmDecoder) write(b64 string) ([]byte, error) {
	decoded, err := d.decode(b64)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 0, len(d.odd)+len(decoded))
	data = append(data, d.odd...)
	data = append(data, decoded...)
	n := len(data) &^ 1
	d.odd = append([]byte(nil), data[n:]...)
	return data[:n:n], nil
}

// flush decodes whatever base64 is still pending and returns it together with
// a dangling odd byte, so that nothing the upstream sent is dropped.
func (d *pcmDecoder) flush() ([]byte, error) {
	data := d.odd
	d.odd = nil
	if d.pending != "" {
		tail, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(d.pending, "="))
		d.pending = ""
		if err != nil {
			return nil, err
		}
		data = append(data, tail...)
	}
	return data, nil
}

func (d *pcmDecoder) decode(b64 string) ([]byte, error) {
	if d.pending == "" {
		if out, err := base64.StdEncoding.DecodeString(b64); err == nil {
			return out, nil
		}
		// Fall back to raw (no-padding) encoding in case the server omits
		// padding on intermediate chunks.
		if out, err := base64.RawStdEncoding.DecodeString(b64); err == nil {
			return out, nil
		}
	}
	d.pending += b64
	var out []byte
	for len(d.pending) >= 4 {
		end := len(d.pending) &^ 3
		// Padding ends a base64 string; decode up to the quantum holding it
		// and start afresh after it.
		if i := strings.IndexByte(d.pending[:end], '='); i >= 0 {
			end = i&^3 + 4
		}
		decoded, err := base64.StdEncoding.DecodeString(d.pending[:end])
		if err != nil {
			return nil, err
		}
		out = append(out, decoded...)
		d.pending = d.pending[end:]
	}
	return out, nil
}

// buildWAV wraps raw PCM-16 mono data in a standard 44-byte WAV container.
func buildWAV(pcm []byte, sampleRate uint32) []byte {
	return append(wavHeader(uint64(len(pcm)), sampleRate), pcm...)
}

// wavHeader returns the 44-byte header of a PCM-16 mono WAV file whose data
// chunk holds dataSize bytes. Sizes past the 32-bit fields are written as
// 0xFFFFFFFF, which is also how a streamed WAV of unknown length is marked.
func wavHeader(dataSize uint64, sampleRate uint32) []byte {
	const (
		numChannels   uint32 = 1
		bitsPerSample uint32 = 16
	)
	byteRate := sampleRate * numChannels * bitsPerSample / 8
	blockAlign := uint16(numChannels * bitsPerSample / 8)
	clamp := func(n uint64) uint32 {
		if n > math.MaxUint32 {
			return math.MaxUint32
		}
		return uint32(n)
	}

	buf := bytes.NewBuffer(make([]byte, 0, 44))
	buf.WriteString("RIFF")
	_ = binary.Write(buf, binary.LittleEndian, clamp(36+dataSize))
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
	_ = binary.Write(buf, binary.LittleEndian, clamp(dataSize))
	return buf.Bytes()
}
