package speech

import (
	"context"
	"fmt"
	"strings"

	sdk "github.com/felinics/twilight/sdk"
)

const (
	defaultModelID   = "edge-read-aloud"
	contentTypeAudio = "audio/mpeg"
)

// Option configures the Edge TTS provider.
type Option func(*Provider)

// WithBaseURL overrides the Edge TTS WebSocket endpoint (useful for testing).
func WithBaseURL(url string) Option {
	return func(p *Provider) { p.client.BaseURL = url }
}

// Provider implements sdk.SpeechProvider for Microsoft Edge TTS.
type Provider struct {
	client *edgeWsClient
}

// New creates a new Edge TTS provider.
func New(opts ...Option) *Provider {
	p := &Provider{client: newEdgeWsClient()}
	for _, o := range opts {
		o(p)
	}
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
func (p *Provider) ListModels(context.Context) ([]*sdk.SpeechModel, error) {
	return nil, fmt.Errorf("edge speech: provider does not expose a model discovery API; voices are bundled locally")
}

// DoSynthesize synthesizes speech and returns the complete audio.
func (p *Provider) DoSynthesize(ctx context.Context, params sdk.SpeechParams) (*sdk.SpeechResult, error) {
	cfg := parseConfig(params.Config)
	audio, err := p.client.synthesize(ctx, params.Text, cfg)
	if err != nil {
		return nil, err
	}
	return &sdk.SpeechResult{
		Audio:       audio,
		ContentType: resolveContentType(cfg.Format),
	}, nil
}

// DoStream synthesizes speech and returns a streaming result.
func (p *Provider) DoStream(ctx context.Context, params sdk.SpeechParams) (*sdk.SpeechStreamResult, error) {
	cfg := parseConfig(params.Config)
	dataCh, errCh := p.client.stream(ctx, params.Text, cfg)
	if dataCh == nil {
		select {
		case err := <-errCh:
			return nil, err
		default:
			return nil, context.Canceled
		}
	}
	return sdk.NewSpeechStreamResult(dataCh, resolveContentType(cfg.Format), errCh), nil
}

// parseConfig extracts Edge-specific audio configuration from a generic map.
//
// Supported keys:
//   - "voice" (string): voice ID, e.g. "en-US-EmmaMultilingualNeural"
//   - "language" (string): BCP-47 language tag, e.g. "en-US"
//   - "format" (string): output format, e.g. "audio-24khz-48kbitrate-mono-mp3"
//   - "speed" (float64): speech rate, 1.0 = normal
//   - "pitch" (float64): pitch adjustment in Hz
func parseConfig(cfg map[string]any) audioConfig {
	ac := audioConfig{}
	if cfg == nil {
		return ac
	}
	if v, ok := cfg["voice"].(string); ok {
		ac.Voice = v
	}
	if v, ok := cfg["language"].(string); ok {
		ac.Language = v
	}
	if ac.Language == "" && ac.Voice != "" {
		if lang, ok := LookupVoiceLang(ac.Voice); ok {
			ac.Language = lang
		}
	}
	if v, ok := cfg["format"].(string); ok {
		ac.Format = v
	}
	if v, ok := toFloat(cfg["speed"]); ok {
		ac.Speed = v
	}
	if v, ok := toFloat(cfg["pitch"]); ok {
		ac.Pitch = v
	}
	return ac
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}

func resolveContentType(format string) string {
	switch {
	case strings.Contains(format, "mp3"):
		return contentTypeAudio
	case strings.Contains(format, "opus"):
		return "audio/opus"
	case strings.Contains(format, "ogg"):
		return "audio/ogg"
	case strings.Contains(format, "webm"):
		return "audio/webm"
	case strings.Contains(format, "wav"):
		return "audio/wav"
	default:
		return contentTypeAudio
	}
}
