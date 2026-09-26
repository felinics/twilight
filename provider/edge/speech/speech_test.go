package speech

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	sdk "github.com/felinics/twilight/sdk"
)

func newWithClient(client *edgeWsClient) *Provider {
	return &Provider{client: client}
}

func TestProvider_DoSynthesize(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(mockEdgeTTSHandler(t))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/edge/v1"
	client := newEdgeWsClient()
	client.BaseURL = wsURL
	p := newWithClient(client)

	result, err := p.DoSynthesize(context.Background(), sdk.SpeechParams{
		Text: "Hello",
		Config: map[string]any{
			"voice":    "en-US-JennyNeural",
			"language": "en-US",
		},
	})
	if err != nil {
		t.Fatalf("DoSynthesize: %v", err)
	}
	if len(result.Audio) == 0 {
		t.Fatal("expected non-empty audio")
	}
	if !bytes.Equal(result.Audio, []byte("fake-webm-audio-data")) {
		t.Errorf("audio = %q", string(result.Audio))
	}
	if result.ContentType != "audio/mpeg" {
		t.Errorf("content type = %q, want audio/mpeg", result.ContentType)
	}
}

func TestProvider_DoStream(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(mockEdgeTTSHandler(t))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/edge/v1"
	client := newEdgeWsClient()
	client.BaseURL = wsURL
	p := newWithClient(client)

	result, err := p.DoStream(context.Background(), sdk.SpeechParams{
		Text: "Hi",
		Config: map[string]any{
			"voice":    "en-US-JennyNeural",
			"language": "en-US",
		},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	audioData, err := result.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	if !bytes.Equal(audioData, []byte("fake-webm-audio-data")) {
		t.Errorf("audio = %q", string(audioData))
	}
}

func TestProvider_DoSynthesize_ConnectionFailure(t *testing.T) {
	t.Parallel()
	client := newEdgeWsClient()
	client.BaseURL = "ws://127.0.0.1:0/edge/v1"
	p := newWithClient(client)

	_, err := p.DoSynthesize(context.Background(), sdk.SpeechParams{
		Text: "x",
	})
	if err == nil {
		t.Fatal("expected error when connection fails")
	}
}

func TestProvider_SpeechModel(t *testing.T) {
	t.Parallel()
	p := New()

	m := p.SpeechModel("edge-read-aloud")
	if m.ID != "edge-read-aloud" {
		t.Errorf("ID = %q, want edge-read-aloud", m.ID)
	}
	if m.Provider != p {
		t.Error("provider mismatch")
	}

	m2 := p.SpeechModel("")
	if m2.ID != defaultModelID {
		t.Errorf("default ID = %q, want %q", m2.ID, defaultModelID)
	}
}

func TestProvider_ListModels(t *testing.T) {
	t.Parallel()
	p := New()

	models, err := p.ListModels(context.Background())
	if err == nil {
		t.Fatal("expected unsupported error")
	}
	if len(models) != 0 {
		t.Fatalf("len(models) = %d, want 0", len(models))
	}
}

func TestParseConfig(t *testing.T) {
	t.Parallel()

	cfg := parseConfig(map[string]any{
		"voice":    "zh-CN-XiaoxiaoNeural",
		"language": "zh-CN",
		"format":   "webm-24khz-16bit-mono-opus",
		"speed":    1.5,
		"pitch":    float64(-10),
	})
	if cfg.Voice != "zh-CN-XiaoxiaoNeural" {
		t.Errorf("voice = %q", cfg.Voice)
	}
	if cfg.Language != "zh-CN" {
		t.Errorf("language = %q", cfg.Language)
	}
	if cfg.Format != "webm-24khz-16bit-mono-opus" {
		t.Errorf("format = %q", cfg.Format)
	}
	if cfg.Speed != 1.5 {
		t.Errorf("speed = %v", cfg.Speed)
	}
	if cfg.Pitch != -10 {
		t.Errorf("pitch = %v", cfg.Pitch)
	}
}

func TestParseConfig_AutoLanguage(t *testing.T) {
	t.Parallel()
	cfg := parseConfig(map[string]any{
		"voice": "zh-CN-XiaoxiaoNeural",
	})
	if cfg.Language != "zh-CN" {
		t.Errorf("auto language = %q, want zh-CN", cfg.Language)
	}
}

func TestParseConfig_Nil(t *testing.T) {
	t.Parallel()
	cfg := parseConfig(nil)
	if cfg.Voice != "" || cfg.Language != "" || cfg.Format != "" {
		t.Errorf("nil config should produce empty audioConfig, got %+v", cfg)
	}
}

func TestResolveContentType(t *testing.T) {
	t.Parallel()
	cases := []struct {
		format string
		want   string
	}{
		{"audio-24khz-48kbitrate-mono-mp3", "audio/mpeg"},
		{"webm-24khz-16bit-mono-opus", "audio/opus"},
		{"audio/ogg", "audio/ogg"},
		{"audio/wav", "audio/wav"},
		{"", "audio/mpeg"},
	}
	for _, tc := range cases {
		got := resolveContentType(tc.format)
		if got != tc.want {
			t.Errorf("resolveContentType(%q) = %q, want %q", tc.format, got, tc.want)
		}
	}
}

func TestGenerateSpeech_PackageLevel(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(mockEdgeTTSHandler(t))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/edge/v1"
	p := New(WithBaseURL(wsURL))
	model := p.SpeechModel("edge-read-aloud")

	result, err := sdk.GenerateSpeech(context.Background(),
		sdk.WithSpeechModel(model),
		sdk.WithText("Hello"),
		sdk.WithSpeechConfig(map[string]any{
			"voice": "en-US-JennyNeural",
		}),
	)
	if err != nil {
		t.Fatalf("GenerateSpeech: %v", err)
	}
	if !bytes.Equal(result.Audio, []byte("fake-webm-audio-data")) {
		t.Errorf("audio = %q", string(result.Audio))
	}
}

func TestStreamSpeech_PackageLevel(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(mockEdgeTTSHandler(t))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/edge/v1"
	p := New(WithBaseURL(wsURL))
	model := p.SpeechModel("edge-read-aloud")

	sr, err := sdk.StreamSpeech(context.Background(),
		sdk.WithSpeechModel(model),
		sdk.WithText("Hi"),
		sdk.WithSpeechConfig(map[string]any{
			"voice": "en-US-JennyNeural",
		}),
	)
	if err != nil {
		t.Fatalf("StreamSpeech: %v", err)
	}
	audio, err := sr.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	if !bytes.Equal(audio, []byte("fake-webm-audio-data")) {
		t.Errorf("audio = %q", string(audio))
	}
}
