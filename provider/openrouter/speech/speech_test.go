package speech

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sdk "github.com/felinics/twilight/sdk"
)

// makePCMChunk creates a small raw PCM-16 byte slice (silence) and returns its
// base64 encoding, suitable for embedding in a fake SSE audio delta event.
func makePCMChunk(samples int) string {
	pcm := make([]byte, samples*2) // 16-bit = 2 bytes per sample, all zero = silence
	return base64.StdEncoding.EncodeToString(pcm)
}

// fakeSSEBody builds an SSE response with the given base64 PCM chunks.
func fakeSSEBody(chunks []string) string {
	var sb strings.Builder
	for _, chunk := range chunks {
		evt := map[string]any{
			"choices": []map[string]any{
				{
					"delta": map[string]any{"audio": map[string]any{
						"data": chunk,
					}},
				},
			},
		}
		data, _ := json.Marshal(evt)
		fmt.Fprintf(&sb, "data: %s\n\n", data)
	}
	sb.WriteString("data: [DONE]\n\n")
	return sb.String()
}

// mockOpenRouterHandler returns a handler that validates the request and returns
// a fake SSE audio stream.
func mockOpenRouterHandler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/chat/completions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") == "" {
			http.Error(w, "missing auth", http.StatusUnauthorized)
			return
		}

		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		// Verify audio modality is present.
		mods, _ := body["modalities"].([]any)
		hasAudio := false
		for _, m := range mods {
			if m == "audio" {
				hasAudio = true
			}
		}
		if !hasAudio {
			t.Error("request missing audio modality")
		}
		if body["stream"] != true {
			t.Error("stream must be true")
		}

		// Respond with two small PCM chunks then [DONE].
		chunk := makePCMChunk(16)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, fakeSSEBody([]string{chunk, chunk}))
	})
}

func TestProvider_DoSynthesize(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(mockOpenRouterHandler(t))
	defer srv.Close()

	p := New(WithAPIKey("test-key"), WithBaseURL(srv.URL))

	result, err := p.DoSynthesize(context.Background(), sdk.SpeechParams{
		Text:   "Hello OpenRouter",
		Config: map[string]any{"voice": "coral"},
	})
	if err != nil {
		t.Fatalf("DoSynthesize: %v", err)
	}
	if result.ContentType != contentTypeAudio {
		t.Errorf("content type = %q, want %q", result.ContentType, contentTypeAudio)
	}
	// Result should be a valid WAV file (starts with "RIFF").
	if !bytes.HasPrefix(result.Audio, []byte("RIFF")) {
		t.Errorf("expected WAV header, got: %q", result.Audio[:min(len(result.Audio), 8)])
	}
}

func TestProvider_DoStream(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(mockOpenRouterHandler(t))
	defer srv.Close()

	p := New(WithAPIKey("test-key"), WithBaseURL(srv.URL))

	result, err := p.DoStream(context.Background(), sdk.SpeechParams{
		Text: "Streaming test",
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	audio, err := result.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	if !bytes.HasPrefix(audio, []byte("RIFF")) {
		t.Errorf("expected WAV, got: %q", audio[:min(len(audio), 8)])
	}
}

func TestProvider_DoSynthesize_HTTPError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"invalid key"}`, http.StatusUnauthorized)
	}))
	defer srv.Close()

	p := New(WithAPIKey("bad"), WithBaseURL(srv.URL))
	_, err := p.DoSynthesize(context.Background(), sdk.SpeechParams{Text: "test"})
	if err == nil {
		t.Fatal("expected error for 401")
	}
}

func TestProvider_DoSynthesize_NoAudio(t *testing.T) {
	t.Parallel()
	// Server returns a valid SSE stream but with no audio chunks.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	p := New(WithAPIKey("key"), WithBaseURL(srv.URL))
	_, err := p.DoSynthesize(context.Background(), sdk.SpeechParams{Text: "test"})
	if err == nil {
		t.Fatal("expected error when no audio data returned")
	}
}

func TestProvider_SpeechModel(t *testing.T) {
	t.Parallel()
	p := New()
	m := p.SpeechModel("openai/gpt-4o-audio-preview")
	if m.ID != "openai/gpt-4o-audio-preview" {
		t.Errorf("ID = %q", m.ID)
	}
	m2 := p.SpeechModel("")
	if m2.ID != defaultModelID {
		t.Errorf("default ID = %q, want %q", m2.ID, defaultModelID)
	}
}

func TestProvider_ListModels(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/models" {
			t.Errorf("path = %s, want /models", r.URL.Path)
		}
		if r.URL.Query().Get("output_modalities") != "all" {
			t.Errorf("output_modalities = %q, want all", r.URL.Query().Get("output_modalities"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"openai/gpt-audio-mini","architecture":{"input_modalities":["text"],"output_modalities":["audio"]}},{"id":"openai/gpt-4o-audio-preview","architecture":{"input_modalities":["text"],"output_modalities":["audio"]}},{"id":"openai/gpt-4.1-mini","architecture":{"input_modalities":["text"],"output_modalities":["text"]}}]}`))
	}))
	defer srv.Close()

	p := New(WithAPIKey("key"), WithBaseURL(srv.URL))
	models, err := p.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models) != 2 {
		t.Fatalf("len(models) = %d, want 2", len(models))
	}
	if models[0].ID != "openai/gpt-audio-mini" || models[1].ID != "openai/gpt-4o-audio-preview" {
		t.Fatalf("unexpected models: %q, %q", models[0].ID, models[1].ID)
	}
}

func TestProvider_ListModels_ArrayResponse(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"id":"openai/gpt-audio-mini","architecture":{"input_modalities":["text"],"output_modalities":["audio"]}},{"id":"openai/gpt-4o-audio-preview","architecture":{"input_modalities":["text"],"output_modalities":["speech"]}},{"id":"openai/gpt-4.1-mini","architecture":{"input_modalities":["text"],"output_modalities":["text"]}}]`))
	}))
	defer srv.Close()

	p := New(WithAPIKey("key"), WithBaseURL(srv.URL))
	models, err := p.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models) != 2 {
		t.Fatalf("len(models) = %d, want 2", len(models))
	}
}

func TestBuildWAV(t *testing.T) {
	t.Parallel()
	pcm := make([]byte, 100) // 50 samples of silence
	wav := buildWAV(pcm, 24000)

	// RIFF header
	if string(wav[0:4]) != "RIFF" {
		t.Fatalf("missing RIFF marker: %q", wav[0:4])
	}
	if string(wav[8:12]) != "WAVE" {
		t.Fatalf("missing WAVE marker: %q", wav[8:12])
	}
	// Chunk size = 36 + len(pcm)
	chunkSize := binary.LittleEndian.Uint32(wav[4:8])
	if chunkSize != 36+100 {
		t.Errorf("chunk size = %d, want %d", chunkSize, 36+100)
	}
	// data sub-chunk
	if string(wav[36:40]) != "data" {
		t.Fatalf("missing data marker: %q", wav[36:40])
	}
	dataSize := binary.LittleEndian.Uint32(wav[40:44])
	if dataSize != 100 {
		t.Errorf("data size = %d, want 100", dataSize)
	}
}

func TestReadAudioStream(t *testing.T) {
	t.Parallel()
	chunk1 := base64.StdEncoding.EncodeToString([]byte("hello"))
	chunk2 := base64.StdEncoding.EncodeToString([]byte("world"))
	chunk3 := base64.StdEncoding.EncodeToString([]byte("!"))
	sse := fakeSSEBody([]string{chunk1, chunk2, chunk3})

	var got [][]byte
	err := readAudioStream(strings.NewReader(sse), nil, func(pcm []byte) error {
		got = append(got, pcm)
		return nil
	})
	if err != nil {
		t.Fatalf("readAudioStream: %v", err)
	}
	// "hello" is 5 bytes: the odd byte is carried into the next delta, and the
	// trailing odd byte of the stream comes out last.
	want := []string{"hell", "oworld", "!"}
	if len(got) != len(want) {
		t.Fatalf("got %d chunks %q, want %q", len(got), got, want)
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Errorf("chunk %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestParseConfig_Defaults(t *testing.T) {
	t.Parallel()
	cfg := parseConfig(nil)
	if cfg.Model != defaultModel {
		t.Errorf("default model = %q, want %q", cfg.Model, defaultModel)
	}
	if cfg.Voice != defaultVoice {
		t.Errorf("default voice = %q, want %q", cfg.Voice, defaultVoice)
	}
}

func TestParseConfig(t *testing.T) {
	t.Parallel()
	cfg := parseConfig(map[string]any{
		"model": "openai/gpt-4o-audio-preview",
		"voice": "ash",
		"speed": 1.5,
	})
	if cfg.Model != "openai/gpt-4o-audio-preview" {
		t.Errorf("model = %q", cfg.Model)
	}
	if cfg.Voice != "ash" {
		t.Errorf("voice = %q", cfg.Voice)
	}
	if cfg.Speed != 1.5 {
		t.Errorf("speed = %v", cfg.Speed)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
