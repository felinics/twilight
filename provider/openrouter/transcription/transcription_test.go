package transcription

import (
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/felinics/twilight/provider/providertest"
	sdk "github.com/felinics/twilight/sdk"
)

func TestProvider_ListModels(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/models" {
			t.Errorf("path = %s, want /models", r.URL.Path)
		}
		if r.URL.Query().Get("output_modalities") != "all" {
			t.Errorf("output_modalities = %q, want all", r.URL.Query().Get("output_modalities"))
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"openai/gpt-4o-mini-transcribe","architecture":{"input_modalities":["audio"],"output_modalities":["text"]}},{"id":"openai/gpt-audio-mini","architecture":{"input_modalities":["text"],"output_modalities":["audio"]}},{"id":"openai/gpt-4o-mini","architecture":{"input_modalities":["text"],"output_modalities":["text"]}}]}`))
	}))
	defer srv.Close()

	p := New(WithAPIKey("key"), WithBaseURL(srv.URL))
	models, err := p.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models) != 1 || models[0].ID != "openai/gpt-4o-mini-transcribe" {
		t.Fatalf("unexpected models: %+v", models)
	}
}

func TestProvider_ListModels_ArrayResponse(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"id":"openai/gpt-4o-mini-transcribe","architecture":{"input_modalities":["audio"],"output_modalities":["text"]}},{"id":"openai/gpt-4o-mini","architecture":{"input_modalities":["text"],"output_modalities":["text"]}}]`))
	}))
	defer srv.Close()

	p := New(WithAPIKey("key"), WithBaseURL(srv.URL))
	models, err := p.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models) != 1 || models[0].ID != "openai/gpt-4o-mini-transcribe" {
		t.Fatalf("unexpected models: %+v", models)
	}
}

// catalogBody lists a transcription-only model, a chat model with audio input,
// and a model whose ID looks like a transcription model but is a chat model.
const catalogBody = `{"data":[
{"id":"openai/gpt-4o-mini-transcribe","architecture":{"modality":"audio->transcription","input_modalities":["audio"],"output_modalities":["transcription"]}},
{"id":"openai/whisper-large-v3-turbo","architecture":{"modality":"audio->transcription","input_modalities":["audio"],"output_modalities":["transcription"]}},
{"id":"openai/gpt-audio-mini","architecture":{"modality":"text+audio->text+audio","input_modalities":["text","audio"],"output_modalities":["text","audio"]}},
{"id":"acme/transcribe-chat","architecture":{"modality":"text+audio->text","input_modalities":["text","audio"],"output_modalities":["text"]}}
]}`

// fakeOpenRouter serves /models, /audio/transcriptions and /chat/completions
// and records which endpoint each request hit.
type fakeOpenRouter struct {
	catalogStatus atomic.Int32 // 0 means 200 with catalogBody
	catalogHits   atomic.Int32
	mu            sync.Mutex
	paths         []string
	lastModel     string
}

func (f *fakeOpenRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/models":
		f.catalogHits.Add(1)
		if status := int(f.catalogStatus.Load()); status != 0 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"code":500,"message":"boom"}}`))
			return
		}
		_, _ = w.Write([]byte(catalogBody))
		return
	case "/audio/transcriptions":
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.record(r.URL.Path, r.FormValue("model"))
		_, _ = w.Write([]byte(`{"text":"from transcriptions","usage":{"seconds":1.5}}`))
	case "/chat/completions":
		var body struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.record(r.URL.Path, body.Model)
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"from chat"}}]}`))
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeOpenRouter) record(path, model string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paths = append(f.paths, path)
	f.lastModel = model
}

func (f *fakeOpenRouter) last() (path, model string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.paths) == 0 {
		return "", ""
	}
	return f.paths[len(f.paths)-1], f.lastModel
}

func newFake(t *testing.T, catalogStatus int) (*fakeOpenRouter, *Provider) {
	t.Helper()
	fake := &fakeOpenRouter{}
	fake.catalogStatus.Store(int32(catalogStatus))
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	return fake, New(WithAPIKey("key"), WithBaseURL(srv.URL))
}

func transcribeWith(t *testing.T, p *Provider, modelID string) *sdk.TranscriptionResult {
	t.Helper()
	params := sdk.TranscriptionParams{Audio: []byte("audio"), Filename: "test.wav", ContentType: "audio/wav"}
	if modelID != "-" {
		params.Model = p.TranscriptionModel(modelID)
	}
	result, err := p.DoTranscribe(context.Background(), params)
	if err != nil {
		t.Fatalf("DoTranscribe(%q): %v", modelID, err)
	}
	return result
}

func TestProvider_DoTranscribe_RoutesByCatalog(t *testing.T) {
	t.Parallel()
	cases := []struct {
		model     string // "-" leaves params.Model nil
		wantPath  string
		wantModel string
		wantText  string
	}{
		{"", "/audio/transcriptions", defaultModelID, "from transcriptions"},
		{"-", "/audio/transcriptions", defaultModelID, "from transcriptions"},
		{"openai/gpt-4o-mini-transcribe", "/audio/transcriptions", "openai/gpt-4o-mini-transcribe", "from transcriptions"},
		{"openai/whisper-large-v3-turbo:free", "/audio/transcriptions", "openai/whisper-large-v3-turbo:free", "from transcriptions"},
		{"openai/gpt-audio-mini", "/chat/completions", "openai/gpt-audio-mini", "from chat"},
		// The catalog overrides the ID heuristic.
		{"acme/transcribe-chat", "/chat/completions", "acme/transcribe-chat", "from chat"},
		// Unknown to the catalog: heuristic decides.
		{"acme/new-whisper", "/audio/transcriptions", "acme/new-whisper", "from transcriptions"},
		{"acme/audio-chat", "/chat/completions", "acme/audio-chat", "from chat"},
	}
	fake, p := newFake(t, 0)
	for _, c := range cases {
		result := transcribeWith(t, p, c.model)
		path, model := fake.last()
		if path != c.wantPath || model != c.wantModel || result.Text != c.wantText {
			t.Errorf("model %q: path, model, text = %q, %q, %q; want %q, %q, %q",
				c.model, path, model, result.Text, c.wantPath, c.wantModel, c.wantText)
		}
	}
	if hits := fake.catalogHits.Load(); hits != 1 {
		t.Errorf("catalog fetched %d times, want 1 (cached)", hits)
	}
}

func TestProvider_DoTranscribe_CatalogFetchedOnceConcurrently(t *testing.T) {
	t.Parallel()
	fake, p := newFake(t, 0)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.DoTranscribe(context.Background(), sdk.TranscriptionParams{
				Model: p.TranscriptionModel("openai/gpt-audio-mini"), Audio: []byte("a"), Filename: "a.wav",
			})
			if err != nil {
				t.Errorf("DoTranscribe: %v", err)
			}
		}()
	}
	wg.Wait()
	if hits := fake.catalogHits.Load(); hits != 1 {
		t.Errorf("catalog fetched %d times, want 1", hits)
	}
}

func TestProvider_DoTranscribe_CatalogFailureFallsBackToHeuristic(t *testing.T) {
	t.Parallel()
	fake, p := newFake(t, http.StatusInternalServerError)
	cases := map[string]string{
		"openai/gpt-4o-mini-transcribe": "/audio/transcriptions",
		"openai/whisper-large-v3-turbo": "/audio/transcriptions",
		"openai/gpt-audio-mini":         "/chat/completions",
		// Without the catalog the heuristic is all there is.
		"acme/transcribe-chat": "/audio/transcriptions",
	}
	for model, want := range cases {
		transcribeWith(t, p, model)
		if path, _ := fake.last(); path != want {
			t.Errorf("model %q: path = %q, want %q", model, path, want)
		}
	}
	if hits := fake.catalogHits.Load(); hits != 1 {
		t.Fatalf("catalog fetched %d times, want 1 (retry is throttled)", hits)
	}

	// Once the retry delay has passed the catalog is fetched again, and a
	// successful fetch replaces the heuristic.
	fake.catalogStatus.Store(0)
	p.catalog.sem <- struct{}{}
	p.catalog.retryAt = time.Now().Add(-time.Second)
	<-p.catalog.sem
	transcribeWith(t, p, "acme/transcribe-chat")
	if path, _ := fake.last(); path != "/chat/completions" {
		t.Errorf("after recovery: path = %q, want /chat/completions", path)
	}
	if hits := fake.catalogHits.Load(); hits != 2 {
		t.Errorf("catalog fetched %d times, want 2", hits)
	}
}

func TestLooksLikeTranscriptionModel(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		"openai/gpt-4o-mini-transcribe":  true,
		"openai/whisper-large-v3-turbo":  true,
		"elevenlabs/scribe-v2":           true,
		"qwen/qwen3-asr-1.7b":            true,
		"x-ai/grok-stt-1.0":              true,
		"openai/gpt-audio-mini":          false,
		"google/gemini-3.8-flash":        false,
		"acme/fastrouter":                false,
		"thinkingmachines/inkling-small": false,
	}
	for id, want := range cases {
		if got := looksLikeTranscriptionModel(id); got != want {
			t.Errorf("looksLikeTranscriptionModel(%q) = %v, want %v", id, got, want)
		}
	}
}

func TestProvider_ListModels_FillsCatalog(t *testing.T) {
	t.Parallel()
	fake, p := newFake(t, 0)
	if _, err := p.ListModels(context.Background()); err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	transcribeWith(t, p, "acme/transcribe-chat")
	if path, _ := fake.last(); path != "/chat/completions" {
		t.Errorf("path = %q, want /chat/completions", path)
	}
	if hits := fake.catalogHits.Load(); hits != 1 {
		t.Errorf("catalog fetched %d times, want 1 (reused from ListModels)", hits)
	}
}

func TestProvider_DoTranscribe_MultipartFields(t *testing.T) {
	t.Parallel()
	var (
		form     *multipart.Form
		fileHead *multipart.FileHeader
		fileData []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/models" {
			_, _ = w.Write([]byte(catalogBody))
			return
		}
		if r.URL.Path != "/audio/transcriptions" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer key" {
			t.Errorf("Authorization = %q", got)
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("ParseMultipartForm: %v", err)
			return
		}
		form = r.MultipartForm
		f, h, err := r.FormFile("file")
		if err != nil {
			t.Errorf("FormFile: %v", err)
			return
		}
		defer f.Close()
		fileHead = h
		fileData, _ = io.ReadAll(f)
		_, _ = w.Write([]byte(`{"text":"ok"}`))
	}))
	defer srv.Close()

	p := New(WithAPIKey("key"), WithBaseURL(srv.URL))
	_, err := p.DoTranscribe(context.Background(), sdk.TranscriptionParams{
		Model:       p.TranscriptionModel("openai/gpt-4o-mini-transcribe"),
		Audio:       []byte("mp3-bytes"),
		ContentType: "audio/mpeg",
		Config: map[string]any{
			"prompt":                  "ignored on this endpoint",
			"language":                "en",
			"temperature":             0.2,
			"response_format":         "verbose_json",
			"timestamp_granularities": []any{"word", "segment"},
			"keyterms":                []string{"OpenRouter", "Twilight"},
			"diarize":                 true,
		},
	})
	if err != nil {
		t.Fatalf("DoTranscribe: %v", err)
	}
	want := map[string][]string{
		"model":                     {"openai/gpt-4o-mini-transcribe"},
		"language":                  {"en"},
		"temperature":               {"0.2"},
		"response_format":           {"verbose_json"},
		"timestamp_granularities[]": {"word", "segment"},
		"keyterms":                  {"OpenRouter", "Twilight"},
		"diarize":                   {"true"},
	}
	if !reflect.DeepEqual(form.Value, want) {
		t.Errorf("form values = %v, want %v", form.Value, want)
	}
	if fileHead.Filename != "audio.mp3" {
		t.Errorf("filename = %q, want audio.mp3 (derived from content type)", fileHead.Filename)
	}
	// OpenRouter rejects unquoted Content-Disposition parameters.
	if cd := fileHead.Header.Get("Content-Disposition"); cd != `form-data; name="file"; filename="audio.mp3"` {
		t.Errorf("file Content-Disposition = %q", cd)
	}
	if ct := fileHead.Header.Get("Content-Type"); ct != "audio/mpeg" {
		t.Errorf("file Content-Type = %q, want audio/mpeg", ct)
	}
	if string(fileData) != "mp3-bytes" {
		t.Errorf("file data = %q", fileData)
	}
}

func TestProvider_DoTranscribe_ChatPromptAndAudio(t *testing.T) {
	t.Parallel()
	var body struct {
		Model    string `json:"model"`
		Messages []struct {
			Content []struct {
				Type       string `json:"type"`
				Text       string `json:"text"`
				InputAudio struct {
					Data   string `json:"data"`
					Format string `json:"format"`
				} `json:"input_audio"`
			} `json:"content"`
		} `json:"messages"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/models" {
			_, _ = w.Write([]byte(catalogBody))
			return
		}
		if r.URL.Path != "/chat/completions" {
			t.Errorf("path = %s", r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":[{"type":"text","text":"hello "},{"type":"text","text":"world"}]}}]}`))
	}))
	defer srv.Close()

	p := New(WithAPIKey("key"), WithBaseURL(srv.URL))
	result, err := p.DoTranscribe(context.Background(), sdk.TranscriptionParams{
		Model:    p.TranscriptionModel("openai/gpt-audio-mini"),
		Audio:    []byte("audio"),
		Filename: "clip.flac",
		Config:   map[string]any{"prompt": "Transcribe in French."},
	})
	if err != nil {
		t.Fatalf("DoTranscribe: %v", err)
	}
	if result.Text != "hello world" {
		t.Errorf("text = %q", result.Text)
	}
	if len(body.Messages) != 1 || len(body.Messages[0].Content) != 2 {
		t.Fatalf("unexpected messages: %+v", body.Messages)
	}
	parts := body.Messages[0].Content
	if parts[0].Text != "Transcribe in French." {
		t.Errorf("prompt = %q", parts[0].Text)
	}
	if parts[1].Type != "input_audio" || parts[1].InputAudio.Format != "flac" || parts[1].InputAudio.Data != "YXVkaW8=" {
		t.Errorf("audio part = %+v", parts[1])
	}
}

func TestProvider_DoTranscribe_ResponseMapping(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		body string
		want sdk.TranscriptionResult
	}{
		"json": {
			body: `{"text":"hi there","usage":{"seconds":2.5,"cost":0.0001}}`,
			want: sdk.TranscriptionResult{Text: "hi there", DurationSeconds: 2.5},
		},
		"verbose_json": {
			body: `{"text":"hi there","task":"transcribe","language":"english","duration":2.4,
"segments":[{"id":0,"start":0,"end":2.4,"text":"hi there"}],
"words":[{"word":"hi","start":0,"end":0.3,"speaker":0},{"word":"there","start":0.3,"end":0.8,"speaker":1,"speaker_label":"speaker_b"}],
"usage":{"seconds":2.5}}`,
			want: sdk.TranscriptionResult{
				Text: "hi there", Language: "english", DurationSeconds: 2.4,
				Words: []sdk.TranscriptionWord{
					{Text: "hi", Start: 0, End: 0.3, SpeakerID: "0"},
					{Text: "there", Start: 0.3, End: 0.8, SpeakerID: "speaker_b"},
				},
			},
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/models" {
					_, _ = w.Write([]byte(catalogBody))
					return
				}
				_, _ = w.Write([]byte(c.body))
			}))
			defer srv.Close()
			p := New(WithAPIKey("key"), WithBaseURL(srv.URL))
			got := transcribeWith(t, p, "openai/whisper-large-v3-turbo")
			if !reflect.DeepEqual(*got, c.want) {
				t.Errorf("result = %+v, want %+v", *got, c.want)
			}
		})
	}
}

func TestProvider_DoTranscribe_TranscriptionsError(t *testing.T) {
	t.Parallel()
	const body = `{"error":{"code":403,"message":"The request is prohibited due to a violation of provider Terms Of Service.","metadata":{"error_type":"forbidden"}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/models" {
			_, _ = w.Write([]byte(catalogBody))
			return
		}
		if r.URL.Path != "/audio/transcriptions" {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	p := New(WithAPIKey("key"), WithBaseURL(srv.URL))
	_, err := p.DoTranscribe(context.Background(), sdk.TranscriptionParams{
		Model: p.TranscriptionModel(""), Audio: []byte("a"), Filename: "a.wav",
	})
	apiErr := providertest.WantAPIError(t, err, providerName, http.StatusForbidden, sdk.KindPermissionDenied)
	if !strings.Contains(apiErr.Message, "Terms Of Service") {
		t.Errorf("Message = %q", apiErr.Message)
	}
}
