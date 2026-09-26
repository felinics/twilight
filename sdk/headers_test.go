package sdk_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/felinics/twilight/provider/anthropic/messages"
	"github.com/felinics/twilight/provider/github/copilot"
	"github.com/felinics/twilight/provider/google/generativeai"
	"github.com/felinics/twilight/provider/openai/codex"
	"github.com/felinics/twilight/provider/openai/completions"
	"github.com/felinics/twilight/provider/openai/embedding"
	"github.com/felinics/twilight/provider/openai/images"
	"github.com/felinics/twilight/provider/openai/responses"
	"github.com/felinics/twilight/provider/openai/speech"
	"github.com/felinics/twilight/provider/openai/transcription"
	"github.com/felinics/twilight/sdk"
)

// Exercise every network entry point, including fallback model probes. Errors
// are intentional: the server rejects the request after observing its headers.
func TestChatProviderHeaders(t *testing.T) {
	cases := []struct {
		name         string
		localCatalog bool
		new          func(string, map[string]string) sdk.Provider
	}{
		{"completions", false, func(url string, h map[string]string) sdk.Provider {
			return completions.New(completions.WithBaseURL(url), completions.WithAPIKey("default"), completions.WithHeaders(h))
		}},
		{"responses", false, func(url string, h map[string]string) sdk.Provider {
			return responses.New(responses.WithBaseURL(url), responses.WithAPIKey("default"), responses.WithHeaders(h))
		}},
		{"messages", false, func(url string, h map[string]string) sdk.Provider {
			return messages.New(messages.WithBaseURL(url), messages.WithAPIKey("default"), messages.WithHeaders(h))
		}},
		{"google", false, func(url string, h map[string]string) sdk.Provider {
			return generativeai.New(generativeai.WithBaseURL(url), generativeai.WithAPIKey("default"), generativeai.WithHeaders(h))
		}},
		{"codex", true, func(url string, h map[string]string) sdk.Provider {
			return codex.New(codex.WithBaseURL(url), codex.WithAPIKey("default"), codex.WithHeaders(h))
		}},
		{"copilot", true, func(url string, h map[string]string) sdk.Provider {
			return copilot.New(copilot.WithBaseURL(url), copilot.WithAPIKey("default"), copilot.WithHeaders(h))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, operation := range []string{"list", "test", "probe", "generate", "stream"} {
				t.Run(operation, func(t *testing.T) {
					seen := make(chan http.Header, 4)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						seen <- r.Header.Clone()
						if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/model") {
							http.NotFound(w, r)
							return
						}
						http.Error(w, "observed", http.StatusTeapot)
					}))
					defer server.Close()
					defaults := map[string]string{"X-Provider": "original", "X-Trace": "provider", "authorization": "provider-auth", "Accept": "provider-accept"}
					p := tc.new(server.URL, defaults)
					defaults["X-Provider"] = "mutated"
					parent := sdk.WithRequestHeaders(context.Background(), map[string]string{"x-trace": "parent", "X-Parent": "inherited"})
					call := map[string]string{"X-TRACE": "call", "Authorization": "call-auth", "accept": "call-accept", "content-type": "application/custom"}
					ctx := sdk.WithRequestHeaders(parent, call)
					call["X-TRACE"] = "mutated"
					req := sdk.Request{Model: "model", Messages: []sdk.Message{sdk.UserMessage("hi")}}
					switch operation {
					case "list":
						_, _ = p.ListModels(ctx)
					case "test":
						_ = p.Test(ctx)
					case "probe":
						_, _ = p.TestModel(ctx, "model")
					case "generate":
						_, _ = p.DoGenerate(ctx, req)
					case "stream":
						if parts, err := p.DoStream(ctx, req); err == nil {
							for range parts {
							}
						}
					}
					want := 1
					if operation == "list" && tc.localCatalog {
						want = 0
					}
					if operation == "probe" && !tc.localCatalog {
						want = 2
					}
					if len(seen) != want {
						t.Fatalf("saw %d requests, want %d", len(seen), want)
					}
					for range want {
						h := <-seen
						for key, value := range map[string]string{"X-Provider": "original", "X-Trace": "call", "X-Parent": "inherited", "Authorization": "call-auth"} {
							if h.Get(key) != value {
								t.Errorf("%s = %q, want %q", key, h.Get(key), value)
							}
							if len(h.Values(key)) != 1 {
								t.Errorf("duplicate header %s: %v", key, h.Values(key))
							}
						}
						if operation == "stream" && h.Get("Accept") != "text/event-stream" {
							t.Errorf("stream Accept = %q", h.Get("Accept"))
						}
						if (operation == "generate" || operation == "stream") && h.Get("Content-Type") != "application/json" {
							t.Errorf("%s Content-Type = %q", operation, h.Get("Content-Type"))
						}
					}
				})
			}
		})
	}
}

func TestOpenAIMediaHeaders(t *testing.T) {
	const (
		jsonContentType      = "application/json"
		multipartContentType = "multipart/form-data; boundary="
	)
	cases := []struct {
		name        string
		contentType string // expected prefix; empty for bodiless requests
		call        func(context.Context, string, map[string]string)
	}{
		{"embedding", jsonContentType, func(ctx context.Context, url string, h map[string]string) {
			p := embedding.New(embedding.WithBaseURL(url), embedding.WithHeaders(h))
			_, _ = p.DoEmbed(ctx, sdk.EmbedParams{Model: p.EmbeddingModel("model"), Values: []string{"hi"}})
		}},
		{"image-generate", jsonContentType, func(ctx context.Context, url string, h map[string]string) {
			p := images.New(images.WithBaseURL(url), images.WithHeaders(h))
			_, _ = p.DoGenerate(ctx, &sdk.ImageGenerationParams{Model: p.GenerationModel("model"), Prompt: "hi"})
		}},
		{"image-edit-json", jsonContentType, func(ctx context.Context, url string, h map[string]string) {
			p := images.New(images.WithBaseURL(url), images.WithHeaders(h))
			_, _ = p.DoEdit(ctx, &sdk.ImageEditParams{Model: p.EditModel("model"), Prompt: "hi", Images: []sdk.ImageInput{{URL: "https://example.test/image.png"}}})
		}},
		{"image-edit-multipart", multipartContentType, func(ctx context.Context, url string, h map[string]string) {
			p := images.New(images.WithBaseURL(url), images.WithHeaders(h))
			_, _ = p.DoEdit(ctx, &sdk.ImageEditParams{Model: p.EditModel("model"), Prompt: "hi", Images: []sdk.ImageInput{{Data: []byte("image"), Filename: "image.png"}}})
		}},
		{"speech-list", "", func(ctx context.Context, url string, h map[string]string) {
			p := speech.New(speech.WithBaseURL(url), speech.WithHeaders(h))
			_, _ = p.ListModels(ctx)
		}},
		{"speech-generate", jsonContentType, func(ctx context.Context, url string, h map[string]string) {
			p := speech.New(speech.WithBaseURL(url), speech.WithHeaders(h))
			_, _ = p.DoSynthesize(ctx, sdk.SpeechParams{Model: p.SpeechModel("tts-1"), Text: "hi"})
		}},
		{"speech-stream", jsonContentType, func(ctx context.Context, url string, h map[string]string) {
			p := speech.New(speech.WithBaseURL(url), speech.WithHeaders(h))
			_, _ = p.DoStream(ctx, sdk.SpeechParams{Model: p.SpeechModel("tts-1"), Text: "hi"})
		}},
		{"transcription-list", "", func(ctx context.Context, url string, h map[string]string) {
			p := transcription.New(transcription.WithBaseURL(url), transcription.WithHeaders(h))
			_, _ = p.ListModels(ctx)
		}},
		{"transcription", multipartContentType, func(ctx context.Context, url string, h map[string]string) {
			p := transcription.New(transcription.WithBaseURL(url), transcription.WithHeaders(h))
			_, _ = p.DoTranscribe(ctx, sdk.TranscriptionParams{Model: p.TranscriptionModel("whisper-1"), Audio: []byte("audio"), Filename: "audio.wav"})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seen := make(chan http.Header, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen <- r.Header.Clone()
				if tc.contentType == multipartContentType {
					if err := r.ParseMultipartForm(1024); err != nil {
						t.Errorf("invalid multipart body: %v", err)
					}
				}
				http.Error(w, "observed", http.StatusTeapot)
			}))
			defer srv.Close()
			ctx := sdk.WithRequestHeaders(context.Background(), map[string]string{"x-trace": "call", "Content-Type": "application/custom"})
			tc.call(ctx, srv.URL, map[string]string{"X-Trace": "provider", "X-Provider": "kept"})
			if len(seen) != 1 {
				t.Fatal("request did not reach server")
			}
			h := <-seen
			if h.Get("X-Trace") != "call" || h.Get("X-Provider") != "kept" {
				t.Fatalf("headers = %v", h)
			}
			if tc.contentType != "" && !strings.HasPrefix(h.Get("Content-Type"), tc.contentType) {
				t.Fatalf("Content-Type was overwritten, want prefix %q: %v", tc.contentType, h)
			}
		})
	}
}

func TestRequestHeadersBeforeBedrockSigning(t *testing.T) {
	seen := make(chan http.Header, 3)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
		http.Error(w, "observed", http.StatusTeapot)
	}))
	defer srv.Close()
	ctx := sdk.WithRequestHeaders(context.Background(), map[string]string{"X-Trace": "signed", "Authorization": "replace-me"})
	c := completions.New(completions.WithBaseURL(srv.URL), completions.WithHeaders(map[string]string{"X-Provider": "kept"}), completions.WithBedrockCredentials("us-east-1", "access", "secret", ""))
	r := responses.New(responses.WithBaseURL(srv.URL), responses.WithHeaders(map[string]string{"X-Provider": "kept"}), responses.WithBedrockCredentials("us-east-1", "access", "secret", ""))
	e := embedding.New(embedding.WithBaseURL(srv.URL), embedding.WithHeaders(map[string]string{"X-Provider": "kept"}), embedding.WithBedrockCredentials("us-east-1", "access", "secret", ""))
	_, _ = c.DoGenerate(ctx, sdk.Request{Model: "model"})
	_, _ = r.DoGenerate(ctx, sdk.Request{Model: "model"})
	_, _ = e.DoEmbed(ctx, sdk.EmbedParams{Model: e.EmbeddingModel("model"), Values: []string{"hi"}})
	if len(seen) != 3 {
		t.Fatalf("saw %d requests", len(seen))
	}
	for range 3 {
		h := <-seen
		if h.Get("X-Trace") != "signed" || h.Get("X-Provider") != "kept" || !strings.HasPrefix(h.Get("Authorization"), "AWS4-HMAC-SHA256 ") || !strings.Contains(h.Get("Authorization"), "x-trace") {
			t.Errorf("incorrect signed headers: %v", h)
		}
	}
}
