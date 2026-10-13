package embedding_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/provider/google/embedding"
	"github.com/felinics/twilight/provider/providertest"
	"github.com/felinics/twilight/sdk"
)

func newTestServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *embedding.Provider) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	p := embedding.New(
		embedding.WithAPIKey("test-key"),
		embedding.WithBaseURL(srv.URL),
	)
	return srv, p
}

// ---------- single value (embedContent) ----------

func TestDoEmbed_Single(t *testing.T) {
	var capturedPath string
	var capturedBody map[string]any

	_, p := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		if r.Header.Get("x-goog-api-key") != "test-key" {
			t.Errorf("unexpected auth header: %s", r.Header.Get("x-goog-api-key"))
		}
		json.NewDecoder(r.Body).Decode(&capturedBody)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"embedding": map[string]any{
				"values": []float64{0.1, 0.2, 0.3, 0.4, 0.5},
			},
		})
	})

	model := p.EmbeddingModel("gemini-embedding-001")
	result, err := p.DoEmbed(context.Background(), sdk.EmbedParams{
		Model:  model,
		Values: []string{"hello world"},
	})
	if err != nil {
		t.Fatalf("DoEmbed failed: %v", err)
	}

	if capturedPath != "/models/gemini-embedding-001:embedContent" {
		t.Errorf("unexpected path: %s", capturedPath)
	}
	if capturedBody["model"] != "models/gemini-embedding-001" {
		t.Errorf("unexpected model in body: %v", capturedBody["model"])
	}

	if len(result.Embeddings) != 1 {
		t.Fatalf("expected 1 embedding, got %d", len(result.Embeddings))
	}
	if len(result.Embeddings[0]) != 5 {
		t.Errorf("expected 5 dimensions, got %d", len(result.Embeddings[0]))
	}
	if result.Embeddings[0][0] != 0.1 {
		t.Errorf("expected 0.1, got %f", result.Embeddings[0][0])
	}
}

// ---------- batch (batchEmbedContents) ----------

func TestDoEmbed_Batch(t *testing.T) {
	var capturedPath string
	var capturedBody map[string]any

	_, p := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		json.NewDecoder(r.Body).Decode(&capturedBody)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"embeddings": []map[string]any{
				{"values": []float64{0.1, 0.2, 0.3}},
				{"values": []float64{0.4, 0.5, 0.6}},
			},
		})
	})

	model := p.EmbeddingModel("gemini-embedding-001")
	result, err := p.DoEmbed(context.Background(), sdk.EmbedParams{
		Model:  model,
		Values: []string{"sunny day", "rainy day"},
	})
	if err != nil {
		t.Fatalf("DoEmbed failed: %v", err)
	}

	if capturedPath != "/models/gemini-embedding-001:batchEmbedContents" {
		t.Errorf("unexpected path: %s", capturedPath)
	}

	requests, ok := capturedBody["requests"].([]any)
	if !ok || len(requests) != 2 {
		t.Fatalf("expected 2 requests, got %v", capturedBody["requests"])
	}

	if len(result.Embeddings) != 2 {
		t.Fatalf("expected 2 embeddings, got %d", len(result.Embeddings))
	}
	if result.Embeddings[0][0] != 0.1 {
		t.Errorf("expected 0.1, got %f", result.Embeddings[0][0])
	}
	if result.Embeddings[1][0] != 0.4 {
		t.Errorf("expected 0.4, got %f", result.Embeddings[1][0])
	}
}

// ---------- dimensions (outputDimensionality) ----------

func TestDoEmbed_WithDimensions(t *testing.T) {
	var capturedBody map[string]any

	_, p := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&capturedBody)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"embedding": map[string]any{
				"values": []float64{0.1, 0.2},
			},
		})
	})

	model := p.EmbeddingModel("gemini-embedding-001")
	dims := 256
	_, err := p.DoEmbed(context.Background(), sdk.EmbedParams{
		Model:      model,
		Values:     []string{"hello"},
		Dimensions: &dims,
	})
	if err != nil {
		t.Fatalf("DoEmbed failed: %v", err)
	}

	if capturedBody["outputDimensionality"] != float64(256) {
		t.Errorf("expected outputDimensionality 256, got %v", capturedBody["outputDimensionality"])
	}
}

// ---------- taskType ----------

func TestDoEmbed_WithTaskType(t *testing.T) {
	var capturedBody map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&capturedBody)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"embedding": map[string]any{
				"values": []float64{0.1},
			},
		})
	}))
	defer srv.Close()

	p := embedding.New(
		embedding.WithAPIKey("test-key"),
		embedding.WithBaseURL(srv.URL),
		embedding.WithTaskType("RETRIEVAL_QUERY"),
	)

	model := p.EmbeddingModel("gemini-embedding-001")
	_, err := p.DoEmbed(context.Background(), sdk.EmbedParams{
		Model:  model,
		Values: []string{"search query"},
	})
	if err != nil {
		t.Fatalf("DoEmbed failed: %v", err)
	}

	if capturedBody["taskType"] != "RETRIEVAL_QUERY" {
		t.Errorf("expected taskType RETRIEVAL_QUERY, got %v", capturedBody["taskType"])
	}
}

// ---------- custom model path ----------

func TestDoEmbed_CustomModelPath(t *testing.T) {
	var capturedPath string

	_, p := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"embedding": map[string]any{
				"values": []float64{0.1},
			},
		})
	})

	model := p.EmbeddingModel("publishers/google/models/gemini-embedding-001")
	_, err := p.DoEmbed(context.Background(), sdk.EmbedParams{
		Model:  model,
		Values: []string{"hello"},
	})
	if err != nil {
		t.Fatalf("DoEmbed failed: %v", err)
	}

	expected := "/publishers/google/models/gemini-embedding-001:embedContent"
	if capturedPath != expected {
		t.Errorf("expected path %q, got %q", expected, capturedPath)
	}
}

// ---------- nil model ----------

func TestDoEmbed_NilModel(t *testing.T) {
	p := embedding.New(embedding.WithAPIKey("test-key"))

	_, err := p.DoEmbed(context.Background(), sdk.EmbedParams{
		Values: []string{"hello"},
	})
	if err == nil {
		t.Fatal("expected error for nil model")
	}
}

// ---------- API error ----------

func TestDoEmbed_APIError(t *testing.T) {
	_, p := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
			"message": "API key not valid",
		}})
	})

	model := p.EmbeddingModel("gemini-embedding-001")
	_, err := p.DoEmbed(context.Background(), sdk.EmbedParams{
		Model:  model,
		Values: []string{"hello"},
	})
	providertest.WantAPIError(t, err, "google-embedding", 403, sdk.KindPermissionDenied)
}

// ---------- EmbeddingModel factory ----------

func TestEmbeddingModel(t *testing.T) {
	p := embedding.New(embedding.WithAPIKey("test-key"))
	model := p.EmbeddingModel("gemini-embedding-001")

	if model.ID != "gemini-embedding-001" {
		t.Errorf("expected model ID 'gemini-embedding-001', got %q", model.ID)
	}
	if model.MaxEmbeddingsPerCall != 2048 {
		t.Errorf("expected MaxEmbeddingsPerCall 2048, got %d", model.MaxEmbeddingsPerCall)
	}
	if model.Provider == nil {
		t.Error("expected non-nil provider")
	}
}

// ---------- sdk.Embed convenience ----------

func TestClientEmbed(t *testing.T) {
	_, p := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"embedding": map[string]any{
				"values": []float64{0.7, 0.8, 0.9},
			},
		})
	})

	model := p.EmbeddingModel("gemini-embedding-001")

	vec, err := sdk.Embed(context.Background(), "test",
		sdk.WithEmbeddingModel(model),
	)
	if err != nil {
		t.Fatalf("Embed failed: %v", err)
	}
	if len(vec) != 3 {
		t.Errorf("expected 3 dimensions, got %d", len(vec))
	}
	if vec[0] != 0.7 {
		t.Errorf("expected 0.7, got %f", vec[0])
	}
}

func TestClientEmbedMany(t *testing.T) {
	_, p := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"embeddings": []map[string]any{
				{"values": []float64{0.1, 0.2}},
				{"values": []float64{0.3, 0.4}},
			},
		})
	})

	model := p.EmbeddingModel("gemini-embedding-001")

	result, err := sdk.EmbedMany(context.Background(), []string{"a", "b"},
		sdk.WithEmbeddingModel(model),
	)
	if err != nil {
		t.Fatalf("EmbedMany failed: %v", err)
	}
	if len(result.Embeddings) != 2 {
		t.Errorf("expected 2 embeddings, got %d", len(result.Embeddings))
	}
}

// ---------- ListModels ----------

func TestListModels(t *testing.T) {
	_, p := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/models" {
			t.Errorf("request: got %s %s, want GET /models", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"models": []map[string]any{
				{
					"name":                       "models/gemini-2.5-flash",
					"supportedGenerationMethods": []string{"generateContent"},
				},
				{
					"name":                       "models/gemini-embedding-001",
					"displayName":                "Gemini Embedding 001",
					"description":                "Text embeddings",
					"inputTokenLimit":            2048,
					"supportedGenerationMethods": []string{"embedContent", "countTokens"},
				},
			},
		})
	})

	models, err := p.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models) != 1 {
		t.Fatalf("models: got %d, want the embedding model only", len(models))
	}
	m := models[0]
	if m.ID != "gemini-embedding-001" || m.DisplayName != "Gemini Embedding 001" || m.Description != "Text embeddings" {
		t.Errorf("model: %+v", m)
	}
	if m.MaxInputTokens != 2048 || m.MaxEmbeddingsPerCall != 2048 {
		t.Errorf("limits: input %d, per call %d", m.MaxInputTokens, m.MaxEmbeddingsPerCall)
	}
	if m.Provider != p {
		t.Error("a listed model must be bound to the provider")
	}
}
