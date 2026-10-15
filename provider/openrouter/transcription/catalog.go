package transcription

import (
	"context"
	"strings"
	"time"
)

const (
	// catalogTTL is how long a fetched model catalog is trusted before refresh.
	catalogTTL = time.Hour
	// catalogRetryDelay throttles catalog refetches after a failed fetch so a
	// broken /models endpoint does not add a request to every transcription.
	catalogRetryDelay = 30 * time.Second
	// catalogTimeout bounds one catalog fetch, independent of the caller's
	// deadline, so a slow /models endpoint cannot stall transcriptions.
	catalogTimeout = 15 * time.Second
)

// modelCatalog caches, per Provider, which OpenRouter models are
// transcription-only and therefore must be called through
// /audio/transcriptions instead of /chat/completions.
//
// All fields are guarded by sem, a one-slot semaphore. A channel is used
// instead of a sync.Mutex so that callers waiting for an in-flight fetch can
// give up when their context ends.
type modelCatalog struct {
	sem chan struct{}

	// transcriptionOnly maps model ID to whether it is transcription-only.
	// nil until the first successful fetch.
	transcriptionOnly map[string]bool
	expiresAt         time.Time
	retryAt           time.Time
}

func newModelCatalog() *modelCatalog {
	return &modelCatalog{sem: make(chan struct{}, 1)}
}

func (c *modelCatalog) lock(ctx context.Context) bool {
	select {
	case c.sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *modelCatalog) unlock() { <-c.sem }

// get returns the cached catalog, fetching it when missing or expired. It
// returns nil when no catalog is available; a stale catalog is returned when
// a refresh fails.
func (c *modelCatalog) get(ctx context.Context, fetch func(context.Context) ([]openRouterModel, error)) map[string]bool {
	if !c.lock(ctx) {
		return nil
	}
	defer c.unlock()

	now := time.Now()
	if c.transcriptionOnly != nil && now.Before(c.expiresAt) {
		return c.transcriptionOnly
	}
	if now.Before(c.retryAt) {
		return c.transcriptionOnly
	}

	fetchCtx, cancel := context.WithTimeout(ctx, catalogTimeout)
	defer cancel()
	models, err := fetch(fetchCtx)
	if err != nil {
		// A cancelled caller says nothing about the endpoint's health.
		if ctx.Err() == nil {
			c.retryAt = now.Add(catalogRetryDelay)
		}
		return c.transcriptionOnly
	}
	c.storeLocked(models, now)
	return c.transcriptionOnly
}

// put records a catalog fetched elsewhere (ListModels).
func (c *modelCatalog) put(ctx context.Context, models []openRouterModel) {
	if !c.lock(ctx) {
		return
	}
	defer c.unlock()
	c.storeLocked(models, time.Now())
}

func (c *modelCatalog) storeLocked(models []openRouterModel, now time.Time) {
	m := make(map[string]bool, len(models))
	for _, model := range models {
		if model.ID != "" {
			m[model.ID] = isTranscriptionOnlyModel(model)
		}
	}
	c.transcriptionOnly = m
	c.expiresAt = now.Add(catalogTTL)
	c.retryAt = time.Time{}
}

// isTranscriptionOnlyModel reports whether the catalog lists the model as
// audio->transcription, which OpenRouter serves only on /audio/transcriptions.
// Chat models with audio input output "text" and stay on /chat/completions.
func isTranscriptionOnlyModel(m openRouterModel) bool {
	outputs := m.Architecture.OutputModalities
	return hasModality(outputs, "transcription") && !hasModality(outputs, "text")
}

func hasModality(modalities []string, want string) bool {
	for _, m := range modalities {
		if strings.EqualFold(m, want) {
			return true
		}
	}
	return false
}

// looksLikeTranscriptionModel is the fallback used when the catalog is
// unavailable or does not list the model: "transcribe"/"scribe" or "whisper"
// anywhere in the ID, or an "asr"/"stt" name segment (qwen/qwen3-asr-1.7b,
// x-ai/grok-stt-1.0).
func looksLikeTranscriptionModel(id string) bool {
	id = strings.ToLower(id)
	if strings.Contains(id, "scribe") || strings.Contains(id, "whisper") {
		return true
	}
	segments := strings.FieldsFunc(id, func(r rune) bool {
		return r == '/' || r == '-' || r == '_' || r == '.' || r == ':'
	})
	for _, s := range segments {
		if s == "asr" || s == "stt" {
			return true
		}
	}
	return false
}

// usesTranscriptionsEndpoint decides the endpoint for modelID: catalog first
// (exact ID, then without a ":variant" suffix), then the ID heuristic.
func (p *Provider) usesTranscriptionsEndpoint(ctx context.Context, modelID string) bool {
	if catalog := p.catalog.get(ctx, p.fetchModels); catalog != nil {
		if only, ok := catalog[modelID]; ok {
			return only
		}
		if base, _, cut := strings.Cut(modelID, ":"); cut {
			if only, ok := catalog[base]; ok {
				return only
			}
		}
	}
	return looksLikeTranscriptionModel(modelID)
}
