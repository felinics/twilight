package execution

import (
	"context"
	"sync"

	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// Planner is the between-steps hook: it runs while a Run is Open and about
// to plan a model request, with the Writer of the Session being driven, so
// what it commits (an in-turn checkpoint) is what the PromptBuilder reads
// next. Errors stop the drive.
type Planner interface {
	BeforePrepare(ctx context.Context, w writer.Writer, input run.PromptInput) error
}

// builders resolves the prompt Builder of each AgentPreset once and hands
// the same Builder to every drive of a Run under that preset. A Builder is
// all a preset contributes to a drive: what it decides for a step travels in
// the Prompt it returns and is frozen onto the step, so settling the step
// later needs no preset.
type builders struct {
	presets preset.Registry
	catalog *prompt.Catalog
	sources prompt.Sources

	mu    sync.Mutex
	built map[preset.PresetRef]prompt.Builder
}

// For returns the Builder of the preset, resolving it on first use.
func (b *builders) For(ref preset.PresetRef) (prompt.Builder, error) {
	ap, err := b.presets.Resolve(ref)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if built, ok := b.built[ref]; ok {
		return built, nil
	}
	built, err := b.catalog.Resolve(ap, b.sources)
	if err != nil {
		return nil, err
	}
	if b.built == nil {
		b.built = make(map[preset.PresetRef]prompt.Builder)
	}
	b.built[ref] = built
	return built, nil
}
