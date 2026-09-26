package chatlog

import (
	"fmt"
	"testing"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
)

// benchEvents builds n model_step_completed facts, the cheapest event that
// grows both the maps and EntryOrder of each projection. Each event carries
// the ledger Position a fold would stamp on it.
func benchEvents(n int) []extension.DecodedEvent {
	out := make([]extension.DecodedEvent, n)
	for i := range out {
		out[i] = extension.DecodedEvent{
			Position: session.Position{Commit: session.CommitSeq(i)},
			Value:    runmod.Event{RunID: "r", Fact: run.ModelStepCompleted{StepID: run.StepID(fmt.Sprint(i)), FinishReason: model.FinishReasonStop, ResultDigest: "sha256:x"}},
		}
	}
	return out
}

func benchFold(b *testing.B, def extension.ProjectionDefinition, n int) {
	events := benchEvents(n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state, err := def.Initial()
		if err != nil {
			b.Fatal(err)
		}
		for _, e := range events {
			if state, err = def.Apply(state, e); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// Both folds once copied the whole derived state per event, so folding an
// N-event log cost O(N^2) (3200 events: 194 ms Surface, 66 ms Context).
// Context now appends and copies only the map an event writes; Surface keeps
// its content in persistent Tables (O(sqrt(n)) per write). These benchmarks
// pin the shape: doubling N should roughly double elapsed time. The cost is
// paid by OpenWriter's rebuild and ProjectionReader.Load, which fold the log.
func BenchmarkSurfaceFold(b *testing.B) {
	for _, n := range []int{400, 800, 1600, 3200} {
		b.Run(fmt.Sprint(n), func(b *testing.B) { benchFold(b, SurfaceProjection, n) })
	}
}

func BenchmarkContextFold(b *testing.B) {
	for _, n := range []int{400, 800, 1600, 3200} {
		b.Run(fmt.Sprint(n), func(b *testing.B) { benchFold(b, ContextProjection, n) })
	}
}
