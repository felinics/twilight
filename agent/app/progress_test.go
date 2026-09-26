package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/sdk"
)

// streamingModel streams its reply as text deltas; Generate is never used
// once the local backend streams.
type streamingModel struct{ deltas []string }

func (streamingModel) Generate(context.Context, sdk.Request) (sdk.ModelResult, error) {
	return sdk.ModelResult{}, errors.New("streaming model must be streamed")
}

func (m streamingModel) Stream(context.Context, sdk.Request) (sdk.ModelStream, error) {
	parts := make(chan sdk.StreamPart, len(m.deltas))
	text := ""
	for _, d := range m.deltas {
		parts <- &sdk.TextDeltaPart{Text: d}
		text += d
	}
	close(parts)
	return sdk.ModelStream{Parts: parts, Result: func() (*sdk.ModelResult, error) {
		return &sdk.ModelResult{Text: text, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}}, nil
}

// RUN-EXE-12, OBS-1: a model's text deltas produced by the executor reach
// the Session's event stream as transient progress before the committed
// step lands on the same stream.
func TestModelDeltasReachTheEventStream(t *testing.T) {
	ctx := context.Background()
	h := newHost(t, app.Config{Store: filestoretest.Store(t), Content: durableContent(t), Ownership: session.OpenOptions{Takeover: true}},
		map[run.ModelRef]loop.ModelInvoker{"m-1": streamingModel{deltas: []string{"hel", "lo"}}})
	preset, err := h.RegisterPreset("b1", mustPreset("m-1", nil))
	if err != nil {
		t.Fatal(err)
	}
	s, err := h.OpenSession(ctx, "s-stream", app.SessionOptions{Preset: preset})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	events := h.Events(subCtx, "s-stream")
	results, err := s.Send(ctx, "hi")
	if err != nil || len(results) == 0 || results[0].Reply != "hello" {
		t.Fatalf("send = %+v %v", results, err)
	}
	var deltas []string
	sawCommittedStep := false
	deadline := time.After(3 * time.Second)
	for len(deltas) < 2 || !sawCommittedStep {
		select {
		case e := <-events:
			if e.Progress != nil {
				if e.Progress.Kind == string(loop.EventModelTextDelta) {
					var text string
					_ = json.Unmarshal(e.Progress.Payload, &text)
					deltas = append(deltas, text)
					if sawCommittedStep {
						t.Fatal("delta delivered after the committed model step")
					}
				}
				continue
			}
			if e.Row.Type == "twilight/run/model_step_completed" {
				sawCommittedStep = true
			}
		case <-deadline:
			t.Fatalf("deltas = %v, committed step seen = %v", deltas, sawCommittedStep)
		}
	}
	if deltas[0] != "hel" || deltas[1] != "lo" {
		t.Fatalf("deltas = %v, want [hel lo]", deltas)
	}
}
