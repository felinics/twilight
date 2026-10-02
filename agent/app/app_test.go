package app_test

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// The product layer assembles over any effect port the deployment root
// decides on: here a recorder standing in for a remote executor, with no
// model or tool implementation on this side of the port.
func TestNewAcceptsEffectPort(t *testing.T) {
	p, err := app.NewPresetFromDefinitions("model", nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := durablePorts(t, app.Config{
		Presets: []app.Preset{{ID: "default", Value: p}},
	})
	cfg.Execution.Executor = effect.PortsOf(&recordingExecutor{reply: "hello from the executor"})
	a, err := app.New(cfg.Config)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	ref, err := a.RegisterPreset("second", p)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Digest != mustDigest(p) {
		t.Fatalf("preset digest = %q, want %q", ref.Digest, mustDigest(p))
	}
	if p.Model != run.ModelRef("model") {
		t.Fatalf("model = %q", p.Model)
	}
}

func mustDigest(p preset.AgentPreset) run.Digest {
	d, err := preset.DigestPreset(&p)
	if err != nil {
		panic(err)
	}
	return d
}
