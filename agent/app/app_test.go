package app_test

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/turn"
)

func TestBuildRemoteApplication(t *testing.T) {
	p, err := app.NewPresetFromDefinitions("model", nil)
	if err != nil {
		t.Fatal(err)
	}
	a, err := app.Build(durablePorts(t, app.Config{
		Executor: app.ExecutorConfig{Mode: app.ExecutorRemote, Endpoint: "http://executor"},
		Presets:  []app.Preset{{ID: "default", Value: p}},
	}))
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

func mustDigest(p turn.AgentPreset) run.Digest {
	d, err := turn.DigestPreset(&p)
	if err != nil {
		panic(err)
	}
	return d
}
