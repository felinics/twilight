package turn_test

import (
	"testing"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/turn"
)

// TestPresetDigestGolden freezes the preset digest and its field boundary
// (TRN-PST-1): all frozen decision inputs change the digest.
func TestPresetDigestGolden(t *testing.T) {
	base := turn.AgentPreset{SchemaVersion: 1, Model: "m-1", Prompt: "twilight/decision/prompt/context-v1"}
	d, err := turn.DigestPreset(&base)
	if err != nil {
		t.Fatal(err)
	}
	const want = "sha256:909a5efb72e9ae77ad6ada8b972b642396802dbc7b0bd2a00e0856050fd33cc3"
	if want == "" {
		t.Errorf("UNSET preset digest = %s", d)
	} else if string(d) != want {
		t.Errorf("golden preset digest drifted — an intentional wire change must update this fixture and agent-turn.md TRN-PST-1:\n got: %s\nwant: %s", d, want)
	}

	cases := []struct {
		name   string
		mutate func(*turn.AgentPreset)
	}{
		{"system prompt", func(p *turn.AgentPreset) { p.SystemPrompt = "be brief" }},
		{"streaming", func(p *turn.AgentPreset) { p.Streaming = true }},
		{"builder", func(p *turn.AgentPreset) { p.Prompt = "other/builder" }},
		{"scheduling mode", func(p *turn.AgentPreset) { p.Scheduling.Mode = run.ToolScheduleSequential }},
		{"scheduling bound", func(p *turn.AgentPreset) { p.Scheduling.MaxParallel = 2 }},
		{"malformed retries", func(p *turn.AgentPreset) { p.MalformedRetries = 1 }},
	}
	for _, tc := range cases {
		p := base
		tc.mutate(&p)
		if cd, _ := turn.DigestPreset(&p); cd == d {
			t.Fatalf("%s does not affect the preset digest", tc.name)
		}
	}
}

func TestValidatePreset(t *testing.T) {
	ok := turn.AgentPreset{SchemaVersion: 1, Model: "m", Prompt: "p"}
	if err := turn.ValidatePreset(&ok); err != nil {
		t.Fatalf("valid preset rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*turn.AgentPreset)
	}{
		{"schema", func(p *turn.AgentPreset) { p.SchemaVersion = 0 }},
		{"model", func(p *turn.AgentPreset) { p.Model = "" }},
		{"builder", func(p *turn.AgentPreset) { p.Prompt = "" }},
		{"scheduling mode", func(p *turn.AgentPreset) { p.Scheduling.Mode = "round-robin" }},
		{"scheduling bound", func(p *turn.AgentPreset) { p.Scheduling.MaxParallel = -1 }},
	}
	for _, tc := range cases {
		p := ok
		tc.mutate(&p)
		if err := turn.ValidatePreset(&p); err == nil {
			t.Fatalf("%s: invalid preset accepted", tc.name)
		}
	}
}
