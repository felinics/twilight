package preset

import (
	"testing"

	"github.com/felinics/twilight/agentcore/run"
)

// TestPresetDigestGolden freezes the preset digest and its field boundary:
// every frozen field changes the digest.
func TestPresetDigestGolden(t *testing.T) {
	base := AgentPreset{Model: "m-1", PromptBuilder: "twilight/contextprompt/v1"}
	d, err := DigestPreset(&base)
	if err != nil {
		t.Fatal(err)
	}
	const want = "sha256:3694be1915266edfc3f7892f85ce91c2f541bb6a9b62d43f6c7cc29dffb78c79"
	if want == "" {
		t.Errorf("UNSET preset digest = %s", d)
	} else if string(d) != want {
		t.Errorf("golden preset digest drifted — an intentional wire change must update this fixture:\n got: %s\nwant: %s", d, want)
	}

	cases := []struct {
		name   string
		mutate func(*AgentPreset)
	}{
		{"system prompt", func(p *AgentPreset) { p.SystemPrompt = "be brief" }},
		{"builder", func(p *AgentPreset) { p.PromptBuilder = "other/builder" }},
		{"scheduling mode", func(p *AgentPreset) { p.Scheduling.Mode = run.ToolScheduleSequential }},
		{"scheduling bound", func(p *AgentPreset) { p.Scheduling.MaxParallel = 2 }},
		{"malformed retries", func(p *AgentPreset) { p.MalformedRetries = 1 }},
	}
	for _, tc := range cases {
		p := base
		tc.mutate(&p)
		if cd, _ := DigestPreset(&p); cd == d {
			t.Fatalf("%s does not affect the preset digest", tc.name)
		}
	}
}

func TestValidatePreset(t *testing.T) {
	ok := AgentPreset{Model: "m", PromptBuilder: "p"}
	if err := ValidatePreset(&ok); err != nil {
		t.Fatalf("valid preset rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*AgentPreset)
	}{
		{"model", func(p *AgentPreset) { p.Model = "" }},
		{"builder", func(p *AgentPreset) { p.PromptBuilder = "" }},
		{"scheduling mode", func(p *AgentPreset) { p.Scheduling.Mode = "round-robin" }},
		{"scheduling bound", func(p *AgentPreset) { p.Scheduling.MaxParallel = -1 }},
	}
	for _, tc := range cases {
		p := ok
		tc.mutate(&p)
		if err := ValidatePreset(&p); err == nil {
			t.Fatalf("%s: invalid preset accepted", tc.name)
		}
	}
}
