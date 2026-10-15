package openaimodels_test

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/felinics/twilight/internal/openaimodels"
	"github.com/felinics/twilight/sdk"
)

func decodeFixture(t *testing.T, name string) openaimodels.List {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var list openaimodels.List
	if err := json.Unmarshal(data, &list); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	return list
}

func boolPtr(v bool) *bool { return &v }

// TestModelsOpenAI checks that an OpenAI /models response, which carries no
// metadata, maps to models with only an ID and the provider.
func TestModelsOpenAI(t *testing.T) {
	list := decodeFixture(t, "openai_models.json")
	models := list.Models(nil)
	want := []sdk.Model{{ID: "gpt-4o"}, {ID: "o3-mini"}, {ID: "text-embedding-3-small"}}
	if !reflect.DeepEqual(models, want) {
		t.Errorf("models:\n got %#v\nwant %#v", models, want)
	}
}

// TestModelsOpenRouter maps a trimmed OpenRouter /models response.
func TestModelsOpenRouter(t *testing.T) {
	list := decodeFixture(t, "openrouter_models.json")
	byID := make(map[string]sdk.Model)
	for _, m := range list.Models(nil) {
		byID[m.ID] = m
	}
	if len(byID) != 7 {
		t.Fatalf("got %d models, want 7", len(byID))
	}

	text := []sdk.Modality{sdk.ModalityText}
	tests := []sdk.Model{
		{
			// Reasoning is mandatory and has no default_enabled: it reasons by
			// default and cannot be turned off.
			ID:            "anthropic/claude-sonnet-5.5",
			DisplayName:   "Anthropic: Claude Sonnet 5.5",
			ContextWindow: 1000000, MaxOutputTokens: 128000,
			Modalities: sdk.Modalities{
				Input:  []sdk.Modality{sdk.ModalityText, sdk.ModalityImage, sdk.ModalityPDF},
				Output: text,
			},
			ToolCall: true, StructuredOutput: true,
			Reasoning: &sdk.ModelReasoning{
				Efforts: []string{"low", "medium", "high", "xhigh", "max"},
				Off:     sdk.ReasoningOffRejected,
				Dialect: sdk.ReasoningDialectTier,
				Default: &sdk.ReasoningDefault{Enabled: boolPtr(true), Effort: "high"},
			},
		},
		{
			ID:            "deepseek/deepseek-v4.1-flash",
			DisplayName:   "DeepSeek: DeepSeek V4.1 Flash",
			ContextWindow: 1048576, MaxOutputTokens: 943718,
			Modalities: sdk.Modalities{Input: []sdk.Modality{sdk.ModalityText, sdk.ModalityImage}, Output: text},
			ToolCall:   true, StructuredOutput: true,
			Reasoning: &sdk.ModelReasoning{
				Efforts: []string{"low", "high", "max"},
				Off:     sdk.ReasoningOffAccepted,
				Dialect: sdk.ReasoningDialectTier,
				Default: &sdk.ReasoningDefault{Enabled: boolPtr(true), Effort: "high"},
			},
		},
		{
			// No reasoning: the object is null and no reasoning parameter.
			ID:            "openai/gpt-4o",
			DisplayName:   "OpenAI: GPT-4o",
			ContextWindow: 128000, MaxOutputTokens: 16384,
			Modalities: sdk.Modalities{
				Input:  []sdk.Modality{sdk.ModalityText, sdk.ModalityImage, sdk.ModalityPDF},
				Output: text,
			},
			ToolCall: true, StructuredOutput: true,
		},
		{
			// A reasoning object saying only "not mandatory", with no reasoning
			// parameter, does not make the model a reasoning model.
			ID:            "rekaai/reka-edge",
			DisplayName:   "Reka Edge",
			ContextWindow: 16384, MaxOutputTokens: 14745,
			Modalities: sdk.Modalities{
				Input:  []sdk.Modality{sdk.ModalityImage, sdk.ModalityText, sdk.ModalityVideo},
				Output: text,
			},
			ToolCall: true, StructuredOutput: true,
		},
		{
			// A reasoning token budget, with no stated bounds.
			ID:            "nvidia/nemotron-3-super-120b-a12b",
			DisplayName:   "NVIDIA: Nemotron 3 Super",
			ContextWindow: 262144, MaxOutputTokens: 235929,
			Modalities: sdk.Modalities{Input: text, Output: text},
			ToolCall:   true, StructuredOutput: true,
			Reasoning: &sdk.ModelReasoning{
				Efforts: []string{"low", "medium"},
				Budget:  &sdk.ReasoningBudget{},
				Off:     sdk.ReasoningOffAccepted,
				Dialect: sdk.ReasoningDialectTier,
				Default: &sdk.ReasoningDefault{Enabled: boolPtr(true), Effort: "medium"},
			},
		},
		{
			// A router: reasoning parameters without a reasoning object, and no
			// output limit.
			ID:            "openrouter/auto",
			DisplayName:   "Auto Router",
			ContextWindow: 2000000,
			Modalities: sdk.Modalities{
				Input: []sdk.Modality{
					sdk.ModalityText, sdk.ModalityImage, sdk.ModalityAudio, sdk.ModalityPDF, sdk.ModalityVideo,
				},
				Output: []sdk.Modality{sdk.ModalityText, sdk.ModalityImage},
			},
			ToolCall: true, StructuredOutput: true,
			Reasoning: &sdk.ModelReasoning{},
		},
	}
	for _, want := range tests {
		t.Run(want.ID, func(t *testing.T) {
			got, ok := byID[want.ID]
			if !ok {
				t.Fatal("model missing")
			}
			if got.Description == "" {
				t.Error("Description is empty")
			}
			got.Description = ""
			if !reflect.DeepEqual(got, want) {
				t.Errorf("model:\n got %#v\nwant %#v", got, want)
				if !reflect.DeepEqual(got.Reasoning, want.Reasoning) {
					t.Errorf("reasoning:\n got %#v\nwant %#v", got.Reasoning, want.Reasoning)
				}
			}
		})
	}
}

// TestObjectMalformedExtension checks that extension fields in an unexpected
// shape are dropped without failing the list.
func TestObjectMalformedExtension(t *testing.T) {
	var list openaimodels.List
	err := json.Unmarshal([]byte(`{"data":[
		{"id":"a","object":"model","context_length":"128k","supported_parameters":["tools"]},
		{"id":"b","object":"model","context_length":4096}
	]}`), &list)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	models := list.Models(nil)
	want := []sdk.Model{{ID: "a"}, {ID: "b", ContextWindow: 4096}}
	if !reflect.DeepEqual(models, want) {
		t.Errorf("models:\n got %#v\nwant %#v", models, want)
	}
}

// TestObjectMalformedCore keeps the strictness of the identifying fields.
func TestObjectMalformedCore(t *testing.T) {
	var list openaimodels.List
	if err := json.Unmarshal([]byte(`{"data":[{"id":42}]}`), &list); err == nil {
		t.Error("decode: want an error for a non-string id")
	}
}

func TestReasoningOffFromNoneEffort(t *testing.T) {
	var list openaimodels.List
	err := json.Unmarshal([]byte(`{"data":[{"id":"m","reasoning":
		{"supported_efforts":["high","none","low","turbo"],"default_effort":"none"}}]}`), &list)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := list.Models(nil)[0].Reasoning
	want := &sdk.ModelReasoning{
		Efforts: []string{"low", "high", "turbo"},
		Off:     sdk.ReasoningOffAccepted,
		Dialect: sdk.ReasoningDialectTier,
		Default: &sdk.ReasoningDefault{Enabled: boolPtr(false)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("reasoning:\n got %#v\nwant %#v", got, want)
	}
}
