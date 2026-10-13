package sdk_test

import (
	"testing"

	"github.com/felinics/twilight/sdk"
)

func TestModelReasoningControllable(t *testing.T) {
	tests := []struct {
		name      string
		reasoning sdk.ModelReasoning
		want      bool
	}{
		{"toggle", sdk.ModelReasoning{Toggle: true}, true},
		{"efforts", sdk.ModelReasoning{Efforts: []string{sdk.ReasoningEffortLow, sdk.ReasoningEffortHigh}}, true},
		{"budget", sdk.ModelReasoning{Budget: &sdk.ReasoningBudget{}}, true},
		{"reasons without control", sdk.ModelReasoning{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.reasoning.Controllable(); got != tc.want {
				t.Errorf("Controllable: got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestModelReasoningCanTurnOff(t *testing.T) {
	tests := []struct {
		name      string
		reasoning sdk.ModelReasoning
		want      bool
	}{
		{"accepted", sdk.ModelReasoning{Off: sdk.ReasoningOffAccepted}, true},
		{"low effort only", sdk.ModelReasoning{Off: sdk.ReasoningOffLowEffortOnly}, true},
		{"rejected wins over a toggle", sdk.ModelReasoning{Off: sdk.ReasoningOffRejected, Toggle: true}, false},
		{"unknown with a toggle", sdk.ModelReasoning{Toggle: true}, true},
		{"unknown without a toggle", sdk.ModelReasoning{Efforts: []string{sdk.ReasoningEffortHigh}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.reasoning.CanTurnOff(); got != tc.want {
				t.Errorf("CanTurnOff: got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestModelModalities(t *testing.T) {
	model := &sdk.Model{Modalities: sdk.Modalities{
		Input:  []sdk.Modality{sdk.ModalityText, sdk.ModalityImage, sdk.ModalityPDF},
		Output: []sdk.Modality{sdk.ModalityText},
	}}
	if !model.Accepts(sdk.ModalityImage) || !model.Accepts(sdk.ModalityPDF) {
		t.Error("Accepts: want image and pdf input")
	}
	if model.Accepts(sdk.ModalityAudio) {
		t.Error("Accepts: audio is not an input")
	}
	if !model.Produces(sdk.ModalityText) || model.Produces(sdk.ModalityImage) {
		t.Error("Produces: want text only")
	}
}
