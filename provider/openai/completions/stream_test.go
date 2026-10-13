package completions

import (
	"context"
	"reflect"
	"testing"

	"github.com/felinics/twilight/sdk"
)

func TestMergeReasoningDetailsUsesWireOrderAndType(t *testing.T) {
	tests := []struct {
		name  string
		steps [][]chatReasoningDetail
		want  []chatReasoningDetail
	}{
		{
			name: "reused zero index",
			steps: [][]chatReasoningDetail{
				{{"type": "reasoning.summary", "summary": "Plan", "format": "f", "index": float64(0)}},
				{{"type": "reasoning.summary", "summary": " ahead", "id": "s1", "index": float64(0)}},
				{{"type": "reasoning.summary", "summary": " carefully", "signature": "sig", "index": float64(0)}},
				{{"type": "reasoning.text", "text": "Think", "format": "f", "index": float64(0)}},
				{{"type": "reasoning.text", "text": " it", "id": "t1", "index": float64(0)}},
				{{"type": "reasoning.text", "text": " through", "signature": "text-sig", "index": float64(0)}},
				{{"type": "reasoning.encrypted", "data": "enc-1", "format": "f", "index": float64(0)}},
				{{"type": "reasoning.encrypted", "data": "enc-2", "format": "f", "index": float64(0)}},
				{{"type": "reasoning.text", "text": "After", "id": "t2", "index": float64(0)}},
			},
			want: []chatReasoningDetail{
				{"type": "reasoning.summary", "summary": "Plan ahead carefully", "format": "f", "id": "s1", "signature": "sig", "index": float64(0)},
				{"type": "reasoning.text", "text": "Think it through", "format": "f", "id": "t1", "signature": "text-sig", "index": float64(0)},
				{"type": "reasoning.encrypted", "data": "enc-1", "format": "f", "index": float64(0)},
				{"type": "reasoning.encrypted", "data": "enc-2", "format": "f", "index": float64(0)},
				{"type": "reasoning.text", "text": "After", "id": "t2", "index": float64(0)},
			},
		},
		{
			name: "missing index",
			steps: [][]chatReasoningDetail{
				{{"type": "reasoning.summary", "summary": "First"}},
				{{"type": "reasoning.summary", "summary": " summary", "signature": "summary-sig"}},
				{{"type": "reasoning.text", "text": "Then"}},
				{{"type": "reasoning.text", "text": " text", "format": "f"}},
				{{"type": "reasoning.encrypted", "data": "enc-1"}},
				{{"type": "reasoning.encrypted", "data": "enc-2"}},
			},
			want: []chatReasoningDetail{
				{"type": "reasoning.summary", "summary": "First summary", "signature": "summary-sig"},
				{"type": "reasoning.text", "text": "Then text", "format": "f"},
				{"type": "reasoning.encrypted", "data": "enc-1"},
				{"type": "reasoning.encrypted", "data": "enc-2"},
			},
		},
		{
			name: "identical incremental fragments",
			steps: [][]chatReasoningDetail{
				{{"type": "reasoning.summary", "summary": "a"}},
				{{"type": "reasoning.summary", "summary": "a"}},
				{{"type": "reasoning.summary", "summary": "\n"}},
				{{"type": "reasoning.summary", "summary": "\n"}},
				{{"type": "reasoning.encrypted", "data": "enc"}},
				{{"type": "reasoning.text", "text": "a"}},
				{{"type": "reasoning.text", "text": "a"}},
				{{"type": "reasoning.text", "text": "\n"}},
				{{"type": "reasoning.text", "text": "\n"}},
			},
			want: []chatReasoningDetail{
				{"type": "reasoning.summary", "summary": "aa\n\n"},
				{"type": "reasoning.encrypted", "data": "enc"},
				{"type": "reasoning.text", "text": "aa\n\n"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []chatReasoningDetail
			for _, step := range tt.steps {
				got = mergeReasoningDetails(got, step)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("mergeReasoningDetails()\n got: %#v\nwant: %#v", got, tt.want)
			}
		})
	}
}

func TestProcessReasoningPreservesIdenticalDetailDeltas(t *testing.T) {
	sp := &streamProcessor{
		ctx: context.Background(),
		ch:  make(chan sdk.StreamPart, 8),
	}
	for _, text := range []string{"a", "a", "\n", "\n"} {
		sp.processReasoning(&chatChunkDelta{ReasoningDetails: []chatReasoningDetail{{
			"type": "reasoning.text",
			"text": text,
		}}}, "chunk")
	}

	var got string
	for len(sp.ch) > 0 {
		if delta, ok := (<-sp.ch).(*sdk.ReasoningDeltaPart); ok {
			got += delta.Text
		}
	}
	if want := "aa\n\n"; got != want {
		t.Fatalf("reasoning deltas: got %q, want %q", got, want)
	}
}
