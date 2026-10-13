package completions

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/felinics/twilight/sdk"
)

type streamProcessor struct {
	ctx                context.Context
	ch                 chan sdk.StreamPart
	textStartSent      bool
	reasoningStartSent bool
	reasoningBlockSeen bool
	rawFinishReason    string
	finishReason       sdk.FinishReason
	usage              sdk.Usage
	chunkID            string
	chunkModel         string
	chunkCreated       int64
	flushed            bool
	finishStepPending  bool
	pendingToolCalls   map[int]*streamingToolCall
	reasoningDetails   []chatReasoningDetail
	reasoningText      strings.Builder
	// done is set by the chunk that carries a finish_reason or by the [DONE]
	// sentinel that follows it
	// (https://platform.openai.com/docs/api-reference/chat-streaming); either
	// one means the response is complete.
	done bool
}

func (sp *streamProcessor) send(part sdk.StreamPart) bool {
	select {
	case sp.ch <- part:
		return true
	case <-sp.ctx.Done():
		return false
	}
}

func (sp *streamProcessor) endReasoning(id string) {
	if sp.reasoningStartSent {
		sp.send(&sdk.ReasoningEndPart{ID: id, Format: sdk.ReasoningFormatOpenAIChat, Model: sp.chunkModel, ProviderMetadata: minimaxReasoningMetadata(sp.reasoningDetails)})
		sp.reasoningStartSent = false
	}
}

func (sp *streamProcessor) endText(id string) {
	if sp.textStartSent {
		sp.send(&sdk.TextEndPart{ID: id})
		sp.textStartSent = false
	}
}

func (sp *streamProcessor) flush() {
	if sp.flushed {
		return
	}
	sp.flushed = true
	sp.endReasoning(sp.chunkID)
	sp.endText(sp.chunkID)
	for _, stc := range sp.pendingToolCalls {
		sp.finishToolCall(stc)
	}
}

func (sp *streamProcessor) finishToolCall(stc *streamingToolCall) {
	if stc.finished {
		return
	}
	sp.send(&sdk.ToolInputEndPart{ID: stc.id})
	// An empty buffer is a no-argument call; text that is not a JSON document
	// is kept as the call's Text, so the caller can answer the model instead
	// of running the tool on it (sdk.ToolArguments).
	input := sdk.ParseToolArguments(stc.args.String())
	sp.send(&sdk.StreamToolCallPart{
		ToolCallID: stc.id,
		ToolName:   stc.name,
		Input:      input,
	})
	stc.finished = true
}

func (sp *streamProcessor) processChunk(chunk *chatChunkResponse) error {
	if sp.chunkID == "" {
		sp.chunkID = chunk.ID
		sp.chunkModel = chunk.Model
		sp.chunkCreated = chunk.Created
	}

	if chunk.Usage != nil {
		sp.usage = convertUsage(chunk.Usage)
	}

	if len(chunk.Choices) == 0 {
		return nil
	}
	choice := chunk.Choices[0]

	sp.processReasoning(&choice.Delta, chunk.ID)
	sp.processContent(&choice.Delta, chunk.ID)
	sp.processToolCallDeltas(choice.Delta.ToolCalls, chunk.ID)
	sp.processImages(choice.Delta.Images, chunk.ID)
	sp.processFinishReason(&choice)

	return nil
}

func (sp *streamProcessor) processReasoning(delta *chatChunkDelta, chunkID string) {
	reasoningContent := reasoningFromDelta(delta)
	if reasoningContent != "" && len(delta.ReasoningDetails) > 0 {
		// Builder.String() is a zero-copy view, so the per-delta reads below
		// stay O(1) while appends stay amortized O(1).
		reasoningContent = trimReasoningPrefix(reasoningContent, sp.reasoningText.String())
		sp.reasoningText.WriteString(reasoningContent)
		sp.reasoningDetails = reasoningDetailsWithText(delta.ReasoningDetails, sp.reasoningText.String())
	}
	if reasoningContent == "" {
		// A delta that carries reasoning_content as "" (not null) announces a
		// thinking-mode step with no reasoning text; DeepSeek sends it on the
		// first delta. Open the block so the step is recorded as a reasoning
		// step and replays with the key the endpoint validates. Only the first
		// such delta opens a block: a closed block is never reopened by an
		// empty marker.
		if delta.ReasoningContent != nil && !sp.reasoningBlockSeen {
			sp.startReasoning(chunkID)
		}
		return
	}
	sp.startReasoning(chunkID)
	sp.send(&sdk.ReasoningDeltaPart{ID: chunkID, Text: reasoningContent, Format: sdk.ReasoningFormatOpenAIChat, Model: sp.chunkModel})
}

func (sp *streamProcessor) startReasoning(chunkID string) {
	if sp.reasoningStartSent {
		return
	}
	sp.send(&sdk.ReasoningStartPart{ID: chunkID, Format: sdk.ReasoningFormatOpenAIChat, Model: sp.chunkModel})
	sp.reasoningStartSent = true
	sp.reasoningBlockSeen = true
}

func trimReasoningPrefix(text, previous string) string {
	if previous == "" {
		return text
	}
	if len(text) >= len(previous) && text[:len(previous)] == previous {
		return text[len(previous):]
	}
	return text
}

func reasoningDetailsWithText(details []chatReasoningDetail, text string) []chatReasoningDetail {
	if len(details) == 0 {
		return nil
	}
	out := copyReasoningDetails(details)
	out[len(out)-1]["text"] = text
	return out
}

func (sp *streamProcessor) processContent(delta *chatChunkDelta, chunkID string) {
	if delta.Content == "" {
		return
	}
	sp.endReasoning(chunkID)
	if !sp.textStartSent {
		sp.send(&sdk.TextStartPart{ID: chunkID})
		sp.textStartSent = true
	}
	sp.send(&sdk.TextDeltaPart{ID: chunkID, Text: delta.Content})
}

func (sp *streamProcessor) processToolCallDeltas(toolCalls []chatToolCallChunk, chunkID string) {
	if len(toolCalls) == 0 {
		return
	}
	sp.endReasoning(chunkID)
	sp.endText(chunkID)

	for _, tc := range toolCalls {
		idx := tc.Index
		stc, exists := sp.pendingToolCalls[idx]
		if !exists {
			id := tc.ID
			if id == "" {
				id = generateID()
			}
			stc = &streamingToolCall{id: id, name: tc.Function.Name}
			sp.pendingToolCalls[idx] = stc
			sp.send(&sdk.ToolInputStartPart{
				ID:       stc.id,
				ToolName: stc.name,
			})
		}
		if tc.Function.Arguments != "" {
			stc.args.WriteString(tc.Function.Arguments)
			sp.send(&sdk.ToolInputDeltaPart{
				ID:    stc.id,
				Delta: tc.Function.Arguments,
			})

			if !stc.finished && json.Valid([]byte(stc.args.String())) {
				sp.finishToolCall(stc)
			}
		}
	}
}

func (sp *streamProcessor) processImages(images []chatImagePart, chunkID string) {
	for _, img := range images {
		url := img.ImageURL.URL
		if url == "" {
			continue
		}
		sp.endText(chunkID)
		sp.endReasoning(chunkID)
		mediaType, data := parseDataURL(url)
		sp.send(&sdk.StreamFilePart{
			File: sdk.GeneratedFile{
				Data:      data,
				MediaType: mediaType,
			},
		})
	}
}

func (sp *streamProcessor) processFinishReason(choice *chatChunkChoice) {
	if choice.FinishReason == nil || *choice.FinishReason == "" {
		return
	}
	sp.rawFinishReason = *choice.FinishReason
	sp.finishReason = mapFinishReason(sp.rawFinishReason)
	sp.done = true

	sp.flush()

	// Defer FinishStepPart until the stream completes. The OpenAI Chat
	// Completions spec (and llama.cpp's server) may deliver the `usage` block
	// in a trailing chunk with `choices: []` that arrives AFTER this
	// finish_reason chunk. Emitting FinishStepPart here would capture an empty
	// sp.usage. emitFinishStep is called once the SSE loop has consumed every
	// chunk, so sp.usage is complete by then.
	sp.finishStepPending = true
}

// emitFinishStep sends the deferred FinishStepPart. It is a no-op unless a
// finish_reason was observed, preserving the previous behaviour of only
// emitting a step boundary for streams that actually finish.
func (sp *streamProcessor) emitFinishStep() {
	if !sp.finishStepPending {
		return
	}
	sp.finishStepPending = false

	sp.send(&sdk.FinishStepPart{
		FinishReason:    sp.finishReason,
		RawFinishReason: sp.rawFinishReason,
		Usage:           sp.usage,
		Response: sdk.ResponseMetadata{
			ID:        sp.chunkID,
			ModelID:   sp.chunkModel,
			Timestamp: sdk.TimestampFromUnix(sp.chunkCreated),
		},
	})
}
