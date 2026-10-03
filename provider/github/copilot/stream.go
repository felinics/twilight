package copilot

import (
	"context"
	"encoding/json"

	"github.com/felinics/twilight/sdk"
)

type streamProcessor struct {
	ctx                context.Context
	ch                 chan sdk.StreamPart
	textStartSent      bool
	reasoningStartSent bool
	// reasoningOpaque is the block's token, delivered on a delta and applied
	// when the block closes.
	reasoningOpaque  string
	rawFinishReason  string
	finishReason     sdk.FinishReason
	usage            sdk.Usage
	chunkID          string
	chunkModel       string
	chunkCreated     int64
	flushed          bool
	pendingToolCalls map[int]*streamingToolCall
	// finishStepPending defers the step boundary until trailing usage chunks
	// have been read.
	finishStepPending bool
	// done is set by the chunk that carries a finish_reason or by the [DONE]
	// sentinel that follows it; either one means the response is complete.
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
		sp.send(&sdk.ReasoningEndPart{
			ID:               id,
			Format:           sdk.ReasoningFormatCopilot,
			Model:            sp.chunkModel,
			ProviderMetadata: reasoningOpaqueMetadata(sp.reasoningOpaque),
		})
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
	// is kept as the call's Text (sdk.ToolArguments).
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
	if delta.ReasoningOpaque != "" {
		sp.reasoningOpaque = delta.ReasoningOpaque
	}
	reasoningContent := reasoningFromDelta(delta)
	// The opaque token may arrive on a delta carrying no text; that delta still
	// opens the block, because without the token the text cannot be replayed.
	if reasoningContent == "" && delta.ReasoningOpaque == "" {
		return
	}
	if !sp.reasoningStartSent {
		sp.send(&sdk.ReasoningStartPart{ID: chunkID, Format: sdk.ReasoningFormatCopilot, Model: sp.chunkModel})
		sp.reasoningStartSent = true
	}
	if reasoningContent != "" {
		sp.send(&sdk.ReasoningDeltaPart{ID: chunkID, Text: reasoningContent, Format: sdk.ReasoningFormatCopilot, Model: sp.chunkModel})
	}
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
	sp.finishStepPending = true
}

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
