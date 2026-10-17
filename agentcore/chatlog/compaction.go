package chatlog

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// ErrContextChanged means the context no longer matches the snapshot a
// compaction was planned against. Generate a new plan and summary before retrying.
var ErrContextChanged = errors.New("chatlog: compaction context changed")

// CompactionPlan pins the context before summary generation and the ordered
// subset to preserve. Planning and summary generation do not write any facts.
type CompactionPlan struct {
	BaseContextDigest jsonstable.Digest `json:"baseContextDigest"`
	CoveredThrough    ledger.Position   `json:"coveredThrough"`
	Retained          []EntryDigestPair `json:"retained,omitempty"`
}

// PlanCompaction selects the last retainEntries entries (or all if shorter),
// extending the suffix for tool-call closure. Unsettled calls always retain
// their assistant and the suffix after it, even when retainEntries is zero.
// The bool reports a replaceable prefix.
func PlanCompaction(entries []Entry, retainEntries int) (CompactionPlan, bool, error) {
	if retainEntries < 0 {
		return CompactionPlan{}, false, errors.New("chatlog: retainEntries must not be negative")
	}
	pairs := contextPairs(entries)
	base, err := DigestBaseContext(pairs)
	if err != nil {
		return CompactionPlan{}, false, err
	}
	plan := CompactionPlan{BaseContextDigest: base}
	if len(entries) == 0 {
		return plan, false, nil
	}
	plan.CoveredThrough = entries[len(entries)-1].Position
	start := 0
	if retainEntries < len(entries) {
		start = len(entries) - retainEntries
	}
	owner := make(map[CallID]int)
	settled := make(map[CallID]bool)
	for i := range entries {
		e := &entries[i]
		switch {
		case e.Kind == EntryAssistant && e.Assistant != nil:
			for _, call := range e.Assistant.CallIDs {
				owner[call] = i
			}
		case e.Kind == EntryToolResult && e.ToolResult != nil:
			settled[e.ToolResult.CallID] = true
		}
	}
	for call, at := range owner {
		if !settled[call] && at < start {
			start = at
		}
	}
	for changed := true; changed; {
		changed = false
		for i := start; i < len(entries); i++ {
			e := &entries[i]
			if e.Kind == EntryToolResult && e.ToolResult != nil {
				if at, ok := owner[e.ToolResult.CallID]; ok && at < start {
					start = at
					changed = true
				}
			}
		}
	}
	plan.Retained = append([]EntryDigestPair(nil), pairs[start:]...)
	if err := plan.Validate(entries); err != nil {
		return CompactionPlan{}, false, err
	}
	return plan, start > 0, nil
}

// Validate compares the current ordered context and boundary to the planned
// snapshot, then checks the retained ordered subset and tool-call closure.
// Only snapshot mismatches wrap ErrContextChanged; invalid retention is an
// ordinary validation error.
func (p CompactionPlan) Validate(entries []Entry) error {
	base, err := DigestBaseContext(contextPairs(entries))
	if err != nil {
		return err
	}
	var through ledger.Position
	if len(entries) > 0 {
		through = entries[len(entries)-1].Position
	}
	if base != p.BaseContextDigest || through != p.CoveredThrough {
		return fmt.Errorf("%w: base digest or covered position differs", ErrContextChanged)
	}
	if len(entries) == 0 {
		return errors.New("chatlog: compaction over an empty context")
	}
	if _, err := selectRetained(entries, p.Retained); err != nil {
		return err
	}
	return CheckRetainClosure(entries, p.Retained)
}

func contextPairs(entries []Entry) []EntryDigestPair {
	pairs := make([]EntryDigestPair, len(entries))
	for i := range entries {
		pairs[i] = entries[i].Pair()
	}
	return pairs
}

// CompactContext contributes a summary and compaction to an atomic unit. It
// validates the plan on the unit's Writer View, not at summary-generation time.
// Turn quiescence is the caller's policy and belongs in a separate unit Part.
func CompactContext(id CompactionID, summary Summary, plan CompactionPlan) unit.Part {
	return &compactContext{id: id, summary: summary, plan: plan}
}

type compactContext struct {
	id      CompactionID
	summary Summary
	plan    CompactionPlan
}

func (c *compactContext) Prepare(_ context.Context, view writer.View, now int64) ([]writer.TypedBatch, error) {
	if strings.TrimSpace(string(c.id)) == "" {
		return nil, errors.New("chatlog: compaction requires an id")
	}
	if strings.TrimSpace(string(c.summary.ID)) == "" || strings.TrimSpace(PartsText(c.summary.Parts)) == "" {
		return nil, errors.New("chatlog: compaction requires a nonempty summary and summary id")
	}
	if err := checkSummary(&SummaryPayload{Summary: c.summary}); err != nil {
		return nil, err
	}
	state, err := view.Projection(ContextProjectionID, ContextProjection.Version)
	if err != nil {
		return nil, err
	}
	current, ok := state.(Context)
	if !ok {
		return nil, fmt.Errorf("chatlog: context projection is %T", state)
	}
	if err := c.plan.Validate(current.Entries); err != nil {
		return nil, err
	}
	payload := CompactionCreatedPayload{
		CompactionID: c.id, CoveredThrough: c.plan.CoveredThrough,
		BaseContextDigest: c.plan.BaseContextDigest, SummaryID: c.summary.ID,
		SummaryDigest: c.summary.Digest, Retained: c.plan.Retained,
	}
	if payload.Digest, err = DigestCompaction(&payload); err != nil {
		return nil, err
	}
	return []writer.TypedBatch{{Domain: Stream, Events: []writer.TypedEvent{
		{Type: TypeSummary, RecordedAtUnixMilli: now, Value: SummaryPayload{Summary: c.summary}},
		{Type: TypeCompactionCreated, RecordedAtUnixMilli: now, Value: payload},
	}}}, nil
}
