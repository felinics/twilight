package runtimetest

import (
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/schema"
)

func testRespond(t *testing.T, factory Factory) {
	for _, viaStore := range []bool{false, true} {
		name := "coordinator"
		if viaStore {
			name = "turn store"
		}
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				policy turn.ResponsePolicy
				status turn.ToolCallStatus
			}{
				{"approve", turn.ApprovalRequired, turn.ToolPending},
				{"reject approval", turn.ApprovalRequired, turn.ToolFailed},
				{"answer", turn.ExternalResponse, turn.ToolCompleted},
				{"reject external", turn.ExternalResponse, turn.ToolFailed},
			} {
				t.Run(tc.name, func(t *testing.T) {
					h := newHarness(t, factory(t))
					h.start("t1", "in-1")
					h.waitingToolWithPolicy("t1", tc.policy)
					req := h.status("t1").Waiting[0]
					decisionDigest := func(decision turn.ResponseDecision, reason string) turn.Digest {
						t.Helper()
						digest, err := schema.Canonical().DigestToolResponseDecision(req.Kind, decision, reason)
						if err != nil {
							t.Fatal(err)
						}
						return digest
					}
					approve := turn.ApproveToolCall{StepID: req.StepID, CallID: req.CallID, ResponseID: req.ID,
						ResponseDigest: decisionDigest(turn.ResponseDecisionApproved, "")}
					reject := func(reason string) turn.RejectToolCall {
						return turn.RejectToolCall{StepID: req.StepID, CallID: req.CallID, ResponseID: req.ID,
							ResponseDigest: decisionDigest(turn.ResponseDecisionRejected, reason), Reason: reason}
					}
					answer := func(body string) turn.SubmitToolResponse {
						t.Helper()
						payload := turn.MustParseCanonicalJSON(body)
						digest, err := schema.Canonical().DigestToolResponsePayload(payload)
						if err != nil {
							t.Fatal(err)
						}
						return turn.SubmitToolResponse{StepID: req.StepID, CallID: req.CallID, ResponseID: req.ID,
							ResponseDigest: digest, Payload: payload}
					}
					var cmd turn.AgentCommand
					var conflicts []turn.AgentCommand
					switch tc.name {
					case "approve":
						cmd = approve
						bad := approve
						bad.ResponseDigest = changedDigest
						conflicts = []turn.AgentCommand{reject("denied"), bad}
					case "answer":
						cmd = answer(`{"answer":"first"}`)
						conflicts = []turn.AgentCommand{answer(`{"answer":"changed"}`), reject("denied")}
					default:
						cmd = reject("denied")
						bad := reject("denied")
						bad.ResponseDigest = changedDigest
						conflicts = []turn.AgentCommand{reject("changed reason"), bad}
						if tc.policy == turn.ApprovalRequired {
							conflicts = append(conflicts, approve)
						} else {
							conflicts = append(conflicts, answer(`{"answer":"first"}`))
						}
					}
					respond := func(cmd turn.AgentCommand) error {
						if viaStore {
							id := schema.Identity().DeriveResponseCommandID(req.TurnID, req.StepID, req.CallID, req.ID)
							_, err := h.runCommit(req.TurnID, id, 0, cmd)
							return err
						}
						_, err := h.c.Respond(h.ctx, h.writer(), req, cmd)
						return err
					}
					if err := respond(cmd); err != nil {
						t.Fatal(err)
					}
					state := h.load(req.TurnID).State
					step, open := state.Current.(turn.ToolStep)
					if !open && state.LastToolStep != nil {
						step = *state.LastToolStep
					}
					if len(step.Calls) != 1 || step.Calls[0].Status != tc.status || len(h.status(req.TurnID).Waiting) != 0 {
						t.Fatalf("response did not resolve the waiting call: %+v", step)
					}
					checkReplay := func() {
						t.Helper()
						head := h.head()
						if err := respond(cmd); err != nil {
							t.Fatalf("same response replay: %v", err)
						}
						for _, changed := range conflicts {
							err := respond(changed)
							if !errors.Is(err, turn.ErrCommandConflict) {
								t.Fatalf("changed response %T: %v, want conflict", changed, err)
							}
						}
						if h.head() != head {
							t.Fatal("response replay or conflict changed the ledger")
						}
					}
					checkReplay()
					h.open()
					checkReplay()
					h.appCancel(req.TurnID)
					checkReplay()
				})
			}
		})
	}
}
