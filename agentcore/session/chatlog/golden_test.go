package chatlog_test

import (
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/attempt"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/extension"
)

func freezeChatlog(t *testing.T, name, got, want string) {
	t.Helper()
	if got == want {
		return
	}
	if want == "" {
		t.Errorf("UNSET %s = %s", name, got)
		return
	}
	t.Errorf("golden %s drifted — an intentional wire change must update this fixture and agent-session-chatlog.md:\n got: %s\nwant: %s", name, got, want)
}

// TestChatlogWireGolden freezes the compaction digest preimages and one
// registry-encoded payload of payload version 1.
func TestChatlogWireGolden(t *testing.T) {
	pairs := []chatlog.EntryDigestPair{
		{Kind: chatlog.EntryInput, ID: "in-1", Digest: "sha256:aa"},
		{Kind: chatlog.EntryAssistant, ID: "as-1", Digest: "sha256:bb"},
	}
	base, err := chatlog.DigestBaseContext(pairs)
	if err != nil {
		t.Fatal(err)
	}
	freezeChatlog(t, "base context digest", string(base), "sha256:5b01f17f53ad3e349e1b4d89e673a1647bb1721c3f3c0768a951e2d4a57cac5b")

	emptyA, err := chatlog.DigestBaseContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	emptyB, err := chatlog.DigestBaseContext([]chatlog.EntryDigestPair{})
	if err != nil {
		t.Fatal(err)
	}
	if emptyA != emptyB {
		t.Fatal("nil and empty base must digest identically")
	}
	freezeChatlog(t, "empty base context digest", string(emptyA), "sha256:5b94ad57d55dbade56535b5c515bee09d39d60bc16440746d97d417c934ec004")

	cp := chatlog.CompactionCreatedPayload{
		CompactionID:      "ckpt-1",
		CoveredThrough:    session.Position{Commit: 7},
		BaseContextDigest: base,
		SummaryID:         "sum-1",
		SummaryDigest:     "sha256:cc",
		Retained:          pairs[1:],
	}
	cpd, err := chatlog.DigestCompaction(&cp)
	if err != nil {
		t.Fatal(err)
	}
	freezeChatlog(t, "compaction digest", string(cpd), "sha256:bbfcad9e58bc7a287bd5519b5ecde07dc2926ba9f88363bf15bc3b6ec10b630f")

	reg, err := extension.BuildRegistry(runmod.Module, attempt.Module, chatlog.Module)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := reg.Encode(chatlog.TypeInputSubmitted, chatlog.InputSubmittedPayload{
		InputID: "in-1", Content: jsonstable.MustParse(`{"text":"hi"}`), SubmittedAtUnixMilli: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	freezeChatlog(t, "input_submitted wire", wire.String(), `{"content":{"text":"hi"},"inputId":"in-1","submittedAtUnixMilli":1,"v":1}`)
}
