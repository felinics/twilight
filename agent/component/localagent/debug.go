package localagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session"
)

// debugObserver writes a readable sidecar trace without changing the
// canonical ledger or content-addressed store. Each atomic commit is one
// JSON line, preserving its domain batches; frozen bodies named by a digest
// are expanded under the owning event's bodies.
type debugObserver struct {
	root    string
	content artifact.ContentStore
	mu      sync.Mutex
}

func newDebugObserver(root string, content artifact.ContentStore) (*debugObserver, error) {
	if err := os.MkdirAll(filepath.Join(root, "sessions"), 0o700); err != nil {
		return nil, fmt.Errorf("localagent: debug: %w", err)
	}
	return &debugObserver{root: root, content: content}, nil
}

func (o *debugObserver) Committed(ctx context.Context, sid session.SessionID, commit ledger.Commit) {
	record := map[string]any{
		"schema":    "twilight.debug.v1",
		"kind":      "commit",
		"sessionId": sid,
		"seq":       commit.Seq,
		"commitId":  commit.CommitID,
		"batches":   make([]any, 0, len(commit.Batches)),
	}
	batches := record["batches"].([]any)
	for _, batch := range commit.Batches {
		out := map[string]any{
			"domain": batch.Domain,
			"events": make([]any, 0, len(batch.Events)),
		}
		events := out["events"].([]any)
		for _, event := range batch.Events {
			payload, err := event.Payload.Any()
			if err != nil {
				payload = map[string]any{"error": err.Error(), "raw": event.Payload.String()}
			}
			item := map[string]any{
				"type":                event.Type,
				"recordedAtUnixMilli": event.RecordedAtUnixMilli,
				"payload":             payload,
			}
			bodies := map[string]any{}
			collectDebugBodies(ctx, o.content, payload, bodies)
			if len(bodies) > 0 {
				item["bodies"] = bodies
			}
			events = append(events, item)
		}
		out["events"] = events
		batches = append(batches, out)
	}
	record["batches"] = batches

	o.mu.Lock()
	defer o.mu.Unlock()
	path := filepath.Join(o.root, "sessions", safeDebugID(string(sid))+".jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	defer file.Close()
	_ = json.NewEncoder(file).Encode(record)
}

func collectDebugBodies(ctx context.Context, content artifact.ContentStore, value any, out map[string]any) {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if debugBodyKey(key) {
				if digest, ok := child.(string); ok && strings.HasPrefix(digest, "sha256:") {
					if _, seen := out[digest]; !seen {
						if body, found := debugBody(ctx, content, run.Digest(digest)); found {
							out[digest] = body
						}
					}
				}
			}
			collectDebugBodies(ctx, content, child, out)
		}
	case []any:
		for _, child := range v {
			collectDebugBodies(ctx, content, child, out)
		}
	}
}

func debugBody(ctx context.Context, content artifact.ContentStore, digest run.Digest) (any, bool) {
	if content == nil {
		return nil, false
	}
	ref, err := sessionstore.FrozenRef(digest)
	if err != nil {
		return nil, false
	}
	// A Writer observer can run just before the command's frozen body becomes
	// visible to the content store. Debug output is best effort, so wait a
	// short bounded interval rather than recording a misleading error.
	for attempt := 0; attempt < 20; attempt++ {
		reader, _, openErr := content.Open(ctx, ref)
		if openErr == nil {
			raw, readErr := io.ReadAll(reader)
			_ = reader.Close()
			if readErr != nil {
				return nil, false
			}
			body, ok := frozenBodyJSON(raw)
			if !ok {
				return map[string]any{"raw": string(raw)}, true
			}
			var decoded any
			if err := json.Unmarshal(body, &decoded); err != nil {
				return map[string]any{"raw": string(body)}, true
			}
			return decoded, true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil, false
}

func debugBodyKey(key string) bool {
	switch key {
	case "requestDigest", "resultDigest", "outputDigest", "responseDigest":
		return true
	default:
		return false
	}
}

// frozenBodyJSON removes the typed envelope while preserving the canonical
// JSON body. The envelope is v<version>:<type-length>:<type>:<json>.
func frozenBodyJSON(raw []byte) ([]byte, bool) {
	first := bytes.IndexByte(raw, ':')
	if first < 0 {
		return nil, false
	}
	secondRel := bytes.IndexByte(raw[first+1:], ':')
	if secondRel < 0 {
		return nil, false
	}
	second := first + 1 + secondRel
	thirdRel := bytes.IndexByte(raw[second+1:], ':')
	if thirdRel < 0 {
		return nil, false
	}
	third := second + 1 + thirdRel
	body := raw[third+1:]
	if len(body) == 0 || !json.Valid(body) {
		return nil, false
	}
	return body, true
}

func safeDebugID(id string) string {
	var b strings.Builder
	const hex = "0123456789ABCDEF"
	for i := 0; i < len(id); i++ {
		c := id[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&0xf])
	}
	if b.Len() == 0 {
		return "session"
	}
	return b.String()
}
