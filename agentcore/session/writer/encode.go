package writer

import (
	"fmt"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// bindingRef is one artifact reference an event declares: the BindingID, the
// declaration it must satisfy and where in the group it sits, for the
// commit's detail string. encode extracts them; the admission stage judges
// them.
type bindingRef struct {
	id    artifact.BindingID
	decl  *extension.BindingReferenceDefinition
	where string
}

func bindingIDs(refs []bindingRef) []artifact.BindingID {
	if len(refs) == 0 {
		return nil
	}
	out := make([]artifact.BindingID, len(refs))
	for i, r := range refs {
		out[i] = r.id
	}
	return out
}

// encode is the pure stage of the pipeline: it validates and encodes every
// event against the Registry under its type's write version (EXT-REG-2), checks
// each batch's stream attribution against the stream domain its event types
// declare, extracts the artifact references the events declare and returns
// the proposal batches. It touches no store.
func encode(registry *extension.Registry, group *SemanticGroup) ([]session.StreamBatch, []bindingRef, string) {
	batches := make([]session.StreamBatch, len(group.Batches))
	var refs []bindingRef
	for bi, tb := range group.Batches {
		events := make([]session.Event, len(tb.Events))
		for i, te := range tb.Events {
			where := fmt.Sprintf("batch %d event %d", bi, i)
			_, def, ok := registry.LookupEvent(te.Type)
			if !ok {
				return nil, nil, fmt.Sprintf("%s: unknown type %s", where, te.Type)
			}
			payload, err := registry.Encode(te.Type, te.Value)
			if err != nil {
				return nil, nil, fmt.Sprintf("%s: %v", where, err)
			}
			_, stream, declared := registry.LookupStream(def.Stream)
			if !declared {
				return nil, nil, fmt.Sprintf("%s: event type %s names stream domain %q, which no module declares", where, te.Type, def.Stream)
			}
			if verdict := checkStreamAffinity(tb.Stream, stream, te.Value); verdict != "" {
				return nil, nil, fmt.Sprintf("%s: %s", where, verdict)
			}
			for d := range def.Bindings {
				decl := &def.Bindings[d]
				ids, err := decl.Extractor.BindingIDs(te.Value)
				if err != nil {
					return nil, nil, fmt.Sprintf("%s: binding extraction: %v", where, err)
				}
				if n := len(ids); n < int(decl.Cardinality.Min) || (decl.Cardinality.Max != nil && n > int(*decl.Cardinality.Max)) {
					return nil, nil, fmt.Sprintf("%s: binding cardinality violated", where)
				}
				for _, id := range ids {
					refs = append(refs, bindingRef{id: id, decl: decl, where: where})
				}
			}
			events[i] = session.Event{Type: te.Type, RecordedAtUnixMilli: te.RecordedAtUnixMilli, Payload: payload}
		}
		batches[bi] = session.StreamBatch{Stream: tb.Stream, Events: events}
	}
	return batches, refs, ""
}

// checkStreamAffinity verifies a batch's stream attribution against the
// declaration of the domain the event type names (EXT-STR-1). It returns a
// human verdict for the commit's detail string; the declarations themselves
// are validated at BuildRegistry.
func checkStreamAffinity(stream session.StreamRef, def extension.StreamDefinition, value any) string {
	if stream.Domain != def.Domain {
		return fmt.Sprintf("event belongs to stream domain %q but the batch is %s", def.Domain, stream)
	}
	if !def.Keyed() {
		if stream.ID != "" {
			return fmt.Sprintf("stream domain %q is a singleton but the batch is %s", def.Domain, stream)
		}
		return ""
	}
	if stream.ID == "" {
		return fmt.Sprintf("stream domain %q is keyed but the batch names no stream ID", def.Domain)
	}
	id, err := def.Key(value)
	if err != nil {
		return fmt.Sprintf("stream key of domain %q: %v", def.Domain, err)
	}
	if id == "" {
		return fmt.Sprintf("event names no stream of domain %q", def.Domain)
	}
	if id != stream.ID {
		return fmt.Sprintf("event belongs to stream %s/%s but the batch is %s", def.Domain, id, stream)
	}
	return ""
}
