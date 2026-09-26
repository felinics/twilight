package extension

import (
	"errors"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
	"strings"
	"testing"
)

type notePayload struct {
	Text string   `json:"text"`
	Refs []string `json:"refs,omitempty"`
}

type noteState struct {
	Notes []string `json:"notes"`
}

var refsExtractor = BindingExtractorFunc(func(value any) ([]artifact.BindingID, error) {
	var out []artifact.BindingID
	for _, r := range value.(notePayload).Refs {
		out = append(out, artifact.BindingID(r))
	}
	return out, nil
})

// tpfx is the first-party prefix of a test module.
func tpfx(id ModuleID) session.EventType { return ModulePrefix(SourceTwilight, id) }

// ownStream declares one singleton, session-lineage domain: the shape of a
// module that writes one stream per Session.
func ownStream(domain string) []StreamDefinition {
	return []StreamDefinition{{Domain: domain, Lineage: session.LineageSession}}
}

func noteModule(id ModuleID, requires ...ModuleRequirement) ModuleDescriptor {
	typ := tpfx(id) + "note"
	return ModuleDescriptor{Source: SourceTwilight, ID: id, Requires: requires, Streams: ownStream(string(id)),
		Events: []EventDefinition{
			{Type: typ, Stream: string(id), Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}},
				Bindings: []BindingReferenceDefinition{{Extractor: refsExtractor, RequiredDurability: artifact.EventBound}}},
			{Type: tpfx(id) + "hint", Stream: string(id), Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}}, Ignorable: true},
		},
		Projections: []ProjectionDefinition{{
			ID: ProjectionID(string(typ) + "s"), Version: 1, Consumes: []session.EventType{typ},
			Initial: func() (any, error) { return noteState{}, nil },
			Apply: func(state any, e DecodedEvent) (any, error) {
				s := state.(noteState)
				text := e.Value.(notePayload).Text
				if text == "reject" {
					return nil, errors.New("rejected by projection")
				}
				s.Notes = append(append([]string(nil), s.Notes...), text)
				return s, nil
			},
			StateCodec: JSONStateCodec[noteState]{},
		}},
	}
}

func TestBuildRegistryValidatesRequires(t *testing.T) {
	cases := map[string][]ModuleDescriptor{
		"unregistered dependency": {noteModule("a", ModuleRequirement{Source: SourceTwilight, Module: "zzz"})},
		"cycle":                   {noteModule("a", ModuleRequirement{Source: SourceTwilight, Module: "b"}), noteModule("b", ModuleRequirement{Source: SourceTwilight, Module: "a"})},
		"event not owned by the dependency": {noteModule("a"), noteModule("b", ModuleRequirement{Source: SourceTwilight, Module: "a",
			Events: []session.EventType{tpfx("b") + "note"}})},
		"event outside module": {{Source: SourceTwilight, ID: "a", Events: []EventDefinition{{Type: "twilight/b/x", Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}}}}}},
		"projection outside scope": {noteModule("a"), {Source: SourceTwilight, ID: "b", Projections: []ProjectionDefinition{{ID: "p", Version: 1, Consumes: []session.EventType{tpfx("a") + "note"},
			Initial: func() (any, error) { return nil, nil }, Apply: func(s any, _ DecodedEvent) (any, error) { return s, nil }, StateCodec: JSONStateCodec[noteState]{}}}}},
	}
	for name, modules := range cases {
		if _, err := BuildRegistry(modules...); err == nil {
			t.Errorf("%s: registry built", name)
		}
	}
	if _, err := BuildRegistry(noteModule("a"), noteModule("b", ModuleRequirement{Source: SourceTwilight, Module: "a",
		Events: []session.EventType{tpfx("a") + "note"}})); err != nil {
		t.Fatalf("valid registry: %v", err)
	}
}

// srcModule is a minimal module under an arbitrary source.
func srcModule(source SourceID, id ModuleID) ModuleDescriptor {
	domain := string(source) + "." + string(id)
	return ModuleDescriptor{Source: source, ID: id, Streams: ownStream(domain), Events: []EventDefinition{{
		Type: ModulePrefix(source, id) + "note", Stream: domain,
		Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}},
	}}}
}

// EXT-REG-1: module identity is (Source, ID); sources are validated segments.
func TestBuildRegistryValidatesSource(t *testing.T) {
	rejects := map[string][]ModuleDescriptor{
		"empty source":           {srcModule("", "a")},
		"source with slash":      {srcModule("x/y", "a")},
		"source not utf8":        {srcModule(SourceID([]byte{0xff, 0xfe}), "a")},
		"duplicate (source, id)": {srcModule("app", "a"), srcModule("app", "a")},
		"app id colliding first-party under twilight": {noteModule("a"), srcModule(SourceTwilight, "a")},
		"requirement without source":                  {noteModule("a"), {Source: "app", ID: "b", Requires: []ModuleRequirement{{Module: "a"}}}},
	}
	for name, modules := range rejects {
		if _, err := BuildRegistry(modules...); err == nil {
			t.Errorf("%s: registry built", name)
		}
	}
	// The same ID under two sources coexists and both prefixes resolve.
	r, err := BuildRegistry(noteModule("a"), srcModule("app", "a"))
	if err != nil {
		t.Fatalf("two sources, one id: %v", err)
	}
	if key, ok := r.ModuleOf("app/a/note"); !ok || key != (ModuleKey{Source: "app", ID: "a"}) {
		t.Fatalf("ModuleOf app/a/note = %+v %v", key, ok)
	}
	if key, ok := r.ModuleOf(tpfx("a") + "note"); !ok || key != TwilightModule("a") {
		t.Fatalf("ModuleOf twilight/a/note = %+v %v", key, ok)
	}
	if _, ok := r.ModuleOf("ghost/a/note"); ok {
		t.Fatal("unregistered source resolved")
	}
}

// Encode adds v; Decode selects the codec by v and keeps unknown versions raw.
func TestRegistrySchemaVersion(t *testing.T) {
	r, err := BuildRegistry(noteModule("a"))
	if err != nil {
		t.Fatal(err)
	}
	typ := tpfx("a") + "note"
	wire, err := r.Encode(typ, notePayload{Text: "hi"})
	if err != nil || wire.String() != `{"text":"hi","v":1}` {
		t.Fatalf("encode = %s %v", wire, err)
	}
	decoded, err := r.Decode(session.Event{Type: typ, Payload: wire})
	if err != nil || decoded.Unknown || decoded.Value.(notePayload).Text != "hi" {
		t.Fatalf("decode = %+v %v", decoded, err)
	}
	future, err := r.Decode(session.Event{Type: typ, Payload: jsonstable.MustParse(`{"text":"hi","v":2}`)})
	if err != nil || !future.Unknown || future.Version != 2 {
		t.Fatalf("future version = %+v %v", future, err)
	}
	if _, err := r.Encode("twilight/a/other", notePayload{}); err == nil {
		t.Fatal("unknown type encoded")
	}
}

// EXT-SCH-2: every codec entry names a real Schema and a real codec. Encode
// selects def.Codecs[segment schema], so an unusable entry must be refused
// when the registry is built rather than discovered on first write.
func TestBuildRegistryRequiresCodec(t *testing.T) {
	for _, tc := range []struct {
		name   string
		codecs map[PayloadVersion]PayloadCodec
		detail string
	}{
		{"no codec at all", nil, "no codec for any payload version"},
		{"zero payload version", map[PayloadVersion]PayloadCodec{0: JSONCodec[notePayload]{}}, "nil codec or zero payload version"},
		{"nil codec", map[PayloadVersion]PayloadCodec{1: nil}, "nil codec or zero payload version"},
	} {
		_, err := BuildRegistry(ModuleDescriptor{Source: SourceTwilight, ID: "a", Streams: ownStream("a"),
			Events: []EventDefinition{{Type: tpfx("a") + "note", Stream: "a", Codecs: tc.codecs}}})
		if err == nil {
			t.Fatalf("%s: registry built", tc.name)
		}
		if !strings.Contains(err.Error(), tc.detail) {
			t.Fatalf("%s: error = %v", tc.name, err)
		}
	}
	// A type with codecs for two versions is the supported shape: it keeps
	// building, and the write Version defaults to the highest codec. A
	// declared write Version without a codec is refused.
	r, err := BuildRegistry(ModuleDescriptor{Source: SourceTwilight, ID: "a", Streams: ownStream("a"),
		Events: []EventDefinition{{
			Type: tpfx("a") + "note", Stream: "a",
			Codecs: map[PayloadVersion]PayloadCodec{1: legacyCodec{}, 2: JSONCodec[notePayload]{}},
		}}})
	if err != nil {
		t.Fatalf("coexisting versions: %v", err)
	}
	if _, def, _ := r.LookupEvent(tpfx("a") + "note"); def.Version != 2 {
		t.Fatalf("default write version = %d, want the highest codec, 2", def.Version)
	}
	if _, err := BuildRegistry(ModuleDescriptor{Source: SourceTwilight, ID: "a", Streams: ownStream("a"),
		Events: []EventDefinition{{Type: tpfx("a") + "note", Stream: "a", Version: 3,
			Codecs: map[PayloadVersion]PayloadCodec{1: legacyCodec{}, 2: JSONCodec[notePayload]{}}}}}); err == nil || !strings.Contains(err.Error(), "write version 3 has no codec") {
		t.Fatalf("write version without codec: %v", err)
	}
}

// legacyCodec decodes the v1 wire of the note event, which predates the
// optional refs field. It deliberately produces a distinguishable value so a
// test can prove the version, not the current codec, selected it.
type legacyCodec struct{}

func (legacyCodec) Encode(v any) (jsonstable.Value, error) {
	return jsonstable.FromValue(struct {
		Text string `json:"text"`
	}{v.(notePayload).Text})
}
func (legacyCodec) Decode(w jsonstable.Value) (any, error) {
	var body struct {
		Text string `json:"text"`
	}
	if err := w.Decode(&body); err != nil {
		return nil, err
	}
	return notePayload{Text: "v1:" + body.Text}, nil
}
func (legacyCodec) Validate(v any) error {
	if v.(notePayload).Text == "" {
		return errors.New("text is required")
	}
	return nil
}

// SES-VER-1, EXT-REG-2, EXT-COD-1/2: one type has codecs for two versions.
// Encode writes the type's write Version and records it as `v` (the highest
// codec by default, or the one the module declares); a row of either version
// decodes through its own codec, while a version no codec claims stays
// Unknown with its raw payload.
func TestRegistryMultiVersionCodecsCoexist(t *testing.T) {
	typ := tpfx("v") + "note"
	codecs := map[PayloadVersion]PayloadCodec{1: legacyCodec{}, 2: JSONCodec[notePayload]{}}
	upgraded := ModuleDescriptor{Source: SourceTwilight, ID: "v", Streams: ownStream("v"), Events: []EventDefinition{{
		Type: typ, Stream: "v", Codecs: codecs,
	}}}
	r, err := BuildRegistry(upgraded)
	if err != nil {
		t.Fatal(err)
	}

	// Writing uses the type's write Version: the highest codec by default.
	wire, err := r.Encode(typ, notePayload{Text: "hi"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if wire.String() != `{"text":"hi","v":2}` {
		t.Fatalf("default write version wire = %s", wire)
	}
	// A module still writing the older version declares it.
	pinned := upgraded
	pinned.Events = []EventDefinition{{Type: typ, Stream: "v", Codecs: codecs, Version: 1}}
	r1, err := BuildRegistry(pinned)
	if err != nil {
		t.Fatal(err)
	}
	if w1, err := r1.Encode(typ, notePayload{Text: "hi"}); err != nil || w1.String() != `{"text":"hi","v":1}` {
		t.Fatalf("pinned write version wire = %s %v", w1, err)
	}

	// A row written before the upgrade still decodes, through its own codec.
	old, err := r.Decode(session.Event{Type: typ, Payload: jsonstable.MustParse(`{"text":"old","v":1}`)})
	if err != nil {
		t.Fatalf("decode v1: %v", err)
	}
	if old.Unknown {
		t.Fatal("a retained older version decoded as Unknown")
	}
	if old.Version != 1 || old.Value.(notePayload).Text != "v1:old" {
		t.Fatalf("v1 row = version %d value %+v: the v1 codec did not run", old.Version, old.Value)
	}
	current, err := r.Decode(session.Event{Type: typ, Payload: wire})
	if err != nil || current.Unknown || current.Value.(notePayload).Text != "hi" {
		t.Fatalf("v2 row = %+v %v", current, err)
	}

	// A version no codec claims is preserved raw rather than reinterpreted.
	future, err := r.Decode(session.Event{Type: typ, Payload: jsonstable.MustParse(`{"text":"x","v":3}`)})
	if err != nil || !future.Unknown || future.Version != 3 {
		t.Fatalf("v3 row = %+v %v, want Unknown v3", future, err)
	}
	if future.Event.Payload.String() != `{"text":"x","v":3}` {
		t.Fatalf("unknown-version payload was not preserved: %s", future.Event.Payload)
	}
}

// EXT-PRJ-9: Authoritative is a capability of trusted core modules. An
// extension cannot declare it, nor claim the first-party source to get it.
func TestExtensionsCannotBeAuthoritative(t *testing.T) {
	core := ModuleDescriptor{Source: SourceTwilight, ID: "core", Projections: []ProjectionDefinition{{
		ID: "twilight/core/p", Version: 1, Authoritative: true,
		Initial: func() (any, error) { return struct{}{}, nil }, Apply: func(s any, _ DecodedEvent) (any, error) { return s, nil }, StateCodec: JSONStateCodec[struct{}]{}}}}
	ext := ModuleDescriptor{Source: "acme", ID: "plugin", Projections: []ProjectionDefinition{{
		ID: "acme/plugin/p", Version: 1, Authoritative: true,
		Initial: func() (any, error) { return struct{}{}, nil }, Apply: func(s any, _ DecodedEvent) (any, error) { return s, nil }, StateCodec: JSONStateCodec[struct{}]{}}}}
	if _, err := BuildRegistryWithExtensions([]ModuleDescriptor{core}, []ModuleDescriptor{ext}); err == nil {
		t.Fatal("extension declared an authoritative projection")
	}
	impostor := ext
	impostor.Source = SourceTwilight
	impostor.Projections[0].Authoritative = false
	if _, err := BuildRegistryWithExtensions([]ModuleDescriptor{core}, []ModuleDescriptor{impostor}); err == nil {
		t.Fatal("extension claimed the twilight source")
	}
	ext.Projections[0].Authoritative = false
	if _, err := BuildRegistryWithExtensions([]ModuleDescriptor{core}, []ModuleDescriptor{ext}); err != nil {
		t.Fatalf("derived extension refused: %v", err)
	}
	// The same descriptor is fine when the caller vouches for it as core.
	if _, err := BuildRegistry(core); err != nil {
		t.Fatalf("trusted core refused: %v", err)
	}
}
