package writer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
)

type notePayload struct {
	Text string   `json:"text"`
	Refs []string `json:"refs,omitempty"`
}

type noteState struct {
	Notes []string `json:"notes"`
}

var refsExtractor = extension.BindingExtractorFunc(func(value any) ([]artifact.BindingID, error) {
	var out []artifact.BindingID
	for _, r := range value.(notePayload).Refs {
		out = append(out, artifact.BindingID(r))
	}
	return out, nil
})

// tpfx is the first-party prefix of a test module.
func tpfx(id extension.ModuleID) session.EventType {
	return extension.ModulePrefix(extension.SourceTwilight, id)
}

// noteDomain is the singleton stream domain every writer test module
// declares; each registry these tests build holds one module.
const noteDomain = "note"

func noteStreams() []extension.StreamDefinition {
	return []extension.StreamDefinition{{Domain: noteDomain, Lineage: session.LineageSession}}
}

func noteModule(id extension.ModuleID, requires ...extension.ModuleRequirement) extension.ModuleDescriptor {
	typ := tpfx(id) + "note"
	return extension.ModuleDescriptor{Source: extension.SourceTwilight, ID: id, Requires: requires, Streams: noteStreams(),
		Events: []extension.EventDefinition{
			{Type: typ, Stream: noteDomain, Codecs: map[extension.PayloadVersion]extension.PayloadCodec{1: extension.JSONCodec[notePayload]{}},
				Bindings: []extension.BindingReferenceDefinition{{Extractor: refsExtractor, RequiredDurability: artifact.EventBound}}},
			{Type: tpfx(id) + "hint", Stream: noteDomain, Codecs: map[extension.PayloadVersion]extension.PayloadCodec{1: extension.JSONCodec[notePayload]{}}, Ignorable: true},
		},
		Projections: []extension.ProjectionDefinition{{
			ID: extension.ProjectionID(string(typ) + "s"), Version: 1, Consumes: []session.EventType{typ}, Authoritative: true,
			Initial: func() (any, error) { return noteState{}, nil },
			Apply: func(state any, e extension.DecodedEvent) (any, error) {
				s := state.(noteState)
				text := e.Value.(notePayload).Text
				if text == "reject" {
					return nil, errors.New("rejected by projection")
				}
				s.Notes = append(append([]string(nil), s.Notes...), text)
				return s, nil
			},
			StateCodec: extension.JSONStateCodec[noteState]{},
		}},
	}
}

// noteBatch wraps events as the single note-stream batch tests write.
func noteBatch(events ...TypedEvent) []TypedBatch {
	return []TypedBatch{{Stream: session.StreamRef{Domain: noteDomain}, Events: events}}
}

type fixture struct {
	store    session.Store
	registry *extension.Registry
	bindings artifact.BindingStore
	ledger   artifact.RetentionLedger
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{}
	f.store = filestoretest.Store(t)
	r, err := extension.BuildRegistry(noteModule("a"))
	if err != nil {
		t.Fatal(err)
	}
	f.registry = r
	f.bindings, f.ledger = artifacttest.Stores(t)
	if _, err := f.store.Create(context.Background(), session.CreateRequest{SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fixture) admission() Admission { return Admission{Bindings: f.bindings, Ledger: f.ledger} }

func (f *fixture) open(t *testing.T, takeover bool) Writer {
	t.Helper()
	w, err := OpenWriter(context.Background(), f.store, f.registry, f.admission(), "s", session.OpenOptions{Takeover: takeover})
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	return w
}

func noteGroup(id string, texts ...string) CommitFn {
	return func(View) (*SemanticGroup, error) {
		g := &SemanticGroup{CommitID: session.CommitID(id)}
		var events []TypedEvent
		for _, tx := range texts {
			events = append(events, TypedEvent{Type: tpfx("a") + "note", Value: notePayload{Text: tx}})
		}
		g.Batches = noteBatch(events...)
		return g, nil
	}
}

func notes(t *testing.T, w Writer) []string {
	t.Helper()
	state, _, err := w.Projections().Load(context.Background(), "s", extension.ProjectionID(string(tpfx("a"))+"notes"), 1)
	if err != nil {
		t.Fatal(err)
	}
	return state.(noteState).Notes
}

// EXT-WRT-1/2: serial commits, in-memory idempotency, rebuild on reopen,
// projections visible through View and Projections().
func TestWriterCommitReplayAndRebuild(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	w := f.open(t, false)
	res, err := w.Commit(ctx, noteGroup("c1", "one", "two"))
	if err != nil || res.Outcome != CommitApplied || res.Commit.Seq != 0 || len(res.Commit.Batches[0].Events) != 2 {
		t.Fatalf("commit = %+v %v", res, err)
	}
	if got := notes(t, w); len(got) != 2 || got[1] != "two" {
		t.Fatalf("projection after commit = %v", got)
	}
	replay, _ := w.Commit(ctx, noteGroup("c1", "one", "two"))
	if replay.Outcome != CommitAlreadyApplied || replay.Commit.Seq != res.Commit.Seq || len(replay.Commit.Batches[0].Events) != 2 {
		t.Fatalf("replay = %+v", replay)
	}
	// The CommitID names the operation: a second group under it is the
	// same operation whatever it carries (EXT-WRT-2).
	if again, _ := w.Commit(ctx, noteGroup("c1", "changed")); again.Outcome != CommitAlreadyApplied || again.Commit.Seq != res.Commit.Seq {
		t.Fatalf("replay with other content = %+v, want already applied", again)
	}
	noop, _ := w.Commit(ctx, func(View) (*SemanticGroup, error) { return nil, nil })
	if noop.Outcome != CommitNoop {
		t.Fatalf("noop = %+v", noop)
	}
	invalid, _ := w.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c2", Batches: noteBatch(TypedEvent{Type: "twilight/a/unknown", Value: notePayload{}})}, nil
	})
	if invalid.Outcome != CommitInvalid {
		t.Fatalf("invalid = %+v", invalid)
	}
	rejected, _ := w.Commit(ctx, noteGroup("c3", "fine", "reject"))
	if rejected.Outcome != CommitInvalid {
		t.Fatalf("projection rejection must block the append: %+v", rejected)
	}
	if page, _ := f.store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"}); len(page.Commits) != 1 {
		t.Fatalf("rejected commits wrote commits: %d", len(page.Commits))
	}
	// The View sees head, commit history and projection; fn may use them.
	_, err = w.Commit(ctx, func(v View) (*SemanticGroup, error) {
		if v.Head().Next != 1 || v.Epoch() != 1 {
			t.Fatalf("view head/epoch = %+v %d", v.Head(), v.Epoch())
		}
		if !v.Committed("c1") {
			t.Fatal("view does not see the committed commit")
		}
		if c, ok, err := v.LookupCommit("c1"); err != nil || !ok || len(c.Batches) != 1 || len(c.Batches[0].Events) != 2 {
			t.Fatalf("view lookup = %+v %v %v", c, ok, err)
		}
		if s, err := v.Projection(extension.ProjectionID(string(tpfx("a"))+"notes"), 1); err != nil || len(s.(noteState).Notes) != 2 {
			t.Fatalf("view projection = %+v %v", s, err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWriter(ctx, f.store, f.registry, f.admission(), "s", session.OpenOptions{}); !session.IsCode(err, session.ErrOwned) {
		t.Fatalf("second writer = %v, want owned", err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(ctx, noteGroup("c4", "x")); err == nil {
		t.Fatal("closed writer accepted a commit")
	}
	w2 := f.open(t, false)
	if w2.Epoch() != 2 {
		t.Fatalf("epoch = %d", w2.Epoch())
	}
	if got := notes(t, w2); len(got) != 2 {
		t.Fatalf("rebuilt projection = %v", got)
	}
	if again, _ := w2.Commit(ctx, noteGroup("c1", "one", "two")); again.Outcome != CommitAlreadyApplied {
		t.Fatalf("index not rebuilt: %+v", again)
	}
	reader := extension.NewProjectionReader(f.store, f.registry, nil)
	state, through, err := reader.Load(ctx, "s", extension.ProjectionID(string(tpfx("a"))+"notes"), 1)
	if err != nil || len(state.(noteState).Notes) != 2 || through.Next != 1 {
		t.Fatalf("store reader = %+v %+v %v", state, through, err)
	}
}

// EXT-WRT-4: a superseded writer fails closed.
func TestWriterOwnershipLost(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	w1 := f.open(t, false)
	if _, err := w1.Commit(ctx, noteGroup("c1", "one")); err != nil {
		t.Fatal(err)
	}
	w2 := f.open(t, true)
	if _, err := w1.Commit(ctx, noteGroup("c2", "late")); !errors.Is(err, &extension.Error{Code: extension.ErrOwnershipLost}) {
		t.Fatalf("stale writer commit = %v, want ownership_lost", err)
	}
	if _, err := w1.Commit(ctx, noteGroup("c3", "again")); !errors.Is(err, &extension.Error{Code: extension.ErrOwnershipLost}) {
		t.Fatal("writer did not stay failed")
	}
	if got := notes(t, w2); len(got) != 1 {
		t.Fatalf("fenced write leaked: %v", got)
	}
	ws := NewWriters(f.store, f.registry, f.admission(), session.OpenOptions{}, WritersConfig{})
	if _, err := ws.Writer(ctx, "s"); !session.IsCode(err, session.ErrOwned) {
		t.Fatalf("writers while owned = %v", err)
	}
	_ = w2.Close(ctx)
	a, err := ws.Writer(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := ws.Writer(ctx, "s"); a != b {
		t.Fatal("Writers handed out two writers for one session")
	}
}

// EXT-PRJ-2: unknown events in scope fail the fold unless the module marked
// the type Ignorable; unknown events of other modules are skipped. The
// registry is the only authority on Ignorable: an in-scope event whose type
// is not registered at all cannot prove it is ignorable and fails the fold.
func TestProjectionUnknownEvents(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	w := f.open(t, false)
	if _, err := w.Commit(ctx, noteGroup("c1", "one")); err != nil {
		t.Fatal(err)
	}
	hint, _ := w.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c2", Batches: noteBatch(TypedEvent{Type: tpfx("a") + "hint", Value: notePayload{Text: "h"}})}, nil
	})
	if hint.Outcome != CommitApplied {
		t.Fatalf("ignorable event commit = %+v", hint)
	}
	_ = w.Close(ctx)
	kw, _ := f.store.Open(ctx, "s", session.OpenOptions{})
	raw := func(id string, typ session.EventType, payload string) {
		if _, err := kw.Append(ctx, session.Proposal{CommitID: session.CommitID(id), Batches: []session.StreamBatch{
			{Stream: session.StreamRef{Domain: noteDomain}, Events: []session.Event{{Type: typ, Payload: jsonstable.MustParse(payload)}}},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	raw("other", "twilight/zzz/thing", `{"v":1}`)         // out of scope: skipped
	raw("hinted", tpfx("a")+"hint", `{"text":"x","v":2}`) // in scope, ignorable, unknown version: skipped
	_ = kw.Close(ctx)
	w = f.open(t, false)
	if got := notes(t, w); len(got) != 1 {
		t.Fatalf("notes = %v, want [one]", got)
	}
	_ = w.Close(ctx)
	kw, _ = f.store.Open(ctx, "s", session.OpenOptions{})
	raw("strict", "twilight/a/strict", `{"v":1}`) // in scope, not registered: fold fails
	_ = kw.Close(ctx)
	if _, err := OpenWriter(ctx, f.store, f.registry, f.admission(), "s", session.OpenOptions{}); !errors.Is(err, &extension.Error{Code: extension.ErrUnknownEvent}) {
		t.Fatalf("open with unknown strict event = %v", err)
	}
}

// EXT-WRT-3 and ART-RET-3: claims are Active before the commit exists; an
// orphan claim is released on the next OpenWriter; a live claim survives.
// EXT-REF-1/2, EXT-WRT-3: a missing resolver or ledger is a configuration
// error, so it must surface as an error rather than as a CommitInvalid outcome
// that reads like a verdict on the commit. It must not be rejected earlier
// either: an event type declaring Bindings only means its payloads may carry
// references, so a deployment that never attaches an artifact needs neither.
func TestCommitWithoutAdmission(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	// The resolver case below must reach the ledger check, so the referenced
	// binding has to exist.
	b, err := artifact.NewBinding("b1", artifact.Ref{Scheme: "spill", Authority: "local", Key: "k", Durability: artifact.EventBound})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.bindings.CreateBinding(ctx, b); err != nil {
		t.Fatal(err)
	}
	empty, err := OpenWriter(ctx, f.store, f.registry, Admission{}, "s", session.OpenOptions{})
	if err != nil {
		t.Fatalf("open without admission: %v", err)
	}
	defer empty.Close(ctx)

	// A payload with no references never consults admission, so a text-only
	// deployment must keep working.
	res, err := empty.Commit(ctx, noteGroup("c1", "no refs here"))
	if err != nil || res.Outcome != CommitApplied {
		t.Fatalf("commit without references = %+v %v", res, err)
	}

	// A payload that does carry a reference against a nil resolver is a
	// configuration error, not an invalid commit.
	withRef, err := OpenWriter(ctx, f.store, f.registry, Admission{}, "s", session.OpenOptions{Takeover: true})
	if err != nil {
		t.Fatal(err)
	}
	defer withRef.Close(ctx)
	res, err = withRef.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c2", Batches: noteBatch(TypedEvent{
			Type: tpfx("a") + "note", Value: notePayload{Text: "file", Refs: []string{"b1"}}}),
		}, nil
	})
	if err == nil {
		t.Fatalf("commit with a reference and no resolver = %+v, want an error (got no error)", res)
	}
	if !strings.Contains(err.Error(), "no binding resolver") {
		t.Fatalf("missing resolver error = %v", err)
	}

	// A resolver without a ledger fails the same way, at claim time.
	noLedger, err := OpenWriter(ctx, f.store, f.registry, Admission{Bindings: f.bindings}, "s", session.OpenOptions{Takeover: true})
	if err != nil {
		t.Fatal(err)
	}
	defer noLedger.Close(ctx)
	res, err = noLedger.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c3", Batches: noteBatch(TypedEvent{
			Type: tpfx("a") + "note", Value: notePayload{Text: "file", Refs: []string{"b1"}}}),
		}, nil
	})
	if err == nil {
		t.Fatalf("commit with a reference and no ledger = %+v, want an error (got no error)", res)
	}
	if !strings.Contains(err.Error(), "no retention ledger") {
		t.Fatalf("missing ledger error = %v", err)
	}

	// None of the rejected commits may have written anything.
	page, err := f.store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Commits) != 1 || page.Commits[0].CommitID != "c1" {
		t.Fatalf("rejected commits wrote commits: %+v", page.Commits)
	}
}

// EXT-WRT-1: the Writer is the single serialization point. Concurrent callers
// must each see the state left by the previous one, so every commit applies
// exactly once and the stream stays contiguous.
func TestWriterSerializesConcurrentCommits(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	w := f.open(t, false)
	defer w.Close(ctx)

	const n = 8
	texts := make([]string, n)
	results := make([]CommitResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		texts[i] = fmt.Sprintf("n%d", i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = w.Commit(ctx, noteGroup(fmt.Sprintf("c%d", i), texts[i]))
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil || results[i].Outcome != CommitApplied {
			t.Fatalf("commit %d = %+v %v", i, results[i], errs[i])
		}
	}
	// Every commit was serialized onto its own Seq.
	seen := map[session.CommitSeq]bool{}
	for _, res := range results {
		if seen[res.Commit.Seq] {
			t.Fatalf("seq %d assigned twice: commits raced", res.Commit.Seq)
		}
		seen[res.Commit.Seq] = true
	}
	if len(seen) != n {
		t.Fatalf("distinct seqs = %d, want %d", len(seen), n)
	}
	for i := 0; i < n; i++ {
		if !seen[session.CommitSeq(i)] {
			t.Fatalf("seq %d missing; head is not contiguous", i)
		}
	}
	if got := notes(t, w); len(got) != n {
		t.Fatalf("folded notes = %v, want all %d: a commit did not observe its predecessor", got, n)
	}
}

// EXT-REF-1/2: the extractor returns every reference in appearance order,
// cardinality and scheme/durability admission bound what may commit, and a
// rejected commit writes nothing.
func TestBindingAdmission(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	for _, b := range []struct {
		id  artifact.BindingID
		ref artifact.Ref
	}{
		{"ok1", artifact.Ref{Scheme: "spill", Authority: "local", Key: "k1", Durability: artifact.EventBound}},
		{"ok2", artifact.Ref{Scheme: "spill", Authority: "local", Key: "k2", Durability: artifact.Pinned}},
		{"other", artifact.Ref{Scheme: "other", Authority: "local", Key: "k3", Durability: artifact.EventBound}},
		{"weak", artifact.Ref{Scheme: "spill", Authority: "local", Key: "k4", Durability: artifact.Ephemeral}},
	} {
		binding, err := artifact.NewBinding(b.id, b.ref)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.bindings.CreateBinding(ctx, binding); err != nil {
			t.Fatal(err)
		}
	}

	maxTwo := uint32(2)
	typ := tpfx("r") + "ref"
	reg, err := extension.BuildRegistry(extension.ModuleDescriptor{Source: extension.SourceTwilight, ID: "r", Streams: noteStreams(),
		Events: []extension.EventDefinition{{
			Type: typ, Stream: noteDomain, Codecs: map[extension.PayloadVersion]extension.PayloadCodec{1: extension.JSONCodec[notePayload]{}},
			Bindings: []extension.BindingReferenceDefinition{{
				Extractor: refsExtractor, Cardinality: extension.Cardinality{Min: 1, Max: &maxTwo},
				AllowedSchemes:     []artifact.Scheme{"spill"},
				RequiredDurability: artifact.EventBound,
			}},
		}}})
	if err != nil {
		t.Fatal(err)
	}
	w, err := OpenWriter(ctx, f.store, reg, Admission{Bindings: f.bindings, Ledger: f.ledger}, "s", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)

	commit := func(id string, refs ...string) CommitResult {
		t.Helper()
		res, err := w.Commit(ctx, func(View) (*SemanticGroup, error) {
			return &SemanticGroup{CommitID: session.CommitID(id), Batches: noteBatch(TypedEvent{
				Type: typ, Value: notePayload{Text: "r", Refs: refs}}),
			}, nil
		})
		if err != nil {
			t.Fatalf("commit %s: %v", id, err)
		}
		return res
	}

	// Admissible: the claim covers every extracted reference, in order.
	res := commit("c1", "ok1", "ok2")
	if res.Outcome != CommitApplied || res.Claim == nil {
		t.Fatalf("admissible commit = %+v", res)
	}
	claim, ok, err := f.ledger.LookupClaim(ctx, res.Claim.ID)
	if err != nil || !ok {
		t.Fatalf("claim lookup = %v %v", ok, err)
	}
	if got := claim.BindingSet.BindingIDs; len(got) != 2 || got[0] != "ok1" || got[1] != "ok2" {
		t.Fatalf("claim set = %v, want every extracted reference in appearance order", got)
	}

	for i, tc := range []struct {
		name string
		refs []string
		want string
	}{
		{"below cardinality", nil, "cardinality"},
		{"above cardinality", []string{"ok1", "ok2", "ok1"}, "cardinality"},
		{"scheme not allowed", []string{"other"}, "scheme other not allowed"},
		{"durability below required", []string{"weak"}, "below required"},
	} {
		got := commit(fmt.Sprintf("r%d", i), tc.refs...)
		if got.Outcome != CommitInvalid {
			t.Fatalf("%s = %+v, want invalid", tc.name, got)
		}
		// Assert the reason so a commit rejected for some other cause cannot
		// make this pass.
		if !strings.Contains(got.Detail, tc.want) {
			t.Fatalf("%s detail = %q, want mention of %q", tc.name, got.Detail, tc.want)
		}
	}

	// Only the admissible commit may have landed.
	page, err := f.store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Commits) != 1 || page.Commits[0].CommitID != "c1" {
		t.Fatalf("rejected commits wrote commits: %+v", page.Commits)
	}
}

func TestWriterClaimsAndReconcile(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	b, _ := artifact.NewBinding("b1", artifact.Ref{Scheme: "spill", Authority: "local", Key: "k", Durability: artifact.EventBound})
	if _, err := f.bindings.CreateBinding(ctx, b); err != nil {
		t.Fatal(err)
	}
	w := f.open(t, false)
	res, err := w.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c1", Batches: noteBatch(TypedEvent{Type: tpfx("a") + "note", Value: notePayload{Text: "file", Refs: []string{"b1"}}})}, nil
	})
	if err != nil || res.Outcome != CommitApplied || res.Claim == nil || res.Claim.State != artifact.ClaimActive {
		t.Fatalf("commit with binding = %+v %v", res, err)
	}
	missing, _ := w.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c2", Batches: noteBatch(TypedEvent{Type: tpfx("a") + "note", Value: notePayload{Text: "x", Refs: []string{"nope"}}})}, nil
	})
	if missing.Outcome != CommitInvalid {
		t.Fatalf("unknown binding = %+v", missing)
	}
	// Simulate a crash between claim and append: an Active claim whose owner
	// commit never made it into the ledger.
	set, _ := artifact.SetBuilder{Resolver: f.bindings}.Build(ctx, []artifact.BindingID{"b1"})
	seg := tipSegment(t, f.store, "s")
	orphanID := DeriveClaimID(seg, "never", set.RefSetDigest)
	if _, err := f.ledger.Activate(ctx, orphanID, CommitOwner(seg, "never"), set); err != nil {
		t.Fatal(err)
	}
	_ = w.Close(ctx)
	w = f.open(t, false)
	defer w.Close(ctx)
	if c, ok, _ := f.ledger.LookupClaim(ctx, orphanID); !ok || c.State != artifact.ClaimReleased {
		t.Fatalf("orphan claim = %+v", c)
	}
	if c, ok, _ := f.ledger.LookupClaim(ctx, res.Claim.ID); !ok || c.State != artifact.ClaimActive {
		t.Fatalf("live claim = %+v", c)
	}
}

// EXT-PRJ-3/4: cache entry plus tail equals the full fold; a stale or missing
// entry falls back to a full fold; Writer and Store readers agree.
func TestProjectionCache(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	w := f.open(t, false)
	defer w.Close(ctx)
	id := extension.ProjectionID(string(tpfx("a")) + "notes")
	_, _ = w.Commit(ctx, noteGroup("c1", "one"))
	cache := extension.NewMemoryProjectionCache()
	state, through, _ := w.Projections().Load(ctx, "s", id, 1)
	if err := extension.SaveProjection(ctx, cache, f.registry, "s", id, 1, state, through); err != nil {
		t.Fatal(err)
	}
	_, _ = w.Commit(ctx, noteGroup("c2", "two"))
	reader := extension.NewProjectionReader(f.store, f.registry, cache)
	got, head, err := reader.Load(ctx, "s", id, 1)
	if err != nil || len(got.(noteState).Notes) != 2 || head.Next != 2 {
		t.Fatalf("cache+tail = %+v %+v %v", got, head, err)
	}
	// A cache entry claiming a head the stream does not have is ignored.
	_ = cache.Save(ctx, "s", id, 1, jsonstable.MustParse(`{"notes":["bogus"]}`), session.Head{Next: 9})
	got, _, err = reader.Load(ctx, "s", id, 1)
	if err != nil || got.(noteState).Notes[0] != "one" {
		t.Fatalf("stale cache used: %+v %v", got, err)
	}
	cache.Delete("s", id, 1)
	got, _, _ = reader.Load(ctx, "s", id, 1)
	mem, _, _ := w.Projections().Load(ctx, "s", id, 1)
	if len(got.(noteState).Notes) != len(mem.(noteState).Notes) {
		t.Fatal("store reader and writer reader disagree")
	}
}

// tipSegment is the segment sid's root currently appends to: the owner
// authority of the claims its Writer activates.
func tipSegment(t *testing.T, store session.Store, sid session.SessionID) session.SegmentID {
	t.Helper()
	h, err := store.Header(context.Background(), sid)
	if err != nil {
		t.Fatal(err)
	}
	return h.ID
}
