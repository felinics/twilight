// Package artifacttest is the conformance suite of the Artifact Core
// (agent-artifact.md section 8). It is parameterized by the adapter under
// test so the Memory implementations and any durable adapter run the same
// assertions.
package artifacttest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/artifact"
)

// Fixture is one adapter under test. Ledger must verify sets through a
// BindingSetBuilder over Bindings (ART-RET-1). NewContent builds a cas
// ContentStore for one Authority; Advance moves the clock those stores judge
// expiry by, nil skips the expiry checks.
type Fixture struct {
	Bindings   artifact.BindingStore
	Ledger     artifact.RetentionLedger
	NewContent func(t *testing.T, authority artifact.Authority) artifact.ContentStore
	Advance    func(time.Duration)
}

// Factory builds a fresh, empty Fixture for one subtest.
type Factory func(t *testing.T) Fixture

// Run executes the suite.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("ref", func(t *testing.T) { testRef(t) })
	t.Run("binding", func(t *testing.T) { testBinding(t, factory(t)) })
	t.Run("set", func(t *testing.T) { testSet(t, factory(t)) })
	t.Run("ledger", func(t *testing.T) { testLedger(t, factory(t)) })
	t.Run("reconcile", func(t *testing.T) { testReconcile(t, factory(t)) })
	t.Run("paging", func(t *testing.T) { testPaging(t, factory(t)) })
	t.Run("content", func(t *testing.T) { testContent(t, factory(t)) })
	t.Run("promote", func(t *testing.T) { testPromote(t, factory(t)) })
	t.Run("registry", func(t *testing.T) { testRegistry(t) })
}

func isCode(err error, code artifact.ErrorCode) bool {
	var e *artifact.Error
	for err != nil {
		var ae *artifact.Error
		if errors.As(err, &ae) {
			e = ae
			break
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			break
		}
		err = u.Unwrap()
	}
	return e != nil && e.Code == code
}

func sampleRef(key artifact.Key, durability artifact.Durability) artifact.Ref {
	size := uint64(3)
	return artifact.Ref{Scheme: artifact.SchemeCAS, Authority: "a", Key: key, MediaType: "text/plain", SizeBytes: &size,
		Integrity: &artifact.Integrity{Algorithm: "sha256", Value: string(key)}, Durability: durability}
}

func mustBinding(t *testing.T, ctx context.Context, store artifact.BindingStore, id artifact.BindingID, ref artifact.Ref) artifact.Binding {
	t.Helper()
	b, err := artifact.NewBinding(id, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateBinding(ctx, b); err != nil {
		t.Fatalf("create binding %s: %v", id, err)
	}
	return b
}

// ART-ID-1, ART-REF-1, ART-REF-2: validation, identity stability and the
// identity-bound MediaType.
func testRef(t *testing.T) {
	expires := int64(5)
	rejects := []struct {
		name string
		ref  artifact.Ref
	}{
		{"empty scheme", artifact.Ref{Authority: "a", Key: "k", Durability: artifact.Pinned}},
		{"empty authority", artifact.Ref{Scheme: "spill", Key: "k", Durability: artifact.Pinned}},
		{"empty key", artifact.Ref{Scheme: "spill", Authority: "a", Durability: artifact.Pinned}},
		{"unknown durability", artifact.Ref{Scheme: "spill", Authority: "a", Key: "k", Durability: "forever"}},
		{"cas without integrity", artifact.Ref{Scheme: artifact.SchemeCAS, Authority: "a", Key: "k", Durability: artifact.Pinned}},
		{"expiry on event_bound", artifact.Ref{Scheme: "spill", Authority: "a", Key: "k", Durability: artifact.EventBound, ExpiresAtUnixMilli: &expires}},
	}
	for _, tc := range rejects {
		if err := tc.ref.Validate(); !isCode(err, artifact.ErrInvalid) {
			t.Fatalf("%s: err = %v, want invalid", tc.name, err)
		}
		if _, err := tc.ref.Identity(); err == nil {
			t.Fatalf("%s: identity of an invalid ref succeeded", tc.name)
		}
	}
	ok := artifact.Ref{Scheme: "spill", Authority: "a", Key: "k", Durability: artifact.Ephemeral, ExpiresAtUnixMilli: &expires}
	if err := ok.Validate(); err != nil {
		t.Fatalf("ephemeral with expiry: %v", err)
	}
	a := sampleRef("k1", artifact.EventBound)
	id1, _ := a.Identity()
	id2, _ := a.Identity()
	if id1 == "" || id1 != id2 {
		t.Fatal("identity is not stable")
	}
	b := a
	b.MediaType = "application/json"
	idB, _ := b.Identity()
	if idB == id1 {
		t.Fatal("MediaType must be identity-bound")
	}
	da, _ := artifact.DigestBinding("b", a)
	db, _ := artifact.DigestBinding("b", b)
	if da == db {
		t.Fatal("binding digest must cover the full ref identity")
	}
	if artifact.Ephemeral.Rank() >= artifact.EventBound.Rank() || artifact.EventBound.Rank() >= artifact.Pinned.Rank() {
		t.Fatal("durability order")
	}
}

// ART-BND-1: immutable bindings, exact-rebuild idempotency, conflict.
func testBinding(t *testing.T, f Fixture) {
	ctx := context.Background()
	ref := sampleRef("k1", artifact.EventBound)
	b := mustBinding(t, ctx, f.Bindings, "b1", ref)
	if again, err := f.Bindings.CreateBinding(ctx, b); err != nil || again.Digest != b.Digest {
		t.Fatalf("identical create is not idempotent: %+v %v", again, err)
	}
	other := ref
	other.Key = "k2"
	other.Integrity.Value = "k2"
	ob, _ := artifact.NewBinding("b1", other)
	if _, err := f.Bindings.CreateBinding(ctx, ob); !isCode(err, artifact.ErrConflict) {
		t.Fatalf("different ref under the same id = %v, want conflict", err)
	}
	forged := b
	forged.Digest = "sha256:0"
	if _, err := f.Bindings.CreateBinding(ctx, forged); !isCode(err, artifact.ErrInvalid) {
		t.Fatalf("mismatched digest = %v, want invalid", err)
	}
	if got, err := f.Bindings.ResolveBinding(ctx, "b1"); err != nil || got.Ref.Key != "k1" {
		t.Fatalf("resolve = %+v %v", got, err)
	}
	if _, err := f.Bindings.ResolveBinding(ctx, "missing"); !isCode(err, artifact.ErrNotFound) {
		t.Fatalf("resolve missing = %v, want not_found", err)
	}
	if _, found, err := f.Bindings.LookupBinding(ctx, "missing"); err != nil || found {
		t.Fatal("lookup missing must report absence without error")
	}
}

// ART-RET-1, ART-RET-2 (builder half): sorted-unique ids, order-independent
// digest, ephemeral rejection.
func testSet(t *testing.T, f Fixture) {
	ctx := context.Background()
	mustBinding(t, ctx, f.Bindings, "b1", sampleRef("k1", artifact.EventBound))
	mustBinding(t, ctx, f.Bindings, "b2", sampleRef("k2", artifact.Pinned))
	mustBinding(t, ctx, f.Bindings, "eph", sampleRef("k3", artifact.Ephemeral))
	builder := artifact.SetBuilder{Resolver: f.Bindings}
	s1, err := builder.Build(ctx, []artifact.BindingID{"b2", "b1", "b2"})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := builder.Build(ctx, []artifact.BindingID{"b1", "b2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(s1.BindingIDs) != 2 || s1.BindingIDs[0] != "b1" || s1.RefSetDigest != s2.RefSetDigest {
		t.Fatalf("set not canonical: %+v vs %+v", s1, s2)
	}
	if _, err := builder.Build(ctx, []artifact.BindingID{"b1", "eph"}); !isCode(err, artifact.ErrInvalid) {
		t.Fatalf("ephemeral binding = %v, want invalid", err)
	}
	if _, err := builder.Build(ctx, []artifact.BindingID{"nope"}); !isCode(err, artifact.ErrNotFound) {
		t.Fatalf("unknown binding = %v, want not_found", err)
	}
}

// ART-RET-2: the state table, exact verification of incoming sets.
func testLedger(t *testing.T, f Fixture) {
	ctx := context.Background()
	mustBinding(t, ctx, f.Bindings, "b1", sampleRef("k1", artifact.EventBound))
	mustBinding(t, ctx, f.Bindings, "b2", sampleRef("k2", artifact.EventBound))
	builder := artifact.SetBuilder{Resolver: f.Bindings}
	set, _ := builder.Build(ctx, []artifact.BindingID{"b1"})
	other, _ := builder.Build(ctx, []artifact.BindingID{"b2"})
	owner := artifact.ClaimOwner{Kind: "k", Authority: "s", Identity: "c1"}
	c, err := f.Ledger.Activate(ctx, "claim-1", owner, set)
	if err != nil || c.State != artifact.ClaimActive {
		t.Fatalf("activate = %+v %v", c, err)
	}
	if again, err := f.Ledger.Activate(ctx, "claim-1", owner, set); err != nil || again.ID != "claim-1" {
		t.Fatalf("exact re-activate must be idempotent: %v", err)
	}
	if _, err := f.Ledger.Activate(ctx, "claim-1", owner, other); !isCode(err, artifact.ErrConflict) {
		t.Fatalf("other set under same id = %v, want conflict", err)
	}
	if _, err := f.Ledger.Activate(ctx, "claim-1", artifact.ClaimOwner{Kind: "k", Authority: "s", Identity: "c2"}, set); !isCode(err, artifact.ErrConflict) {
		t.Fatalf("other owner under same id = %v, want conflict", err)
	}
	forged := set
	forged.RefSetDigest = "sha256:0"
	if _, err := f.Ledger.Activate(ctx, "claim-2", owner, forged); !isCode(err, artifact.ErrInvalid) {
		t.Fatalf("set that does not verify = %v, want invalid", err)
	}
	if _, err := f.Ledger.Activate(ctx, "", owner, set); !isCode(err, artifact.ErrInvalid) {
		t.Fatal("empty claim id must be invalid")
	}
	if err := f.Ledger.ReleaseActive(ctx, "claim-1"); err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := f.Ledger.LookupClaim(ctx, "claim-1"); !ok || got.State != artifact.ClaimReleased {
		t.Fatalf("after release = %+v %v", got, ok)
	}
	if err := f.Ledger.ReleaseActive(ctx, "claim-1"); err != nil {
		t.Fatalf("releasing a released claim must be idempotent: %v", err)
	}
	if err := f.Ledger.ReleaseActive(ctx, "nope"); !isCode(err, artifact.ErrNotFound) {
		t.Fatalf("release unknown = %v, want not_found", err)
	}
	if _, err := f.Ledger.Activate(ctx, "claim-1", owner, set); !isCode(err, artifact.ErrConflict) {
		t.Fatalf("re-activating a released claim = %v, want conflict", err)
	}
}

type ownerSet map[string]bool

func (s ownerSet) OwnerExists(_ context.Context, o artifact.ClaimOwner) (bool, error) {
	return s[o.Identity], nil
}

// ART-RET-3: reconciliation releases orphans and leaves owned claims alone.
func testReconcile(t *testing.T, f Fixture) {
	ctx := context.Background()
	mustBinding(t, ctx, f.Bindings, "b1", sampleRef("k1", artifact.EventBound))
	set, _ := artifact.SetBuilder{Resolver: f.Bindings}.Build(ctx, []artifact.BindingID{"b1"})
	for _, id := range []string{"c1", "c2", "c3"} {
		if _, err := f.Ledger.Activate(ctx, artifact.ClaimID("claim-"+id), artifact.ClaimOwner{Kind: "k", Authority: "s", Identity: id}, set); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.Ledger.Activate(ctx, "elsewhere", artifact.ClaimOwner{Kind: "k", Authority: "other", Identity: "c9"}, set); err != nil {
		t.Fatal(err)
	}
	released, err := artifact.Reconcile(ctx, f.Ledger, artifact.ClaimOwnerScope{Kind: "k", Authority: "s"}, ownerSet{"c1": true, "c3": true})
	if err != nil || released != 1 {
		t.Fatalf("reconcile released %d, err %v", released, err)
	}
	for id, want := range map[artifact.ClaimID]artifact.ClaimState{"claim-c1": artifact.ClaimActive, "claim-c2": artifact.ClaimReleased, "claim-c3": artifact.ClaimActive, "elsewhere": artifact.ClaimActive} {
		got, _, _ := f.Ledger.LookupClaim(ctx, id)
		if got.State != want {
			t.Fatalf("%s = %s, want %s", id, got.State, want)
		}
	}
}

// ART-RET-3: watermark cursor pagination and owner filters.
func testPaging(t *testing.T, f Fixture) {
	ctx := context.Background()
	mustBinding(t, ctx, f.Bindings, "b1", sampleRef("k1", artifact.EventBound))
	set, _ := artifact.SetBuilder{Resolver: f.Bindings}.Build(ctx, []artifact.BindingID{"b1"})
	ids := []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7"}
	for _, id := range ids {
		if _, err := f.Ledger.Activate(ctx, artifact.ClaimID("claim-"+id), artifact.ClaimOwner{Kind: "k", Authority: "s", Identity: id}, set); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Ledger.ReleaseActive(ctx, "claim-c4"); err != nil {
		t.Fatal(err)
	}
	q := artifact.ClaimOwnerQuery{Kind: "k", Authority: "s", Limit: 3}
	var seen []artifact.ClaimID
	cursor := artifact.ClaimCursor{}
	pages := 0
	for {
		page, err := f.Ledger.ClaimsByOwner(ctx, q, cursor)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, c := range page.Items {
			seen = append(seen, c.ID)
		}
		if pages == 1 {
			// A claim activated mid-enumeration sits above the watermark.
			if _, err := f.Ledger.Activate(ctx, "claim-c9", artifact.ClaimOwner{Kind: "k", Authority: "s", Identity: "c9"}, set); err != nil {
				t.Fatal(err)
			}
		}
		if page.Next == nil {
			break
		}
		cursor = *page.Next
	}
	if pages != 3 || len(seen) != 7 {
		t.Fatalf("pages=%d seen=%v", pages, seen)
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] <= seen[i-1] {
			t.Fatalf("not in ClaimID order: %v", seen)
		}
	}
	if seen[3] != "claim-c4" {
		t.Fatal("released claims must be enumerated too; the caller filters by state")
	}
	filtered, err := f.Ledger.ClaimsByOwner(ctx, artifact.ClaimOwnerQuery{Kind: "k", Authority: "s", Identities: []string{"c2", "c9", ""}}, artifact.ClaimCursor{})
	if err != nil || len(filtered.Items) != 2 || filtered.Next != nil {
		t.Fatalf("identity filter = %+v %v", filtered, err)
	}
	// An identity rule sparser than the page: matching claims are spread
	// over many non-matching ones, and every page still fills, in order,
	// with the watermark fixed on the first.
	for i := range 20 {
		id := fmt.Sprintf("d%02d", i)
		if _, err := f.Ledger.Activate(ctx, artifact.ClaimID("sparse-"+id), artifact.ClaimOwner{Kind: "k", Authority: "sparse", Identity: id}, set); err != nil {
			t.Fatal(err)
		}
	}
	sparse := artifact.ClaimOwnerQuery{Kind: "k", Authority: "sparse", Identities: []string{"d03", "d09", "d15", "d19"}, Limit: 2}
	var sparseSeen []artifact.ClaimID
	cursor = artifact.ClaimCursor{}
	for pages = 0; ; pages++ {
		page, err := f.Ledger.ClaimsByOwner(ctx, sparse, cursor)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range page.Items {
			sparseSeen = append(sparseSeen, c.ID)
		}
		if page.Next == nil {
			break
		}
		if page.Next.Watermark != "sparse-d19" {
			t.Fatalf("sparse watermark = %s, want the last matching claim", page.Next.Watermark)
		}
		cursor = *page.Next
	}
	if want := []artifact.ClaimID{"sparse-d03", "sparse-d09", "sparse-d15", "sparse-d19"}; fmt.Sprint(sparseSeen) != fmt.Sprint(want) || pages > 3 {
		t.Fatalf("sparse enumeration = %v in %d pages, want %v in at most 3", sparseSeen, pages+1, want)
	}
	empty, err := f.Ledger.ClaimsByOwner(ctx, artifact.ClaimOwnerQuery{Kind: "k", Authority: "s", Identities: []string{""}}, artifact.ClaimCursor{})
	if err != nil || len(empty.Items) != 0 {
		t.Fatalf("empty identity matched: %+v %v", empty, err)
	}
	if _, err := f.Ledger.ClaimsByOwner(ctx, artifact.ClaimOwnerQuery{Kind: "", Authority: "s"}, artifact.ClaimCursor{}); !isCode(err, artifact.ErrInvalid) {
		t.Fatalf("empty kind = %v, want invalid", err)
	}
	active, err := artifact.ActiveClaims(ctx, f.Ledger, artifact.ClaimOwnerScope{Kind: "k", Authority: "s"})
	if err != nil || len(active) != 7 {
		t.Fatalf("active = %d %v", len(active), err)
	}
}

func put(t *testing.T, store artifact.Store, body string, d artifact.Durability) artifact.Ref {
	t.Helper()
	ref, err := store.Put(context.Background(), artifact.PutRequest{MediaType: "text/plain", Reader: strings.NewReader(body), Durability: d})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	return ref
}

func readAll(t *testing.T, r artifact.Resolver, ref artifact.Ref) string {
	t.Helper()
	rc, info, err := r.Open(context.Background(), ref)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if info.SizeBytes == nil || *info.SizeBytes != uint64(len(data)) {
		t.Fatalf("info size %v for %d bytes", info.SizeBytes, len(data))
	}
	return string(data)
}

// ART-BND-2, ART-CAP-1, ART-CAP-2: cas keys, idempotent Put, verified reads
// and the error classification.
func testContent(t *testing.T, f Fixture) {
	ctx := context.Background()
	store := f.NewContent(t, "a")
	ref := put(t, store, "hello", artifact.EventBound)
	if ref.Scheme != artifact.SchemeCAS || ref.Authority != "a" || ref.Integrity == nil || ref.SizeBytes == nil || *ref.SizeBytes != 5 {
		t.Fatalf("put ref = %+v", ref)
	}
	if string(ref.Key) != ref.Integrity.Algorithm+":"+ref.Integrity.Value {
		t.Fatalf("cas key %s must equal the integrity", ref.Key)
	}
	if ref.ExpiresAtUnixMilli != nil {
		t.Fatal("event_bound ref must not expire")
	}
	again := put(t, store, "hello", artifact.EventBound)
	if again.Key != ref.Key {
		t.Fatal("repeated put of the same bytes must be idempotent")
	}
	if _, err := store.Put(ctx, artifact.PutRequest{MediaType: "image/png", Reader: strings.NewReader("hello"), Durability: artifact.EventBound}); !isCode(err, artifact.ErrConflict) {
		t.Fatalf("same bytes under another media type = %v, want conflict", err)
	}
	if got := readAll(t, store, ref); got != "hello" {
		t.Fatalf("open = %q", got)
	}
	if info, err := store.Stat(ctx, ref); err != nil || info.MediaType != "text/plain" || *info.Integrity != *ref.Integrity {
		t.Fatalf("stat = %+v %v", info, err)
	}
	// Classification (ART-CAP-1).
	missing := ref
	missing.Key = artifact.Key("sha256:" + strings.Repeat("0", 64))
	missing.Integrity = &artifact.Integrity{Algorithm: "sha256", Value: strings.Repeat("0", 64)}
	foreign := ref
	foreign.Authority = "b"
	corrupt := ref
	corrupt.Integrity = &artifact.Integrity{Algorithm: "sha256", Value: strings.Repeat("1", 64)}
	wrongSize := ref
	six := uint64(6)
	wrongSize.SizeBytes = &six
	wrongMedia := ref
	wrongMedia.MediaType = "image/png"
	otherScheme := ref
	otherScheme.Scheme = "spill"
	cases := []struct {
		name string
		ref  artifact.Ref
		code artifact.ErrorCode
	}{
		{"missing", missing, artifact.ErrNotFound},
		{"foreign authority", foreign, artifact.ErrUnauthorized},
		{"integrity mismatch", corrupt, artifact.ErrCorrupt},
		{"size mismatch", wrongSize, artifact.ErrCorrupt},
		{"media type mismatch", wrongMedia, artifact.ErrCorrupt},
		{"other scheme", otherScheme, artifact.ErrUnsupported},
	}
	for _, tc := range cases {
		if _, err := store.Stat(ctx, tc.ref); !isCode(err, tc.code) {
			t.Fatalf("stat %s = %v, want %s", tc.name, err, tc.code)
		}
		if _, _, err := store.Open(ctx, tc.ref); !isCode(err, tc.code) {
			t.Fatalf("open %s = %v, want %s", tc.name, err, tc.code)
		}
	}
	if _, err := store.Put(ctx, artifact.PutRequest{Reader: strings.NewReader("x"), Durability: "forever"}); !isCode(err, artifact.ErrInvalid) {
		t.Fatal("unknown durability must be invalid")
	}
	if f.Advance == nil {
		return
	}
	eph := put(t, store, "temp", artifact.Ephemeral)
	if eph.ExpiresAtUnixMilli == nil {
		t.Fatal("ephemeral ref must carry its expiry when the store records one")
	}
	if got := readAll(t, store, eph); got != "temp" {
		t.Fatalf("ephemeral open = %q", got)
	}
	f.Advance(2 * time.Hour)
	if _, err := store.Stat(ctx, eph); !isCode(err, artifact.ErrExpired) {
		t.Fatalf("expired ref = %v, want expired", err)
	}
}

// ART-REF-2, ART-BND-2: promotion never lowers durability, clears expiry,
// yields a new Ref and a new Binding without touching the old one.
func testPromote(t *testing.T, f Fixture) {
	ctx := context.Background()
	store := f.NewContent(t, "a")
	eph := put(t, store, "keep me", artifact.Ephemeral)
	old := mustBinding(t, ctx, f.Bindings, "tmp", eph)
	promoted, err := store.Promote(ctx, eph, artifact.PromoteRequest{TargetScheme: artifact.SchemeCAS, TargetAuthority: "a", Durability: artifact.EventBound})
	if err != nil {
		t.Fatal(err)
	}
	if promoted.Durability != artifact.EventBound || promoted.ExpiresAtUnixMilli != nil || *promoted.Integrity != *eph.Integrity {
		t.Fatalf("promoted = %+v", promoted)
	}
	if got := readAll(t, store, promoted); got != "keep me" {
		t.Fatalf("promoted open = %q", got)
	}
	durable := mustBinding(t, ctx, f.Bindings, "durable", promoted)
	if kept, _ := f.Bindings.ResolveBinding(ctx, "tmp"); kept.Digest != old.Digest {
		t.Fatal("promotion must not rewrite the old binding")
	}
	if _, err := (artifact.SetBuilder{Resolver: f.Bindings}).Build(ctx, []artifact.BindingID{durable.ID}); err != nil {
		t.Fatalf("promoted binding must be claimable: %v", err)
	}
	if _, err := store.Promote(ctx, promoted, artifact.PromoteRequest{TargetScheme: artifact.SchemeCAS, TargetAuthority: "a", Durability: artifact.Ephemeral}); !isCode(err, artifact.ErrInvalid) {
		t.Fatalf("demotion = %v, want invalid", err)
	}
	if same, err := store.Promote(ctx, promoted, artifact.PromoteRequest{TargetScheme: artifact.SchemeCAS, TargetAuthority: "a", Durability: artifact.EventBound}); err != nil || same.Durability != artifact.EventBound {
		t.Fatalf("same-level promotion = %+v %v", same, err)
	}
	if _, err := store.Promote(ctx, promoted, artifact.PromoteRequest{TargetScheme: artifact.SchemeCAS, TargetAuthority: "b", Durability: artifact.Pinned}); !isCode(err, artifact.ErrUnauthorized) {
		t.Fatalf("foreign target authority = %v, want unauthorized", err)
	}
	// Cross-store promotion copies the bytes and keeps the content identity.
	target := f.NewContent(t, "b")
	moved, err := artifact.CopyPromoter{Source: store, Target: target}.Promote(ctx, promoted, artifact.PromoteRequest{TargetScheme: artifact.SchemeCAS, TargetAuthority: "b", Durability: artifact.Pinned})
	if err != nil {
		t.Fatal(err)
	}
	if moved.Authority != "b" || moved.Durability != artifact.Pinned || *moved.Integrity != *promoted.Integrity {
		t.Fatalf("copied = %+v", moved)
	}
	if got := readAll(t, target, moved); got != "keep me" {
		t.Fatalf("copied open = %q", got)
	}
	if _, err := (artifact.CopyPromoter{Source: store, Target: target}).Promote(ctx, moved, artifact.PromoteRequest{TargetScheme: artifact.SchemeCAS, TargetAuthority: "b", Durability: artifact.Ephemeral}); !isCode(err, artifact.ErrInvalid) {
		t.Fatalf("copy demotion = %v, want invalid", err)
	}
}

// ART-PRO-1: immutable registry, unique scheme and (scheme, authority),
// verification outcomes.
func testRegistry(t *testing.T) {
	cas := artifact.CASScheme()
	spill := artifact.SchemeDefinition{Scheme: "spill", SupportedDurabilities: []artifact.Durability{artifact.Ephemeral}}
	rejects := []struct {
		name     string
		schemes  []artifact.SchemeDefinition
		bindings []artifact.ProviderBinding
		code     artifact.ErrorCode
	}{
		{"duplicate scheme", []artifact.SchemeDefinition{cas, cas}, nil, artifact.ErrConflict},
		{"empty scheme", []artifact.SchemeDefinition{{SupportedDurabilities: []artifact.Durability{artifact.Pinned}}}, nil, artifact.ErrInvalid},
		{"no durability", []artifact.SchemeDefinition{{Scheme: "x"}}, nil, artifact.ErrInvalid},
		{"binding for unregistered scheme", []artifact.SchemeDefinition{cas}, []artifact.ProviderBinding{{Scheme: "spill", Authority: "a", InstanceID: "i"}}, artifact.ErrInvalid},
		{"authority bound twice", []artifact.SchemeDefinition{cas}, []artifact.ProviderBinding{{Scheme: "cas", Authority: "a", InstanceID: "i1"}, {Scheme: "cas", Authority: "a", InstanceID: "i2"}}, artifact.ErrConflict},
	}
	for _, tc := range rejects {
		if _, err := artifact.BuildRegistry(tc.schemes, tc.bindings); !isCode(err, tc.code) {
			t.Fatalf("%s: err = %v, want %s", tc.name, err, tc.code)
		}
	}
	reg, err := artifact.BuildRegistry([]artifact.SchemeDefinition{cas, spill}, []artifact.ProviderBinding{{Scheme: "cas", Authority: "a", InstanceID: "i1"}, {Scheme: "cas", Authority: "b", InstanceID: "i2"}})
	if err != nil {
		t.Fatal(err)
	}
	if p, ok := reg.Provider("cas", "b"); !ok || p.InstanceID != "i2" {
		t.Fatalf("provider isolation: %+v %v", p, ok)
	}
	good := artifact.Ref{Scheme: "cas", Authority: "a", Key: "sha256:ab", MediaType: "text/plain", Integrity: &artifact.Integrity{Algorithm: "sha256", Value: "ab"}, Durability: artifact.Pinned}
	badKey := good
	badKey.Key = "sha256:zz"
	unbound := good
	unbound.Authority = "c"
	unknown := good
	unknown.Scheme = "ext:mod/thing"
	spillPinned := artifact.Ref{Scheme: "spill", Authority: "a", Key: "k", Durability: artifact.Pinned}
	verify := []struct {
		name string
		ref  artifact.Ref
		code artifact.ErrorCode
	}{
		{"ok", good, ""},
		{"cas key not the integrity", badKey, artifact.ErrInvalid},
		{"unbound authority", unbound, artifact.ErrUnavailable},
		{"unknown scheme is conservative", unknown, artifact.ErrUnsupported},
		{"durability not supported by scheme", spillPinned, artifact.ErrInvalid},
	}
	for _, tc := range verify {
		err := reg.Verify(tc.ref)
		if tc.code == "" && err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if tc.code != "" && !isCode(err, tc.code) {
			t.Fatalf("%s: err = %v, want %s", tc.name, err, tc.code)
		}
	}
}
