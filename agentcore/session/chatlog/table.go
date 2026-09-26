package chatlog

import "encoding/json"

// Table is the persistent map of the Surface projection. A state derived by
// Apply shares the previous state's storage and never writes it, so every
// state a reader holds keeps the contents it had (EXT-PRJ-1); Set returns a new
// Table and leaves the receiver untouched.
//
// Storage is two layers that are both immutable once built: base carries the
// bulk and is shared by every state derived from the one that merged it;
// overlay carries the writes since that merge and is copied by each Set. A Set
// therefore costs the overlay's size, and the overlay is folded into a fresh
// base once it exceeds a limit that grows with the square root of the base, so
// the amortised cost of a write is O(sqrt(n)) instead of the O(n) a plain map
// copy pays. Reads look in the overlay first.
//
// JSON: a Table encodes and decodes as the plain object a map would, so the
// projection's cached state keeps its shape.
type Table[K comparable, V any] struct {
	base    map[K]V
	overlay map[K]V
	n       int
}

// Get returns the value for k, overlay first.
func (t Table[K, V]) Get(k K) (V, bool) {
	if v, ok := t.overlay[k]; ok {
		return v, true
	}
	v, ok := t.base[k]
	return v, ok
}

// Has reports whether k is present.
func (t Table[K, V]) Has(k K) bool {
	_, ok := t.Get(k)
	return ok
}

// Len is the number of distinct keys.
func (t Table[K, V]) Len() int { return t.n }

// IsZero reports an empty Table; encoding/json's omitzero consults it.
func (t Table[K, V]) IsZero() bool { return t.n == 0 }

// Set returns a Table with k bound to v. The receiver is unchanged.
func (t Table[K, V]) Set(k K, v V) Table[K, V] {
	next := Table[K, V]{base: t.base, n: t.n}
	if !t.Has(k) {
		next.n++
	}
	if len(t.overlay)+1 > mergeLimit(len(t.base)) {
		merged := make(map[K]V, len(t.base)+len(t.overlay)+1)
		for bk, bv := range t.base {
			merged[bk] = bv
		}
		for ok, ov := range t.overlay {
			merged[ok] = ov
		}
		merged[k] = v
		next.base = merged
		return next
	}
	over := make(map[K]V, len(t.overlay)+1)
	for ok, ov := range t.overlay {
		over[ok] = ov
	}
	over[k] = v
	next.overlay = over
	return next
}

// Delete returns a Table without k; the receiver is unchanged. It rebuilds the
// base, so it costs O(n): callers delete from tables that stay small (the
// Runs table, bounded by the Runs active now), not from the entry tables.
func (t Table[K, V]) Delete(k K) Table[K, V] {
	if !t.Has(k) {
		return t
	}
	merged := make(map[K]V, len(t.base)+len(t.overlay))
	for bk, bv := range t.base {
		if bk != k {
			merged[bk] = bv
		}
	}
	for ok, ov := range t.overlay {
		if ok != k {
			merged[ok] = ov
		}
	}
	return Table[K, V]{base: merged, n: t.n - 1}
}

// mergeLimit is the overlay size that triggers a merge: at least 16 writes,
// or the square root of the base when that is larger.
func mergeLimit(base int) int {
	limit := 16
	if r := isqrt(base); r > limit {
		limit = r
	}
	return limit
}

func isqrt(n int) int {
	r := 0
	for (r+1)*(r+1) <= n {
		r++
	}
	return r
}

// Range calls fn for every entry until it returns false. Order is unspecified,
// as with a map.
func (t Table[K, V]) Range(fn func(K, V) bool) {
	for k, v := range t.overlay {
		if !fn(k, v) {
			return
		}
	}
	for k, v := range t.base {
		if _, shadowed := t.overlay[k]; shadowed {
			continue
		}
		if !fn(k, v) {
			return
		}
	}
}

// Map returns a fresh map with every entry.
func (t Table[K, V]) Map() map[K]V {
	out := make(map[K]V, t.n)
	t.Range(func(k K, v V) bool {
		out[k] = v
		return true
	})
	return out
}

// TableOf builds a Table holding m; m becomes the base and must not be written
// afterwards.
func TableOf[K comparable, V any](m map[K]V) Table[K, V] {
	return Table[K, V]{base: m, n: len(m)}
}

func (t Table[K, V]) MarshalJSON() ([]byte, error) { return json.Marshal(t.Map()) }

func (t *Table[K, V]) UnmarshalJSON(raw []byte) error {
	var m map[K]V
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	*t = TableOf(m)
	return nil
}
