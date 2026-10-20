package filestore

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
)

func headerFixture(t testing.TB) (*Store, string, session.SegmentHeader) {
	t.Helper()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h := session.SegmentHeader{ID: "segment", Parent: &session.CommitRef{Segment: "parent", Seq: 3},
		Ext: module.Extensions{{Source: "test", ID: "extension"}: module.RawValue(`{"value":1}`)}}
	path := filepath.Join(s.segmentDir(h.ID), headerFile)
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0640); err != nil {
		t.Fatal(err)
	}
	return s, path, h
}

func TestHeaderFileCacheTracksChanges(t *testing.T) {
	for _, change := range []string{"overwrite_preserving_mtime", "atomic_replace", "corrupt", "foreign_identity", "delete", "permissions"} {
		t.Run(change, func(t *testing.T) {
			s, path, h := headerFixture(t)
			ctx := context.Background()
			if _, err := s.Segment(ctx, h.ID); err != nil {
				t.Fatal(err)
			}
			old, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			wantCode := session.ErrorCode("")
			switch change {
			case "overwrite_preserving_mtime", "atomic_replace":
				// Same length and mtime: neither alone is a safe cache identity.
				raw = []byte(strings.Replace(string(raw), `"seq":3`, `"seq":4`, 1))
				target := path
				if change == "atomic_replace" {
					target = path + ".next"
				}
				if err := os.WriteFile(target, raw, 0640); err != nil {
					t.Fatal(err)
				}
				if err := os.Chtimes(target, old.ModTime(), old.ModTime()); err != nil {
					t.Fatal(err)
				}
				if target != path {
					if err := os.Rename(target, path); err != nil {
						t.Fatal(err)
					}
				}
			case "corrupt":
				if err := os.WriteFile(path, []byte(strings.Repeat("!", len(raw))), 0640); err != nil {
					t.Fatal(err)
				}
				if err := os.Chtimes(path, old.ModTime(), old.ModTime()); err != nil {
					t.Fatal(err)
				}
				wantCode = session.ErrCorrupt
			case "foreign_identity":
				raw = []byte(strings.Replace(string(raw), `"segment"`, `"foreign"`, 1))
				if err := os.WriteFile(path, raw, 0640); err != nil {
					t.Fatal(err)
				}
				if err := os.Chtimes(path, old.ModTime(), old.ModTime()); err != nil {
					t.Fatal(err)
				}
				wantCode = session.ErrCorrupt
			case "delete":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				wantCode = session.ErrNotFound
			case "permissions":
				if os.Geteuid() == 0 {
					t.Skip("root bypasses file permissions")
				}
				if err := os.Chmod(path, 0000); err != nil {
					t.Fatal(err)
				}
				defer os.Chmod(path, 0640)
				wantCode = session.ErrCorrupt
			}
			got, err := s.Segment(ctx, h.ID)
			if wantCode != "" {
				if !session.IsCode(err, wantCode) {
					t.Fatalf("read after %s: %v, want %s", change, err, wantCode)
				}
				return
			}
			if err != nil || got.Header.Parent.Seq != 4 {
				t.Fatalf("stale header: %+v, %v", got, err)
			}
		})
	}
}

func TestHeaderFileCacheReturnsDetachedState(t *testing.T) {
	s, _, h := headerFixture(t)
	ctx := context.Background()
	first, err := s.Segment(ctx, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	first.Header.Parent.Seq = 100
	key := module.ModuleKey{Source: "test", ID: "extension"}
	first.Header.Ext[key][0] = '!'
	delete(first.Header.Ext, key)
	again, err := s.Segment(ctx, h.ID)
	if err != nil || again.Header.Parent.Seq != 3 || string(again.Header.Ext[key]) != `{"value":1}` {
		t.Fatalf("caller mutated cached header: %+v, %v", again, err)
	}
}

func TestProjectionFileCacheTracksOtherStoreAndCorruption(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	a, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	const sid session.SessionID = "session"
	const id module.ProjectionID = "test/projection"
	value := func(s string) jsonstable.Value {
		v, e := jsonstable.Parse([]byte(s))
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	save := func(s *Store, raw string, seq ledger.CommitSeq) {
		t.Helper()
		if e := s.ProjectionCache().Save(ctx, sid, id, 1, value(raw), ledger.Head{Next: seq}); e != nil {
			t.Fatal(e)
		}
	}
	load := func(raw string, seq ledger.CommitSeq) {
		t.Helper()
		v, h, ok, e := a.ProjectionCache().Load(ctx, sid, id, 1)
		if e != nil || !ok || h.Next != seq || v.String() != raw {
			t.Fatalf("load = %s, %+v, %v, %v", v, h, ok, e)
		}
		bytes := v.Bytes()
		bytes[0] = '!'
	}
	save(a, `{"value":1}`, 1)
	load(`{"value":1}`, 1)
	load(`{"value":1}`, 1)
	save(b, `{"value":2}`, 2)
	load(`{"value":2}`, 2)
	path := projectionPath(a, sid, id, 1)
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Another writer need not use atomic rename; preserve mtime and length.
	raw = []byte(strings.Replace(string(raw), `"value":2`, `"value":3`, 1))
	if err := os.WriteFile(path, raw, 0640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, st.ModTime(), st.ModTime()); err != nil {
		t.Fatal(err)
	}
	load(`{"value":3}`, 2)
	for _, change := range []string{"corrupt", "delete"} {
		if change == "corrupt" {
			err = os.WriteFile(path, []byte(strings.Repeat("!", len(raw))), 0640)
		} else {
			err = os.Remove(path)
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, _, ok, e := a.ProjectionCache().Load(ctx, sid, id, 1); e != nil || ok {
			t.Fatalf("%s must be a cache miss: %v, %v", change, ok, e)
		}
	}
	save(b, `{"value":4}`, 4)
	load(`{"value":4}`, 4)
}

func TestFileCacheAvoidsRepeatedDecodingAndBoundsRetention(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "entry")
	if err := os.WriteFile(path, []byte("record"), 0640); err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !sameFileVersion(stat, stat) {
		t.Skip("platform uses uncached reads")
	}
	var c fileCache[string]
	calls := 0
	decode := func(b []byte) (string, error) { calls++; return string(b), nil }
	for range 8 {
		if _, err := c.load(path, decode); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("unchanged file decoded %d times", calls)
	}
	// Eviction must permit a new read, and neither count nor byte budget grows.
	for i := range fileCacheEntries + 1 {
		p := filepath.Join(dir, strconv.Itoa(i))
		if err := os.WriteFile(p, []byte("record"), 0640); err != nil {
			t.Fatal(err)
		}
		if _, err := c.load(p, decode); err != nil {
			t.Fatal(err)
		}
	}
	if len(c.entries) > fileCacheEntries || c.bytes > fileCacheBytes {
		t.Fatalf("unbounded cache: %d entries, %d bytes", len(c.entries), c.bytes)
	}
	before := calls
	if _, err := c.load(path, decode); err != nil {
		t.Fatal(err)
	}
	if calls != before+1 {
		t.Fatal("evicted record was retained")
	}
}

func TestFileCacheConcurrentReaders(t *testing.T) {
	s, _, h := headerFixture(t)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 16 {
				got, err := s.Segment(context.Background(), h.ID)
				if err != nil {
					t.Error(err)
					return
				}
				got.Header.Parent.Seq = 99
			}
		})
	}
	wg.Wait()
}

func BenchmarkSegmentHeader(b *testing.B) {
	s, _, h := headerFixture(b)
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		if _, err := s.Segment(ctx, h.ID); err != nil {
			b.Fatal(err)
		}
	}
}

func TestFileCacheByteBudgetAndOversizedRecords(t *testing.T) {
	if !hasFileChangeTime {
		t.Skip("platform uses uncached reads")
	}
	var c fileCache[int]
	dir := t.TempDir()
	load := func(name string, size int) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, make([]byte, size), 0640); err != nil {
			t.Fatal(err)
		}
		if _, err := c.load(path, func(raw []byte) (int, error) { return len(raw), nil }); err != nil {
			t.Fatal(err)
		}
		return path
	}
	first := load("first", int(fileCacheBytes/2)+1)
	second := load("second", int(fileCacheBytes/2)+1)
	if c.entries[first] != nil || c.entries[second] == nil || c.bytes > fileCacheBytes {
		t.Fatal("byte budget did not evict the oldest record")
	}
	large := load("oversized", int(fileCacheBytes)+1)
	if c.entries[large] != nil || c.bytes > fileCacheBytes {
		t.Fatal("oversized record retained")
	}
}

func TestFileCacheDoesNotCacheRacingReplacement(t *testing.T) {
	if !hasFileChangeTime {
		t.Skip("platform uses uncached reads")
	}
	path := filepath.Join(t.TempDir(), "entry")
	if err := os.WriteFile(path, []byte("old"), 0640); err != nil {
		t.Fatal(err)
	}
	var c fileCache[string]
	calls := 0
	decode := func(raw []byte) (string, error) {
		calls++
		if calls == 1 {
			next := path + ".next"
			if err := os.WriteFile(next, []byte("new"), 0640); err != nil {
				return "", err
			}
			if err := os.Rename(next, path); err != nil {
				return "", err
			}
		}
		return string(raw), nil
	}
	if _, err := c.load(path, decode); err != nil {
		t.Fatal(err)
	}
	if got, err := c.load(path, decode); err != nil || got != "new" || calls != 2 {
		t.Fatalf("racing replacement poisoned cache: %q, %v (%d decodes)", got, err, calls)
	}
}
