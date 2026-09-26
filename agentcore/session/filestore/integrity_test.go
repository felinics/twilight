package filestore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/felinics/twilight/agentcore/session"
)

// A segment directory whose header names another segment, or a root
// whose record cannot be read, is corrupt to every entry point: the Store
// must not serve it under the requested identity nor treat it as unowned.
func TestSessionDirectoryIntegrity(t *testing.T) {
	ctx := context.Background()
	create := func(t *testing.T, s *Store, sid session.SessionID) session.SegmentHeader {
		t.Helper()
		h, err := s.Create(ctx, session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: 1})
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	cases := []struct {
		name          string
		damage        func(t *testing.T, s *Store, a session.SegmentHeader)
		headerCorrupt bool
	}{
		{"header of another segment", func(t *testing.T, s *Store, a session.SegmentHeader) {
			b := create(t, s, "b")
			raw, err := os.ReadFile(filepath.Join(s.segmentDir(b.ID), headerFile))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(s.segmentDir(a.ID), headerFile), raw, 0o644); err != nil {
				t.Fatal(err)
			}
		}, true},
		{"unreadable root record", func(t *testing.T, s *Store, _ session.SegmentHeader) {
			if err := os.WriteFile(s.rootPath("a"), []byte("{not json"), 0o644); err != nil {
				t.Fatal(err)
			}
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			a := create(t, s, "a")
			tc.damage(t, s, a)
			for _, takeover := range []bool{false, true} {
				if _, err := s.Open(ctx, "a", session.OpenOptions{Takeover: takeover}); !session.IsCode(err, session.ErrCorrupt) {
					t.Fatalf("Open(takeover=%v) = %v, want corrupt", takeover, err)
				}
			}
			_, err = s.Header(ctx, "a")
			if got := session.IsCode(err, session.ErrCorrupt); got != tc.headerCorrupt {
				t.Fatalf("Header = %v, corrupt=%v want %v", err, got, tc.headerCorrupt)
			}
		})
	}
}
