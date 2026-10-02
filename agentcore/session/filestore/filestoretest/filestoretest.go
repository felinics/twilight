// Package filestoretest opens throwaway file-backed stores for tests: the
// JSONL Session ledger and the cas content store, each under t.TempDir().
// Tests run over the durable implementations; there is no memory store to
// fall back to.
package filestoretest

import (
	"testing"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore"
)

// Store opens a fresh Session ledger under t.TempDir().
func Store(t testing.TB, opts ...session.LedgerOption) *filestore.Store {
	t.Helper()
	s, err := filestore.New(t.TempDir(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// Content opens a fresh cas content store for authority under t.TempDir().
func Content(t testing.TB, authority artifact.Authority, opts ...filestore.ContentStoreOptions) *filestore.ContentStore {
	t.Helper()
	var o filestore.ContentStoreOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	s, err := filestore.NewContentStore(t.TempDir(), authority, o)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
