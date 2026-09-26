// Package sqlitetest opens throwaway SQLite databases for tests.
package sqlitetest

import (
	"path/filepath"
	"testing"

	"github.com/felinics/twilight/agent/store/sqlite"
	"github.com/felinics/twilight/agentcore/artifact"
)

// Open opens a fresh database under t.TempDir() and closes it when the test
// ends. The reference agent's tests run over this adapter; the kernel's run
// over the reference stores of its xxxtest packages.
func Open(t testing.TB, options ...sqlite.Options) *sqlite.DB {
	t.Helper()
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "twilight.db"), options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// Artifacts opens a fresh database and returns its binding store and the
// retention ledger that verifies sets against it.
func Artifacts(t testing.TB) (artifact.BindingStore, artifact.RetentionLedger) {
	t.Helper()
	db := Open(t)
	bindings := db.Bindings()
	return bindings, db.Ledger(artifact.SetBuilder{Resolver: bindings})
}
