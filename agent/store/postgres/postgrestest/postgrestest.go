// Package postgrestest opens throwaway schemas in a Postgres a test run is
// given. The database is named by the -postgres.dsn test flag; without it
// every test that needs one is skipped, so the suite runs everywhere and
// exercises Postgres where a database is provided (a service container in
// CI, a local docker run).
package postgrestest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/felinics/twilight/agent/store/postgres"
)

var dsn = flag.String("postgres.dsn", "", "Postgres DSN the store tests run against; empty skips them")

// DSN is the configured database, empty when none.
func DSN() string { return *dsn }

// Open creates a fresh schema in the configured database and returns a DB
// bound to it; the schema is dropped when the test ends. It skips the test
// when no database is configured.
func Open(t testing.TB, options ...postgres.Options) *postgres.DB {
	t.Helper()
	return OpenDSN(t, Schema(t), options...)
}

// OpenDSN opens a DB over a schema DSN from Schema: a second handle over the
// same tables stands for a second process.
func OpenDSN(t testing.TB, schemaDSN string, options ...postgres.Options) *postgres.DB {
	t.Helper()
	db, err := postgres.Open(context.Background(), schemaDSN, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// Schema creates a fresh schema and returns a DSN whose search_path is that
// schema.
func Schema(t testing.TB) string {
	t.Helper()
	if *dsn == "" {
		t.Skip("no -postgres.dsn given; Postgres store tests skipped")
	}
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	schema := "tw_test_" + hex.EncodeToString(b[:])
	ctx := context.Background()
	boot, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := boot.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = boot.Close(ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = boot.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		_ = boot.Close(ctx)
	})
	u, err := url.Parse(*dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}
