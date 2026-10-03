package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/config"
)

type sample struct {
	Listen string          `json:"listen"`
	Lease  config.Duration `json:"lease"`
	Who    config.Identity `json:"who"`
}

func TestLoadIsStrict(t *testing.T) {
	doc, err := config.Load[sample](strings.NewReader(`{"listen":":8080","lease":"30s","who":{"name":"a"}}`))
	if err != nil || doc.Listen != ":8080" || doc.Lease.Std() != 30*time.Second {
		t.Fatalf("load = %+v %v", doc, err)
	}
	for name, raw := range map[string]string{
		"unknown field":    `{"listen":":8080","port":1}`,
		"trailing content": `{"listen":":8080"} {}`,
		"bad duration":     `{"lease":"soon"}`,
		"negative":         `{"lease":"-1s"}`,
		"numeric duration": `{"lease":30}`,
	} {
		if _, err := config.Load[sample](strings.NewReader(raw)); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	if name, err := doc.Who.Resolve(); err != nil || name != "a" {
		t.Fatalf("identity = %q %v", name, err)
	}
	path := filepath.Join(t.TempDir(), "podname")
	if err := os.WriteFile(path, []byte("owner-7\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if name, err := (config.Identity{File: path}).Resolve(); err != nil || name != "owner-7" {
		t.Fatalf("identity from file = %q %v", name, err)
	}
	for _, bad := range []config.Identity{{}, {Name: "a", File: path}, {File: filepath.Join(t.TempDir(), "absent")}} {
		if _, err := bad.Resolve(); err == nil {
			t.Fatalf("identity %+v accepted", bad)
		}
	}
	cfgPath := filepath.Join(t.TempDir(), "c.json")
	if err := os.WriteFile(cfgPath, []byte(`{"listen":":1"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if doc, err := config.LoadFile[sample](cfgPath); err != nil || doc.Listen != ":1" {
		t.Fatalf("load file = %+v %v", doc, err)
	}
}

func TestStoreNamesExactlyOneBackend(t *testing.T) {
	dsnFile := filepath.Join(t.TempDir(), "dsn")
	if err := os.WriteFile(dsnFile, []byte("postgres://u:p@h/db\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		store config.Store
		ok    bool
		dsn   string
	}{
		{name: "sqlite", store: config.Store{SQLite: "a.db"}, ok: true},
		{name: "postgres dsn", store: config.Store{Postgres: &config.Postgres{DSN: "postgres://x"}}, ok: true, dsn: "postgres://x"},
		{name: "postgres file", store: config.Store{Postgres: &config.Postgres{DSNFile: dsnFile}}, ok: true, dsn: "postgres://u:p@h/db"},
		{name: "neither", store: config.Store{}},
		{name: "both backends", store: config.Store{SQLite: "a.db", Postgres: &config.Postgres{DSN: "x"}}},
		{name: "postgres empty", store: config.Store{Postgres: &config.Postgres{}}},
		{name: "postgres both", store: config.Store{Postgres: &config.Postgres{DSN: "x", DSNFile: dsnFile}}},
		{name: "postgres missing file", store: config.Store{Postgres: &config.Postgres{DSNFile: filepath.Join(t.TempDir(), "none")}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.store.Validate()
			if (err == nil) != tc.ok {
				t.Fatalf("validate = %v, want ok=%v", err, tc.ok)
			}
			if tc.dsn != "" {
				if got, err := tc.store.Postgres.Resolve(); err != nil || got != tc.dsn {
					t.Fatalf("resolve = %q %v", got, err)
				}
			}
		})
	}
}
