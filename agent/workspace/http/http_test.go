package http_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agent/workspace"
	wshttp "github.com/felinics/twilight/agent/workspace/http"
)

type fakeSnapshotter struct{ err map[workspace.ID]error }

func (f fakeSnapshotter) Snapshot(_ context.Context, id workspace.ID) (workspace.Snapshot, error) {
	if err, ok := f.err[id]; ok {
		return workspace.Snapshot{}, err
	}
	return workspace.Snapshot{Ref: "snap-1", Workspace: id, Backend: "local", StateRef: "state-1"}, nil
}

// The client is a Snapshotter over the server: a snapshot comes back whole,
// and the sentinel errors survive the wire.
func TestSnapshotOverHTTP(t *testing.T) {
	fake := fakeSnapshotter{err: map[workspace.ID]error{
		"ws-missing": workspace.ErrNotFound,
		"ws-empty":   workspace.ErrNothingToSnapshot,
		"ws-nosnap":  environment.ErrUnsupported,
		"ws-broken":  errors.New("disk on fire"),
	}}
	server := httptest.NewServer((&wshttp.Server{Snapshots: fake}).Handler())
	defer server.Close()
	client := &wshttp.Client{BaseURL: server.URL}
	ctx := context.Background()
	snap, err := client.Snapshot(ctx, "ws-1")
	if err != nil || snap.Ref != "snap-1" || snap.Workspace != "ws-1" || snap.StateRef != "state-1" {
		t.Fatalf("snapshot = %+v %v", snap, err)
	}
	cases := []struct {
		id   workspace.ID
		want error
	}{
		{"ws-missing", workspace.ErrNotFound},
		{"ws-empty", workspace.ErrNothingToSnapshot},
		{"ws-nosnap", environment.ErrUnsupported},
	}
	for _, tc := range cases {
		if _, err := client.Snapshot(ctx, tc.id); !errors.Is(err, tc.want) {
			t.Fatalf("snapshot %s = %v, want %v", tc.id, err, tc.want)
		}
	}
	if _, err := client.Snapshot(ctx, "ws-broken"); err == nil || errors.Is(err, workspace.ErrNotFound) {
		t.Fatalf("internal failure = %v, want a plain error", err)
	}
	if _, err := client.Snapshot(ctx, ""); err == nil {
		t.Fatal("empty id accepted")
	}
}
