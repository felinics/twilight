package sqlite_test

import (
	"testing"

	"github.com/felinics/twilight/agent/store/sqlite/sqlitetest"
	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agent/workspace/workspacetest"
)

func TestWorkspaceStoreConformance(t *testing.T) {
	workspacetest.Run(t, func(t *testing.T) workspace.Store { return sqlitetest.Open(t).Workspaces() })
}
