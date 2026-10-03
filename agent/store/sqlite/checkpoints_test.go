package sqlite_test

import (
	"testing"

	"github.com/felinics/twilight/agent/store/sqlite/sqlitetest"
	"github.com/felinics/twilight/agentcore/checkpoint"
	"github.com/felinics/twilight/agentcore/checkpoint/checkpointtest"
)

func TestCheckpointStoreConformance(t *testing.T) {
	checkpointtest.Run(t, func(t *testing.T) checkpoint.Store { return sqlitetest.Open(t).Checkpoints() })
}
