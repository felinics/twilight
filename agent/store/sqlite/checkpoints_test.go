package sqlite_test

import (
	"testing"

	"github.com/felinics/twilight/agent/checkpoint"
	"github.com/felinics/twilight/agent/checkpoint/checkpointtest"
	"github.com/felinics/twilight/agent/store/sqlite/sqlitetest"
)

func TestCheckpointStoreConformance(t *testing.T) {
	checkpointtest.Run(t, func(t *testing.T) checkpoint.Store { return sqlitetest.Open(t).Checkpoints() })
}
