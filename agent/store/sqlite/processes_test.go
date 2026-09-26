package sqlite_test

import (
	"testing"

	"github.com/felinics/twilight/agent/store/sqlite/sqlitetest"
	"github.com/felinics/twilight/agentcore/process"
	"github.com/felinics/twilight/agentcore/process/processtest"
)

func TestProcessStoreConformance(t *testing.T) {
	processtest.Run(t, func(t *testing.T) process.Store { return sqlitetest.Open(t).Processes() })
}
