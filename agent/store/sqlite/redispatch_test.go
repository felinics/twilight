package sqlite_test

import (
	"testing"

	"github.com/felinics/twilight/agent/store/sqlite/sqlitetest"
	"github.com/felinics/twilight/agentcore/run/redispatch"
	"github.com/felinics/twilight/agentcore/run/redispatch/redispatchtest"
)

func TestRedispatchStoreConformance(t *testing.T) {
	redispatchtest.Run(t, func(t *testing.T) redispatch.Store { return sqlitetest.Open(t).Redispatches() })
}
