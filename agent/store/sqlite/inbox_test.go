package sqlite_test

import (
	"testing"

	"github.com/felinics/twilight/agent/store/sqlite/sqlitetest"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/inbox/inboxtest"
)

func TestInboxStoreConformance(t *testing.T) {
	inboxtest.Run(t, func(t *testing.T) inbox.Store { return sqlitetest.Open(t).Inbox() })
}
