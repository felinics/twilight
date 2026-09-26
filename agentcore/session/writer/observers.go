package writer

import (
	"context"
	"sync"

	"github.com/felinics/twilight/agentcore/session"
)

// CommitObserver sees every commit a Writer applies, in commit order, after
// it is durable (EXT-WRT-7). It is the one source every observation of a
// Session derives from: Loop events, turn lifecycle and chatlog entries are
// all events of applied commits. Observers run outside the Writer's critical
// section and are best effort: a panic is contained and never reaches the
// committer.
type CommitObserver interface {
	Committed(ctx context.Context, sid session.SessionID, commit session.Commit)
}

// observers is the notification stage of the commit pipeline. notifyMu keeps
// notifications in commit order without holding the Writer's lock: hold is
// taken before that lock is released and release after the notification.
type observers struct {
	list     []CommitObserver
	notifyMu sync.Mutex
}

func (o *observers) any() bool { return len(o.list) > 0 }
func (o *observers) hold()     { o.notifyMu.Lock() }
func (o *observers) release()  { o.notifyMu.Unlock() }

// notify hands an applied commit to every observer. A panicking observer is
// contained: observation is derived work and never fails a Commit.
func (o *observers) notify(ctx context.Context, sid session.SessionID, commit session.Commit) {
	for _, ob := range o.list {
		func() {
			defer func() { _ = recover() }()
			ob.Committed(ctx, sid, commit)
		}()
	}
}
