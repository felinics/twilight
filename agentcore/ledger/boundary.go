package ledger

// CommitAt reports whether c is the commit through records: the commit at
// through.Next-1. History is append-only, so the position names the commit;
// it is the one head-alignment predicate of EXT-PRJ-3, shared by the Writer
// and the store reader so a cache entry is judged the same way on both
// paths. The commit is the atomic unit of the ledger: there is no finer
// boundary to check.
func CommitAt(c Commit, through Head) bool {
	return through.Next > 0 && c.Seq == through.Next-1
}

// OwnBoundary reports whether through is a commit boundary of the tip
// segment itself: the commit before through.Next is one the tip wrote, not
// one it inherits; seed is the head of the tip before its first own commit. A projection state was folded under the inheritance
// policy of the tip that was current when it was recorded (EXT-PRJ-8); a
// fork makes every earlier commit inherited, so an entry ending on an
// inherited boundary is not started from (EXT-PRJ-3) and the fold restarts
// from the initial state until the tip holds a commit of its own. It is
// the second head-alignment predicate of EXT-PRJ-3, shared by the Writer
// and the store reader like CommitAt.
func OwnBoundary(seed, through Head) bool {
	return through.Next > seed.Next
}
