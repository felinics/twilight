//go:build !linux && !darwin

package filestore

import "os"

// A size/mtime match alone cannot detect a same-size rewrite with restored
// mtime. Keep the uncached read semantics on platforms without a native check.
const hasFileChangeTime = false

func sameChangeTime(_, _ os.FileInfo) bool { return false }
