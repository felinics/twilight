//go:build linux

package filestore

import (
	"os"
	"syscall"
)

const hasFileChangeTime = true

func sameChangeTime(a, b os.FileInfo) bool {
	x, ok := a.Sys().(*syscall.Stat_t)
	y, other := b.Sys().(*syscall.Stat_t)
	return ok && other && x.Ctim == y.Ctim
}
