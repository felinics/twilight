package filestore

import (
	"container/list"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session"
)

// Cache only parsed immutable records. Every hit stats the path, including its
// native change time: replacing or rewriting a file, even with its old mtime
// and size, must invalidate it. Ownership records and commit facts never use
// this cache. Platforms without a change-time check simply read every time.
// Both entry count and source bytes are bounded per cache and Store.
const fileCacheEntries = 256
const fileCacheBytes int64 = 16 << 20

type cachedFile[T any] struct {
	path  string
	info  os.FileInfo
	value T
}

type fileCache[T any] struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	lru     list.List
	bytes   int64
}

func sameFileVersion(a, b os.FileInfo) bool {
	return a != nil && b != nil && a.Mode().IsRegular() && b.Mode().IsRegular() &&
		a.Mode() == b.Mode() && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime()) &&
		os.SameFile(a, b) && sameChangeTime(a, b)
}

func (c *fileCache[T]) discard(e *list.Element) {
	entry := e.Value.(cachedFile[T]) //nolint:errcheck // this private list only contains cachedFile[T] values
	delete(c.entries, entry.path)
	c.bytes -= entry.info.Size()
	c.lru.Remove(e)
}

func (c *fileCache[T]) load(path string, decode func([]byte) (T, error)) (T, error) {
	if !hasFileChangeTime {
		raw, err := os.ReadFile(path)
		if err != nil {
			var zero T
			return zero, err
		}
		return decode(raw)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var zero T
	before, err := os.Stat(path)
	if e := c.entries[path]; e != nil {
		entry := e.Value.(cachedFile[T]) //nolint:errcheck // this private list only contains cachedFile[T] values
		if err == nil && sameFileVersion(entry.info, before) {
			c.lru.MoveToFront(e)
			return entry.value, nil
		}
		c.discard(e)
	}
	if err != nil {
		return zero, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return zero, err
	}
	value, err := decode(raw)
	if err != nil {
		return zero, err
	}
	// Never attach bytes to metadata from a different file version. A racing
	// replacement can still be observed on this call, just as with ReadFile;
	// it cannot poison subsequent reads with a mismatched cached value.
	after, err := os.Stat(path)
	if err == nil && sameFileVersion(before, after) && after.Size() <= fileCacheBytes {
		for c.lru.Len() > 0 && (c.lru.Len() >= fileCacheEntries || c.bytes+after.Size() > fileCacheBytes) {
			c.discard(c.lru.Back())
		}
		if c.entries == nil {
			c.entries = make(map[string]*list.Element)
		}
		c.entries[path] = c.lru.PushFront(cachedFile[T]{path, after, value})
		c.bytes += after.Size()
	}
	return value, nil
}

func decodeHeader(raw []byte) (session.SegmentHeader, error) {
	var h session.SegmentHeader
	if err := json.Unmarshal(raw, &h); err != nil {
		return h, fmt.Errorf("%s: %w", headerFile, err)
	}
	return h, nil
}

// jsonstable.Value owns its immutable bytes; callers obtain copies via Bytes.
type cachedProjection struct {
	state   jsonstable.Value
	through ledger.Head
}

func decodeProjection(raw []byte) (cachedProjection, error) {
	var rec projectionRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return cachedProjection{}, err
	}
	state, err := jsonstable.Parse(rec.State)
	return cachedProjection{state, rec.Through}, err
}
