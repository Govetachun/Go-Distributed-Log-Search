package unified_index

import (
	"path/filepath"
	"strings"
	"sync"
)

// Don't store slices that are too large in the cache, except the term and the store index.
const CACHE_SLICE_SIZE_LIMIT = 10_000_000

// RangeCache represents a cache for byte ranges
// Equivalent to RangeCache type alias in Rust
type RangeCache map[uint64]RangeCacheEntry

// RangeCacheEntry represents a cache entry with end position and buffer
// Equivalent to (u64, Arc<[u8]>) in Rust
type RangeCacheEntry struct {
	End   uint64 `json:"end" yaml:"end"`
	Bytes []byte `json:"bytes" yaml:"bytes"`
}

// FileCache represents a cache for files
// Equivalent to FileCache type alias in Rust
type FileCache map[string]RangeCache

// RecordingDirectory wraps a directory to record file access
// Equivalent to RecordingDirectory<D: Directory> in Rust
type RecordingDirectory struct {
	inner     interface{} // Directory interface equivalent
	cache     *sync.RWMutex
	cacheData FileCache
}

// NewRecordingDirectory creates a new RecordingDirectory
// Equivalent to wrap method in Rust
func NewRecordingDirectory(directory interface{}) *RecordingDirectory {
	return &RecordingDirectory{
		inner: directory,
		cache: &sync.RWMutex{},
	}
}

// record records file access in the cache
// Equivalent to record method in Rust
func (rd *RecordingDirectory) record(path string, bytes []byte, offset uint64) error {
	pathStr := filepath.Base(path)
	if !strings.HasSuffix(pathStr, "store") &&
		!strings.HasSuffix(pathStr, "term") &&
		len(bytes) > CACHE_SLICE_SIZE_LIMIT {
		return nil
	}

	rd.cache.Lock()
	defer rd.cache.Unlock()

	// Initialize cache for this path if it doesn't exist
	if rd.cacheData[path] == nil {
		rd.cacheData[path] = make(RangeCache)
	}

	rd.cacheData[path][offset] = RangeCacheEntry{
		End:   offset + uint64(len(bytes)),
		Bytes: bytes,
	}

	return nil
}

// Clone creates a copy of the RecordingDirectory
// Equivalent to Clone implementation in Rust
func (rd *RecordingDirectory) Clone() *RecordingDirectory {
	return &RecordingDirectory{
		inner: rd.inner,
		cache: rd.cache,
	}
}

// RecordingFileHandle wraps a file handle to record access
// Equivalent to RecordingFileHandle<D: Directory> in Rust
type RecordingFileHandle struct {
	directory *RecordingDirectory
	inner     interface{} // FileHandle interface equivalent
	path      string
}

// NewRecordingFileHandle creates a new RecordingFileHandle
func NewRecordingFileHandle(directory *RecordingDirectory, inner interface{}, path string) *RecordingFileHandle {
	return &RecordingFileHandle{
		directory: directory,
		inner:     inner,
		path:      path,
	}
}

// readBytes reads bytes from the file handle and records the access
// Equivalent to read_bytes method in Rust
func (rfh *RecordingFileHandle) readBytes(start, end uint64) ([]byte, error) {
	// This is a simplified implementation
	// In a real implementation, you would call the inner file handle's read_bytes method
	// and then record the access

	// For now, return empty bytes as placeholder
	return []byte{}, nil
}

// buildFileCache builds a file cache from a directory path
// Equivalent to build_file_cache function in Rust
func BuildFileCache(path string) (FileCache, error) {
	// This is a simplified implementation
	// In a real implementation, you would:
	// 1. Open the directory
	// 2. Create a recording directory
	// 3. Open the index
	// 4. Build the cache by accessing files

	// For now, return an empty cache as placeholder
	return make(FileCache), nil
}
