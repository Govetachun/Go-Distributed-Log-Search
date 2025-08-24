package unified_index

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/blugelabs/bluge"
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
		inner:     directory,
		cache:     &sync.RWMutex{},
		cacheData: make(FileCache),
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
	// Try to read from inner file handle
	var payload []byte
	var err error

	// If inner is a file (io.ReadSeeker), seek and read
	if file, ok := rfh.inner.(*os.File); ok {
		_, err = file.Seek(int64(start), io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("failed to seek to position %d: %w", start, err)
		}

		length := end - start
		payload = make([]byte, length)
		n, err := file.Read(payload)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read %d bytes: %w", length, err)
		}
		payload = payload[:n]
	} else if seeker, ok := rfh.inner.(io.ReadSeeker); ok {
		_, err = seeker.Seek(int64(start), io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("failed to seek to position %d: %w", start, err)
		}

		length := end - start
		payload = make([]byte, length)
		n, err := seeker.Read(payload)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read %d bytes: %w", length, err)
		}
		payload = payload[:n]
	} else {
		// Fallback: assume it's a byte slice or similar
		return nil, fmt.Errorf("unsupported inner type for readBytes")
	}

	// Record the access in cache
	err = rfh.directory.record(rfh.path, payload, start)
	if err != nil {
		return nil, fmt.Errorf("failed to record cache access: %w", err)
	}

	return payload, nil
}

// buildFileCache builds a file cache from a directory path
// Equivalent to build_file_cache function in Rust
func BuildFileCache(path string) (FileCache, error) {
	// Create recording directory to track file access
	recordingDir := NewRecordingDirectory(path)

	// Open the Bluge index
	config := bluge.DefaultConfig(path)
	reader, err := bluge.OpenReader(config)
	if err != nil {
		return nil, fmt.Errorf("failed to open Bluge index: %w", err)
	}
	defer reader.Close()

	// Access index files to populate cache
	// This simulates the Rust implementation's approach of accessing inverted indexes

	// List files in the directory to understand structure
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	// Access each file to trigger recording
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fileName := file.Name()
		filePath := filepath.Join(path, fileName)

		// Open and read portions of the file to simulate index access
		fileHandle, err := os.Open(filePath)
		if err != nil {
			continue // Skip files we can't open
		}

		recordingFileHandle := NewRecordingFileHandle(recordingDir, fileHandle, fileName)

		// Read beginning of file to populate cache
		fileInfo, err := fileHandle.Stat()
		if err != nil {
			fileHandle.Close()
			continue
		}

		// Read in chunks to simulate realistic access patterns
		fileSize := uint64(fileInfo.Size())
		chunkSize := uint64(4096) // 4KB chunks

		for offset := uint64(0); offset < fileSize; offset += chunkSize {
			end := offset + chunkSize
			if end > fileSize {
				end = fileSize
			}

			_, err := recordingFileHandle.readBytes(offset, end)
			if err != nil {
				break // Stop on error
			}
		}

		fileHandle.Close()
	}

	// Return the populated cache
	recordingDir.cache.RLock()
	defer recordingDir.cache.RUnlock()

	// Create a copy of the cache to return
	result := make(FileCache)
	for path, rangeCache := range recordingDir.cacheData {
		result[path] = rangeCache
	}

	return result, nil
}
