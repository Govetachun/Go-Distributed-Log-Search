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
	ReadOnlyDirectory // Embed read-only methods for write operations
	inner             Directory
	cache             *sync.RWMutex
	cacheData         FileCache
}

// NewRecordingDirectory creates a new RecordingDirectory
// Equivalent to wrap method in Rust
func NewRecordingDirectory(directory Directory) *RecordingDirectory {
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
		inner:     rd.inner,
		cache:     rd.cache,
		cacheData: rd.cacheData,
	}
}

// GetFileHandle gets a file handle for a path
// Implements Directory interface
func (rd *RecordingDirectory) GetFileHandle(path string) (FileHandle, error) {
	inner, err := rd.inner.GetFileHandle(path)
	if err != nil {
		return nil, err
	}

	return NewRecordingFileHandle(rd, inner, path), nil
}

// Exists checks if a file exists
// Implements Directory interface
func (rd *RecordingDirectory) Exists(path string) bool {
	return rd.inner.Exists(path)
}

// AtomicRead reads a file atomically and records the access
// Implements Directory interface
func (rd *RecordingDirectory) AtomicRead(path string) ([]byte, error) {
	payload, err := rd.inner.AtomicRead(path)
	if err != nil {
		return nil, err
	}

	// Record the access
	err = rd.record(path, payload, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to record cache access: %w", err)
	}

	return payload, nil
}

// RecordingFileHandle wraps a file handle to record access
// Equivalent to RecordingFileHandle<D: Directory> in Rust
type RecordingFileHandle struct {
	directory *RecordingDirectory
	inner     FileHandle
	path      string
}

// NewRecordingFileHandle creates a new RecordingFileHandle
func NewRecordingFileHandle(directory *RecordingDirectory, inner FileHandle, path string) *RecordingFileHandle {
	return &RecordingFileHandle{
		directory: directory,
		inner:     inner,
		path:      path,
	}
}

// ReadBytes reads bytes from the file handle and records the access
// Implements FileHandle interface
func (rfh *RecordingFileHandle) ReadBytes(start, end uint64) ([]byte, error) {
	// Read from inner file handle
	payload, err := rfh.inner.ReadBytes(start, end)
	if err != nil {
		return nil, err
	}

	// Record the access in cache
	err = rfh.directory.record(rfh.path, payload, start)
	if err != nil {
		return nil, fmt.Errorf("failed to record cache access: %w", err)
	}

	return payload, nil
}

// Len returns the length of the file
// Implements FileHandle interface
func (rfh *RecordingFileHandle) Len() int64 {
	return rfh.inner.Len()
}

// FilesystemDirectory provides basic filesystem directory operations
type FilesystemDirectory struct {
	ReadOnlyDirectory // Embed read-only methods
	basePath          string
}

// NewFilesystemDirectory creates a new filesystem directory
func NewFilesystemDirectory(basePath string) *FilesystemDirectory {
	return &FilesystemDirectory{
		basePath: basePath,
	}
}

// GetFileHandle gets a file handle for a path
func (fsd *FilesystemDirectory) GetFileHandle(path string) (FileHandle, error) {
	fullPath := filepath.Join(fsd.basePath, path)
	file, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	return NewOSFileHandle(file), nil
}

// AtomicRead reads a file atomically
func (fsd *FilesystemDirectory) AtomicRead(path string) ([]byte, error) {
	fullPath := filepath.Join(fsd.basePath, path)
	return os.ReadFile(fullPath)
}

// Exists checks if a file exists
func (fsd *FilesystemDirectory) Exists(path string) bool {
	fullPath := filepath.Join(fsd.basePath, path)
	_, err := os.Stat(fullPath)
	return err == nil
}

// OSFileHandle wraps an os.File to implement FileHandle
type OSFileHandle struct {
	file *os.File
}

// NewOSFileHandle creates a new OSFileHandle
func NewOSFileHandle(file *os.File) *OSFileHandle {
	return &OSFileHandle{file: file}
}

// ReadBytes reads bytes from the file
func (ofh *OSFileHandle) ReadBytes(start, end uint64) ([]byte, error) {
	_, err := ofh.file.Seek(int64(start), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to position %d: %w", start, err)
	}

	length := end - start
	buffer := make([]byte, length)
	n, err := ofh.file.Read(buffer)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read %d bytes: %w", length, err)
	}
	return buffer[:n], nil
}

// Len returns the length of the file
func (ofh *OSFileHandle) Len() int64 {
	info, err := ofh.file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

// buildFileCache builds a file cache from a directory path
// Equivalent to build_file_cache function in Rust
func BuildFileCache(path string) (FileCache, error) {
	// Create filesystem directory and recording directory
	fsDir := NewFilesystemDirectory(path)
	recordingDir := NewRecordingDirectory(fsDir)

	// Try to open the Bluge index to trigger realistic access patterns
	config := bluge.DefaultConfig(path)
	reader, err := bluge.OpenReader(config)
	if err != nil {
		// If we can't open as Bluge index, fall back to file enumeration
		return buildFileCacheFromFiles(recordingDir, path)
	}
	defer reader.Close()

	// Access files through the recording directory to populate cache
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

		// Read file through recording directory to populate cache
		_, err := recordingDir.AtomicRead(fileName)
		if err != nil {
			continue // Skip files we can't read
		}
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

// buildFileCacheFromFiles builds cache by reading files directly
func buildFileCacheFromFiles(recordingDir *RecordingDirectory, basePath string) (FileCache, error) {
	files, err := os.ReadDir(basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	// Access each file to trigger recording
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fileName := file.Name()

		// Read file through recording directory to populate cache
		_, err := recordingDir.AtomicRead(fileName)
		if err != nil {
			continue // Skip files we can't read
		}
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
