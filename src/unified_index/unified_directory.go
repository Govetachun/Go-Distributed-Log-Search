package unified_index

import (
	"encoding/json"
	"fmt"
)

// CachedFileHandle represents a cached file handle
// Equivalent to CachedFileHandle struct in Rust
type CachedFileHandle struct {
	rawFileHandle FileHandle
	cache         RangeCache
}

// NewCachedFileHandle creates a new CachedFileHandle
// Equivalent to new method in Rust
func NewCachedFileHandle(rawFileHandle FileHandle, cache RangeCache) *CachedFileHandle {
	return &CachedFileHandle{
		rawFileHandle: rawFileHandle,
		cache:         cache,
	}
}

// ReadBytes reads bytes from the cached file handle
// Implements FileHandle interface
func (cfh *CachedFileHandle) ReadBytes(start, end uint64) ([]byte, error) {
	// Check cache first
	if entry, exists := cfh.cache[start]; exists {
		if entry.End == end {
			return entry.Bytes, nil
		}
	}

	// Fall back to raw file handle
	return cfh.rawFileHandle.ReadBytes(start, end)
}

// Len returns the length of the file
// Implements FileHandle interface
func (cfh *CachedFileHandle) Len() int64 {
	return cfh.rawFileHandle.Len()
}

// FileSlice represents a slice of a file
type FileSlice struct {
	data   []byte
	offset int64
	length int64
}

// NewFileSlice creates a new FileSlice
func NewFileSlice(data []byte) *FileSlice {
	return &FileSlice{
		data:   data,
		offset: 0,
		length: int64(len(data)),
	}
}

// Slice creates a sub-slice
func (fs *FileSlice) Slice(start, end int64) *FileSlice {
	if start < 0 || end > fs.length || start > end {
		return &FileSlice{data: []byte{}, offset: 0, length: 0}
	}

	return &FileSlice{
		data:   fs.data[fs.offset+start : fs.offset+end],
		offset: 0,
		length: end - start,
	}
}

// SplitFromEnd splits the slice from the end
func (fs *FileSlice) SplitFromEnd(footerLen int) (*FileSlice, *FileSlice) {
	if int64(footerLen) > fs.length {
		return fs, &FileSlice{data: []byte{}, offset: 0, length: 0}
	}

	splitPoint := fs.length - int64(footerLen)
	main := fs.Slice(0, splitPoint)
	footer := fs.Slice(splitPoint, fs.length)

	return main, footer
}

// ReadAllBytes reads all bytes from the slice
func (fs *FileSlice) ReadAllBytes() ([]byte, error) {
	return fs.data[fs.offset : fs.offset+fs.length], nil
}

// Len returns the length of the slice
// Implements FileHandle interface
func (fs *FileSlice) Len() int64 {
	return fs.length
}

// ReadBytes reads bytes from the file slice
// Implements FileHandle interface
func (fs *FileSlice) ReadBytes(start, end uint64) ([]byte, error) {
	if start > uint64(fs.length) || end > uint64(fs.length) || start > end {
		return nil, fmt.Errorf("invalid byte range: [%d, %d) for slice of length %d", start, end, fs.length)
	}

	actualStart := fs.offset + int64(start)
	actualEnd := fs.offset + int64(end)

	if actualEnd > int64(len(fs.data)) {
		return nil, fmt.Errorf("byte range exceeds data bounds")
	}

	return fs.data[actualStart:actualEnd], nil
}

// UnifiedDirectory represents a unified directory
// Equivalent to UnifiedDirectory struct in Rust
type UnifiedDirectory struct {
	ReadOnlyDirectory // Embed read-only methods
	slice             *FileSlice
	footer            *IndexFooter
}

// OpenWithLen opens a unified directory with footer length
// Equivalent to open_with_len method in Rust
func OpenWithLen(slice interface{}, footerLen int) (*UnifiedDirectory, error) {
	var fileSlice *FileSlice

	// Convert slice to FileSlice
	switch s := slice.(type) {
	case []byte:
		fileSlice = NewFileSlice(s)
	case *FileSlice:
		fileSlice = s
	default:
		return nil, fmt.Errorf("unsupported slice type: %T", slice)
	}

	// Split the slice to get the footer
	mainSlice, footerSlice := fileSlice.SplitFromEnd(footerLen)

	// Read footer bytes
	footerBytes, err := footerSlice.ReadAllBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to read footer bytes: %w", err)
	}

	// Deserialize the footer
	var footer IndexFooter
	err = json.Unmarshal(footerBytes, &footer)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize footer: %w", err)
	}

	return &UnifiedDirectory{
		slice:  mainSlice,
		footer: &footer,
	}, nil
}

// GetFileHandle gets a file handle for a path
// Implements Directory interface
func (ud *UnifiedDirectory) GetFileHandle(path string) (FileHandle, error) {
	fileRange, exists := ud.footer.FileOffsets[path]
	if !exists {
		return nil, fmt.Errorf("file does not exist: %s", path)
	}

	// Create a slice for the file using the range
	fileSlice := ud.slice.Slice(int64(fileRange.Start), int64(fileRange.End))

	if cache, exists := ud.footer.Cache[path]; exists {
		return NewCachedFileHandle(fileSlice, cache), nil
	}

	return fileSlice, nil
}

// AtomicRead reads a file atomically
// Implements Directory interface
func (ud *UnifiedDirectory) AtomicRead(path string) ([]byte, error) {
	fileHandle, err := ud.GetFileHandle(path)
	if err != nil {
		return nil, err
	}

	// Read all bytes from the file handle
	return fileHandle.ReadBytes(0, uint64(fileHandle.Len()))
}

// Exists checks if a file exists
// Implements Directory interface
func (ud *UnifiedDirectory) Exists(path string) bool {
	_, exists := ud.footer.FileOffsets[path]
	return exists
}
