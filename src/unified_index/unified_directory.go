package unified_index

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// CachedFileHandle represents a cached file handle
// Equivalent to CachedFileHandle struct in Rust
type CachedFileHandle struct {
	rawFileHandle interface{} // FileHandle interface equivalent
	cache         RangeCache
}

// NewCachedFileHandle creates a new CachedFileHandle
// Equivalent to new method in Rust
func NewCachedFileHandle(rawFileHandle interface{}, cache RangeCache) *CachedFileHandle {
	return &CachedFileHandle{
		rawFileHandle: rawFileHandle,
		cache:         cache,
	}
}

// readBytes reads bytes from the cached file handle
// Equivalent to read_bytes method in Rust
func (cfh *CachedFileHandle) readBytes(start, end uint64) ([]byte, error) {
	// Check cache first
	if entry, exists := cfh.cache[start]; exists {
		if entry.End == end {
			return entry.Bytes, nil
		}
	}

	// Fall back to raw file handle
	if file, ok := cfh.rawFileHandle.(*os.File); ok {
		_, err := file.Seek(int64(start), io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("failed to seek to position %d: %w", start, err)
		}

		length := end - start
		buffer := make([]byte, length)
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read %d bytes: %w", length, err)
		}
		return buffer[:n], nil
	} else if seeker, ok := cfh.rawFileHandle.(io.ReadSeeker); ok {
		_, err := seeker.Seek(int64(start), io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("failed to seek to position %d: %w", start, err)
		}

		length := end - start
		buffer := make([]byte, length)
		n, err := seeker.Read(buffer)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read %d bytes: %w", length, err)
		}
		return buffer[:n], nil
	}

	return nil, fmt.Errorf("unsupported raw file handle type")
}

// len returns the length of the file
// Equivalent to len method in Rust
func (cfh *CachedFileHandle) len() int64 {
	if file, ok := cfh.rawFileHandle.(*os.File); ok {
		info, err := file.Stat()
		if err != nil {
			return 0
		}
		return info.Size()
	}

	// For other types, we can't easily determine length
	return 0
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

// ReadBytes reads all bytes from the slice
func (fs *FileSlice) ReadBytes() ([]byte, error) {
	return fs.data[fs.offset : fs.offset+fs.length], nil
}

// Len returns the length of the slice
func (fs *FileSlice) Len() int64 {
	return fs.length
}

// UnifiedDirectory represents a unified directory
// Equivalent to UnifiedDirectory struct in Rust
type UnifiedDirectory struct {
	slice  *FileSlice
	footer *IndexFooter
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
	footerBytes, err := footerSlice.ReadBytes()
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

// getFileHandle gets a file handle for a path
// Equivalent to get_file_handle method in Rust
func (ud *UnifiedDirectory) getFileHandle(path string) (interface{}, error) {
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

// atomicRead reads a file atomically
// Equivalent to atomic_read method in Rust
func (ud *UnifiedDirectory) atomicRead(path string) ([]byte, error) {
	fileHandle, err := ud.getFileHandle(path)
	if err != nil {
		return nil, err
	}

	// Read the file based on handle type
	switch handle := fileHandle.(type) {
	case *FileSlice:
		return handle.ReadBytes()
	case *CachedFileHandle:
		// For cached file handle, read all bytes from the beginning
		return handle.readBytes(0, uint64(handle.len()))
	default:
		return nil, fmt.Errorf("unsupported file handle type: %T", fileHandle)
	}
}

// exists checks if a file exists
// Equivalent to exists method in Rust
func (ud *UnifiedDirectory) exists(path string) bool {
	_, exists := ud.footer.FileOffsets[path]
	return exists
}
