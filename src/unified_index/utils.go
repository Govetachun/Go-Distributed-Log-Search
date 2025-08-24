package unified_index

import (
	"fmt"
	"io"
)

// Directory interface represents a directory that can store and retrieve files
// Equivalent to tantivy::Directory trait in Rust
type Directory interface {
	// Read operations
	GetFileHandle(path string) (FileHandle, error)
	AtomicRead(path string) ([]byte, error)
	Exists(path string) bool

	// Write operations (read-only directories will return errors)
	AtomicWrite(path string, data []byte) error
	Delete(path string) error
	OpenWrite(path string) (WriteHandle, error)
	SyncDirectory() error

	// Lock and watch operations
	Watch(callback WatchCallback) (WatchHandle, error)
	AcquireLock(lock Lock) (DirectoryLock, error)
}

// FileHandle interface represents a handle to a file within a directory
// Equivalent to tantivy::directory::FileHandle trait in Rust
type FileHandle interface {
	ReadBytes(start, end uint64) ([]byte, error)
	Len() int64
}

// WriteHandle interface represents a handle for writing to a file
// Equivalent to tantivy::directory::WritePtr in Rust
type WriteHandle interface {
	io.Writer
	Flush() error
	Close() error
}

// WatchCallback represents a callback function for file system events
// Equivalent to tantivy::directory::WatchCallback in Rust
type WatchCallback func(WatchEvent)

// WatchEvent represents a file system event
type WatchEvent struct {
	Path string
	Kind WatchEventKind
}

// WatchEventKind represents the type of file system event
type WatchEventKind int

const (
	WatchEventCreate WatchEventKind = iota
	WatchEventModify
	WatchEventDelete
)

// WatchHandle represents a handle to a file system watcher
// Equivalent to tantivy::directory::WatchHandle in Rust
type WatchHandle interface {
	Stop() error
}

// Lock represents a directory lock
// Equivalent to tantivy::directory::Lock in Rust
type Lock interface {
	Name() string
}

// DirectoryLock represents an acquired directory lock
// Equivalent to tantivy::directory::DirectoryLock in Rust
type DirectoryLock interface {
	Release() error
}

// EmptyWatchHandle provides a no-op implementation of WatchHandle
type EmptyWatchHandle struct{}

func (ewh *EmptyWatchHandle) Stop() error {
	return nil
}

// EmptyDirectoryLock provides a no-op implementation of DirectoryLock
type EmptyDirectoryLock struct{}

func (edl *EmptyDirectoryLock) Release() error {
	return nil
}

// ReadOnlyDirectory provides read-only directory operations
// Equivalent to read_only_directory! macro in Rust
// This struct can be embedded in other directory implementations
type ReadOnlyDirectory struct{}

// AtomicWrite is not implemented for read-only directories
// Equivalent to atomic_write in Rust macro
func (rd *ReadOnlyDirectory) AtomicWrite(path string, data []byte) error {
	return fmt.Errorf("read-only: atomic_write not implemented")
}

// Delete is not implemented for read-only directories
// Equivalent to delete in Rust macro
func (rd *ReadOnlyDirectory) Delete(path string) error {
	return fmt.Errorf("read-only: delete not implemented")
}

// OpenWrite is not implemented for read-only directories
// Equivalent to open_write in Rust macro
func (rd *ReadOnlyDirectory) OpenWrite(path string) (WriteHandle, error) {
	return nil, fmt.Errorf("read-only: open_write not implemented")
}

// SyncDirectory is not implemented for read-only directories
// Equivalent to sync_directory in Rust macro
func (rd *ReadOnlyDirectory) SyncDirectory() error {
	return fmt.Errorf("read-only: sync_directory not implemented")
}

// Watch returns an empty watch handle for read-only directories
// Equivalent to watch in Rust macro
func (rd *ReadOnlyDirectory) Watch(callback WatchCallback) (WatchHandle, error) {
	// Return empty watch handle
	return &EmptyWatchHandle{}, nil
}

// AcquireLock returns an empty lock for read-only directories
// Equivalent to acquire_lock in Rust macro
func (rd *ReadOnlyDirectory) AcquireLock(lock Lock) (DirectoryLock, error) {
	// Return empty lock
	return &EmptyDirectoryLock{}, nil
}
