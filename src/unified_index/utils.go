package unified_index

import (
	"fmt"
)

// ReadOnlyDirectory provides read-only directory operations
// Equivalent to read_only_directory! macro in Rust
type ReadOnlyDirectory struct{}

// atomicWrite is not implemented for read-only directories
// Equivalent to atomic_write in Rust macro
func (rd *ReadOnlyDirectory) atomicWrite(path string, data []byte) error {
	return fmt.Errorf("read-only: atomic_write not implemented")
}

// delete is not implemented for read-only directories
// Equivalent to delete in Rust macro
func (rd *ReadOnlyDirectory) delete(path string) error {
	return fmt.Errorf("read-only: delete not implemented")
}

// openWrite is not implemented for read-only directories
// Equivalent to open_write in Rust macro
func (rd *ReadOnlyDirectory) openWrite(path string) (interface{}, error) {
	return nil, fmt.Errorf("read-only: open_write not implemented")
}

// syncDirectory is not implemented for read-only directories
// Equivalent to sync_directory in Rust macro
func (rd *ReadOnlyDirectory) syncDirectory() error {
	return fmt.Errorf("read-only: sync_directory not implemented")
}

// watch returns an empty watch handle for read-only directories
// Equivalent to watch in Rust macro
func (rd *ReadOnlyDirectory) watch(callback interface{}) (interface{}, error) {
	// Return empty watch handle
	return nil, nil
}

// acquireLock returns an empty lock for read-only directories
// Equivalent to acquire_lock in Rust macro
func (rd *ReadOnlyDirectory) acquireLock(lock interface{}) (interface{}, error) {
	// Return empty lock
	return nil, nil
}
