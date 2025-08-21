package sources

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

// BufSource represents a buffered source for reading documents
// Equivalent to BufSource struct in Rust
type BufSource struct {
	reader  io.ReadCloser
	scanner *bufio.Scanner
	isStdin bool
}

// NewBufSourceFromPath creates a new BufSource from a file path
// Equivalent to BufSource::from_path in Rust
func NewBufSourceFromPath(path string) (*BufSource, error) {
	logrus.Debugf("Reading from '%s'", path)

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}

	return newBufSource(file, false), nil
}

// NewBufSourceFromStdin creates a new BufSource from stdin
// Equivalent to BufSource::from_stdin in Rust
func NewBufSourceFromStdin() *BufSource {
	logrus.Debug("Reading from stdin")
	return newBufSource(os.Stdin, true)
}

// newBufSource creates a new BufSource from a reader
// Equivalent to BufSource::from_buf_reader in Rust
func newBufSource(reader io.ReadCloser, isStdin bool) *BufSource {
	scanner := bufio.NewScanner(reader)
	return &BufSource{
		reader:  reader,
		scanner: scanner,
		isStdin: isStdin,
	}
}

// GetOne implements Source interface
// Equivalent to Source::get_one implementation for BufSource in Rust
func (bs *BufSource) GetOne(ctx context.Context) (*SourceItem, error) {
	if !bs.scanner.Scan() {
		// Check for error first
		if err := bs.scanner.Err(); err != nil {
			return nil, fmt.Errorf("scanner error: %w", err)
		}
		// EOF reached
		return &SourceItem{Type: SourceItemTypeClose}, nil
	}

	line := bs.scanner.Text()
	if line == "" {
		// Empty line, recursively try next
		return bs.GetOne(ctx)
	}

	var jsonMap JsonMap
	if err := json.Unmarshal([]byte(line), &jsonMap); err != nil {
		return nil, fmt.Errorf("failed to parse JSON line: %w", err)
	}

	return &SourceItem{
		Type:     SourceItemTypeDocument,
		Document: jsonMap,
	}, nil
}

// GetCheckpointCommitter implements Source interface
// Equivalent to Source::get_checkpoint_commiter implementation for BufSource in Rust
func (bs *BufSource) GetCheckpointCommitter(ctx context.Context) (CheckpointCommitter, error) {
	// BufSource doesn't support checkpointing
	return nil, nil
}

// Close closes the underlying reader if it's not stdin
func (bs *BufSource) Close() error {
	if !bs.isStdin && bs.reader != nil {
		return bs.reader.Close()
	}
	return nil
}
