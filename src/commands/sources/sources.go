package sources

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SourceItem represents an item from a data source
// Equivalent to SourceItem enum in Rust
type SourceItem struct {
	Type     SourceItemType         `json:"type"`
	Document map[string]interface{} `json:"document,omitempty"`
}

type SourceItemType string

const (
	SourceItemTypeDocument SourceItemType = "document"
	SourceItemTypeClose    SourceItemType = "close"
	SourceItemTypeRestart  SourceItemType = "restart"
)

// CheckpointCommitter represents a checkpoint committer for sources
// Equivalent to CheckpointCommiter trait in Rust
type CheckpointCommitter interface {
	Commit(ctx context.Context) error
}

// Source represents a data source interface
// Equivalent to Source trait in Rust
type Source interface {
	// GetOne gets the next item from the source
	GetOne(ctx context.Context) (*SourceItem, error)

	// GetCheckpointCommitter returns a checkpoint committer if available
	GetCheckpointCommitter(ctx context.Context) (CheckpointCommitter, error)

	// Close closes the source
	Close() error
}

// FileSource represents a file-based data source
type FileSource struct {
	reader  io.ReadCloser
	scanner *bufio.Scanner
	isStdin bool
	closed  bool
}

// NewFileSource creates a new file source
func NewFileSource(path string) (*FileSource, error) {
	var reader io.ReadCloser
	var isStdin bool

	if path == "" || path == "-" {
		// Read from stdin
		reader = os.Stdin
		isStdin = true
	} else {
		// Read from file
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %w", path, err)
		}
		reader = file
		isStdin = false
	}

	scanner := bufio.NewScanner(reader)
	return &FileSource{
		reader:  reader,
		scanner: scanner,
		isStdin: isStdin,
		closed:  false,
	}, nil
}

// GetOne implements Source interface
func (fs *FileSource) GetOne(ctx context.Context) (*SourceItem, error) {
	if fs.closed {
		return &SourceItem{Type: SourceItemTypeClose}, nil
	}

	if !fs.scanner.Scan() {
		if err := fs.scanner.Err(); err != nil {
			return nil, fmt.Errorf("scanner error: %w", err)
		}
		// End of file reached
		fs.closed = true
		return &SourceItem{Type: SourceItemTypeClose}, nil
	}

	line := fs.scanner.Text()
	if line == "" {
		// Empty line, try next
		return fs.GetOne(ctx)
	}

	var document map[string]interface{}
	if err := json.Unmarshal([]byte(line), &document); err != nil {
		return nil, fmt.Errorf("failed to parse JSON line: %w", err)
	}

	return &SourceItem{
		Type:     SourceItemTypeDocument,
		Document: document,
	}, nil
}

// GetCheckpointCommitter implements Source interface
func (fs *FileSource) GetCheckpointCommitter(ctx context.Context) (CheckpointCommitter, error) {
	// File sources don't have checkpoint committers
	return nil, nil
}

// Close implements Source interface
func (fs *FileSource) Close() error {
	if fs.closed {
		return nil
	}

	fs.closed = true
	if !fs.isStdin {
		return fs.reader.Close()
	}
	return nil
}

// KafkaSource represents a Kafka-based data source (placeholder)
type KafkaSource struct {
	topic     string
	partition int
	offset    int64
	closed    bool
}

// NewKafkaSource creates a new Kafka source
func NewKafkaSource(topic string, partition int, offset int64) *KafkaSource {
	return &KafkaSource{
		topic:     topic,
		partition: partition,
		offset:    offset,
		closed:    false,
	}
}

// GetOne implements Source interface for Kafka
func (ks *KafkaSource) GetOne(ctx context.Context) (*SourceItem, error) {
	if ks.closed {
		return &SourceItem{Type: SourceItemTypeClose}, nil
	}

	// TODO: Implement actual Kafka message consumption
	// For now, return close to indicate no more messages
	ks.closed = true
	return &SourceItem{Type: SourceItemTypeClose}, nil
}

// GetCheckpointCommitter implements Source interface for Kafka
func (ks *KafkaSource) GetCheckpointCommitter(ctx context.Context) (CheckpointCommitter, error) {
	// TODO: Implement Kafka checkpoint committer
	return nil, nil
}

// Close implements Source interface for Kafka
func (ks *KafkaSource) Close() error {
	ks.closed = true
	return nil
}

// ConnectToSource connects to a data source based on the input parameters
// Equivalent to connect_to_source function in Rust
func ConnectToSource(ctx context.Context, input *string, stream bool, pool *pgxpool.Pool) (Source, error) {
	if input == nil || *input == "" {
		// Read from stdin
		return NewFileSource("")
	}

	// Check if input is a file path or a Kafka topic specification
	// For simplicity, we'll assume it's a file path for now
	// In a real implementation, you'd parse different source formats

	return NewFileSource(*input)
}
