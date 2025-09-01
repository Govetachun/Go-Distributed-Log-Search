package sources

import (
	"context"
	"fmt"

	"toshokan/src/database"
)

// JsonMap represents a JSON map type
type JsonMap = map[string]interface{}

// SourceItem represents an item from a data source
type SourceItem struct {
	Type     SourceItemType `json:"type"`
	Document JsonMap        `json:"document,omitempty"`
}

type SourceItemType string

const (
	// SourceItemTypeDocument - A document to index
	SourceItemTypeDocument SourceItemType = "document"
	// SourceItemTypeClose - The source is closed, can't read more from it
	SourceItemTypeClose SourceItemType = "close"
	// SourceItemTypeRestart - The source decided to reload from the last checkpoint (example: kafka rebalance)
	SourceItemTypeRestart SourceItemType = "restart"
)

// Source represents a data source interface
type Source interface {
	// GetOne gets a document from the source
	GetOne(ctx context.Context) (*SourceItem, error)

	// GetCheckpointCommitter returns a checkpoint committer if the source supports checkpointing
	// It creates a checkpoint committer that stores a snapshot of the last read state.
	// Once the indexer has successfully committed and uploaded the new index file,
	// it tells the checkpoint committer to commit the snapshot.
	GetCheckpointCommitter(ctx context.Context) (CheckpointCommitter, error)

	// Close closes the source and releases resources
	Close() error
}

// CheckpointCommitter represents a checkpoint committer interface
type CheckpointCommitter interface {
	// Commit commits the stored state snapshot
	Commit(ctx context.Context) error
}

// ConnectToSource connects to a data source based on the input parameters
func ConnectToSource(ctx context.Context, input *string, stream bool, db database.DBAdapter) (Source, error) {
	switch {
	case input != nil && len(*input) > len(KafkaPrefix) && (*input)[:len(KafkaPrefix)] == KafkaPrefix:
		// Kafka source
		return NewKafkaSourceFromURL(ctx, *input, stream, db)
	case input != nil:
		// File source
		if stream {
			return nil, fmt.Errorf("streaming from a file is not currently supported")
		}
		return NewBufSourceFromPath(*input)
	default:
		// Stdin source
		return NewBufSourceFromStdin(), nil
	}
}
