package sources

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// KafkaCheckpoint handles Kafka checkpoint operations
// Equivalent to KafkaCheckpoint struct in Rust
type KafkaCheckpoint struct {
	SourceID string
	Pool     *pgxpool.Pool
}

// KafkaCheckpointCommitter handles committing Kafka checkpoints
// Equivalent to KafkaCheckpointCommiter struct in Rust
type KafkaCheckpointCommitter struct {
	Checkpoint           *KafkaCheckpoint
	PartitionsAndOffsets []PartitionOffset
}

// PartitionOffset represents a partition and its offset
type PartitionOffset struct {
	Partition int32
	Offset    int64
}

// PartitionOffsetWithOptional represents a partition with optional offset
type PartitionOffsetWithOptional struct {
	Partition int32
	Offset    *int64 // nil means no offset stored
}

// NewKafkaCheckpoint creates a new KafkaCheckpoint
// Equivalent to KafkaCheckpoint::new in Rust
func NewKafkaCheckpoint(sourceID string, pool *pgxpool.Pool) *KafkaCheckpoint {
	return &KafkaCheckpoint{
		SourceID: sourceID,
		Pool:     pool,
	}
}

// Load loads checkpoint data for the given partitions
// Equivalent to KafkaCheckpoint::load in Rust
func (kc *KafkaCheckpoint) Load(ctx context.Context, partitions []int32) ([]PartitionOffsetWithOptional, error) {
	if len(partitions) == 0 {
		return []PartitionOffsetWithOptional{}, nil
	}

	// Build placeholders for the query
	placeholders := make([]string, len(partitions))
	for i := range partitions {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
	}

	sql := fmt.Sprintf(
		"SELECT partition, offset_value FROM kafka_checkpoints WHERE source_id = $1 AND partition IN (%s)",
		strings.Join(placeholders, ", "),
	)

	// Build args slice
	args := make([]interface{}, len(partitions)+1)
	args[0] = kc.SourceID
	for i, partition := range partitions {
		args[i+1] = partition
	}

	rows, err := kc.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query checkpoints: %w", err)
	}
	defer rows.Close()

	// Collect loaded offsets
	partitionsToOffsets := make(map[int32]int64)
	for rows.Next() {
		var partition int32
		var offset int64
		if err := rows.Scan(&partition, &offset); err != nil {
			return nil, fmt.Errorf("failed to scan checkpoint row: %w", err)
		}
		partitionsToOffsets[partition] = offset
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating checkpoint rows: %w", err)
	}

	logrus.Debugf("Loaded checkpoints: %v", partitionsToOffsets)

	// Build result with optional offsets
	result := make([]PartitionOffsetWithOptional, len(partitions))
	for i, partition := range partitions {
		result[i] = PartitionOffsetWithOptional{
			Partition: partition,
		}
		if offset, exists := partitionsToOffsets[partition]; exists {
			result[i].Offset = &offset
		}
	}

	return result, nil
}

// Save saves checkpoint data for the given partitions and offsets
// Equivalent to KafkaCheckpoint::save in Rust
func (kc *KafkaCheckpoint) Save(ctx context.Context, partitionsAndOffsets []PartitionOffset) error {
	if len(partitionsAndOffsets) == 0 {
		return nil
	}

	// Build SQL for batch insert/update
	valuePlaceholders := make([]string, len(partitionsAndOffsets))
	args := make([]interface{}, len(partitionsAndOffsets)*3)

	for i, po := range partitionsAndOffsets {
		valuePlaceholders[i] = fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3)
		args[i*3] = kc.SourceID
		args[i*3+1] = po.Partition
		args[i*3+2] = po.Offset
	}

	sql := fmt.Sprintf(
		"INSERT INTO kafka_checkpoints (source_id, partition, offset_value) VALUES %s "+
			"ON CONFLICT (source_id, partition) DO UPDATE SET offset_value = EXCLUDED.offset_value",
		strings.Join(valuePlaceholders, ", "),
	)

	logrus.Debugf("Saving checkpoints: %v", partitionsAndOffsets)

	_, err := kc.Pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("failed to save checkpoints: %w", err)
	}

	return nil
}

// Committer creates a new checkpoint committer
// Equivalent to KafkaCheckpoint::commiter in Rust
func (kc *KafkaCheckpoint) Committer(partitionsAndOffsets []PartitionOffset) *KafkaCheckpointCommitter {
	return NewKafkaCheckpointCommitter(kc, partitionsAndOffsets)
}

// NewKafkaCheckpointCommitter creates a new KafkaCheckpointCommitter
// Equivalent to KafkaCheckpointCommiter::new in Rust
func NewKafkaCheckpointCommitter(checkpoint *KafkaCheckpoint, partitionsAndOffsets []PartitionOffset) *KafkaCheckpointCommitter {
	return &KafkaCheckpointCommitter{
		Checkpoint:           checkpoint,
		PartitionsAndOffsets: partitionsAndOffsets,
	}
}

// Commit implements CheckpointCommitter interface
// Equivalent to CheckpointCommiter::commit implementation for KafkaCheckpointCommiter in Rust
func (kcc *KafkaCheckpointCommitter) Commit(ctx context.Context) error {
	return kcc.Checkpoint.Save(ctx, kcc.PartitionsAndOffsets)
}

