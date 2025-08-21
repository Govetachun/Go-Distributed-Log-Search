package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"

	"toshokan/src/args"
	"toshokan/src/commands/sources"
	"toshokan/src/config"
)

// BatchResult represents the result of a batch operation
// Equivalent to BatchResult enum in Rust
type BatchResult int

const (
	BatchResultEOF BatchResult = iota
	BatchResultTimeout
	BatchResultRestart
)

// IndexCommitter handles committing index operations
type IndexCommitter struct {
	IndexName           string
	IndexPath           string
	Pool                *pgxpool.Pool
	CheckpointCommitter sources.CheckpointCommitter
}

// IndexRunner manages the indexing process
// Equivalent to IndexRunner struct in Rust
type IndexRunner struct {
	source       sources.Source
	fieldParsers []*FieldParser
	args         *args.IndexArgs
	config       *config.IndexConfig
	pool         *pgxpool.Pool
	commitLock   sync.Mutex
}

// NewIndexRunner creates a new IndexRunner
// Equivalent to IndexRunner::new in Rust
func NewIndexRunner(ctx context.Context, indexArgs *args.IndexArgs, pool *pgxpool.Pool) (*IndexRunner, error) {
	source, err := sources.ConnectToSource(ctx, &indexArgs.Input, indexArgs.Stream, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to source: %w", err)
	}

	return NewIndexRunnerWithSource(ctx, indexArgs, pool, source)
}

// NewIndexRunnerWithSource creates a new IndexRunner with a specific source
// Equivalent to IndexRunner::new_with_source in Rust
func NewIndexRunnerWithSource(
	ctx context.Context,
	indexArgs *args.IndexArgs,
	pool *pgxpool.Pool,
	source sources.Source,
) (*IndexRunner, error) {
	indexConfig, err := getIndexConfig(ctx, indexArgs.Name, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to get index config: %w", err)
	}

	// Build field parsers from config
	fieldParsers, err := BuildParsersFromFieldConfigs(indexConfig.Schema.Fields)
	if err != nil {
		return nil, fmt.Errorf("failed to build field parsers: %w", err)
	}

	return &IndexRunner{
		source:       source,
		fieldParsers: fieldParsers,
		args:         indexArgs,
		config:       indexConfig,
		pool:         pool,
	}, nil
}

// RunOneBatch reads documents from the source and indexes them into a new index file
// Equivalent to run_one_batch in Rust
func (ir *IndexRunner) RunOneBatch(ctx context.Context) (BatchResult, error) {
	id := uuid.New().String()
	indexDir := filepath.Join(ir.args.BuildDir, id)

	// Create the index directory
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return BatchResultEOF, fmt.Errorf("failed to create index directory: %w", err)
	}

	// Initialize Bluge index
	blugeConfig := bluge.DefaultConfig(indexDir)
	writer, err := bluge.OpenWriter(blugeConfig)
	if err != nil {
		return BatchResultEOF, fmt.Errorf("failed to create Bluge writer: %w", err)
	}
	defer writer.Close()

	added := 0
	result := BatchResultEOF

	// Set up timeout for streaming mode
	var timeoutChan <-chan time.Time
	if ir.args.Stream {
		timeoutChan = time.After(ir.args.CommitInterval)
	} else {
		// Create a channel that will never fire
		timeoutChan = make(<-chan time.Time)
	}

	logrus.Debugf("Piping source -> index of id '%s'", id)

	// Main indexing loop
	for {
		select {
		case <-timeoutChan:
			result = BatchResultTimeout
			goto commit

		case <-ctx.Done():
			return BatchResultEOF, ctx.Err()

		default:
			// Get next item from source
			item, err := ir.source.GetOne(ctx)
			if err != nil {
				return BatchResultEOF, fmt.Errorf("failed to get item from source: %w", err)
			}

			switch item.Type {
			case sources.SourceItemTypeDocument:
				// Process the document
				doc := make(map[string]interface{})

				// Parse fields using field parsers
				for _, fieldParser := range ir.fieldParsers {
					if value, exists := item.Document[fieldParser.Name]; exists {
						if err := fieldParser.AddParsedFieldValue(doc, value); err != nil {
							logrus.Errorf("Failed to parse '%s' (on %d iteration): %v", fieldParser.Name, added, err)
							continue
						}
						// Remove processed field from original document
						delete(item.Document, fieldParser.Name)
					} else {
						logrus.Debugf("Field '%s' in schema but not found", fieldParser.Name)
					}
				}

				// Add remaining fields to dynamic field
				if len(item.Document) > 0 {
					doc[DynamicFieldName] = item.Document
				}

				// Create Bluge document
				blugeDoc := bluge.NewDocument(fmt.Sprintf("%s_%d", id, added))
				for field, value := range doc {
					if field == DynamicFieldName {
						// Handle dynamic field as JSON
						if jsonData, ok := value.(map[string]interface{}); ok {
							for k, v := range jsonData {
								blugeDoc.AddField(bluge.NewTextField(k, fmt.Sprintf("%v", v)).StoreValue())
							}
						}
					} else {
						blugeDoc.AddField(bluge.NewTextField(field, fmt.Sprintf("%v", value)).StoreValue())
					}
				}

				// Add document to index
				err := writer.Update(blugeDoc.ID(), blugeDoc)
				if err != nil {
					logrus.Errorf("Failed to index document: %v", err)
					continue
				}
				added++

			case sources.SourceItemTypeClose:
				logrus.Debugf("Source closed for index of id '%s'", id)
				goto commit

			case sources.SourceItemTypeRestart:
				logrus.Debugf("Aborting index of id '%s' with %d documents", id, added)
				if err := os.RemoveAll(indexDir); err != nil {
					logrus.Warnf("Failed to remove aborted index of id '%s': %v", id, err)
				}
				return BatchResultRestart, nil
			}
		}
	}

commit:
	if added == 0 {
		logrus.Debug("Not writing index: no documents added")
		if err := os.RemoveAll(indexDir); err != nil {
			logrus.Warnf("Failed to remove empty index of id '%s': %v", id, err)
		}
		return result, nil
	}

	logrus.Infof("Committing %d documents", added)

	committer, err := ir.indexCommitter(ctx)
	if err != nil {
		return BatchResultEOF, fmt.Errorf("failed to create index committer: %w", err)
	}

	if ir.args.Stream && result != BatchResultEOF {
		// Commit in the background to not block and continue indexing next batch
		go func() {
			ir.commitLock.Lock()
			defer ir.commitLock.Unlock()

			if err := ir.commitIndex(ctx, committer, id, indexDir); err != nil {
				logrus.Errorf("Failed to commit index of id '%s': %v", id, err)
			}
		}()
	} else {
		ir.commitLock.Lock()
		err := ir.commitIndex(ctx, committer, id, indexDir)
		ir.commitLock.Unlock()
		if err != nil {
			return BatchResultEOF, fmt.Errorf("failed to commit index: %w", err)
		}
	}

	return result, nil
}

// indexCommitter creates an IndexCommitter for the current configuration
func (ir *IndexRunner) indexCommitter(ctx context.Context) (*IndexCommitter, error) {
	checkpointCommitter, err := ir.source.GetCheckpointCommitter(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get checkpoint committer: %w", err)
	}

	return &IndexCommitter{
		IndexName:           ir.config.Name,
		IndexPath:           ir.config.Path,
		Pool:                ir.pool,
		CheckpointCommitter: checkpointCommitter,
	}, nil
}

// commitIndex commits the index to storage
func (ir *IndexRunner) commitIndex(
	ctx context.Context,
	committer *IndexCommitter,
	id string,
	inputDir string,
) error {
	logrus.Debug("Committing Bluge index...")

	// Write unified index
	if err := writeUnifiedIndex(
		ctx,
		id,
		inputDir,
		committer.IndexName,
		committer.IndexPath,
		committer.Pool,
	); err != nil {
		return fmt.Errorf("failed to write unified index: %w", err)
	}

	// Commit checkpoint if available
	if committer.CheckpointCommitter != nil {
		if err := committer.CheckpointCommitter.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit checkpoint: %w", err)
		}
	}

	return nil
}

// RunIndex executes the index command
// Equivalent to run_index function in Rust
func RunIndex(ctx context.Context, indexArgs *args.IndexArgs, pool *pgxpool.Pool) error {
	runner, err := NewIndexRunner(ctx, indexArgs, pool)
	if err != nil {
		return fmt.Errorf("failed to create index runner: %w", err)
	}
	defer runner.source.Close()

	for {
		result, err := runner.RunOneBatch(ctx)
		if err != nil {
			return fmt.Errorf("failed to run batch: %w", err)
		}

		if result == BatchResultEOF {
			break
		}
	}

	return nil
}
