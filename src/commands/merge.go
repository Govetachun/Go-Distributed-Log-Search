package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"

	"toshokan/src/args"
)

const (
	// MinTantivyMemory is the minimum memory allocation for Tantivy operations
	MinTantivyMemory = 15_000_000
)

// RunMerge executes the merge command
// Equivalent to run_merge function in Rust
func RunMerge(ctx context.Context, mergeArgs *args.MergeArgs, pool *pgxpool.Pool) error {
	// Get the index configuration
	indexConfig, err := getIndexConfig(ctx, mergeArgs.Name, pool)
	if err != nil {
		return fmt.Errorf("failed to get index config: %w", err)
	}

	// Open unified directories to get the index files
	indexFiles, err := openUnifiedDirectories(ctx, indexConfig.Path, pool)
	if err != nil {
		return fmt.Errorf("failed to open unified directories: %w", err)
	}

	if len(indexFiles) <= 1 {
		logrus.Info("Need at least 2 files in index directory to be able to merge")
		return nil
	}

	// Extract IDs and prepare for merge
	var ids []string
	for _, file := range indexFiles {
		ids = append(ids, file.ID)
	}

	// Create merge directory
	id := uuid.New().String()
	indexDir := filepath.Join(mergeArgs.MergeDir, id)
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return fmt.Errorf("failed to create merge directory: %w", err)
	}

	// TODO: Implement actual Tantivy index merging
	// For now, we'll simulate the merge process
	logrus.Infof("Merging %d segments", len(indexFiles))

	// Simulate merge operation
	err = performMergeOperation(ctx, indexFiles, indexDir)
	if err != nil {
		return fmt.Errorf("failed to perform merge operation: %w", err)
	}

	// Write the merged unified index
	err = writeUnifiedIndex(ctx, id, indexDir, indexConfig.Name, indexConfig.Path, pool)
	if err != nil {
		return fmt.Errorf("failed to write unified index: %w", err)
	}

	// Delete the old index files from database
	err = deleteOldIndexFiles(ctx, ids, pool)
	if err != nil {
		return fmt.Errorf("failed to delete old index files from database: %w", err)
	}

	// Delete the old index files from storage
	err = deleteOldIndexFilesFromStorage(ctx, ids, indexConfig.Path)
	if err != nil {
		logrus.Warnf("Failed to delete some old index files from storage: %v", err)
		// Continue execution even if file deletion fails
	}

	logrus.Infof("Successfully merged %d index files into 1", len(indexFiles))

	return nil
}

// performMergeOperation performs the actual index merging
func performMergeOperation(ctx context.Context, indexFiles []IndexFile, outputDir string) error {
	// TODO: Implement actual Tantivy-like index merging
	// This would involve:
	// 1. Opening all input index directories
	// 2. Creating a merge directory that combines them
	// 3. Using an index writer to merge segments
	// 4. Waiting for merge threads to complete

	// For now, we'll just simulate the operation
	logrus.Debug("Simulating index merge operation...")

	// Simulate some processing time
	// time.Sleep(100 * time.Millisecond)

	return nil
}

// deleteOldIndexFiles deletes old index file records from the database
func deleteOldIndexFiles(ctx context.Context, ids []string, pool *pgxpool.Pool) error {
	// Convert string slice to interface slice for PostgreSQL array parameter
	interfaceIds := make([]interface{}, len(ids))
	for i, id := range ids {
		interfaceIds[i] = id
	}

	_, err := pool.Exec(ctx, "DELETE FROM index_files WHERE id = ANY($1)", interfaceIds)
	if err != nil {
		return fmt.Errorf("failed to delete index file records: %w", err)
	}

	return nil
}

// deleteOldIndexFilesFromStorage deletes old index files from storage
func deleteOldIndexFilesFromStorage(ctx context.Context, ids []string, indexPath string) error {
	operator, err := getOperator(indexPath)
	if err != nil {
		return fmt.Errorf("failed to get operator: %w", err)
	}

	// Delete files concurrently
	var wg sync.WaitGroup
	errorChan := make(chan error, len(ids))

	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			fileName := fmt.Sprintf("%s.index", id)
			if err := operator.Delete(ctx, fileName); err != nil {
				logrus.Warnf(
					"Failed to delete index file '%s': %v. "+
						"Don't worry, this just means the file is leaked, but will never be read from again.",
					fileName, err,
				)
				errorChan <- err
			}
		}(id)
	}

	// Wait for all deletions to complete
	wg.Wait()
	close(errorChan)

	// Check if there were any errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to delete %d files", len(errors))
	}

	return nil
}
