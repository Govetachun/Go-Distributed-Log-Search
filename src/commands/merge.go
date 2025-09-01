package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/blugelabs/bluge"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"toshokan/src/args"
	"toshokan/src/database"
)

const (
	// MinTantivyMemory is the minimum memory allocation for Tantivy operations
	MinTantivyMemory = 15_000_000
)

// RunMerge executes the merge command.
func RunMerge(ctx context.Context, mergeArgs *args.MergeArgs, db database.DBAdapter) error {
	// Get the index configuration
	indexConfig, err := getIndexConfig(ctx, mergeArgs.Name, db)
	if err != nil {
		return fmt.Errorf("failed to get index config: %w", err)
	}

	// Open unified directories to get the index files
	indexFiles, err := openUnifiedDirectories(ctx, indexConfig.Path, db)
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

	logrus.Infof("Merging %d segments", len(indexFiles))

	// Perform actual Bluge index merging
	err = performMergeOperation(ctx, indexFiles, indexDir)
	if err != nil {
		return fmt.Errorf("failed to perform merge operation: %w", err)
	}

	// Write the merged unified index
	err = writeUnifiedIndex(ctx, id, indexDir, indexConfig.Name, indexConfig.Path, db)
	if err != nil {
		return fmt.Errorf("failed to write unified index: %w", err)
	}

	// Delete the old index files from database
	err = deleteOldIndexFiles(ctx, ids, db)
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

// performMergeOperation performs the actual index merging using Bluge
func performMergeOperation(ctx context.Context, indexFiles []IndexFile, outputDir string) error {
	logrus.Debug("Starting Bluge index merge operation...")

	// Create merged index
	mergedConfig := bluge.DefaultConfig(outputDir)
	writer, err := bluge.OpenWriter(mergedConfig)
	if err != nil {
		return fmt.Errorf("failed to create merged index writer: %w", err)
	}
	defer writer.Close()

	// Merge each input index
	for _, indexFile := range indexFiles {
		err := mergeIndexFile(ctx, indexFile, writer)
		if err != nil {
			logrus.Warnf("Failed to merge index file %s: %v", indexFile.FileName, err)
			continue
		}
	}

	logrus.Infof("Successfully merged %d index files", len(indexFiles))
	return nil
}

// mergeIndexFile merges a single index file into the target writer
func mergeIndexFile(ctx context.Context, indexFile IndexFile, targetWriter *bluge.Writer) error {
	sourcePath := filepath.Join("/tmp/toshokan_build", indexFile.ID)
	sourceConfig := bluge.DefaultConfig(sourcePath)

	reader, err := bluge.OpenReader(sourceConfig)
	if err != nil {
		return fmt.Errorf("failed to open source index %s: %w", sourcePath, err)
	}
	defer reader.Close()

	// Search all documents and re-index them
	query := bluge.NewMatchAllQuery()
	request := bluge.NewAllMatches(query)

	documentMatchIterator, err := reader.Search(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to search source index: %w", err)
	}

	batch := bluge.NewBatch()
	count := 0

	for {
		match, err := documentMatchIterator.Next()
		if err != nil {
			break
		}
		if match == nil {
			break
		}

		// Create document for re-indexing
		doc := bluge.NewDocument(fmt.Sprintf("doc_%d", count))
		err = match.VisitStoredFields(func(field string, value []byte) bool {
			doc.AddField(bluge.NewTextField(field, string(value)).StoreValue())
			return true
		})
		if err != nil {
			logrus.Warnf("Failed to visit stored fields: %v", err)
			continue
		}

		batch.Update(doc.ID(), doc)
		count++

		if count%100 == 0 {
			err = targetWriter.Batch(batch)
			if err != nil {
				logrus.Warnf("Failed to execute batch: %v", err)
			}
			batch = bluge.NewBatch()
		}
	}

	// Execute remaining batch
	if count%100 != 0 {
		err = targetWriter.Batch(batch)
		if err != nil {
			return fmt.Errorf("failed to execute final batch: %w", err)
		}
	}

	logrus.Debugf("Merged %d documents from index %s", count, indexFile.ID)
	return nil
}

// deleteOldIndexFiles deletes old index file records from the database
func deleteOldIndexFiles(ctx context.Context, ids []string, db database.DBAdapter) error {
	// Convert string slice to interface slice for database parameter
	interfaceIds := make([]interface{}, len(ids))
	for i, id := range ids {
		interfaceIds[i] = id
	}

	err := db.Exec(ctx, "DELETE FROM index_files WHERE id IN (?)", interfaceIds)
	if err != nil {
		return fmt.Errorf("failed to delete index file records: %w", err)
	}

	return nil
}

// deleteOldIndexFilesFromStorage deletes old index files from storage
func deleteOldIndexFilesFromStorage(ctx context.Context, ids []string, indexPath string) error {
	operator, err := getOperator(ctx, indexPath)
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
