package commands

import (
	"context"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"

	"toshokan/src/args"
	"toshokan/src/database"
)

// RunDrop executes the drop command
func RunDrop(ctx context.Context, dropArgs *args.DropArgs, db database.DBAdapter) error {
	// Get the base path for the index
	basePath, err := getIndexPath(ctx, dropArgs.Name, db)
	if err != nil {
		return fmt.Errorf("failed to get index path: %w", err)
	}

	// Get all file names associated with this index
	rows, err := db.Query(ctx,
		"SELECT file_name FROM index_files WHERE index_name=?",
		dropArgs.Name,
	)
	if err != nil {
		return fmt.Errorf("failed to query index files: %w", err)
	}
	defer rows.Close()

	var fileNames []string
	for rows.Next() {
		var fileName string
		if err := rows.Scan(&fileName); err != nil {
			return fmt.Errorf("failed to scan file name: %w", err)
		}
		fileNames = append(fileNames, fileName)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	fileNamesLen := len(fileNames)

	// Delete the index from the database first
	err = db.Exec(ctx,
		"DELETE FROM indexes WHERE name=?",
		dropArgs.Name,
	)
	if err != nil {
		return fmt.Errorf("failed to delete index from database: %w", err)
	}

	// Get the operator for file operations
	operator, err := getOperator(ctx, basePath)
	if err != nil {
		return fmt.Errorf("failed to get operator: %w", err)
	}

	// Delete all index files concurrently
	var wg sync.WaitGroup
	for _, fileName := range fileNames {
		wg.Add(1)
		go func(fileName string) {
			defer wg.Done()
			if err := operator.Delete(ctx, fileName); err != nil {
				logrus.Warnf(
					"Failed to delete index file '%s': %v. "+
						"Don't worry, this just means the file is leaked, but will never be read from again.",
					fileName, err,
				)
			}
		}(fileName)
	}

	// Wait for all deletions to complete
	wg.Wait()

	logrus.Infof(
		"Dropped index: %s (%d number of index files)",
		dropArgs.Name, fileNamesLen,
	)

	return nil
}
