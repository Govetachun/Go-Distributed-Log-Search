package commands

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"

	"toshokan/src/args"
)

// RunDrop executes the drop command
// Equivalent to run_drop function in Rust
func RunDrop(ctx context.Context, dropArgs *args.DropArgs, pool *pgxpool.Pool) error {
	// Get the base path for the index
	basePath, err := getIndexPath(ctx, dropArgs.Name, pool)
	if err != nil {
		return fmt.Errorf("failed to get index path: %w", err)
	}

	// Get all file names associated with this index
	rows, err := pool.Query(ctx,
		"SELECT file_name FROM index_files WHERE index_name=$1",
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
	_, err = pool.Exec(ctx,
		"DELETE FROM indexes WHERE name=$1",
		dropArgs.Name,
	)
	if err != nil {
		return fmt.Errorf("failed to delete index from database: %w", err)
	}

	// Get the operator for file operations
	operator, err := getOperator(basePath)
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
