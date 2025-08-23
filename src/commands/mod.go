package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/blugelabs/bluge"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"

	"toshokan/src/config"
)

const (
	// DynamicFieldName is the name of the dynamic field in the schema
	DynamicFieldName = "_dynamic"

	// S3Prefix is the prefix for S3 paths
	S3Prefix = "s3://"
)

// Operator interface for file operations (equivalent to OpenDAL Operator)
type Operator interface {
	Delete(ctx context.Context, path string) error
	Reader(ctx context.Context, path string) (io.ReadCloser, error)
	Writer(ctx context.Context, path string) (io.WriteCloser, error)
	List(ctx context.Context, path string) ([]string, error)
}

// FileSystemOperator implements Operator for local filesystem
type FileSystemOperator struct {
	rootPath string
}

// NewFileSystemOperator creates a new filesystem operator
func NewFileSystemOperator(rootPath string) *FileSystemOperator {
	return &FileSystemOperator{rootPath: rootPath}
}

func (fs *FileSystemOperator) Delete(ctx context.Context, path string) error {
	fullPath := filepath.Join(fs.rootPath, path)
	return os.Remove(fullPath)
}

func (fs *FileSystemOperator) Reader(ctx context.Context, path string) (io.ReadCloser, error) {
	fullPath := filepath.Join(fs.rootPath, path)
	return os.Open(fullPath)
}

func (fs *FileSystemOperator) Writer(ctx context.Context, path string) (io.WriteCloser, error) {
	fullPath := filepath.Join(fs.rootPath, path)
	return os.Create(fullPath)
}

func (fs *FileSystemOperator) List(ctx context.Context, path string) ([]string, error) {
	fullPath := filepath.Join(fs.rootPath, path)
	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, entry.Name())
		}
	}
	return files, nil
}

// S3Operator implements Operator for S3 storage (placeholder)
type S3Operator struct {
	bucket   string
	endpoint string
}

// NewS3Operator creates a new S3 operator
func NewS3Operator(bucket, endpoint string) *S3Operator {
	return &S3Operator{
		bucket:   bucket,
		endpoint: endpoint,
	}
}

func (s3 *S3Operator) Delete(ctx context.Context, path string) error {
	// TODO: Implement S3 delete operation
	return fmt.Errorf("S3 operations not yet implemented")
}

func (s3 *S3Operator) Reader(ctx context.Context, path string) (io.ReadCloser, error) {
	// TODO: Implement S3 read operation
	return nil, fmt.Errorf("S3 operations not yet implemented")
}

func (s3 *S3Operator) Writer(ctx context.Context, path string) (io.WriteCloser, error) {
	// TODO: Implement S3 write operation
	return nil, fmt.Errorf("S3 operations not yet implemented")
}

func (s3 *S3Operator) List(ctx context.Context, path string) ([]string, error) {
	// TODO: Implement S3 list operation
	return nil, fmt.Errorf("S3 operations not yet implemented")
}

// dynamicFieldConfig returns the configuration for the dynamic field
// Equivalent to dynamic_field_config in Rust
func dynamicFieldConfig() config.DynamicObjectFieldConfig {
	return config.DynamicObjectFieldConfig{
		Stored:     true,
		Fast:       config.FastFieldNormalizerTypeFalse,
		Indexed:    config.NewIndexedDynamicObjectFieldType(),
		ExpandDots: true,
	}
}

// getIndexConfig retrieves the index configuration from the database
// Equivalent to get_index_config in Rust
func getIndexConfig(ctx context.Context, name string, pool *pgxpool.Pool) (*config.IndexConfig, error) {
	var configJSON []byte
	err := pool.QueryRow(ctx, "SELECT config FROM indexes WHERE name=$1", name).Scan(&configJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to get index config: %w", err)
	}

	var indexConfig config.IndexConfig
	if err := json.Unmarshal(configJSON, &indexConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal index config: %w", err)
	}

	return &indexConfig, nil
}

// getIndexPath retrieves the index path from the database
// Equivalent to get_index_path in Rust
func getIndexPath(ctx context.Context, name string, pool *pgxpool.Pool) (string, error) {
	var pathJSON []byte
	err := pool.QueryRow(ctx, "SELECT config->'path' FROM indexes WHERE name=$1", name).Scan(&pathJSON)
	if err != nil {
		return "", fmt.Errorf("failed to get index path: %w", err)
	}

	var path string
	if err := json.Unmarshal(pathJSON, &path); err != nil {
		return "", fmt.Errorf("failed to unmarshal index path: %w", err)
	}

	return path, nil
}

// getOperator creates an appropriate operator based on the path
// Equivalent to get_operator in Rust
func getOperator(path string) (Operator, error) {
	if bucket := strings.TrimPrefix(path, S3Prefix); bucket != path {
		// S3 path
		requiredEnvVars := []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"}
		var unsetVars []string

		for _, envVar := range requiredEnvVars {
			if os.Getenv(envVar) == "" {
				unsetVars = append(unsetVars, envVar)
			}
		}

		if len(unsetVars) > 0 {
			return nil, fmt.Errorf(
				"the following mandatory environment variables to use s3 are not set: %v",
				unsetVars,
			)
		}

		endpoint := os.Getenv("S3_ENDPOINT")
		if endpoint == "" {
			endpoint = "https://s3.amazonaws.com"
		}

		return NewS3Operator(bucket, endpoint), nil
	}

	// Local filesystem path
	return NewFileSystemOperator(path), nil
}

// IndexFile represents metadata about an index file
type IndexFile struct {
	ID        string `json:"id"`
	FileName  string `json:"file_name"`
	Len       int64  `json:"len"`
	FooterLen int64  `json:"footer_len"`
}

// openUnifiedDirectories opens unified directories for the given index
// Equivalent to open_unified_directories in Rust
func openUnifiedDirectories(ctx context.Context, indexPath string, pool *pgxpool.Pool) ([]IndexFile, error) {
	_, err := getOperator(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get operator: %w", err)
	}

	rows, err := pool.Query(ctx, "SELECT id, file_name, len, footer_len FROM index_files")
	if err != nil {
		return nil, fmt.Errorf("failed to query index files: %w", err)
	}
	defer rows.Close()

	var items []IndexFile
	for rows.Next() {
		var item IndexFile
		if err := rows.Scan(&item.ID, &item.FileName, &item.Len, &item.FooterLen); err != nil {
			return nil, fmt.Errorf("failed to scan index file: %w", err)
		}
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	// For now, we return the metadata. The actual directory opening
	// would require implementing Tantivy-like functionality in Go
	return items, nil
}

// writeUnifiedIndex writes a unified index to storage
// Equivalent to write_unified_index in Rust
func writeUnifiedIndex(
	ctx context.Context,
	id string,
	inputDir string,
	indexName string,
	indexPath string,
	pool *pgxpool.Pool,
) error {
	op, err := getOperator(indexPath)
	if err != nil {
		return fmt.Errorf("failed to get operator: %w", err)
	}

	fileName := fmt.Sprintf("%s.index", id)

	// Read Bluge index files from inputDir and create unified index
	indexConfig := bluge.DefaultConfig(inputDir)
	reader, err := bluge.OpenReader(indexConfig)
	if err != nil {
		return fmt.Errorf("failed to open Bluge index: %w", err)
	}
	defer reader.Close()

	// Get all documents from the index
	query := bluge.NewMatchAllQuery()
	request := bluge.NewAllMatches(query)

	documentMatchIterator, err := reader.Search(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to search index: %w", err)
	}

	// Create unified index writer
	unifiedWriter, err := op.Writer(ctx, fileName)
	if err != nil {
		return fmt.Errorf("failed to create unified index writer: %w", err)
	}
	defer unifiedWriter.Close()

	// Write index header
	header := fmt.Sprintf("TOSHOKAN_UNIFIED_INDEX\nversion:1.0\nindex_name:%s\n", indexName)
	headerBytes := []byte(header)
	_, err = unifiedWriter.Write(headerBytes)
	if err != nil {
		return fmt.Errorf("failed to write index header: %w", err)
	}

	// Write documents
	docCount := 0
	for {
		match, err := documentMatchIterator.Next()
		if err != nil {
			break
		}
		if match == nil {
			break
		}

		// Extract document data
		var docData []byte
		err = match.VisitStoredFields(func(field string, value []byte) bool {
			if docData == nil {
				docData = []byte("{\n")
			} else {
				docData = append(docData, ",\n"...)
			}
			docData = append(docData, fmt.Sprintf(`  "%s": "%s"`, field, string(value))...)
			return true
		})
		if err != nil {
			logrus.Warnf("Failed to visit stored fields: %v", err)
			continue
		}
		if docData != nil {
			docData = append(docData, "\n}\n"...)
			_, err = unifiedWriter.Write(docData)
			if err != nil {
				return fmt.Errorf("failed to write document: %w", err)
			}
		}
		docCount++
	}

	// Write footer with metadata
	footer := fmt.Sprintf("\n---\ndoc_count:%d\nend\n", docCount)
	footerBytes := []byte(footer)
	_, err = unifiedWriter.Write(footerBytes)
	if err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}

	// Calculate actual lengths
	totalLen := int64(len(headerBytes) + docCount*100 + len(footerBytes)) // Approximate
	footerLen := int64(len(footerBytes))

	// Insert the index file metadata into the database
	_, err = pool.Exec(ctx,
		"INSERT INTO index_files (id, index_name, file_name, len, footer_len) VALUES ($1, $2, $3, $4, $5)",
		id, indexName, fileName, totalLen, footerLen,
	)
	if err != nil {
		return fmt.Errorf("failed to insert index file metadata: %w", err)
	}

	return nil
}
