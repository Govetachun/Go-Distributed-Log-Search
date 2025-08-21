package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/blugelabs/bluge"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"

	"toshokan/src/args"
	"toshokan/src/config"
)

// SearchResult represents a search result document
type SearchResult struct {
	Score    float64                `json:"score"`
	Document map[string]interface{} `json:"document"`
}

// SearchTask represents a search task for a single index file
type SearchTask struct {
	IndexFile IndexFile
	Query     string
	Limit     int
}

// SearchTaskResult represents the result of a search task
type SearchTaskResult struct {
	Results []SearchResult
	Error   error
}

// runSearchWithCallback executes a search with a callback function for each result
// Equivalent to run_search_with_callback in Rust
func runSearchWithCallback(
	ctx context.Context,
	searchArgs *args.SearchArgs,
	pool *pgxpool.Pool,
	onDocFn func(string),
) error {
	if searchArgs.Limit == 0 {
		return nil
	}

	// Get index configuration
	indexConfig, err := getIndexConfig(ctx, searchArgs.Name, pool)
	if err != nil {
		return fmt.Errorf("failed to get index config: %w", err)
	}

	// Build indexed field list
	indexedFields := getIndexedFields(indexConfig.Schema.Fields)

	// Get index files
	indexFiles, err := openUnifiedDirectories(ctx, indexConfig.Path, pool)
	if err != nil {
		return fmt.Errorf("failed to open unified directories: %w", err)
	}

	if len(indexFiles) == 0 {
		logrus.Infof("No index files for '%s'", searchArgs.Name)
		return nil
	}

	// Create channels for communication between goroutines
	resultChan := make(chan SearchResult, searchArgs.Limit)
	doneChan := make(chan struct{})

	// Start result collector goroutine
	go func() {
		defer close(doneChan)
		count := 0
		var results []SearchResult

		for result := range resultChan {
			results = append(results, result)
			count++
			if count >= searchArgs.Limit {
				break
			}
		}

		// Sort results by score (descending)
		sort.Slice(results, func(i, j int) bool {
			return results[i].Score > results[j].Score
		})

		// Send results to callback
		for i, result := range results {
			if i >= searchArgs.Limit {
				break
			}

			docJSON, err := json.Marshal(result.Document)
			if err != nil {
				logrus.Errorf("Failed to marshal document: %v", err)
				continue
			}

			onDocFn(string(docJSON))
		}
	}()

	// Create search tasks for each index file
	var wg sync.WaitGroup
	for _, indexFile := range indexFiles {
		wg.Add(1)
		go func(file IndexFile) {
			defer wg.Done()
			results, err := performSearchOnIndexFile(ctx, file, searchArgs.Query, indexedFields)
			if err != nil {
				logrus.Errorf("Error in search task for file %s: %v", file.FileName, err)
				return
			}

			// Send results to the result channel
			for _, result := range results {
				select {
				case resultChan <- result:
				case <-ctx.Done():
					return
				}
			}
		}(indexFile)
	}

	// Wait for all search tasks to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Wait for result collection to complete
	select {
	case <-doneChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// performSearchOnIndexFile performs search on a single index file using Bluge
func performSearchOnIndexFile(
	ctx context.Context,
	indexFile IndexFile,
	query string,
	indexedFields []config.FieldConfig,
) ([]SearchResult, error) {
	logrus.Debugf("Searching index file %s with query: %s", indexFile.FileName, query)

	// Open the Bluge index
	indexPath := filepath.Join("/tmp/toshokan_build", indexFile.ID)
	indexConfig := bluge.DefaultConfig(indexPath)

	reader, err := bluge.OpenReader(indexConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to open index %s: %w", indexPath, err)
	}
	defer reader.Close()

	// Create query (using term query for simplicity)
	blugeQuery := bluge.NewTermQuery(query).SetField("_all")

	// Create search request
	request := bluge.NewTopNSearch(1000, blugeQuery)

	// Execute search
	documentMatchIterator, err := reader.Search(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("failed to execute search: %w", err)
	}

	var results []SearchResult
	for {
		match, err := documentMatchIterator.Next()
		if err != nil {
			break
		}
		if match == nil {
			break
		}

		// Extract document fields
		doc := make(map[string]interface{})
		err = match.VisitStoredFields(func(field string, value []byte) bool {
			doc[field] = string(value)
			return true
		})
		if err != nil {
			logrus.Warnf("Failed to visit stored fields: %v", err)
			continue
		}

		result := SearchResult{
			Score:    match.Score,
			Document: doc,
		}
		results = append(results, result)
	}

	return results, nil
}

// getIndexedFields filters and returns only indexed fields
func getIndexedFields(fields config.FieldConfigs) []config.FieldConfig {
	var indexedFields []config.FieldConfig

	for _, field := range fields {
		if field.Type.IsIndexed() {
			indexedFields = append(indexedFields, field)
		}
	}

	// Add the dynamic field
	indexedFields = append(indexedFields, config.FieldConfig{
		Name:  DynamicFieldName,
		Array: false,
		Type:  config.DynamicObjectFieldType{Config: dynamicFieldConfig()},
	})

	return indexedFields
}

// getPrettifiedJSON converts a document to prettified JSON format
// Equivalent to get_prettified_json in Rust
func getPrettifiedJSON(
	doc map[string]interface{},
	fields []config.FieldConfig,
) (string, error) {
	prettifiedFieldMap := make(map[string]interface{})

	for _, field := range fields {
		value, exists := doc[field.Name]
		if !exists {
			continue
		}

		if field.Array {
			prettifiedFieldMap[field.Name] = value
			continue
		}

		if field.Name == DynamicFieldName {
			// Handle dynamic field - merge its contents into the root
			if objectValue, ok := value.(map[string]interface{}); ok {
				for k, v := range objectValue {
					prettifiedFieldMap[k] = v
				}
			}
			continue
		}

		// Handle nested object fields (static objects with dot notation)
		names := config.SplitObjectFieldName(field.Name)
		if len(names) <= 1 {
			prettifiedFieldMap[field.Name] = value
			continue
		}

		// Prettify static object with inner fields like {"hello.world": 1}
		// to look like: {"hello": {"world": 1}}
		current := prettifiedFieldMap
		for i, name := range names[:len(names)-1] {
			unescapedName := config.UnescapedFieldName(name)
			if current[unescapedName] == nil {
				current[unescapedName] = make(map[string]interface{})
			}

			if innerMap, ok := current[unescapedName].(map[string]interface{}); ok {
				current = innerMap
			} else {
				// Invalid state - this shouldn't happen
				return "", fmt.Errorf("invalid state: expected object at %s", strings.Join(names[:i+1], "."))
			}
		}

		finalName := config.UnescapedFieldName(names[len(names)-1])
		current[finalName] = value
	}

	jsonBytes, err := json.Marshal(prettifiedFieldMap)
	if err != nil {
		return "", fmt.Errorf("failed to marshal prettified document: %w", err)
	}

	return string(jsonBytes), nil
}

// RunSearch executes the search command
// Equivalent to run_search function in Rust
func RunSearch(ctx context.Context, searchArgs *args.SearchArgs, pool *pgxpool.Pool) error {
	return runSearchWithCallback(
		ctx,
		searchArgs,
		pool,
		func(doc string) {
			fmt.Println(doc)
		},
	)
}
