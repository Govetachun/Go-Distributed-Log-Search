package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	"toshokan/src/args"
	"toshokan/src/config"
	"toshokan/src/database"
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
func runSearchWithCallback(
	ctx context.Context,
	searchArgs *args.SearchArgs,
	db database.DBAdapter,
	onDocFn func(string),
) error {
	if searchArgs.Limit == 0 {
		return nil
	}

	// Get index configuration
	indexConfig, err := getIndexConfig(ctx, searchArgs.Name, db)
	if err != nil {
		return fmt.Errorf("failed to get index config: %w", err)
	}

	// Build indexed field list
	indexedFields := getIndexedFields(indexConfig.Schema.Fields)

	// Get index files
	indexFiles, err := openUnifiedDirectories(ctx, indexConfig.Path, db)
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
			results, err := performSearchOnIndexFile(ctx, file, searchArgs.Query, indexedFields, indexConfig.Path)
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

// performSearchOnIndexFile performs search on a single unified index file
func performSearchOnIndexFile(
	ctx context.Context,
	indexFile IndexFile,
	query string,
	indexedFields []config.FieldConfig,
	indexPath string,
) ([]SearchResult, error) {
	logrus.Debugf("Searching index file %s with query: %s", indexFile.FileName, query)

	// Get the operator to read the unified index file
	op, err := getOperator(ctx, indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get operator: %w", err)
	}

	// Read the unified index file
	reader, err := op.Reader(ctx, indexFile.FileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file %s: %w", indexFile.FileName, err)
	}
	defer reader.Close()

	// Read the entire file content
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read index file %s: %w", indexFile.FileName, err)
	}

	// Parse the unified index format
	lines := strings.Split(string(content), "\n")
	var results []SearchResult
	var currentDoc strings.Builder
	inDocument := false

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip header and footer lines
		if strings.HasPrefix(line, "TOSHOKAN_UNIFIED_INDEX") ||
			strings.HasPrefix(line, "version:") ||
			strings.HasPrefix(line, "index_name:") ||
			strings.HasPrefix(line, "---") ||
			strings.HasPrefix(line, "doc_count:") ||
			strings.HasPrefix(line, "end") {
			continue
		}

		// Start of a document
		if line == "{" {
			inDocument = true
			currentDoc.Reset()
			currentDoc.WriteString(line)
			continue
		}

		// End of a document
		if line == "}" && inDocument {
			currentDoc.WriteString(line)
			docJSON := currentDoc.String()

			// Parse the document JSON
			var doc map[string]interface{}
			if err := json.Unmarshal([]byte(docJSON), &doc); err != nil {
				logrus.Warnf("Failed to parse document JSON: %v", err)
				continue
			}

			// Simple text search in document fields
			score := 0.0
			queryLower := strings.ToLower(query)

			// Search in title, content, tags, and other text fields
			if title, ok := doc["title"].(string); ok {
				if strings.Contains(strings.ToLower(title), queryLower) {
					score += 10.0 // High score for title matches
				}
			}

			if content, ok := doc["content"].(string); ok {
				if strings.Contains(strings.ToLower(content), queryLower) {
					score += 5.0 // Medium score for content matches
				}
			}

			if tags, ok := doc["tags"].(string); ok {
				if strings.Contains(strings.ToLower(tags), queryLower) {
					score += 4.0 // Good score for tag matches
				}
			}

			if author, ok := doc["author"].(string); ok {
				if strings.Contains(strings.ToLower(author), queryLower) {
					score += 3.0 // Lower score for author matches
				}
			}

			// Only include documents with matches
			if score > 0 {
				result := SearchResult{
					Score:    score,
					Document: doc,
				}
				results = append(results, result)
			}

			inDocument = false
			continue
		}

		// Add line to current document
		if inDocument {
			currentDoc.WriteString(line)
		}
	}

	return results, nil
}

// getIndexedFields filters and returns only indexed fields
func getIndexedFields(fields config.FieldConfigs) []config.FieldConfig {
	var indexedFields []config.FieldConfig

	for _, field := range fields {
		if field.TypeImpl != nil && field.TypeImpl.IsIndexed() {
			indexedFields = append(indexedFields, field)
		}
	}

	// Add the dynamic field
	indexedFields = append(indexedFields, config.FieldConfig{
		Name:     DynamicFieldName,
		Array:    false,
		Type:     "dynamic_object",
		TypeImpl: &config.FieldTypeDynamicObject{Config: *config.NewDynamicObjectFieldConfig()},
	})

	return indexedFields
}

// getPrettifiedJSON converts a document to prettified JSON format
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
func RunSearch(ctx context.Context, searchArgs *args.SearchArgs, db database.DBAdapter) error {
	return runSearchWithCallback(
		ctx,
		searchArgs,
		db,
		func(doc string) {
			fmt.Println(doc)
		},
	)
}
