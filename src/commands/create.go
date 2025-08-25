package commands

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"

	"toshokan/src/args"
	"toshokan/src/config"
	"toshokan/src/database"
)

// RunCreate executes the create command
// Equivalent to run_create function in Rust
func RunCreate(ctx context.Context, createArgs *args.CreateArgs, db database.DBAdapter) error {
	indexConfig, err := config.LoadIndexConfigFromPath(createArgs.ConfigPath)
	if err != nil {
		return fmt.Errorf("failed to load config from path %s: %w", createArgs.ConfigPath, err)
	}

	return RunCreateFromConfig(ctx, indexConfig, db)
}

// RunCreateFromConfig creates an index from a config object
// Equivalent to run_create_from_config function in Rust
func RunCreateFromConfig(ctx context.Context, indexConfig *config.IndexConfig, db database.DBAdapter) error {
	// Check for unsupported array of static objects
	arrayStaticObjectExists := false
	for _, field := range indexConfig.Schema.Fields {
		if field.Array {
			if _, ok := field.TypeImpl.(config.FieldTypeStaticObject); ok {
				arrayStaticObjectExists = true
				break
			}
		}
	}

	if arrayStaticObjectExists {
		return fmt.Errorf("array of static objects are currently unsupported")
	}

	// Convert config to JSON for storage
	configJSON, err := json.Marshal(indexConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal config to JSON: %w", err)
	}

	// Insert index into database
	err = db.Exec(ctx,
		"INSERT INTO indexes (name, config) VALUES (?, ?)",
		indexConfig.Name,
		configJSON,
	)
	if err != nil {
		return fmt.Errorf("failed to insert index into database: %w", err)
	}

	logrus.Infof("Created index: %s", indexConfig.Name)

	return nil
}
