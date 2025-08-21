package commands

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"

	"toshokan/src/args"
	"toshokan/src/config"
)

// RunCreate executes the create command
// Equivalent to run_create function in Rust
func RunCreate(ctx context.Context, createArgs *args.CreateArgs, pool *pgxpool.Pool) error {
	indexConfig, err := config.LoadIndexConfigFromPath(createArgs.ConfigPath)
	if err != nil {
		return fmt.Errorf("failed to load config from path %s: %w", createArgs.ConfigPath, err)
	}

	return RunCreateFromConfig(ctx, indexConfig, pool)
}

// RunCreateFromConfig creates an index from a config object
// Equivalent to run_create_from_config function in Rust
func RunCreateFromConfig(ctx context.Context, indexConfig *config.IndexConfig, pool *pgxpool.Pool) error {
	// Check for unsupported array of static objects
	arrayStaticObjectExists := false
	for _, field := range indexConfig.Schema.Fields {
		if field.Array && field.Type.IsStaticObject() {
			arrayStaticObjectExists = true
			break
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
	_, err = pool.Exec(ctx,
		"INSERT INTO indexes (name, config) VALUES ($1, $2)",
		indexConfig.Name,
		configJSON,
	)
	if err != nil {
		return fmt.Errorf("failed to insert index into database: %w", err)
	}

	logrus.Infof("Created index: %s", indexConfig.Name)

	return nil
}
