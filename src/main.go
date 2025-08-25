package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"

	"toshokan/src/args"
	"toshokan/src/commands"
	"toshokan/src/database"
)

const (
	// Default log levels
	defaultDebugLogLevel   = "debug"
	defaultReleaseLogLevel = "info"

	// Maximum database connections
	maxDBConnections = 100
)

// openDB creates a new database connection
func openDB(ctx context.Context, url string) (database.DBAdapter, error) {
	return database.CreateDatabaseAdapter(ctx, url)
}

// asyncMain is the main async function that handles the application logic
func asyncMain(ctx context.Context, arguments *args.Args) error {
	// Get database URL from args or environment
	dbURL := arguments.DB
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
		if dbURL == "" {
			return fmt.Errorf("database url must be provided using either --db or DATABASE_URL env var")
		}
	}

	// Open database connection
	db, err := openDB(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Handle subcommands
	switch arguments.SubCmd.Name {
	case "":
		// No subcommand provided, show help
		return nil
	case "create":
		createArgs := arguments.SubCmd.CreateArgs
		return commands.RunCreate(ctx, createArgs, db)
	case "drop":
		dropArgs := arguments.SubCmd.DropArgs
		return commands.RunDrop(ctx, dropArgs, db)
	case "index":
		indexArgs := arguments.SubCmd.IndexArgs
		return commands.RunIndex(ctx, indexArgs, db)
	case "merge":
		mergeArgs := arguments.SubCmd.MergeArgs
		return commands.RunMerge(ctx, mergeArgs, db)
	case "search":
		searchArgs := arguments.SubCmd.SearchArgs
		return commands.RunSearch(ctx, searchArgs, db)
	default:
		return fmt.Errorf("unknown subcommand: %s", arguments.SubCmd.Name)
	}
}

// setupLogging configures the logging system
func setupLogging() {
	// Determine default log level based on build mode
	var defaultLogLevel string
	if os.Getenv("DEBUG") == "true" {
		defaultLogLevel = defaultDebugLogLevel
	} else {
		defaultLogLevel = defaultReleaseLogLevel
	}

	// Get log level from environment or use default
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = defaultLogLevel
	}

	// Set log level
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		logrus.Warnf("Invalid log level '%s', using info level", logLevel)
		level = logrus.InfoLevel
	}
	logrus.SetLevel(level)

	// Set log format with timestamp
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
}

func main() {
	// Setup error handling and logging
	setupLogging()

	// Load environment variables from .env file if it exists
	_ = godotenv.Load()

	// Parse command line arguments
	arguments, err := args.ParseArgs()
	if err != nil {
		log.Fatalf("Failed to parse arguments: %v", err)
	}

	// Create context for the application
	ctx := context.Background()

	// Run the main application logic
	if err := asyncMain(ctx, arguments); err != nil {
		logrus.Fatalf("Application error: %v", err)
	}
}
