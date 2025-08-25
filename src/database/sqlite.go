package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	_ "modernc.org/sqlite" // SQLite driver
)

// SQLiteDB wraps SQLite database operations to provide a pgxpool.Pool-like interface
type SQLiteDB struct {
	db *sql.DB
}

// NewSQLiteDB creates a new SQLite database connection
func NewSQLiteDB(ctx context.Context, dbPath string) (*SQLiteDB, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping SQLite database: %w", err)
	}

	sqliteDB := &SQLiteDB{db: db}

	// Initialize schema
	if err := sqliteDB.initSchema(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return sqliteDB, nil
}

// initSchema initializes the SQLite schema
func (s *SQLiteDB) initSchema(ctx context.Context) error {
	// Read schema from migrations/sqlite_schema.sql
	schemaPath := "migrations/sqlite_schema.sql"

	// Try current directory first, then parent directories
	var schemaSQL string
	for i := 0; i < 3; i++ {
		prefix := strings.Repeat("../", i)
		fullPath := filepath.Join(prefix, schemaPath)

		if data, err := os.ReadFile(fullPath); err == nil {
			schemaSQL = string(data)
			break
		}
	}

	if schemaSQL == "" {
		// Fallback to embedded schema
		schemaSQL = `
CREATE TABLE IF NOT EXISTS indexes(
    name TEXT PRIMARY KEY,
    config TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS index_files(
    id TEXT PRIMARY KEY,
    index_name TEXT NOT NULL,
    file_name TEXT NOT NULL,
    len INTEGER NOT NULL,
    footer_len INTEGER NOT NULL,
    FOREIGN KEY (index_name) REFERENCES indexes(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS kafka_checkpoints(
    source_id TEXT NOT NULL,
    partition INTEGER NOT NULL,
    offset_value INTEGER NOT NULL,
    
    PRIMARY KEY (source_id, partition)
);`
	}

	_, err := s.db.ExecContext(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("failed to execute schema: %w", err)
	}

	logrus.Info("SQLite schema initialized successfully")
	return nil
}

// Exec executes a SQL command
func (s *SQLiteDB) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return s.db.ExecContext(ctx, sql, args...)
}

// Query executes a SQL query
func (s *SQLiteDB) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, sql, args...)
}

// QueryRow executes a SQL query that returns a single row
func (s *SQLiteDB) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return s.db.QueryRowContext(ctx, sql, args...)
}

// Close closes the database connection
func (s *SQLiteDB) Close() {
	if s.db != nil {
		s.db.Close()
	}
}

// ParseDatabaseURL parses a database URL and returns the appropriate connection
func ParseDatabaseURL(dbURL string) (string, string, error) {
	if strings.HasPrefix(dbURL, "sqlite:") {
		return "sqlite", strings.TrimPrefix(dbURL, "sqlite:"), nil
	} else if strings.HasPrefix(dbURL, "postgres://") || strings.HasPrefix(dbURL, "postgresql://") {
		return "postgres", dbURL, nil
	}

	return "", "", fmt.Errorf("unsupported database URL format: %s", dbURL)
}

