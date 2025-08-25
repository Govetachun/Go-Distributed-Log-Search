package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DBAdapter provides a unified interface for database operations
type DBAdapter interface {
	Exec(ctx context.Context, sql string, args ...interface{}) error
	Query(ctx context.Context, sql string, args ...interface{}) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) Row
	Close()
}

// Rows interface for database rows
type Rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close()
	Err() error
}

// Row interface for single database row
type Row interface {
	Scan(dest ...interface{}) error
}

// PostgreSQLAdapter wraps pgxpool.Pool
type PostgreSQLAdapter struct {
	pool *pgxpool.Pool
}

// NewPostgreSQLAdapter creates a new PostgreSQL adapter
func NewPostgreSQLAdapter(pool *pgxpool.Pool) *PostgreSQLAdapter {
	return &PostgreSQLAdapter{pool: pool}
}

func (p *PostgreSQLAdapter) Exec(ctx context.Context, sql string, args ...interface{}) error {
	_, err := p.pool.Exec(ctx, sql, args...)
	return err
}

func (p *PostgreSQLAdapter) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	rows, err := p.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRows{rows: rows}, nil
}

func (p *PostgreSQLAdapter) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	row := p.pool.QueryRow(ctx, sql, args...)
	return &pgxRow{row: row}
}

func (p *PostgreSQLAdapter) Close() {
	p.pool.Close()
}

// SQLiteAdapter wraps SQLiteDB
type SQLiteAdapter struct {
	db *SQLiteDB
}

// NewSQLiteAdapter creates a new SQLite adapter
func NewSQLiteAdapter(db *SQLiteDB) *SQLiteAdapter {
	return &SQLiteAdapter{db: db}
}

func (s *SQLiteAdapter) Exec(ctx context.Context, sql string, args ...interface{}) error {
	_, err := s.db.Exec(ctx, sql, args...)
	return err
}

func (s *SQLiteAdapter) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	rows, err := s.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &sqlRows{rows: rows}, nil
}

func (s *SQLiteAdapter) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	row := s.db.QueryRow(ctx, sql, args...)
	return &sqlRow{row: row}
}

func (s *SQLiteAdapter) Close() {
	s.db.Close()
}

// Row implementations
type pgxRow struct {
	row interface {
		Scan(dest ...interface{}) error
	}
}

func (r *pgxRow) Scan(dest ...interface{}) error {
	return r.row.Scan(dest...)
}

type sqlRow struct {
	row *sql.Row
}

func (r *sqlRow) Scan(dest ...interface{}) error {
	return r.row.Scan(dest...)
}

// Rows implementations
type pgxRows struct {
	rows interface {
		Next() bool
		Scan(dest ...interface{}) error
		Close()
		Err() error
	}
}

func (r *pgxRows) Next() bool {
	return r.rows.Next()
}

func (r *pgxRows) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

func (r *pgxRows) Close() {
	r.rows.Close()
}

func (r *pgxRows) Err() error {
	return r.rows.Err()
}

type sqlRows struct {
	rows *sql.Rows
}

func (r *sqlRows) Next() bool {
	return r.rows.Next()
}

func (r *sqlRows) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

func (r *sqlRows) Close() {
	r.rows.Close()
}

func (r *sqlRows) Err() error {
	return r.rows.Err()
}

// CreateDatabaseAdapter creates the appropriate database adapter based on the URL
func CreateDatabaseAdapter(ctx context.Context, dbURL string) (DBAdapter, error) {
	dbType, connStr, err := ParseDatabaseURL(dbURL)
	if err != nil {
		return nil, err
	}

	switch dbType {
	case "sqlite":
		db, err := NewSQLiteDB(ctx, connStr)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite connection: %w", err)
		}
		return NewSQLiteAdapter(db), nil

	case "postgres":
		config, err := pgxpool.ParseConfig(connStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse PostgreSQL URL: %w", err)
		}

		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			return nil, fmt.Errorf("failed to create PostgreSQL connection pool: %w", err)
		}

		return NewPostgreSQLAdapter(pool), nil

	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
}

