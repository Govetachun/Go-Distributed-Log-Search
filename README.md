# Distributed Log Search on S3 storage with Golang

A simple search engine implementation in Go that stores indexes in MinIO (S3-compatible storage) and uses SQLite for metadata.

This is inspired from [Toshokan](https://github.com/tontinton/toshokan/) which written in Rust
## Features

- **Index Creation**: Create search indexes from YAML configuration
- **Document Indexing**: Index JSONL documents with streaming support
- **Search**: Search indexes and display results in console
- **MinIO Storage**: Store index files in S3-compatible object storage
- **SQLite Database**: Store metadata in SQLite database

## Prerequisites

- Go 1.21+
- Docker (for MinIO)
- SQLite

## Quick Start

### 1. Start MinIO

```bash
# Start MinIO container
docker-compose up -d

# Create bucket (optional - will be created automatically)
docker exec minio mc mb myminio/toshokan-test
```

### 2. Build the Application

```bash
go build -o toshokan src/main.go
```

### 3. Create an Index

```bash
./run_toshokan.sh --db "sqlite:./test.db" create tests/config.yaml
```

### 4. Index Documents

```bash
./run_toshokan.sh --db "sqlite:./test.db" index test_index tests/sample_data.jsonl
```

### 5. Search

```bash
./run_toshokan.sh --db "sqlite:./test.db" search test_index "machine learning" --limit 3
```

## Configuration

### Environment Variables

The `run_toshokan.sh` script automatically loads these environment variables:

```bash
# MinIO Configuration
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=password
AWS_REGION=us-east-1
S3_ENDPOINT=http://localhost:9000

# Database
DATABASE_URL=sqlite:./test.db
```

### Index Configuration

Example `tests/config.yaml`:

```yaml
name: "test_index"
path: "s3://toshokan-test"
schema:
  fields:
    - name: "id"
      type: "text"
      stored: true
      indexed:
        tokenizer: "keyword"
        record: "position"
      fast: false
    - name: "title"
      type: "text"
      stored: true
      indexed:
        tokenizer: "default"
        record: "position"
      fast: false
    - name: "content"
      type: "text"
      stored: true
      indexed:
        tokenizer: "default"
        record: "position"
      fast: false
```

## Commands

### Create Index

```bash
./run_toshokan.sh --db "sqlite:./test.db" create config.yaml
```

### Index Documents

```bash
./run_toshokan.sh --db "sqlite:./test.db" index index_name documents.jsonl
```

### Search

```bash
./run_toshokan.sh --db "sqlite:./test.db" search index_name "query" --limit 10
```

### Drop Index

```bash
./run_toshokan.sh --db "sqlite:./test.db" drop index_name
```

## Sample Data

Example `tests/sample_data.jsonl`:

```json
{"id": "doc1", "title": "Introduction to Machine Learning", "content": "Machine learning is a subset of artificial intelligence.", "author": "John Doe", "category": "technology", "tags": ["AI", "ML"], "published_date": "2024-01-15", "rating": 4.5}
{"id": "doc2", "title": "Web Development with Go", "content": "Go is an excellent language for building web applications.", "author": "Alice Brown", "category": "programming", "tags": ["golang", "web"], "published_date": "2024-01-25", "rating": 4.6}
```

## Project Structure

```
toshokan-go/
├── src/
│   ├── main.go              # Application entry point
│   ├── args/                # CLI argument parsing
│   ├── commands/            # Command implementations
│   ├── config/              # Configuration handling
│   ├── database/            # Database adapters (SQLite/PostgreSQL)
│   └── s3/                  # MinIO/S3 operations
├── tests/
│   ├── config.yaml          # Sample index configuration
│   ├── sample_data.jsonl    # Sample documents
│   └── env.test             # Environment variables
├── migrations/              # Database schema
├── run_toshokan.sh          # Wrapper script with environment
└── docker-compose.yml       # MinIO setup
```

## Development

### Running Tests

```bash
# Quick test
./run_toshokan.sh --db "sqlite:./test.db" search test_index "test query"

# Full workflow test
./run_toshokan.sh --db "sqlite:./test.db" create tests/config.yaml
./run_toshokan.sh --db "sqlite:./test.db" index test_index tests/sample_data.jsonl
./run_toshokan.sh --db "sqlite:./test.db" search test_index "machine learning"
```

### Check MinIO Contents

```bash
# List files in bucket
docker exec minio mc ls myminio/toshokan-test

# View file contents
docker exec minio mc cat myminio/toshokan-test/filename.index
```

## Troubleshooting

### Environment Variables Not Loading

Make sure to use the wrapper script:

```bash
# Correct
./run_toshokan.sh --db "sqlite:./test.db" search test_index "query"

# Wrong - environment variables won't be loaded
./toshokan --db "sqlite:./test.db" search test_index "query"
```

### MinIO Connection Issues

Check MinIO is running:

```bash
docker ps | grep minio
docker logs minio
```

### Database Issues

Check SQLite database:

```bash
sqlite3 test.db ".tables"
sqlite3 test.db "SELECT * FROM indexes;"
```





