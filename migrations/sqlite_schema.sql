-- SQLite schema for Toshokan (converted from PostgreSQL)
CREATE TABLE IF NOT EXISTS indexes(
    name TEXT PRIMARY KEY,
    config TEXT NOT NULL -- Changed from JSONB to TEXT for SQLite
);

CREATE TABLE IF NOT EXISTS index_files(
    id TEXT PRIMARY KEY, -- Changed from VARCHAR(36) to TEXT for SQLite
    index_name TEXT NOT NULL,
    file_name TEXT NOT NULL,
    len INTEGER NOT NULL, -- Changed from BIGINT to INTEGER for SQLite
    footer_len INTEGER NOT NULL, -- Changed from BIGINT to INTEGER for SQLite
    FOREIGN KEY (index_name) REFERENCES indexes(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS kafka_checkpoints(
    source_id TEXT NOT NULL,
    partition INTEGER NOT NULL, -- Changed from INT to INTEGER for SQLite
    offset_value INTEGER NOT NULL, -- Changed from BIGINT to INTEGER for SQLite
    
    PRIMARY KEY (source_id, partition)
);

