# DuckPond

A self-hosted data warehouse for teams who need fast analytics without the enterprise overhead. Upload files, stream metrics, and query with SQL using DuckDB's columnar engine.

## What it does

DuckPond is a multi-account analytics platform you run on your own infrastructure:

- Upload CSV, JSON, or Parquet files that get auto-converted to efficient Parquet storage
- Stream data using Arrow IPC or Prometheus remote write protocol
- Query everything with SQL through DuckDB's fast analytical engine
- Give each customer their own isolated account with a DuckLake catalog
- No cloud fees, no vendor lock-in, no per-query billing

Think of it as the data warehouse you'd build yourself if you had time, but without spending six months on it.

## Why use this

You should consider DuckPond if:

- You're building SaaS analytics and don't want Snowflake bills eating your margins
- You have gigabytes of data, not petabytes, and distributed systems seem excessive
- You want multi-account isolation without configuring complex RBAC systems
- You need to store Prometheus metrics longer than two weeks
- You prefer owning your infrastructure over monthly SaaS subscriptions


## How it works

Each account gets:
- Their own DuckLake catalog stored in SQLite (or PostgreSQL for production)
- Isolated storage path for Parquet files
- API keys for authentication
- Configurable storage quotas

When you upload files:
1. Files are validated and converted to Parquet format
2. Parquet files are stored in the account's storage path
3. Tables are registered in the account's DuckLake catalog
4. Data becomes queryable immediately via SQL

When you run queries:
1. DuckDB loads the Parquet files from the catalog
2. Queries run in-process using columnar execution
3. Results export as JSON, CSV, or Parquet

## Architecture

Built with:
- FastAPI for the REST API
- DuckDB for query execution
- DuckLake for ACID-compliant catalogs
- PyArrow for streaming and zero-copy operations
- SQLite or PostgreSQL for metadata
- Local filesystem for storage (S3 support planned)


## Performance expectations

On typical hardware (Macbook Air M1):
- Query latency: under 100ms for million-row tables
- Streaming ingestion: 100K+ records per second
- CSV to Parquet: around 50MB per second
- Concurrent queries: 100+ per instance

These are single-machine numbers. No cluster required.

## Current status

Version 25.1 - Active development

What works now:
- Multi-account system with isolated catalogs
- File uploads with auto-conversion to Parquet
- Arrow IPC and Prometheus streaming
- SQL queries via DuckDB
- Local filesystem storage
- SQLite and PostgreSQL metadata

Coming soon:
- S3-compatible storage backend
- Arrow Flight SQL interface
- Web-based query interface

## License

MIT License

## Contributing

Pull requests welcome. Please include tests for new features.
