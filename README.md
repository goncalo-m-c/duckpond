# DuckPond

A self-hosted multi-account data platform with DuckDB analytics and interactive marimo notebooks. Run your own analytics infrastructure without cloud vendor lock-in.

## What it does

DuckPond is a complete analytics platform you run on your own infrastructure:

- **Interactive Notebooks** - Create and run marimo notebooks with Python, SQL, and data visualization
- **Multi-Account Isolation** - Each account gets its own DuckLake catalog, storage, and API keys
- **File Uploads** - Upload CSV, JSON, or Parquet files with automatic Parquet conversion
- **Streaming Ingestion** - Stream data using Arrow IPC or Prometheus remote write protocol
- **SQL Analytics** - Query everything with DuckDB's fast columnar engine
- **Web Interface** - Simple & Modern UI for managing notebooks, accounts, and API keys
- **Docker Isolation** - Notebooks run in isolated Docker containers with resource limits

Think of it as Jupyter + DuckDB + multi-tenancy in a self-hosted package.

## Why use this

You should consider DuckPond if:

- You want interactive notebooks for data analysis without JupyterHub complexity
- You're building analytics and need account isolation
- You have gigabytes of data, not petabytes, and prefer simplicity
- You need to store Prometheus metrics longer than two weeks
- You want to own your infrastructure and avoid per-query cloud billing
- You prefer Python and SQL over proprietary query languages

## Key Features

### Interactive Notebooks
- Run marimo notebooks with full Python and SQL support
- Start/stop notebook sessions on demand
- Docker-isolated execution with memory and CPU limits
- Built-in DuckDB integration for querying your data

### Multi-Account System
Each account gets:
- Isolated DuckLake catalog (SQLite or PostgreSQL backed)
- Separate storage path for Parquet files
- Configurable storage quotas
- Independent resource limits

### Data Ingestion
- **File Upload**: CSV, JSON, Parquet with auto-conversion
- **Arrow IPC**: Streaming tabular data via Arrow protocol
- **Prometheus**: Remote write protocol for metrics storage
- **Validation**: Schema validation and size limits
- **Catalogs**: Automatic table registration in DuckLake

### Analytics
- **DuckDB**: Fast columnar query execution
- **SQL**: Full SQL support with DuckDB extensions
- **Formats**: Export results as JSON, CSV, or Parquet
- **Performance**: Million-row queries in under 100ms

### Web Interface
- Notebook management (create, start, stop, delete)

## Architecture

### Core Stack
- **FastAPI** - REST API and WebSocket support
- **DuckDB** - Columnar analytical query engine
- **DuckLake** - ACID-compliant table catalogs
- **Marimo** - Interactive notebook environment
- **Docker** - Notebook container isolation
- **SQLAlchemy** - Database ORM for metadata
- **PyArrow** - Zero-copy data operations

### Storage
- **Metadata**: SQLite (development) or PostgreSQL (production)
- **Data**: Local filesystem with Parquet format
- **Notebooks**: Per-account `.py` files
- **S3**: Configuration exists, implementation in progress

### Notebook Infrastructure
- **Process Manager**: Async lifecycle management
- **Health Checks**: HTTP readiness probes
- **Port Allocation**: Dynamic port pool (10000-10099)
- **Resource Limits**: Configurable memory and CPU caps
- **Session Cleanup**: Automatic idle session termination

## Performance

On my hardware (MacBook Air M1):
- Query latency: <100ms for million-row tables
- Streaming ingestion: 100K+ records/second
- CSV to Parquet: ~50MB/second
- Concurrent queries: 100+ per instance
- Notebook startup: 3-5 seconds

Single-machine performance. No cluster required.

## Installation

### Prerequisites
- Python 3.13+
- Docker (for notebook execution)
- PostgreSQL (optional, for production)

### Quick Start

1. **Clone and Install from Source**
```bash
git clone https://github.com/yourusername/duckpond-py.git
cd duckpond-py
pip install -e .

# Or using uv (faster):
uv pip install -e .
```

2. **Initialize Configuration**
```bash
duckpond init
# Creates ~/.duckpond/config.yaml
```

3. **Initialize Database**
```bash
duckpond db upgrade
```

4. **Create an Account**
```bash
duckpond account create myaccount
# Returns API key
```

5. **Start Server**
```bash
duckpond api serve
# Listens on http://localhost:8000
```

6. **Access Web Interface**
Open http://localhost:8000 and log in with your API key.

## Configuration

Example `~/.duckpond/config.yaml`:

```yaml
server:
  host: 0.0.0.0
  port: 8000
  workers: 4

database:
  url: postgresql://user:pass@localhost/duckpond
  # or: sqlite:///~/.duckpond/metadata.db

storage:
  default_backend: local
  local_path: ~/.duckpond/data

duckdb:
  memory_limit: 4GB
  threads: 4

notebooks:
  docker_image: python:3.12-slim
  memory_limit_mb: 2048
  cpu_limit: 2.0
  session_timeout_seconds: 3600
  health_check_interval_seconds: 30

limits:
  max_file_size_mb: 500
  default_max_storage_gb: 10
  default_max_concurrent_queries: 5
```

## CLI Usage

### Account Management
```bash
# Create account
duckpond account create myaccount

# List accounts
duckpond account list

# Generate API key
duckpond account api-key create myaccount
```

### Dataset Operations
```bash
# Upload file
duckpond dataset upload mydata.csv --account myaccount

# List datasets
duckpond dataset list --account myaccount

# Query dataset
duckpond query "SELECT * FROM mydata LIMIT 10" --account myaccount
```

### Streaming
```bash
# Stream Arrow IPC data
cat data.arrow | duckpond stream arrow mytable --account myaccount

# Test Prometheus endpoint
duckpond stream prometheus-test --account myaccount
```

## API Usage

### Authentication
All API requests require an API key:
```bash
curl -H "X-API-Key: your-key-here" http://localhost:8000/api/query
```

## Development

### Setup
```bash
# Clone repository
git clone https://github.com/yourusername/duckpond.git
cd duckpond

# Or using uv (recommended - faster):
uv pip install -e . --group dev

# Run tests
uv run pytest

# Run linter
ruff check .

# Format code
ruff format .
```

### Project Structure
```
duckpond/
├── accounts/          # Account management and authentication
├── api/              # FastAPI application and routers
├── catalog/          # DuckLake catalog integration
├── cli/              # Command-line interface
├── conversion/       # File format conversion (CSV→Parquet)
├── db/               # SQLAlchemy models and migrations
├── ingest/           # File upload and validation
├── notebooks/        # Marimo notebook management
│   ├── manager.py    # Session lifecycle
│   ├── process.py    # Docker container management
│   ├── proxy.py      # WebSocket proxy
│   └── session.py    # Session state tracking
├── query/            # DuckDB query execution
├── static/           # Frontend assets
│   ├── css/          # Stylesheets
│   └── js/           # JavaScript (components, views, utils)
├── storage/          # Storage backend abstraction
├── streaming/        # Arrow IPC and Prometheus ingestion
├── templates/        # HTML templates
└── wal/              # Write-ahead log for streaming
```

## Current Status

**Version**: 25.1 - Active Development

### ✅ Production Ready
- Multi-account system with API key authentication
- File upload with Parquet conversion
- SQL query execution via DuckDB
- Arrow IPC streaming
- Prometheus remote write ingestion
- Notebook creation and management
- Session lifecycle (start/stop/monitor)
- Docker-isolated notebook execution
- Web UI for notebooks and settings
- CLI tools for all operations
- PostgreSQL and SQLite metadata storage

### 🚧 In Progress
- S3-compatible storage backend (configuration exists, needs testing)

### 📋 Planned
- Arrow Flight SQL interface

## Acknowledgments

- **DuckDB** - Fast analytical query engine
- **Marimo** - Modern notebook environment
- **FastAPI** - High-performance web framework
- **PyArrow** - Efficient columnar data structures

## Support

- **Issues**: https://github.com/wundervaflja/duckpond/issues
- **Discussions**: https://github.com/wundervaflja/duckpond/discussions

---
