# DuckPond Docker Service

This module provides a unified Docker container management service for DuckPond, enabling isolated execution of workloads with proper resource limits and security.

## Overview

The Docker service abstracts away the complexity of Docker container management and provides:

- **Generic container lifecycle management**: Start, stop, health checks, log access
- **Resource isolation**: CPU and memory limits
- **Network configuration**: Bridge, host, and custom network modes
- **Volume mounting**: Data directory access
- **Environment management**: AWS credentials, custom variables
- **Specialized runners**: Pre-configured for specific workloads

## Architecture

```
duckpond/docker/
├── config.py           # Container configuration models
├── container.py        # Core container management
├── exceptions.py       # Docker-specific exceptions
└── runners/
    ├── marimo.py      # Marimo notebook runner
    └── query.py       # Query execution runner
```

## Core Components

### DockerContainer

The base container wrapper that handles all Docker operations:

```python
from duckpond.docker import DockerContainer, ContainerConfig

config = ContainerConfig(
    image="python:3.12-slim",
    command=["python", "script.py"],
    name="my-container",
)

container = DockerContainer(config)
await container.start()
is_healthy = await container.check_health()
await container.stop()
```

### ContainerConfig

Configuration model for Docker containers:

```python
from duckpond.docker import ContainerConfig
from pathlib import Path

config = ContainerConfig(
    image="python:3.12-slim",
    command=["python", "script.py"],
    name="my-container",
)

# Add volume mounts
config.add_volume(
    host_path=Path("/data"),
    container_path="/workspace",
    read_only=False,
)

# Add environment variables
config.add_env("DATABASE_URL", "postgresql://...")

# Set resource limits
config.set_resources(
    memory_mb=2048,
    cpu_limit=2.0,
)

# Configure networking
config.use_host_network()
```

## Specialized Runners

### MarimoRunner

Runs Marimo notebooks in isolated containers:

```python
from duckpond.docker.runners import MarimoRunner
from pathlib import Path

runner = MarimoRunner(
    notebook_path=Path("/data/account123/notebook.py"),
    port=8080,
    account_data_dir=Path("/data/account123"),
    account_id="account123",
    memory_limit_mb=2048,
    cpu_limit=2.0,
)

# Start notebook
container_id = await runner.start()

# Check health
is_healthy = await runner.check_health()

# Get URL
url = runner.get_url()  # http://127.0.0.1:8080

# Stop notebook
await runner.stop()
```

Features:
- Automatic Marimo installation
- Health check integration
- AWS credentials passing
- Volume mounting for workspace
- Resource limits

### QueryRunner

Executes SQL queries in isolated containers:

```python
from duckpond.docker.runners import QueryRunner
from pathlib import Path

runner = QueryRunner(
    account_data_dir=Path("/data/account123"),
    account_id="account123",
    catalog_path=Path("/data/account123/catalog.sqlite"),
    memory_limit_mb=4096,
    cpu_limit=2.0,
)

# Start container
await runner.start()

# Execute query
result = await runner.execute_query(
    sql="SELECT * FROM catalog.sales LIMIT 10",
    output_format="json",
    timeout_seconds=30,
)

# Get execution plan
plan = await runner.explain_query("SELECT * FROM catalog.sales")

# Stop container
await runner.stop()
```

Features:
- Automatic DuckDB installation
- Multiple output formats (JSON, CSV, Arrow)
- Catalog mounting
- Query timeout enforcement
- Resource isolation

## Usage in DuckPond

### Notebooks

The `MarimoProcess` class now uses `MarimoRunner` internally:

```python
from duckpond.notebooks.process import MarimoProcess
from pathlib import Path

process = MarimoProcess(
    notebook_path=Path("/data/account123/notebook.py"),
    port=8080,
    account_data_dir=Path("/data/account123"),
    account_id="account123",
)

await process.start()
# Notebook is now running and healthy
await process.stop()
```

### Query Execution

#### CLI with Docker Isolation

```bash
# Execute query in Docker container
duckpond query execute \
  --account abc123 \
  --sql "SELECT * FROM catalog.sales LIMIT 10" \
  --docker

# Explain query in Docker container
duckpond query explain \
  --account abc123 \
  --sql "SELECT * FROM catalog.sales" \
  --docker
```

#### Programmatic Usage

```python
from duckpond.query.docker_executor import DockerQueryExecutor
from pathlib import Path

executor = DockerQueryExecutor(
    account_id="account123",
    account_data_dir=Path("/data/account123"),
    catalog_path=Path("/data/account123/catalog.sqlite"),
)

result = await executor.execute_query(
    sql="SELECT * FROM catalog.sales",
    output_format="json",
    timeout_seconds=30,
)

# Or create from account model
from duckpond.accounts.manager import AccountManager

async with get_session() as session:
    manager = AccountManager(session)
    account = await manager.get_account("account123")
    
    executor = DockerQueryExecutor.from_account(account)
    result = await executor.execute_query(...)
```

## Configuration Options

### Resource Limits

```python
config.set_resources(
    memory_mb=4096,      # 4GB memory limit
    cpu_limit=2.0,       # 2 CPU cores
    cpu_shares=1024,     # CPU scheduling weight
)
```

### Network Configuration

```python
# Use host network (direct host access)
config.use_host_network()

# Use bridge network (isolated)
config.use_bridge_network()

# Custom network
config.network.mode = "container:other-container"
config.network.hostname = "myhost"
config.network.dns = ["8.8.8.8", "8.8.4.4"]
```

### Volume Mounts

```python
# Read-write mount
config.add_volume(
    host_path=Path("/data"),
    container_path="/workspace",
    read_only=False,
)

# Read-only mount
config.add_volume(
    host_path=Path("/config"),
    container_path="/etc/config",
    read_only=True,
)
```

### Environment Variables

```python
# Single variable
config.add_env("DATABASE_URL", "postgresql://...")

# Multiple variables
config.add_env_from_dict({
    "AWS_ACCESS_KEY_ID": "...",
    "AWS_SECRET_ACCESS_KEY": "...",
})

# Automatic AWS credentials
aws_env = DockerContainer.get_aws_credentials_env()
config.add_env_from_dict(aws_env)
```

### Timeouts

```python
config.startup_timeout_seconds = 30       # Container startup
config.stop_timeout_seconds = 10          # Graceful shutdown
config.health_check_interval_seconds = 0.5  # Health check polling
```

## Error Handling

The module provides specific exceptions for different failure scenarios:

```python
from duckpond.docker.exceptions import (
    ContainerStartupException,
    ContainerHealthCheckException,
    ContainerStopException,
    ContainerExecutionException,
)

try:
    await container.start()
except ContainerStartupException as e:
    # Handle startup failure
    logs = await container.get_logs()
    print(f"Startup failed: {e}\nLogs: {logs}")
```

## Best Practices

### 1. Always Clean Up

Use context managers or try/finally blocks:

```python
container = DockerContainer(config)
try:
    await container.start()
    # Do work
finally:
    await container.stop()

# Or use async context manager
async with DockerContainer(config) as container:
    # Do work
    pass
```

### 2. Set Appropriate Timeouts

```python
# Short-lived tasks
config.startup_timeout_seconds = 10
config.stop_timeout_seconds = 5

# Long-running queries
config.startup_timeout_seconds = 60
config.stop_timeout_seconds = 30
```

### 3. Monitor Health

```python
# Regular health checks
while True:
    is_healthy = await container.check_health(health_url="http://...")
    if not is_healthy:
        logs = await container.get_logs()
        logger.error(f"Container unhealthy: {logs}")
        break
    await asyncio.sleep(30)
```

### 4. Resource Limits

Set appropriate limits based on workload:

```python
# Notebooks: moderate resources
config.set_resources(memory_mb=2048, cpu_limit=2.0)

# Queries: higher resources
config.set_resources(memory_mb=4096, cpu_limit=4.0)

# Background tasks: lower resources
config.set_resources(memory_mb=512, cpu_limit=0.5)
```

## Migration Notes

### From Old Process Management

If you're migrating from the old `MarimoProcess` implementation:

**Before:**
```python
# Old implementation with direct Docker commands
process = MarimoProcess(...)
await process.start()
```

**After:**
```python
# New implementation using Docker service
# API is the same, just using MarimoRunner internally
process = MarimoProcess(...)
await process.start()
```

The API remains backward compatible. Existing code should work without changes.

### From Direct Query Execution

**Before:**
```python
# Direct DuckDB execution
ducklake = AccountDuckLakeManager(account)
await ducklake.initialize()
executor = QueryExecutor(ducklake)
result = await executor.execute_query(...)
await ducklake.close()
```

**After (with Docker isolation):**
```python
# Docker-isolated execution
executor = DockerQueryExecutor.from_account(account)
result = await executor.execute_query(...)
# No explicit cleanup needed
```

## Testing

Run Docker service tests:

```bash
pytest tests/docker/
```

Test with real containers:

```bash
# Test marimo runner
pytest tests/docker/test_marimo_runner.py -v

# Test query runner
pytest tests/docker/test_query_runner.py -v
```

## Troubleshooting

### Container Won't Start

1. Check Docker is running: `docker ps`
2. Check image is available: `docker pull python:3.12-slim`
3. Check logs: `await container.get_logs()`
4. Verify resource limits aren't too restrictive

### Health Check Fails

1. Increase startup timeout
2. Check if port is available
3. Verify health endpoint URL
4. Check container logs for errors

### Container Won't Stop

1. Increase stop timeout
2. Use `container.kill()` for immediate termination
3. Check for zombie containers: `docker ps -a`

### Performance Issues

1. Reduce resource limits if host is constrained
2. Use bridge network instead of host network
3. Mount volumes read-only when possible
4. Reuse containers instead of creating new ones