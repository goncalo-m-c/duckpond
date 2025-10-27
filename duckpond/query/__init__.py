"""DuckDB query engine with DuckLake catalog integration."""

from duckpond.query.ducklake import (
    TenantDuckLakeManager,
    TenantDuckLakeManagerRegistry,
    get_registry,
    shutdown_registry,
)
from duckpond.query.executor import QueryExecutor
from duckpond.query.models import QueryMetrics, QueryResult
from duckpond.query.pool import DuckDBConnectionPool
from duckpond.query.validator import (
    SQLValidator,
    hash_query,
    sanitize_for_logging,
    validate_sql,
)

__all__ = [
    "DuckDBConnectionPool",
    "TenantDuckLakeManager",
    "TenantDuckLakeManagerRegistry",
    "get_registry",
    "shutdown_registry",
    "QueryExecutor",
    "QueryResult",
    "QueryMetrics",
    "SQLValidator",
    "validate_sql",
    "hash_query",
    "sanitize_for_logging",
]
