"""Quota enforcement and tracking for DuckPond tenants.

This module provides quota enforcement mechanisms for:
- Storage usage limits
- Query memory limits (DuckDB)
- Concurrent query limits (asyncio.Semaphore)
- Quota usage tracking and reporting
"""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncGenerator, Optional

import duckdb

from duckpond.exceptions import ConcurrentQueryLimitError, QuotaExceededError
from duckpond.storage.backend import StorageBackend
from duckpond.tenants.models import Tenant


@dataclass
class QuotaUsage:
    """Current quota usage for a tenant."""

    tenant_id: str
    storage_used_bytes: int
    storage_limit_bytes: int
    storage_used_gb: float
    storage_limit_gb: int
    concurrent_queries: int
    max_concurrent_queries: int
    query_memory_limit_gb: int

    @property
    def storage_percentage(self) -> float:
        """Calculate storage usage as percentage."""
        if self.storage_limit_bytes == 0:
            return 0.0
        return (self.storage_used_bytes / self.storage_limit_bytes) * 100

    @property
    def is_storage_exceeded(self) -> bool:
        """Check if storage quota is exceeded."""
        return self.storage_used_bytes > self.storage_limit_bytes

    @property
    def is_queries_at_limit(self) -> bool:
        """Check if concurrent queries are at limit."""
        return self.concurrent_queries >= self.max_concurrent_queries

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses."""
        return {
            "tenant_id": self.tenant_id,
            "storage": {
                "used_bytes": self.storage_used_bytes,
                "used_gb": self.storage_used_gb,
                "limit_gb": self.storage_limit_gb,
                "percentage": round(self.storage_percentage, 2),
                "exceeded": self.is_storage_exceeded,
            },
            "queries": {
                "concurrent_active": self.concurrent_queries,
                "max_concurrent": self.max_concurrent_queries,
                "at_limit": self.is_queries_at_limit,
                "memory_limit_gb": self.query_memory_limit_gb,
            },
        }


class TenantQueryLimiter:
    """
    Manages concurrent query limits per tenant using asyncio.Semaphore.

    Each tenant has a dedicated semaphore that tracks active queries.
    When the limit is reached, new queries will raise ConcurrentQueryLimitError.
    """

    def __init__(self) -> None:
        """Initialize query limiter with per-tenant semaphores."""
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        self._active_queries: dict[str, int] = {}
        self._lock = asyncio.Lock()

    def _get_or_create_semaphore(self, tenant_id: str, limit: int) -> asyncio.Semaphore:
        """Get or create semaphore for tenant."""
        if tenant_id not in self._semaphores:
            self._semaphores[tenant_id] = asyncio.Semaphore(limit)
            self._active_queries[tenant_id] = 0
        return self._semaphores[tenant_id]

    def get_active_queries(self, tenant_id: str) -> int:
        """Get number of active queries for tenant."""
        return self._active_queries.get(tenant_id, 0)

    @asynccontextmanager
    async def acquire_query_slot(
        self, tenant_id: str, max_concurrent: int
    ) -> AsyncGenerator[None, None]:
        """
        Acquire a query slot for the tenant.

        This context manager ensures proper acquisition and release of query slots.
        If the concurrent query limit is reached, raises ConcurrentQueryLimitError.

        Args:
            tenant_id: Tenant identifier
            max_concurrent: Maximum concurrent queries allowed

        Raises:
            ConcurrentQueryLimitError: If concurrent query limit is reached

        Example:
            async with limiter.acquire_query_slot(tenant_id, 10):
                result = await execute_query(sql)
        """
        async with self._lock:
            semaphore = self._get_or_create_semaphore(tenant_id, max_concurrent)

            active = self._active_queries.get(tenant_id, 0)
            if active >= max_concurrent:
                raise ConcurrentQueryLimitError(tenant_id, max_concurrent)

        acquired = await semaphore.acquire()
        if not acquired:
            raise ConcurrentQueryLimitError(tenant_id, max_concurrent)

        try:
            async with self._lock:
                self._active_queries[tenant_id] = (
                    self._active_queries.get(tenant_id, 0) + 1
                )

            yield

        finally:
            semaphore.release()
            async with self._lock:
                self._active_queries[tenant_id] = max(
                    0, self._active_queries.get(tenant_id, 1) - 1
                )

    def clear_tenant(self, tenant_id: str) -> None:
        """Clear semaphore for tenant (useful for testing or tenant deletion)."""
        if tenant_id in self._semaphores:
            del self._semaphores[tenant_id]
        if tenant_id in self._active_queries:
            del self._active_queries[tenant_id]


async def check_storage_quota(
    tenant: Tenant, additional_bytes: int, storage_backend: StorageBackend
) -> None:
    """
    Check if adding additional storage would exceed the tenant's storage quota.

    Args:
        tenant: Tenant model with quota limits
        additional_bytes: Number of bytes to be added
        storage_backend: Storage backend to query current usage

    Raises:
        QuotaExceededError: If adding bytes would exceed storage quota
    """
    current_bytes = await storage_backend.get_storage_usage(tenant.tenant_id)

    total_bytes = current_bytes + additional_bytes

    limit_bytes = tenant.max_storage_gb * 1024 * 1024 * 1024

    if total_bytes > limit_bytes:
        current_gb = current_bytes / (1024 * 1024 * 1024)
        total_gb = total_bytes / (1024 * 1024 * 1024)

        raise QuotaExceededError(
            tenant_id=tenant.tenant_id,
            quota_type="storage",
            limit=f"{tenant.max_storage_gb}GB",
            current=f"{total_gb:.2f}GB (current: {current_gb:.2f}GB)",
        )


async def calculate_storage_usage(
    tenant_id: str, storage_backend: StorageBackend
) -> int:
    """
    Calculate current storage usage for a tenant in bytes.

    Args:
        tenant_id: Tenant identifier
        storage_backend: Storage backend to query

    Returns:
        Total storage usage in bytes
    """
    return await storage_backend.get_storage_usage(tenant_id)


def create_tenant_connection(
    tenant: Tenant, database_path: Optional[str] = None
) -> duckdb.DuckDBPyConnection:
    """
    Create a DuckDB connection with tenant-specific memory limits.

    The connection will have the following configurations:
    - memory_limit: Set to tenant's max_query_memory_gb
    - temp_directory: Set to tenant-specific temp directory
    - threads: Auto-configured based on available CPUs

    Args:
        tenant: Tenant model with quota configuration
        database_path: Optional path to DuckDB database file (None for in-memory)

    Returns:
        DuckDB connection with memory limits applied

    Example:
        conn = create_tenant_connection(tenant, "/data/warehouse.duckdb")
        result = conn.execute("SELECT * FROM my_table").fetchall()
        conn.close()
    """
    conn = duckdb.connect(database_path or ":memory:")

    memory_limit_gb = tenant.max_query_memory_gb
    conn.execute(f"SET memory_limit='{memory_limit_gb}GB'")

    conn.execute("SET enable_progress_bar=true")

    conn.execute(f"SET temp_directory='/tmp/duckpond/{tenant.tenant_id}'")

    return conn


async def get_quota_usage(
    tenant: Tenant, storage_backend: StorageBackend, query_limiter: TenantQueryLimiter
) -> QuotaUsage:
    """
    Get current quota usage for a tenant.

    Args:
        tenant: Tenant model with quota limits
        storage_backend: Storage backend to query usage
        query_limiter: Query limiter to get active query count

    Returns:
        QuotaUsage object with current usage statistics
    """
    storage_bytes = await calculate_storage_usage(tenant.tenant_id, storage_backend)
    storage_gb = storage_bytes / (1024 * 1024 * 1024)
    limit_bytes = tenant.max_storage_gb * 1024 * 1024 * 1024

    active_queries = query_limiter.get_active_queries(tenant.tenant_id)

    return QuotaUsage(
        tenant_id=tenant.tenant_id,
        storage_used_bytes=storage_bytes,
        storage_limit_bytes=limit_bytes,
        storage_used_gb=round(storage_gb, 2),
        storage_limit_gb=tenant.max_storage_gb,
        concurrent_queries=active_queries,
        max_concurrent_queries=tenant.max_concurrent_queries,
        query_memory_limit_gb=tenant.max_query_memory_gb,
    )
