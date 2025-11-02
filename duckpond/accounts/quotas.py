"""Quota enforcement and tracking for DuckPond accounts.

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
from duckpond.accounts.models import Account


@dataclass
class QuotaUsage:
    """Current quota usage for a account."""

    account_id: str
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
            "account_id": self.account_id,
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


class AccountQueryLimiter:
    """
    Manages concurrent query limits per account using asyncio.Semaphore.

    Each account has a dedicated semaphore that tracks active queries.
    When the limit is reached, new queries will raise ConcurrentQueryLimitError.
    """

    def __init__(self) -> None:
        """Initialize query limiter with per-account semaphores."""
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        self._active_queries: dict[str, int] = {}
        self._lock = asyncio.Lock()

    def _get_or_create_semaphore(self, account_id: str, limit: int) -> asyncio.Semaphore:
        """Get or create semaphore for account."""
        if account_id not in self._semaphores:
            self._semaphores[account_id] = asyncio.Semaphore(limit)
            self._active_queries[account_id] = 0
        return self._semaphores[account_id]

    def get_active_queries(self, account_id: str) -> int:
        """Get number of active queries for account."""
        return self._active_queries.get(account_id, 0)

    @asynccontextmanager
    async def acquire_query_slot(
        self, account_id: str, max_concurrent: int
    ) -> AsyncGenerator[None, None]:
        """
        Acquire a query slot for the account.

        This context manager ensures proper acquisition and release of query slots.
        If the concurrent query limit is reached, raises ConcurrentQueryLimitError.

        Args:
            account_id: Account identifier
            max_concurrent: Maximum concurrent queries allowed

        Raises:
            ConcurrentQueryLimitError: If concurrent query limit is reached

        Example:
            async with limiter.acquire_query_slot(account_id, 10):
                result = await execute_query(sql)
        """
        async with self._lock:
            semaphore = self._get_or_create_semaphore(account_id, max_concurrent)

            active = self._active_queries.get(account_id, 0)
            if active >= max_concurrent:
                raise ConcurrentQueryLimitError(account_id, max_concurrent)

        acquired = await semaphore.acquire()
        if not acquired:
            raise ConcurrentQueryLimitError(account_id, max_concurrent)

        try:
            async with self._lock:
                self._active_queries[account_id] = (
                    self._active_queries.get(account_id, 0) + 1
                )

            yield

        finally:
            semaphore.release()
            async with self._lock:
                self._active_queries[account_id] = max(
                    0, self._active_queries.get(account_id, 1) - 1
                )

    def clear_account(self, account_id: str) -> None:
        """Clear semaphore for account (useful for testing or account deletion)."""
        if account_id in self._semaphores:
            del self._semaphores[account_id]
        if account_id in self._active_queries:
            del self._active_queries[account_id]


async def check_storage_quota(
    account: Account, additional_bytes: int, storage_backend: StorageBackend
) -> None:
    """
    Check if adding additional storage would exceed the account's storage quota.

    Args:
        account: Account model with quota limits
        additional_bytes: Number of bytes to be added
        storage_backend: Storage backend to query current usage

    Raises:
        QuotaExceededError: If adding bytes would exceed storage quota
    """
    current_bytes = await storage_backend.get_storage_usage(account.account_id)

    total_bytes = current_bytes + additional_bytes

    limit_bytes = account.max_storage_gb * 1024 * 1024 * 1024

    if total_bytes > limit_bytes:
        current_gb = current_bytes / (1024 * 1024 * 1024)
        total_gb = total_bytes / (1024 * 1024 * 1024)

        raise QuotaExceededError(
            account_id=account.account_id,
            quota_type="storage",
            limit=f"{account.max_storage_gb}GB",
            current=f"{total_gb:.2f}GB (current: {current_gb:.2f}GB)",
        )


async def calculate_storage_usage(
    account_id: str, storage_backend: StorageBackend
) -> int:
    """
    Calculate current storage usage for a account in bytes.

    Args:
        account_id: Account identifier
        storage_backend: Storage backend to query

    Returns:
        Total storage usage in bytes
    """
    return await storage_backend.get_storage_usage(account_id)


def create_account_connection(
    account: Account, database_path: Optional[str] = None
) -> duckdb.DuckDBPyConnection:
    """
    Create a DuckDB connection with account-specific memory limits.

    The connection will have the following configurations:
    - memory_limit: Set to account's max_query_memory_gb
    - temp_directory: Set to account-specific temp directory
    - threads: Auto-configured based on available CPUs

    Args:
        account: Account model with quota configuration
        database_path: Optional path to DuckDB database file (None for in-memory)

    Returns:
        DuckDB connection with memory limits applied

    Example:
        conn = create_account_connection(account, "/data/warehouse.duckdb")
        result = conn.execute("SELECT * FROM my_table").fetchall()
        conn.close()
    """
    conn = duckdb.connect(database_path or ":memory:")

    memory_limit_gb = account.max_query_memory_gb
    conn.execute(f"SET memory_limit='{memory_limit_gb}GB'")

    conn.execute("SET enable_progress_bar=true")

    conn.execute(f"SET temp_directory='/tmp/duckpond/{account.account_id}'")

    return conn


async def get_quota_usage(
    account: Account, storage_backend: StorageBackend, query_limiter: AccountQueryLimiter
) -> QuotaUsage:
    """
    Get current quota usage for a account.

    Args:
        account: Account model with quota limits
        storage_backend: Storage backend to query usage
        query_limiter: Query limiter to get active query count

    Returns:
        QuotaUsage object with current usage statistics
    """
    storage_bytes = await calculate_storage_usage(account.account_id, storage_backend)
    storage_gb = storage_bytes / (1024 * 1024 * 1024)
    limit_bytes = account.max_storage_gb * 1024 * 1024 * 1024

    active_queries = query_limiter.get_active_queries(account.account_id)

    return QuotaUsage(
        account_id=account.account_id,
        storage_used_bytes=storage_bytes,
        storage_limit_bytes=limit_bytes,
        storage_used_gb=round(storage_gb, 2),
        storage_limit_gb=account.max_storage_gb,
        concurrent_queries=active_queries,
        max_concurrent_queries=account.max_concurrent_queries,
        query_memory_limit_gb=account.max_query_memory_gb,
    )
