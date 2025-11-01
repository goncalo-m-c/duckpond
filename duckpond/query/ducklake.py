"""DuckLake catalog manager for account-specific catalogs."""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

import duckdb

from duckpond.query.pool import DuckDBConnectionPool
from duckpond.accounts.models import Account

logger = logging.getLogger(__name__)


class AccountDuckLakeManager:
    """
    Manages DuckLake catalog and connection pool for a account.

    This class provides:
    - Account-specific connection pool with DuckLake catalog
    - Automatic pool initialization
    - Connection acquisition with account context
    - Proper cleanup on shutdown

    Each account has:
    - Isolated DuckLake catalog (separate database or file)
    - Dedicated connection pool
    - Memory and concurrency limits

    Usage:
        manager = AccountDuckLakeManager(account)
        await manager.initialize()

        async with manager.get_connection() as conn:
            result = await manager.execute(conn, "SELECT * FROM catalog.sales")
    """

    def __init__(self, account: Account, max_connections: int = 10) -> None:
        """
        Initialize account DuckLake manager.

        Args:
            account: Account model with configuration
            max_connections: Maximum connections in pool (default: 10)
        """
        self.account = account
        self.max_connections = max_connections
        self.pool: DuckDBConnectionPool | None = None

        logger.debug(
            f"Initialized AccountDuckLakeManager for account {account.account_id}",
            extra={
                "account_id": account.account_id,
                "max_connections": max_connections,
            },
        )

    async def initialize(self) -> None:
        """
        Initialize connection pool with DuckLake catalog.

        This creates the connection pool with account-specific configuration:
        - Catalog URL from account.ducklake_catalog_url
        - Memory limit from account.max_query_memory_gb
        - Thread count (fixed at 4 for now)

        Raises:
            Exception: If pool initialization fails
        """
        if self.pool is not None:
            logger.warning(
                f"Pool already initialized for account {self.account.account_id}"
            )
            return

        logger.debug(
            f"Initializing connection pool for account {self.account.account_id}",
            extra={
                "account_id": self.account.account_id,
                "catalog_url": self.account.ducklake_catalog_url,
                "memory_limit": f"{self.account.max_query_memory_gb}GB",
            },
        )

        self.pool = DuckDBConnectionPool(
            max_connections=self.max_connections,
            catalog_url=self.account.ducklake_catalog_url,
            memory_limit=f"{self.account.max_query_memory_gb}GB",
            threads=4,
        )

        await self.pool.initialize(min_connections=1)

        logger.debug(f"Connection pool initialized for account {self.account.account_id}")

    @asynccontextmanager
    async def get_connection(self) -> AsyncIterator[duckdb.DuckDBPyConnection]:
        """
        Get connection with account's DuckLake catalog attached.

        This is a convenience wrapper around pool.acquire() that
        automatically initializes the pool if needed.

        Yields:
            DuckDB connection with catalog attached

        Raises:
            Exception: If pool initialization or connection acquisition fails

        Usage:
            async with manager.get_connection() as conn:
                result = await manager.execute(conn, "SELECT * FROM catalog.sales")
        """
        if self.pool is None:
            await self.initialize()

        assert self.pool is not None

        async with self.pool.acquire() as conn:
            yield conn

    async def execute(self, conn: duckdb.DuckDBPyConnection, sql: str):
        """
        Execute SQL query using the pool's executor.

        This is a convenience wrapper that runs queries in thread executor.

        Args:
            conn: DuckDB connection from get_connection()
            sql: SQL query string

        Returns:
            Query result

        Usage:
            async with manager.get_connection() as conn:
                result = await manager.execute(conn, "SELECT * FROM catalog.sales")
                df = result.df()
        """
        if self.pool is None:
            raise RuntimeError(
                f"Pool not initialized for account {self.account.account_id}"
            )

        return await self.pool.execute(conn, sql)

    async def close(self) -> None:
        """
        Close connection pool and release resources.

        This should be called when account is deleted or system is shutting down.
        """
        if self.pool is not None:
            logger.info(f"Closing connection pool for account {self.account.account_id}")
            await self.pool.close()
            self.pool = None

    @property
    def is_initialized(self) -> bool:
        """Check if pool is initialized."""
        return self.pool is not None

    @property
    def available_connections(self) -> int:
        """Get number of available connections in pool."""
        if self.pool is None:
            return 0
        return self.pool.available_connections

    @property
    def total_connections(self) -> int:
        """Get total number of created connections."""
        if self.pool is None:
            return 0
        return self.pool.total_connections


class AccountDuckLakeManagerRegistry:
    """
    Registry for managing AccountDuckLakeManager instances.

    This singleton maintains a pool manager per account and handles:
    - Lazy creation of managers
    - Manager lifecycle
    - Cleanup on shutdown

    Usage:
        registry = AccountDuckLakeManagerRegistry()
        manager = await registry.get_manager(account)

        async with manager.get_connection() as conn:
            result = await manager.execute(conn, "SELECT * FROM catalog.sales")

        await registry.close_all()
    """

    def __init__(self) -> None:
        """Initialize empty registry."""
        self._managers: dict[str, AccountDuckLakeManager] = {}
        self._lock = asyncio.Lock()
        logger.info("Initialized AccountDuckLakeManagerRegistry")

    async def get_manager(
        self, account: Account, max_connections: int = 10
    ) -> AccountDuckLakeManager:
        """
        Get or create manager for account.

        Args:
            account: Account model
            max_connections: Maximum connections per account

        Returns:
            AccountDuckLakeManager for account
        """
        async with self._lock:
            if account.account_id not in self._managers:
                logger.info(f"Creating new manager for account {account.account_id}")
                manager = AccountDuckLakeManager(account, max_connections)
                await manager.initialize()
                self._managers[account.account_id] = manager

            return self._managers[account.account_id]

    async def remove_manager(self, account_id: str) -> None:
        """
        Remove and close manager for account.

        Args:
            account_id: Account ID to remove
        """
        async with self._lock:
            if account_id in self._managers:
                logger.info(f"Removing manager for account {account_id}")
                manager = self._managers.pop(account_id)
                await manager.close()

    async def close_all(self) -> None:
        """Close all managers and release resources."""
        logger.info("Closing all account managers")

        async with self._lock:
            for account_id, manager in self._managers.items():
                try:
                    await manager.close()
                except Exception as e:
                    logger.error(f"Error closing manager for account {account_id}: {e}")

            self._managers.clear()

        logger.info("All account managers closed")

    @property
    def active_accounts(self) -> list[str]:
        """Get list of account IDs with active managers."""
        return list(self._managers.keys())

    @property
    def total_managers(self) -> int:
        """Get total number of active managers."""
        return len(self._managers)


_registry: AccountDuckLakeManagerRegistry | None = None


def get_registry() -> AccountDuckLakeManagerRegistry:
    """
    Get global AccountDuckLakeManagerRegistry instance.

    Returns:
        Global registry singleton
    """
    global _registry
    if _registry is None:
        _registry = AccountDuckLakeManagerRegistry()
    return _registry


async def shutdown_registry() -> None:
    """Shutdown global registry and close all managers."""
    global _registry
    if _registry is not None:
        await _registry.close_all()
        _registry = None
