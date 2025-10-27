"""DuckDB connection pool with DuckLake catalog integration."""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

import duckdb

from duckpond.exceptions import ConnectionPoolExhaustedError

logger = logging.getLogger(__name__)


class DuckDBConnectionPool:
    """
    Connection pool for DuckDB with per-tenant DuckLake catalogs.

    This pool manages DuckDB connections with DuckLake extension loaded
    and tenant-specific catalog attached. Since DuckDB is not truly async,
    all operations run in a thread executor.

    Features:
    - Lazy connection creation up to max_connections
    - DuckLake extension auto-loading
    - Tenant catalog attachment
    - Memory and thread configuration per tenant
    - Connection health checking
    - Proper cleanup on shutdown

    Usage:
        pool = DuckDBConnectionPool(
            max_connections=10,
            catalog_url="postgresql://host/tenant_catalog",
            memory_limit="4GB",
            threads=4
        )
        await pool.initialize()

        async with pool.acquire() as conn:
            result = await pool.execute(conn, "SELECT * FROM catalog.sales")
    """

    def __init__(
        self,
        max_connections: int,
        catalog_url: str,
        memory_limit: str = "4GB",
        threads: int = 4,
    ) -> None:
        """
        Initialize connection pool.

        Args:
            max_connections: Maximum number of connections in pool
            catalog_url: DuckLake catalog URL (PostgreSQL, MySQL, SQLite, DuckDB)
            memory_limit: DuckDB memory limit (e.g., "4GB", "512MB")
            threads: Number of threads for DuckDB query execution
        """
        self.max_connections = max_connections
        self.catalog_url = catalog_url
        self.memory_limit = memory_limit
        self.threads = threads

        self._pool: asyncio.Queue[duckdb.DuckDBPyConnection] = asyncio.Queue(
            maxsize=max_connections
        )

        self._created_connections = 0
        self._lock = asyncio.Lock()

        self._closed = False

        logger.debug(
            "Initialized DuckDB connection pool",
            extra={
                "max_connections": max_connections,
                "memory_limit": memory_limit,
                "threads": threads,
            },
        )

    async def initialize(self, min_connections: int = 1) -> None:
        """
        Pre-create connections in pool.

        Args:
            min_connections: Minimum number of connections to create upfront

        Raises:
            ConnectionPoolExhaustedError: If unable to create connections
        """
        if self._closed:
            raise ConnectionPoolExhaustedError(self.max_connections)

        connections_to_create = min(min_connections, self.max_connections)

        logger.debug(f"Pre-creating {connections_to_create} connections")

        for i in range(connections_to_create):
            try:
                conn = await self._create_connection()
                await self._pool.put(conn)
                logger.debug(f"Created connection {i + 1}/{connections_to_create}")
            except Exception as e:
                logger.error(f"Failed to create connection: {e}")
                raise

        logger.debug(
            f"Connection pool initialized with {connections_to_create} connections"
        )

    async def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Create new DuckDB connection with DuckLake attached.

        This method runs DuckDB operations in a thread executor since
        DuckDB is not truly async.

        Returns:
            Configured DuckDB connection

        Raises:
            Exception: If connection creation or configuration fails
        """
        loop = asyncio.get_event_loop()

        logger.debug("Creating new DuckDB connection")
        conn = await loop.run_in_executor(None, duckdb.connect)

        try:
            await loop.run_in_executor(None, self._configure_connection, conn)

            async with self._lock:
                self._created_connections += 1

            logger.debug(
                f"Created connection (total: {self._created_connections}/{self.max_connections})"
            )

            return conn

        except Exception as e:
            logger.error(f"Failed to configure connection: {e}")
            await loop.run_in_executor(None, conn.close)
            raise

    def _configure_connection(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        Configure DuckDB connection (runs in thread executor).

        This method:
        1. Sets memory limit and thread count
        2. Disables progress bar for non-interactive use
        3. Installs and loads DuckLake extension
        4. Attaches tenant's DuckLake catalog

        Args:
            conn: DuckDB connection to configure

        Raises:
            Exception: If configuration fails
        """
        logger.debug("Configuring DuckDB connection")

        conn.execute(f"SET memory_limit='{self.memory_limit}'")
        conn.execute(f"SET threads={self.threads}")
        conn.execute("SET enable_progress_bar=false")

        logger.debug(
            "Applied DuckDB settings",
            extra={
                "memory_limit": self.memory_limit,
                "threads": self.threads,
            },
        )

        logger.debug("Installing DuckLake extension")
        conn.execute("INSTALL ducklake")
        conn.execute("LOAD ducklake")

        logger.debug(
            "Attaching DuckLake catalog", extra={"catalog_url": self.catalog_url}
        )

        conn.execute(f"ATTACH '{self.catalog_url}' AS catalog (TYPE ducklake)")

        logger.debug("DuckDB connection configured successfully")

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[duckdb.DuckDBPyConnection]:
        """
        Acquire connection from pool.

        This context manager acquires a connection, yields it,
        and returns it to the pool when done.

        Yields:
            DuckDB connection with DuckLake catalog attached

        Raises:
            ConnectionPoolExhaustedError: If pool is exhausted and cannot create new connection
            asyncio.TimeoutError: If connection acquisition times out

        Usage:
            async with pool.acquire() as conn:
                result = await pool.execute(conn, "SELECT * FROM catalog.sales")
        """
        if self._closed:
            raise ConnectionPoolExhaustedError(self.max_connections)

        conn: duckdb.DuckDBPyConnection | None = None

        try:
            conn = await asyncio.wait_for(self._pool.get(), timeout=5.0)
            logger.debug("Acquired connection from pool")

        except asyncio.TimeoutError:
            async with self._lock:
                if self._created_connections < self.max_connections:
                    logger.debug("Pool empty, creating new connection")
                    conn = await self._create_connection()
                else:
                    logger.error(
                        f"Connection pool exhausted: {self._created_connections}/{self.max_connections}"
                    )
                    raise ConnectionPoolExhaustedError(self.max_connections)

        try:
            await self._check_connection_health(conn)
            yield conn

        finally:
            if conn is not None:
                try:
                    await self._pool.put(conn)
                    logger.debug("Released connection back to pool")
                except Exception as e:
                    logger.error(f"Failed to return connection to pool: {e}")

    async def _check_connection_health(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        Check connection health.

        Args:
            conn: Connection to check

        Raises:
            Exception: If connection is unhealthy
        """
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, conn.execute, "SELECT 1")
        except Exception as e:
            logger.error(f"Connection health check failed: {e}")
            raise

    async def execute(self, conn: duckdb.DuckDBPyConnection, sql: str):
        """
        Execute SQL query in thread executor.

        Args:
            conn: DuckDB connection
            sql: SQL query string

        Returns:
            DuckDB relation result

        Usage:
            async with pool.acquire() as conn:
                result = await pool.execute(conn, "SELECT * FROM catalog.sales")
                df = result.df()
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, conn.execute, sql)

    async def close(self) -> None:
        """
        Close all connections in pool.

        This method should be called when shutting down the application
        to properly clean up resources.
        """
        if self._closed:
            return

        logger.info("Closing connection pool")
        self._closed = True

        closed_count = 0
        while not self._pool.empty():
            try:
                conn = await self._pool.get()
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, conn.close)
                closed_count += 1
            except Exception as e:
                logger.error(f"Error closing connection: {e}")

        logger.info(f"Connection pool closed ({closed_count} connections)")

    @property
    def available_connections(self) -> int:
        """Get number of available connections in pool."""
        return self._pool.qsize()

    @property
    def total_connections(self) -> int:
        """Get total number of created connections."""
        return self._created_connections
