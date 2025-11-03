"""Query executor for DuckDB with DuckLake catalog integration."""

import asyncio
import base64
import logging
import time
from pathlib import Path
from typing import Literal, Optional

import duckdb
import pyarrow as pa

from duckpond.exceptions import QueryExecutionError, QueryTimeoutError
from duckpond.query.ducklake import AccountDuckLakeManager
from duckpond.query.models import QueryMetrics, QueryResult
from duckpond.query.validator import SQLValidator, sanitize_for_logging

logger = logging.getLogger(__name__)


class QueryExecutor:
    """
    Execute SQL queries against account's DuckLake catalog.

    This class provides a high-level interface for executing SQL queries
    with security validation, format conversion, and metrics collection.

    Features:
    - SQL validation (blocks dangerous operations)
    - Multiple output formats (JSON, Arrow, CSV)
    - Query timeout enforcement
    - Error handling and logging
    - Query metrics collection
    - Automatic connection management via pool

    Usage:
        executor = QueryExecutor(ducklake_manager)
        result = await executor.execute_query(
            sql="SELECT * FROM catalog.sales",
            output_format="json",
            timeout_seconds=30
        )
    """

    def __init__(
        self,
        ducklake_manager: AccountDuckLakeManager,
        validator: SQLValidator | None = None,
    ) -> None:
        """
        Initialize query executor.

        Args:
            ducklake_manager: Account's DuckLake manager with connection pool
            validator: SQL validator (uses default if not provided)
        """
        self.ducklake_manager = ducklake_manager
        self.validator = validator or SQLValidator()
        self.account_id = ducklake_manager.account.account_id

        logger.debug(
            f"Initialized QueryExecutor for account {self.account_id}",
            extra={"account_id": self.account_id},
        )

    async def execute_query(
        self,
        sql: str,
        output_format: Literal["json", "arrow", "csv"] = "json",
        limit: int | None = None,
        timeout_seconds: int = 30,
        attach_catalog: str | None = None,
    ) -> QueryResult:
        """
        Execute SQL query and return results.

        Args:
            sql: SQL query string
            output_format: Output format (json, arrow, csv)
            limit: Optional row limit to apply
            timeout_seconds: Query timeout in seconds
            attach_catalog: Optional catalog to attach (format: "name:path" or "name:ducklake:path")

        Returns:
            QueryResult with data in requested format

        Raises:
            SQLValidationError: If SQL validation fails
            QueryTimeoutError: If query exceeds timeout
            QueryExecutionError: If query execution fails
        """
        start_time = time.time()
        query_hash = self.validator.hash_query(sql)
        sanitized_sql = sanitize_for_logging(sql)
        logger.info(
            "Executing query",
            extra={
                "account_id": self.account_id,
                "query_hash": query_hash,
                "output_format": output_format,
                "limit": limit,
                "timeout_seconds": timeout_seconds,
                "query": sanitized_sql,
            },
        )

        try:
            self.validator.validate(sql)

            sql_with_limit = self._apply_limit(sql, limit)

            result = await asyncio.wait_for(
                self._execute_with_connection(sql_with_limit, output_format, attach_catalog),
                timeout=timeout_seconds,
            )

            execution_time = time.time() - start_time
            self._log_metrics(
                query_hash=query_hash,
                execution_time=execution_time,
                row_count=result.row_count,
                output_format=output_format,
                success=True,
            )

            logger.info(
                "Query executed successfully",
                extra={
                    "account_id": self.account_id,
                    "query_hash": query_hash,
                    "row_count": result.row_count,
                    "execution_time": execution_time,
                },
            )

            return result

        except asyncio.TimeoutError:
            execution_time = time.time() - start_time
            self._log_metrics(
                query_hash=query_hash,
                execution_time=execution_time,
                row_count=0,
                output_format=output_format,
                success=False,
                error_type="timeout",
            )

            logger.error(
                "Query timeout",
                extra={
                    "account_id": self.account_id,
                    "query_hash": query_hash,
                    "timeout_seconds": timeout_seconds,
                },
            )
            raise QueryTimeoutError(timeout_seconds)

        except Exception as e:
            execution_time = time.time() - start_time
            self._log_metrics(
                query_hash=query_hash,
                execution_time=execution_time,
                row_count=0,
                output_format=output_format,
                success=False,
                error_type=type(e).__name__,
            )

            logger.error(
                f"Query execution failed: {e}",
                extra={
                    "account_id": self.account_id,
                    "query_hash": query_hash,
                    "error": str(e),
                },
            )
            raise

    async def _execute_with_connection(
        self, sql: str, output_format: str, attach_catalog: str | None = None
    ) -> QueryResult:
        """
        Execute query using connection pool.

        Args:
            sql: SQL query string
            output_format: Output format (json, arrow, csv)
            attach_catalog: Optional catalog to attach before query execution

        Returns:
            QueryResult with data in requested format

        Raises:
            QueryExecutionError: If execution fails
        """
        async with self.ducklake_manager.get_connection() as conn:
            try:
                loop = asyncio.get_event_loop()

                if attach_catalog:
                    await loop.run_in_executor(None, self._attach_catalog, conn, attach_catalog)

                def execute_query():
                    return conn.execute(sql)

                result = await loop.run_in_executor(None, execute_query)

                data, row_count = await self._convert_result(result, output_format)

                return QueryResult(
                    data=data,
                    row_count=row_count,
                    execution_time_seconds=0,
                    query=sql,
                    format=output_format,
                )

            except Exception as e:
                logger.error(f"Query execution error: {e}")
                raise QueryExecutionError(f"Query failed: {e}", query=sql) from e

    async def _convert_result(
        self, result: duckdb.DuckDBPyRelation, output_format: str
    ) -> tuple[list[dict] | pa.Table | str, int]:
        """
        Convert DuckDB result to requested format.

        Args:
            result: DuckDB query result
            output_format: Target format (json, arrow, csv)

        Returns:
            Tuple of (converted data, row count)

        Raises:
            ValueError: If output format is unsupported
        """
        loop = asyncio.get_event_loop()

        if output_format == "json":
            df = await loop.run_in_executor(None, result.fetchdf)
            data = df.to_dict(orient="records")
            row_count = len(data)

        elif output_format == "arrow":
            data = await loop.run_in_executor(None, result.fetch_arrow_table)
            row_count = data.num_rows

        elif output_format == "csv":
            df = await loop.run_in_executor(None, result.fetchdf)
            data = df.to_csv(index=False)
            row_count = len(df)

        else:
            raise ValueError(f"Unsupported output format: {output_format}")

        return data, row_count

    def _attach_catalog(self, conn: duckdb.DuckDBPyConnection, catalog_name: str) -> None:
        """
        Attach additional catalog to connection.

        Args:
            conn: DuckDB connection
            catalog_name: Name of the catalog to attach (e.g., 'default')
                         The catalog path will be resolved from account's data directory

        Raises:
            ValueError: If catalog path cannot be resolved
            QueryExecutionError: If ATTACH fails
        """
        try:
            account = self.ducklake_manager.account
            catalog_path = self._resolve_catalog_path(account, catalog_name)

            ducklake_url = f"sqlite:{catalog_path}"
            attach_sql = f"ATTACH '{ducklake_url}' AS \"{catalog_name}\" (TYPE ducklake)"

            logger.debug(f"Attaching catalog: {attach_sql}")
            conn.execute(attach_sql)
            logger.debug(f"Successfully attached catalog '{catalog_name}'")

        except Exception as e:
            logger.error(f"Failed to attach catalog: {e}")
            raise QueryExecutionError(
                f"Failed to attach catalog '{catalog_name}': {e}", query=catalog_name
            ) from e

    def _resolve_catalog_path(self, account, catalog_name: str) -> str:
        """
        Resolve catalog path from account configuration.

        Args:
            account: Account object
            catalog_name: Name of the catalog

        Returns:
            Full path to the catalog file

        Raises:
            ValueError: If catalog path cannot be determined
        """
        from pathlib import Path
        from duckpond.config import get_settings

        settings = get_settings()

        account_data_dir = (
            Path(settings.local_storage_path).expanduser() / "accounts" / account.account_id
        )
        catalog_path = account_data_dir / f"{catalog_name}_catalog.sqlite"

        if not catalog_path.exists():
            raise ValueError(
                f"Catalog file not found: {catalog_path}. "
                f"Available catalogs should be in {account_data_dir}"
            )

        return str(catalog_path)

    def _apply_limit(self, sql: str, limit: int | None) -> str:
        """
        Apply row limit to SQL query.

        Args:
            sql: Original SQL query
            limit: Row limit to apply

        Returns:
            SQL with limit applied
        """
        if limit is None:
            return sql

        return f"SELECT * FROM ({sql}) AS limited_query LIMIT {limit}"

    def _log_metrics(
        self,
        query_hash: str,
        execution_time: float,
        row_count: int,
        output_format: str,
        success: bool,
        error_type: str | None = None,
    ) -> None:
        """
        Log query metrics.

        Args:
            query_hash: Hash of query
            execution_time: Execution time in seconds
            row_count: Number of rows returned
            output_format: Output format used
            success: Whether query succeeded
            error_type: Type of error if failed
        """
        metrics = QueryMetrics(
            account_id=self.account_id,
            query_hash=query_hash,
            execution_time_seconds=execution_time,
            row_count=row_count,
            output_format=output_format,
            success=success,
            error_type=error_type,
        )

        logger.info(
            "Query metrics",
            extra={"metrics": metrics.to_dict()},
        )

    async def explain_query(self, sql: str, attach_catalog: str | None = None) -> str:
        """
        Get query execution plan.

        Args:
            sql: SQL query to explain
            attach_catalog: Optional catalog to attach before explaining

        Returns:
            Query execution plan as string

        Raises:
            SQLValidationError: If SQL validation fails
            QueryExecutionError: If EXPLAIN fails
        """
        self.validator.validate(sql)

        explain_sql = f"EXPLAIN {sql}"

        async with self.ducklake_manager.get_connection() as conn:
            try:
                loop = asyncio.get_event_loop()

                if attach_catalog:
                    await loop.run_in_executor(None, self._attach_catalog, conn, attach_catalog)

                result = await loop.run_in_executor(None, conn.execute, explain_sql)
                df = await loop.run_in_executor(None, result.fetchdf)

                if not df.empty and "explain_value" in df.columns:
                    plan_text = df.iloc[0]["explain_value"]
                    return plan_text.replace("\\n", "\n")

                return df.to_string()

            except Exception as e:
                logger.error(f"EXPLAIN failed: {e}")
                raise QueryExecutionError(f"EXPLAIN failed: {e}", query=sql) from e

    async def get_query_stats(self) -> dict:
        """
        Get statistics about query execution.

        Returns:
            Dictionary with query statistics
        """
        return {
            "account_id": self.account_id,
            "available_connections": self.ducklake_manager.available_connections,
            "total_connections": self.ducklake_manager.total_connections,
            "is_initialized": self.ducklake_manager.is_initialized,
        }
