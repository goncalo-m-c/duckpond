"""Query execution models and result types."""

from dataclasses import dataclass
from typing import Literal

import pyarrow as pa


@dataclass
class QueryResult:
    """
    Query execution result with data and metadata.

    This dataclass encapsulates the result of a SQL query execution,
    including the data in the requested format, row count, execution
    time, and the original query.

    Attributes:
        data: Query results in requested format:
            - JSON: List of dictionaries (one per row)
            - Arrow: PyArrow Table
            - CSV: String with CSV-formatted data
        row_count: Number of rows returned
        execution_time_seconds: Query execution time in seconds
        query: Original SQL query string (sanitized for logging)
        format: Output format used (json, arrow, csv)
    """

    data: list[dict] | pa.Table | str
    row_count: int
    execution_time_seconds: float
    query: str
    format: Literal["json", "arrow", "csv"] = "json"

    def to_dict(self) -> dict:
        """
        Convert result to dictionary (for JSON serialization).

        Note: Arrow format is not JSON-serializable and will be excluded.

        Returns:
            Dictionary representation of query result
        """
        result = {
            "row_count": self.row_count,
            "execution_time_seconds": self.execution_time_seconds,
            "format": self.format,
        }

        if self.format == "json":
            result["data"] = self.data
        elif self.format == "csv":
            result["data"] = self.data

        return result


@dataclass
class QueryMetrics:
    """
    Query execution metrics for monitoring and logging.

    These metrics are collected during query execution and can be
    used for performance monitoring, debugging, and optimization.

    Attributes:
        account_id: Account executing the query
        query_hash: Hash of query for deduplication
        execution_time_seconds: Total execution time
        row_count: Number of rows returned
        output_format: Format used for results
        success: Whether query succeeded
        error_type: Type of error if query failed
        memory_used_mb: Approximate memory used (if available)
    """

    account_id: str
    query_hash: str
    execution_time_seconds: float
    row_count: int
    output_format: str
    success: bool
    error_type: str | None = None
    memory_used_mb: float | None = None

    def to_dict(self) -> dict:
        """Convert metrics to dictionary for logging."""
        return {
            "account_id": self.account_id,
            "query_hash": self.query_hash,
            "execution_time_seconds": self.execution_time_seconds,
            "row_count": self.row_count,
            "output_format": self.output_format,
            "success": self.success,
            "error_type": self.error_type,
            "memory_used_mb": self.memory_used_mb,
        }
