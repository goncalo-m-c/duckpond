"""Query execution router for DuckPond API.

This module provides endpoints for executing SQL queries against datasets
with support for time travel, result pagination, and multiple output formats.
"""

import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, status
from pydantic import BaseModel, Field

from duckpond.api.dependencies import CurrentAccount
from duckpond.api.exceptions import (
    BadRequestException,
    NotFoundException,
    ValidationException,
)
from duckpond.query.ducklake import AccountDuckLakeManager
from duckpond.query.executor import QueryExecutor

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/query", tags=["query"])


class QueryRequest(BaseModel):
    """SQL query request."""

    query: str = Field(..., description="SQL query to execute")
    version: Optional[int] = Field(None, description="Snapshot version (time travel)")
    limit: Optional[int] = Field(None, ge=1, le=10000, description="Result limit")
    timeout_seconds: Optional[int] = Field(30, ge=1, le=300, description="Query timeout in seconds")
    output_format: Optional[str] = Field("json", description="Output format (json, arrow, csv)")


class QueryResponse(BaseModel):
    """Query execution response."""

    columns: List[str]
    rows: List[Dict[str, Any]]
    row_count: int
    execution_time_ms: float


class ExplainResponse(BaseModel):
    """Query execution plan response."""

    query: str
    plan: str
    execution_time_ms: float


@router.post(
    "/{dataset_name}",
    response_model=QueryResponse,
    status_code=status.HTTP_200_OK,
    summary="Execute SQL query",
    description="""
Execute SQL query against dataset.

Supports:
- Time travel queries via version parameter
- Result pagination via limit parameter
- Multiple output formats (JSON, Arrow, CSV)
- Query timeout enforcement
- SQL validation and security checks

Example:
    POST /api/v1/query/sales
    {
        "query": "SELECT * FROM catalog.sales WHERE amount > 100",
        "version": 5,
        "limit": 1000,
        "timeout_seconds": 30
    }
""",
)
async def execute_query(
    dataset_name: str,
    request: QueryRequest,
    account_id: CurrentAccount,
):
    """Execute SQL query against dataset.

    Args:
        dataset_name: Name of the dataset to query
        request: Query request with SQL and parameters
        account_id: Authenticated account ID

    Returns:
        QueryResponse with query results

    Raises:
        NotFoundException: If dataset not found
        ValidationException: If query validation fails
        BadRequestException: If query execution fails
    """
    start_time = time.time()

    logger.info(
        f"Executing query on dataset {dataset_name}",
        extra={
            "account_id": account_id,
            "dataset_name": dataset_name,
            "limit": request.limit,
            "timeout": request.timeout_seconds,
        },
    )

    try:
        ducklake_manager = AccountDuckLakeManager.create(account_id)
        await ducklake_manager.initialize()

        try:
            await _validate_dataset_exists(ducklake_manager, dataset_name)

            executor = QueryExecutor(ducklake_manager)

            query = request.query
            if request.version is not None:
                query = (
                    f"SELECT * FROM {dataset_name} AS OF VERSION {request.version} WHERE {query}"
                )

            result = await executor.execute_query(
                sql=query,
                output_format=request.output_format or "json",
                limit=request.limit,
                timeout_seconds=request.timeout_seconds or 30,
            )

            if request.output_format == "json" or request.output_format is None:
                columns = list(result.data[0].keys()) if result.data else []
                rows = result.data
            else:
                columns = []
                rows = []

            execution_time_ms = (time.time() - start_time) * 1000

            logger.info(
                f"Query executed successfully: {result.row_count} rows",
                extra={
                    "account_id": account_id,
                    "dataset_name": dataset_name,
                    "row_count": result.row_count,
                    "execution_time_ms": execution_time_ms,
                },
            )

            return QueryResponse(
                columns=columns,
                rows=rows,
                row_count=result.row_count,
                execution_time_ms=execution_time_ms,
            )

        finally:
            await ducklake_manager.close()

    except Exception as e:
        error_msg = str(e)
        logger.error(
            f"Query execution failed: {error_msg}",
            extra={
                "account_id": account_id,
                "dataset_name": dataset_name,
                "error": error_msg,
            },
        )

        if "not found" in error_msg.lower():
            raise NotFoundException(f"Dataset {dataset_name} not found")
        elif "validation" in error_msg.lower() or "invalid" in error_msg.lower():
            raise ValidationException(f"Query validation failed: {error_msg}")
        else:
            raise BadRequestException(f"Query execution failed: {error_msg}")


@router.post(
    "/{dataset_name}/explain",
    response_model=ExplainResponse,
    status_code=status.HTTP_200_OK,
    summary="Get query execution plan",
    description="""
Get the execution plan for a SQL query without executing it.

Useful for:
- Query optimization
- Understanding query performance
- Debugging complex queries

Example:
    POST /api/v1/query/sales/explain
    {
        "query": "SELECT * FROM catalog.sales WHERE amount > 100"
    }
""",
)
async def explain_query(
    dataset_name: str,
    request: QueryRequest,
    account_id: CurrentAccount,
):
    """Get query execution plan.

    Args:
        dataset_name: Name of the dataset to query
        request: Query request with SQL
        account_id: Authenticated account ID

    Returns:
        ExplainResponse with query execution plan

    Raises:
        NotFoundException: If dataset not found
        ValidationException: If query validation fails
        BadRequestException: If explain fails
    """
    start_time = time.time()

    logger.info(
        f"Explaining query on dataset {dataset_name}",
        extra={
            "account_id": account_id,
            "dataset_name": dataset_name,
        },
    )

    try:
        ducklake_manager = AccountDuckLakeManager.create(account_id)
        await ducklake_manager.initialize()

        try:
            await _validate_dataset_exists(ducklake_manager, dataset_name)

            executor = QueryExecutor(ducklake_manager)

            plan = await executor.explain_query(request.query)

            execution_time_ms = (time.time() - start_time) * 1000

            logger.info(
                "Query plan generated successfully",
                extra={
                    "account_id": account_id,
                    "dataset_name": dataset_name,
                    "execution_time_ms": execution_time_ms,
                },
            )

            return ExplainResponse(
                query=request.query,
                plan=plan,
                execution_time_ms=execution_time_ms,
            )

        finally:
            await ducklake_manager.close()

    except Exception as e:
        error_msg = str(e)
        logger.error(
            f"Query explain failed: {error_msg}",
            extra={
                "account_id": account_id,
                "dataset_name": dataset_name,
                "error": error_msg,
            },
        )

        if "not found" in error_msg.lower():
            raise NotFoundException(f"Dataset {dataset_name} not found")
        elif "validation" in error_msg.lower() or "invalid" in error_msg.lower():
            raise ValidationException(f"Query validation failed: {error_msg}")
        else:
            raise BadRequestException(f"Query explain failed: {error_msg}")


async def _validate_dataset_exists(
    ducklake_manager: AccountDuckLakeManager, dataset_name: str
) -> None:
    """Validate that dataset exists in catalog.

    Args:
        ducklake_manager: DuckLake manager
        dataset_name: Name of dataset

    Raises:
        NotFoundException: If dataset not found
    """
    async with ducklake_manager.get_connection() as conn:
        query = f"""
        SELECT COUNT(*) as count
        FROM information_schema.tables
        WHERE table_name = '{dataset_name}'
        """

        import asyncio

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, conn.execute, query)
        df = await loop.run_in_executor(None, result.fetchdf)

        if df.empty or df.iloc[0]["count"] == 0:
            raise NotFoundException(f"Dataset {dataset_name} not found")
