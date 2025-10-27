"""Tests for query CLI commands."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from typer.testing import CliRunner

from duckpond.cli.main import app
from duckpond.query.models import QueryResult
from duckpond.tenants.models import Tenant

runner = CliRunner()


@pytest.fixture
def mock_tenant():
    """Create mock tenant."""
    return Tenant(
        tenant_id="test-tenant",
        name="Test Tenant",
        api_key_hash="hash123",
        ducklake_catalog_url="test_catalog.db",
        storage_backend="local",
        max_storage_gb=100,
        max_query_memory_gb=4,
        max_concurrent_queries=10,
    )


@pytest.fixture
def mock_query_result():
    """Create mock query result."""
    return QueryResult(
        data=[
            {"id": 1, "name": "Alice", "amount": 100},
            {"id": 2, "name": "Bob", "amount": 200},
        ],
        row_count=2,
        execution_time_seconds=0.5,
        query="SELECT * FROM catalog.sales",
        format="json",
    )


class TestQueryExecute:
    """Tests for query execute command."""

    @patch("duckpond.cli.query.get_session")
    @patch("duckpond.cli.query.TenantManager")
    @patch("duckpond.cli.query.TenantDuckLakeManager")
    @patch("duckpond.cli.query.QueryExecutor")
    def test_execute_inline_sql(
        self,
        mock_executor_class,
        mock_ducklake_class,
        mock_manager_class,
        mock_session,
        mock_tenant,
        mock_query_result,
    ):
        """Test executing inline SQL query."""
        # Setup mocks
        mock_session_ctx = AsyncMock()
        mock_session.return_value.__aenter__.return_value = mock_session_ctx

        mock_manager = MagicMock()
        mock_manager.get_tenant = AsyncMock(return_value=mock_tenant)
        mock_manager_class.return_value = mock_manager

        mock_executor = MagicMock()
        mock_executor.execute_query = AsyncMock(return_value=mock_query_result)
        mock_executor_class.return_value = mock_executor

        # Run command
        result = runner.invoke(
            app,
            [
                "query",
                "execute",
                "--tenant",
                "test-tenant",
                "--sql",
                "SELECT * FROM catalog.sales",
            ],
        )

        # Verify
        assert result.exit_code == 0
        assert "2" in result.stdout  # Row count
        assert "0.500" in result.stdout  # Execution time

    @patch("duckpond.cli.query.get_session")
    @patch("duckpond.cli.query.TenantManager")
    @patch("duckpond.cli.query.TenantDuckLakeManager")
    @patch("duckpond.cli.query.QueryExecutor")
    def test_execute_from_file(
        self,
        mock_executor_class,
        mock_ducklake_class,
        mock_manager_class,
        mock_session,
        mock_tenant,
        mock_query_result,
        tmp_path,
    ):
        """Test executing SQL from file."""
        # Create SQL file
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT * FROM catalog.sales WHERE amount > 100")

        # Setup mocks
        mock_session_ctx = AsyncMock()
        mock_session.return_value.__aenter__.return_value = mock_session_ctx

        mock_manager = MagicMock()
        mock_manager.get_tenant = AsyncMock(return_value=mock_tenant)
        mock_manager_class.return_value = mock_manager

        mock_executor = MagicMock()
        mock_executor.execute_query = AsyncMock(return_value=mock_query_result)
        mock_executor_class.return_value = mock_executor

        # Run command
        result = runner.invoke(
            app,
            [
                "query",
                "execute",
                "--tenant",
                "test-tenant",
                "--file",
                str(sql_file),
            ],
        )

        # Verify
        assert result.exit_code == 0
        assert "Reading SQL from" in result.stdout

    @patch("duckpond.cli.query.get_session")
    @patch("duckpond.cli.query.TenantManager")
    @patch("duckpond.cli.query.TenantDuckLakeManager")
    @patch("duckpond.cli.query.QueryExecutor")
    def test_execute_json_output(
        self,
        mock_executor_class,
        mock_ducklake_class,
        mock_manager_class,
        mock_session,
        mock_tenant,
        mock_query_result,
    ):
        """Test query with JSON output format."""
        # Setup mocks
        mock_session_ctx = AsyncMock()
        mock_session.return_value.__aenter__.return_value = mock_session_ctx

        mock_manager = MagicMock()
        mock_manager.get_tenant = AsyncMock(return_value=mock_tenant)
        mock_manager_class.return_value = mock_manager

        mock_executor = MagicMock()
        mock_executor.execute_query = AsyncMock(return_value=mock_query_result)
        mock_executor_class.return_value = mock_executor

        # Run command
        result = runner.invoke(
            app,
            [
                "query",
                "execute",
                "--tenant",
                "test-tenant",
                "--sql",
                "SELECT * FROM catalog.sales",
                "--output",
                "json",
            ],
        )

        # Verify
        assert result.exit_code == 0

    @patch("duckpond.cli.query.get_session")
    @patch("duckpond.cli.query.TenantManager")
    @patch("duckpond.cli.query.TenantDuckLakeManager")
    @patch("duckpond.cli.query.QueryExecutor")
    def test_execute_with_limit(
        self,
        mock_executor_class,
        mock_ducklake_class,
        mock_manager_class,
        mock_session,
        mock_tenant,
        mock_query_result,
    ):
        """Test query with row limit."""
        # Setup mocks
        mock_session_ctx = AsyncMock()
        mock_session.return_value.__aenter__.return_value = mock_session_ctx

        mock_manager = MagicMock()
        mock_manager.get_tenant = AsyncMock(return_value=mock_tenant)
        mock_manager_class.return_value = mock_manager

        mock_executor = MagicMock()
        mock_executor.execute_query = AsyncMock(return_value=mock_query_result)
        mock_executor_class.return_value = mock_executor

        # Run command
        result = runner.invoke(
            app,
            [
                "query",
                "execute",
                "--tenant",
                "test-tenant",
                "--sql",
                "SELECT * FROM catalog.sales",
                "--limit",
                "100",
            ],
        )

        # Verify
        assert result.exit_code == 0
        # Verify limit was passed to executor
        mock_executor.execute_query.assert_called_once()
        call_kwargs = mock_executor.execute_query.call_args[1]
        assert call_kwargs["limit"] == 100

    @patch("duckpond.cli.query.get_session")
    @patch("duckpond.cli.query.TenantManager")
    @patch("duckpond.cli.query.TenantDuckLakeManager")
    @patch("duckpond.cli.query.QueryExecutor")
    def test_execute_with_export(
        self,
        mock_executor_class,
        mock_ducklake_class,
        mock_manager_class,
        mock_session,
        mock_tenant,
        mock_query_result,
        tmp_path,
    ):
        """Test query with result export to file."""
        export_file = tmp_path / "results.json"

        # Setup mocks
        mock_session_ctx = AsyncMock()
        mock_session.return_value.__aenter__.return_value = mock_session_ctx

        mock_manager = MagicMock()
        mock_manager.get_tenant = AsyncMock(return_value=mock_tenant)
        mock_manager_class.return_value = mock_manager

        mock_executor = MagicMock()
        mock_executor.execute_query = AsyncMock(return_value=mock_query_result)
        mock_executor_class.return_value = mock_executor

        # Run command
        result = runner.invoke(
            app,
            [
                "query",
                "execute",
                "--tenant",
                "test-tenant",
                "--sql",
                "SELECT * FROM catalog.sales",
                "--export",
                str(export_file),
            ],
        )

        # Verify
        assert result.exit_code == 0
        assert export_file.exists()
        assert "exported" in result.stdout.lower()

    @patch("duckpond.cli.query.get_session")
    @patch("duckpond.cli.query.TenantManager")
    @patch("duckpond.cli.query.TenantDuckLakeManager")
    @patch("duckpond.cli.query.QueryExecutor")
    def test_execute_time_travel_query(
        self,
        mock_executor_class,
        mock_ducklake_class,
        mock_manager_class,
        mock_session,
        mock_tenant,
        mock_query_result,
    ):
        """Test time travel query with AS OF."""
        # Setup mocks
        mock_session_ctx = AsyncMock()
        mock_session.return_value.__aenter__.return_value = mock_session_ctx

        mock_manager = MagicMock()
        mock_manager.get_tenant = AsyncMock(return_value=mock_tenant)
        mock_manager_class.return_value = mock_manager

        mock_executor = MagicMock()
        mock_executor.execute_query = AsyncMock(return_value=mock_query_result)
        mock_executor_class.return_value = mock_executor

        # Run command
        result = runner.invoke(
            app,
            [
                "query",
                "execute",
                "--tenant",
                "test-tenant",
                "--sql",
                "SELECT * FROM catalog.sales",
                "--as-of",
                "2024-01-01",
            ],
        )

        # Verify
        assert result.exit_code == 0
        assert "Time travel" in result.stdout

    def test_execute_no_sql_provided(self):
        """Test error when no SQL is provided."""
        result = runner.invoke(app, ["query", "execute", "--tenant", "test-tenant"])

        assert result.exit_code == 1
        error_output = result.stdout + result.stderr
        assert "Provide SQL" in error_output or "sql" in error_output.lower()

    def test_execute_both_sql_and_file(self, tmp_path):
        """Test error when both SQL and file are provided."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT 1")

        result = runner.invoke(
            app,
            [
                "query",
                "execute",
                "--tenant",
                "test-tenant",
                "--sql",
                "SELECT 1",
                "--file",
                str(sql_file),
            ],
        )

        assert result.exit_code == 1
        error_output = result.stdout + result.stderr
        assert (
            "either --sql or --file" in error_output.lower()
            or "provide" in error_output.lower()
        )

    def test_execute_invalid_output_format(self):
        """Test error with invalid output format."""
        result = runner.invoke(
            app,
            [
                "query",
                "execute",
                "--tenant",
                "test-tenant",
                "--sql",
                "SELECT 1",
                "--output",
                "xml",
            ],
        )

        assert result.exit_code == 1
        error_output = result.stdout + result.stderr
        assert (
            "Invalid output format" in error_output or "Valid formats" in error_output
        )


class TestQueryExplain:
    """Tests for query explain command."""

    @patch("duckpond.cli.query.get_session")
    @patch("duckpond.cli.query.TenantManager")
    @patch("duckpond.cli.query.TenantDuckLakeManager")
    @patch("duckpond.cli.query.QueryExecutor")
    def test_explain_query(
        self,
        mock_executor_class,
        mock_ducklake_class,
        mock_manager_class,
        mock_session,
        mock_tenant,
    ):
        """Test EXPLAIN command."""
        # Setup mocks
        mock_session_ctx = AsyncMock()
        mock_session.return_value.__aenter__.return_value = mock_session_ctx

        mock_manager = MagicMock()
        mock_manager.get_tenant = AsyncMock(return_value=mock_tenant)
        mock_manager_class.return_value = mock_manager

        mock_executor = MagicMock()
        mock_executor.explain_query = AsyncMock(
            return_value="QUERY PLAN:\nSEQ SCAN on catalog.sales"
        )
        mock_executor_class.return_value = mock_executor

        # Run command
        result = runner.invoke(
            app,
            [
                "query",
                "explain",
                "--tenant",
                "test-tenant",
                "--sql",
                "SELECT * FROM catalog.sales",
            ],
        )

        # Verify
        assert result.exit_code == 0
        assert "Query Execution Plan" in result.stdout
        assert "SEQ SCAN" in result.stdout

    def test_explain_no_sql_provided(self):
        """Test EXPLAIN error when no SQL is provided."""
        result = runner.invoke(app, ["query", "explain", "--tenant", "test-tenant"])

        assert result.exit_code == 1
        error_output = result.stdout + result.stderr
        assert "Provide SQL" in error_output or "sql" in error_output.lower()
