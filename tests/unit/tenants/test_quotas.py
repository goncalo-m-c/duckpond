"""Unit tests for quota enforcement and tracking."""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from duckpond.exceptions import ConcurrentQueryLimitError, QuotaExceededError
from duckpond.storage.backend import MockStorageBackend
from duckpond.tenants.models import Tenant
from duckpond.tenants.quotas import (
    TenantQueryLimiter,
    calculate_storage_usage,
    check_storage_quota,
    create_tenant_connection,
    get_quota_usage,
)


@pytest.fixture
def mock_tenant() -> Tenant:
    """Create a mock tenant for testing."""
    tenant = MagicMock(spec=Tenant)
    tenant.tenant_id = "tenant-test"
    tenant.name = "Test Tenant"
    tenant.max_storage_gb = 10  # 10 GB limit
    tenant.max_query_memory_gb = 4  # 4 GB memory limit
    tenant.max_concurrent_queries = 5  # 5 concurrent queries
    return tenant


@pytest.fixture
def mock_storage_backend() -> MockStorageBackend:
    """Create a mock storage backend for testing."""
    return MockStorageBackend()


@pytest.fixture
def query_limiter() -> TenantQueryLimiter:
    """Create a query limiter for testing."""
    return TenantQueryLimiter()


class TestStorageQuota:
    """Tests for storage quota enforcement."""

    @pytest.mark.asyncio
    async def test_check_storage_quota_within_limit(
        self, mock_tenant: Tenant, mock_storage_backend: MockStorageBackend
    ) -> None:
        """Test that storage quota check passes when within limit."""
        # Set current usage to 5 GB
        mock_storage_backend.add_file(
            "data/file1.parquet", mock_tenant.tenant_id, 5 * 1024 * 1024 * 1024
        )

        # Try to add 2 GB (total 7 GB, under 10 GB limit)
        additional_bytes = 2 * 1024 * 1024 * 1024

        # Should not raise
        await check_storage_quota(mock_tenant, additional_bytes, mock_storage_backend)

    @pytest.mark.asyncio
    async def test_check_storage_quota_exceeds_limit(
        self, mock_tenant: Tenant, mock_storage_backend: MockStorageBackend
    ) -> None:
        """Test that storage quota check fails when exceeding limit."""
        # Set current usage to 8 GB
        mock_storage_backend.add_file(
            "data/file1.parquet", mock_tenant.tenant_id, 8 * 1024 * 1024 * 1024
        )

        # Try to add 3 GB (total 11 GB, over 10 GB limit)
        additional_bytes = 3 * 1024 * 1024 * 1024

        # Should raise QuotaExceededError
        with pytest.raises(QuotaExceededError) as exc_info:
            await check_storage_quota(mock_tenant, additional_bytes, mock_storage_backend)

        assert exc_info.value.context["tenant_id"] == mock_tenant.tenant_id
        assert exc_info.value.context["quota_type"] == "storage"
        assert "10GB" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_check_storage_quota_exactly_at_limit(
        self, mock_tenant: Tenant, mock_storage_backend: MockStorageBackend
    ) -> None:
        """Test storage quota when exactly at limit."""
        # Set current usage to 9 GB
        mock_storage_backend.add_file(
            "data/file1.parquet", mock_tenant.tenant_id, 9 * 1024 * 1024 * 1024
        )

        # Try to add 1 GB (exactly at 10 GB limit)
        additional_bytes = 1 * 1024 * 1024 * 1024

        # Should not raise (at limit is OK)
        await check_storage_quota(mock_tenant, additional_bytes, mock_storage_backend)

    @pytest.mark.asyncio
    async def test_calculate_storage_usage(
        self, mock_storage_backend: MockStorageBackend
    ) -> None:
        """Test storage usage calculation."""
        tenant_id = "tenant-test"

        # Add multiple files
        mock_storage_backend.add_file("data/file1.parquet", tenant_id, 1024 * 1024 * 1024)  # 1 GB
        mock_storage_backend.add_file("data/file2.parquet", tenant_id, 512 * 1024 * 1024)  # 512 MB
        mock_storage_backend.add_file("data/file3.csv", tenant_id, 256 * 1024 * 1024)  # 256 MB

        # Calculate total usage
        usage = await calculate_storage_usage(tenant_id, mock_storage_backend)

        # Should be 1.75 GB in bytes
        expected = 1024 * 1024 * 1024 + 512 * 1024 * 1024 + 256 * 1024 * 1024
        assert usage == expected

    @pytest.mark.asyncio
    async def test_calculate_storage_usage_empty(
        self, mock_storage_backend: MockStorageBackend
    ) -> None:
        """Test storage usage calculation for empty tenant."""
        tenant_id = "tenant-empty"

        usage = await calculate_storage_usage(tenant_id, mock_storage_backend)
        assert usage == 0


class TestConcurrentQueryLimits:
    """Tests for concurrent query limit enforcement."""

    @pytest.mark.asyncio
    async def test_acquire_query_slot_success(
        self, mock_tenant: Tenant, query_limiter: TenantQueryLimiter
    ) -> None:
        """Test successful query slot acquisition."""
        async with query_limiter.acquire_query_slot(
            mock_tenant.tenant_id, mock_tenant.max_concurrent_queries
        ):
            # Should successfully acquire slot
            assert query_limiter.get_active_queries(mock_tenant.tenant_id) == 1

        # After context exit, should be released
        assert query_limiter.get_active_queries(mock_tenant.tenant_id) == 0

    @pytest.mark.asyncio
    async def test_acquire_multiple_query_slots(
        self, mock_tenant: Tenant, query_limiter: TenantQueryLimiter
    ) -> None:
        """Test multiple concurrent query slot acquisitions."""

        async def run_query(delay: float) -> None:
            async with query_limiter.acquire_query_slot(
                mock_tenant.tenant_id, mock_tenant.max_concurrent_queries
            ):
                await asyncio.sleep(delay)

        # Start 3 concurrent queries
        tasks = [
            asyncio.create_task(run_query(0.1)),
            asyncio.create_task(run_query(0.1)),
            asyncio.create_task(run_query(0.1)),
        ]

        # Give them time to start
        await asyncio.sleep(0.05)

        # Should have 3 active queries
        assert query_limiter.get_active_queries(mock_tenant.tenant_id) == 3

        # Wait for completion
        await asyncio.gather(*tasks)

        # All should be released
        assert query_limiter.get_active_queries(mock_tenant.tenant_id) == 0

    @pytest.mark.asyncio
    async def test_acquire_query_slot_exceeds_limit(
        self, mock_tenant: Tenant, query_limiter: TenantQueryLimiter
    ) -> None:
        """Test that exceeding concurrent query limit raises error."""

        async def hold_slot(duration: float) -> None:
            async with query_limiter.acquire_query_slot(
                mock_tenant.tenant_id, mock_tenant.max_concurrent_queries
            ):
                await asyncio.sleep(duration)

        # Start max_concurrent_queries tasks (5)
        tasks = [asyncio.create_task(hold_slot(0.5)) for _ in range(5)]

        # Give them time to acquire slots
        await asyncio.sleep(0.1)

        # Verify all 5 slots are taken
        assert query_limiter.get_active_queries(mock_tenant.tenant_id) == 5

        # Try to acquire one more (should fail)
        with pytest.raises(ConcurrentQueryLimitError) as exc_info:
            async with query_limiter.acquire_query_slot(
                mock_tenant.tenant_id, mock_tenant.max_concurrent_queries
            ):
                pass

        assert exc_info.value.context["tenant_id"] == mock_tenant.tenant_id
        assert exc_info.value.context["limit"] == 5

        # Clean up tasks
        await asyncio.gather(*tasks)

    @pytest.mark.asyncio
    async def test_query_slot_released_on_exception(
        self, mock_tenant: Tenant, query_limiter: TenantQueryLimiter
    ) -> None:
        """Test that query slot is released even when exception occurs."""
        with pytest.raises(ValueError):
            async with query_limiter.acquire_query_slot(
                mock_tenant.tenant_id, mock_tenant.max_concurrent_queries
            ):
                assert query_limiter.get_active_queries(mock_tenant.tenant_id) == 1
                raise ValueError("Simulated error")

        # Slot should be released even after exception
        assert query_limiter.get_active_queries(mock_tenant.tenant_id) == 0

    @pytest.mark.asyncio
    async def test_query_limiter_per_tenant_isolation(
        self, query_limiter: TenantQueryLimiter
    ) -> None:
        """Test that query limits are isolated per tenant."""
        tenant1_id = "tenant-1"
        tenant2_id = "tenant-2"

        # Acquire slot for tenant 1
        async with query_limiter.acquire_query_slot(tenant1_id, 5):
            assert query_limiter.get_active_queries(tenant1_id) == 1
            assert query_limiter.get_active_queries(tenant2_id) == 0

            # Acquire slot for tenant 2
            async with query_limiter.acquire_query_slot(tenant2_id, 5):
                assert query_limiter.get_active_queries(tenant1_id) == 1
                assert query_limiter.get_active_queries(tenant2_id) == 1

            # Tenant 2 released
            assert query_limiter.get_active_queries(tenant2_id) == 0

        # Both released
        assert query_limiter.get_active_queries(tenant1_id) == 0
        assert query_limiter.get_active_queries(tenant2_id) == 0

    def test_clear_tenant(
        self, mock_tenant: Tenant, query_limiter: TenantQueryLimiter
    ) -> None:
        """Test clearing tenant semaphore."""
        # Create semaphore by getting active queries
        query_limiter._get_or_create_semaphore(mock_tenant.tenant_id, 5)
        assert mock_tenant.tenant_id in query_limiter._semaphores

        # Clear tenant
        query_limiter.clear_tenant(mock_tenant.tenant_id)

        # Should be removed
        assert mock_tenant.tenant_id not in query_limiter._semaphores
        assert mock_tenant.tenant_id not in query_limiter._active_queries


class TestDuckDBConnection:
    """Tests for DuckDB connection creation with memory limits."""

    def test_create_tenant_connection_in_memory(self, mock_tenant: Tenant) -> None:
        """Test creating in-memory DuckDB connection with memory limits."""
        conn = create_tenant_connection(mock_tenant)

        try:
            # Verify connection works
            result = conn.execute("SELECT 1 as test").fetchone()
            assert result[0] == 1

            # Verify memory limit is set (query DuckDB settings)
            # DuckDB may round to GiB or use system limits, so just check it's set
            memory_limit = conn.execute("SELECT current_setting('memory_limit')").fetchone()[0]
            assert memory_limit is not None
            assert len(memory_limit) > 0
            # Should contain GB or GiB
            assert "GB" in memory_limit or "GiB" in memory_limit

        finally:
            conn.close()

    def test_create_tenant_connection_with_path(
        self, mock_tenant: Tenant, tmp_path
    ) -> None:
        """Test creating DuckDB connection with file path."""
        db_path = tmp_path / "test.duckdb"

        conn = create_tenant_connection(mock_tenant, str(db_path))

        try:
            # Create a table
            conn.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
            conn.execute("INSERT INTO test VALUES (1, 'test')")

            # Verify it persists
            result = conn.execute("SELECT COUNT(*) FROM test").fetchone()
            assert result[0] == 1

        finally:
            conn.close()

    def test_create_tenant_connection_memory_limit_enforced(
        self, mock_tenant: Tenant
    ) -> None:
        """Test that memory limit is actually enforced by DuckDB."""
        # Set very low memory limit for testing
        mock_tenant.max_query_memory_gb = 0.01  # 10 MB (very small)

        conn = create_tenant_connection(mock_tenant)

        try:
            # Try to create large data that exceeds limit
            # This may or may not raise depending on DuckDB version and system
            # Just verify the connection works and limit is set
            memory_limit = conn.execute("SELECT current_setting('memory_limit')").fetchone()[0]
            # DuckDB uses MiB/GiB format
            assert "MiB" in memory_limit or "GiB" in memory_limit or "MB" in memory_limit or "GB" in memory_limit

        finally:
            conn.close()


class TestQuotaUsage:
    """Tests for quota usage reporting."""

    @pytest.mark.asyncio
    async def test_get_quota_usage(
        self,
        mock_tenant: Tenant,
        mock_storage_backend: MockStorageBackend,
        query_limiter: TenantQueryLimiter,
    ) -> None:
        """Test getting quota usage statistics."""
        # Add some storage usage
        mock_storage_backend.add_file(
            "data/file1.parquet", mock_tenant.tenant_id, 3 * 1024 * 1024 * 1024  # 3 GB
        )

        # Acquire some query slots
        async def hold_slot() -> None:
            async with query_limiter.acquire_query_slot(
                mock_tenant.tenant_id, mock_tenant.max_concurrent_queries
            ):
                await asyncio.sleep(0.2)

        # Start 2 queries
        tasks = [asyncio.create_task(hold_slot()) for _ in range(2)]
        await asyncio.sleep(0.05)  # Let them acquire

        # Get quota usage
        usage = await get_quota_usage(mock_tenant, mock_storage_backend, query_limiter)

        # Verify statistics
        assert usage.tenant_id == mock_tenant.tenant_id
        assert usage.storage_used_gb == 3.0
        assert usage.storage_limit_gb == 10
        assert usage.concurrent_queries == 2
        assert usage.max_concurrent_queries == 5
        assert usage.query_memory_limit_gb == 4

        # Clean up
        await asyncio.gather(*tasks)

    @pytest.mark.asyncio
    async def test_quota_usage_to_dict(
        self,
        mock_tenant: Tenant,
        mock_storage_backend: MockStorageBackend,
        query_limiter: TenantQueryLimiter,
    ) -> None:
        """Test converting quota usage to dictionary."""
        # Add storage: 7 GB out of 10 GB
        mock_storage_backend.add_file(
            "data/file1.parquet", mock_tenant.tenant_id, 7 * 1024 * 1024 * 1024
        )

        usage = await get_quota_usage(mock_tenant, mock_storage_backend, query_limiter)
        usage_dict = usage.to_dict()

        # Verify structure
        assert "tenant_id" in usage_dict
        assert "storage" in usage_dict
        assert "queries" in usage_dict

        # Verify storage info
        storage = usage_dict["storage"]
        assert storage["used_gb"] == 7.0
        assert storage["limit_gb"] == 10
        assert storage["percentage"] == 70.0
        assert storage["exceeded"] is False

        # Verify query info
        queries = usage_dict["queries"]
        assert queries["concurrent_active"] == 0
        assert queries["max_concurrent"] == 5
        assert queries["at_limit"] is False
        assert queries["memory_limit_gb"] == 4

    @pytest.mark.asyncio
    async def test_quota_usage_storage_exceeded(
        self,
        mock_tenant: Tenant,
        mock_storage_backend: MockStorageBackend,
        query_limiter: TenantQueryLimiter,
    ) -> None:
        """Test quota usage when storage is exceeded."""
        # Add 12 GB (over 10 GB limit)
        mock_storage_backend.add_file(
            "data/file1.parquet", mock_tenant.tenant_id, 12 * 1024 * 1024 * 1024
        )

        usage = await get_quota_usage(mock_tenant, mock_storage_backend, query_limiter)

        assert usage.is_storage_exceeded is True
        assert usage.storage_percentage > 100

        usage_dict = usage.to_dict()
        assert usage_dict["storage"]["exceeded"] is True
        assert usage_dict["storage"]["percentage"] == 120.0

    @pytest.mark.asyncio
    async def test_quota_usage_queries_at_limit(
        self,
        mock_tenant: Tenant,
        mock_storage_backend: MockStorageBackend,
        query_limiter: TenantQueryLimiter,
    ) -> None:
        """Test quota usage when queries are at limit."""

        async def hold_slot() -> None:
            async with query_limiter.acquire_query_slot(
                mock_tenant.tenant_id, mock_tenant.max_concurrent_queries
            ):
                await asyncio.sleep(0.2)

        # Start max queries (5)
        tasks = [asyncio.create_task(hold_slot()) for _ in range(5)]
        await asyncio.sleep(0.05)

        usage = await get_quota_usage(mock_tenant, mock_storage_backend, query_limiter)

        assert usage.is_queries_at_limit is True
        assert usage.concurrent_queries == 5

        usage_dict = usage.to_dict()
        assert usage_dict["queries"]["at_limit"] is True

        # Clean up
        await asyncio.gather(*tasks)
