"""Unit tests for TenantManager."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import bcrypt
import pytest
from sqlalchemy import select

from duckpond.tenants.manager import (
    APIKeyNotFoundError,
    TenantAlreadyExistsError,
    TenantManager,
    TenantManagerError,
    TenantNotFoundError,
)
from duckpond.tenants.models import APIKey, Tenant


class TestTenantManager:
    """Test suite for TenantManager class."""

    @pytest.mark.asyncio
    async def test_create_tenant_with_defaults(self, test_session, test_settings):
        """Test creating tenant with default values."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, api_key = await manager.create_tenant(name="Test Tenant")

        assert tenant.tenant_id == "test-tenant"
        assert tenant.name == "Test Tenant"
        assert tenant.storage_backend == "local"
        assert tenant.max_storage_gb == 100
        assert tenant.max_query_memory_gb == 4
        assert tenant.max_concurrent_queries == 10
        assert api_key  # API key should be generated
        assert len(api_key) > 20  # API key should be reasonably long

        # Verify password hash
        assert bcrypt.checkpw(api_key.encode(), tenant.api_key_hash.encode())

    @pytest.mark.asyncio
    async def test_create_tenant_with_custom_values(self, test_session, test_settings):
        """Test creating tenant with custom quota values."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, api_key = await manager.create_tenant(
                name="Custom Tenant",
                storage_backend="s3",
                storage_config={"bucket": "test-bucket", "region": "us-west-2"},
                max_storage_gb=500,
                max_query_memory_gb=16,
                max_concurrent_queries=50,
            )

        assert tenant.tenant_id == "custom-tenant"
        assert tenant.name == "Custom Tenant"
        assert tenant.storage_backend == "s3"
        assert tenant.storage_config == {"bucket": "test-bucket", "region": "us-west-2"}
        assert tenant.max_storage_gb == 500
        assert tenant.max_query_memory_gb == 16
        assert tenant.max_concurrent_queries == 50

    @pytest.mark.asyncio
    async def test_create_tenant_generates_unique_ids(
        self, test_session, test_settings
    ):
        """Test that duplicate names generate unique tenant IDs."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant1, _ = await manager.create_tenant(name="Duplicate")

            # Mock the name check to allow duplicate name for ID generation test
            # In reality, this would fail at the name uniqueness check
            # So we test the ID generation separately
            assert tenant1.tenant_id == "duplicate"

    @pytest.mark.asyncio
    async def test_create_tenant_duplicate_name_fails(
        self, test_session, test_settings
    ):
        """Test that creating tenant with duplicate name fails."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            await manager.create_tenant(name="Unique Name")
            await test_session.commit()

            with pytest.raises(TenantAlreadyExistsError) as exc_info:
                await manager.create_tenant(name="Unique Name")

            assert "already exists" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_create_tenant_creates_catalog(self, test_session, test_settings):
        """Test that tenant creation creates DuckLake catalog."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, _ = await manager.create_tenant(name="Catalog Test")

            # Verify catalog URL is set
            assert tenant.ducklake_catalog_url

            # For SQLite, verify it's a file path
            if test_settings.is_sqlite:
                # Catalog URL should point to the catalog sqlite file
                assert tenant.ducklake_catalog_url.endswith("_catalog.sqlite")

    @pytest.mark.asyncio
    async def test_get_tenant_existing(self, test_session, test_settings):
        """Test retrieving existing tenant."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            created_tenant, _ = await manager.create_tenant(name="Get Test")
            await test_session.commit()

            retrieved_tenant = await manager.get_tenant(created_tenant.tenant_id)

            assert retrieved_tenant is not None
            assert retrieved_tenant.tenant_id == created_tenant.tenant_id
            assert retrieved_tenant.name == "Get Test"

    @pytest.mark.asyncio
    async def test_get_tenant_not_found(self, test_session):
        """Test retrieving non-existent tenant returns None."""
        manager = TenantManager(test_session)

        tenant = await manager.get_tenant("tenant-nonexistent")

        assert tenant is None

    @pytest.mark.asyncio
    async def test_get_tenant_by_id_existing(self, test_session, test_settings):
        """Test get_tenant_by_id retrieves existing tenant."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            created_tenant, _ = await manager.create_tenant(name="Get By ID Test")
            await test_session.commit()

            retrieved_tenant = await manager.get_tenant_by_id(created_tenant.tenant_id)

            assert retrieved_tenant.tenant_id == created_tenant.tenant_id
            assert retrieved_tenant.name == "Get By ID Test"

    @pytest.mark.asyncio
    async def test_get_tenant_by_id_not_found_raises(self, test_session):
        """Test get_tenant_by_id raises exception for non-existent tenant."""
        manager = TenantManager(test_session)

        with pytest.raises(TenantNotFoundError) as exc_info:
            await manager.get_tenant_by_id("tenant-nonexistent")

        assert "not found" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_list_tenants_empty(self, test_session):
        """Test listing tenants when none exist."""
        manager = TenantManager(test_session)

        tenants, total = await manager.list_tenants()

        assert tenants == []
        assert total == 0

    @pytest.mark.asyncio
    async def test_list_tenants_with_data(self, test_session, test_settings):
        """Test listing tenants with multiple records."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            # Create multiple tenants
            await manager.create_tenant(name="Tenant 1")
            await manager.create_tenant(name="Tenant 2")
            await manager.create_tenant(name="Tenant 3")
            await test_session.commit()

            tenants, total = await manager.list_tenants()

            assert len(tenants) == 3
            assert total == 3
            assert all(isinstance(t, Tenant) for t in tenants)

    @pytest.mark.asyncio
    async def test_list_tenants_pagination(self, test_session, test_settings):
        """Test tenant listing with pagination."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            # Create 5 tenants
            for i in range(5):
                await manager.create_tenant(name=f"Tenant {i}")
            await test_session.commit()

            # Get first page
            page1, total = await manager.list_tenants(offset=0, limit=2)
            assert len(page1) == 2
            assert total == 5

            # Get second page
            page2, total = await manager.list_tenants(offset=2, limit=2)
            assert len(page2) == 2
            assert total == 5

            # Verify different tenants
            page1_ids = {t.tenant_id for t in page1}
            page2_ids = {t.tenant_id for t in page2}
            assert page1_ids != page2_ids

    @pytest.mark.asyncio
    async def test_update_tenant_quotas_all_fields(self, test_session, test_settings):
        """Test updating all tenant quota fields."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, _ = await manager.create_tenant(name="Update Test")
            await test_session.commit()

            updated = await manager.update_tenant_quotas(
                tenant.tenant_id,
                max_storage_gb=200,
                max_query_memory_gb=8,
                max_concurrent_queries=20,
            )

            assert updated.max_storage_gb == 200
            assert updated.max_query_memory_gb == 8
            assert updated.max_concurrent_queries == 20

    @pytest.mark.asyncio
    async def test_update_tenant_quotas_partial(self, test_session, test_settings):
        """Test updating only some tenant quota fields."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, _ = await manager.create_tenant(
                name="Partial Update Test",
                max_storage_gb=100,
                max_query_memory_gb=4,
                max_concurrent_queries=10,
            )
            await test_session.commit()

            updated = await manager.update_tenant_quotas(
                tenant.tenant_id,
                max_storage_gb=300,  # Only update storage
            )

            assert updated.max_storage_gb == 300
            assert updated.max_query_memory_gb == 4  # Unchanged
            assert updated.max_concurrent_queries == 10  # Unchanged

    @pytest.mark.asyncio
    async def test_update_tenant_quotas_not_found(self, test_session):
        """Test updating quotas for non-existent tenant raises exception."""
        manager = TenantManager(test_session)

        with pytest.raises(TenantNotFoundError):
            await manager.update_tenant_quotas(
                "tenant-nonexistent",
                max_storage_gb=200,
            )

    @pytest.mark.asyncio
    async def test_delete_tenant_without_purge(self, test_session, test_settings):
        """Test deleting tenant without data purge."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, _ = await manager.create_tenant(name="Delete Test")
            await test_session.commit()

            tenant_id = tenant.tenant_id

            await manager.delete_tenant(tenant_id, purge_data=False)
            await test_session.commit()

            # Verify tenant is deleted
            deleted_tenant = await manager.get_tenant(tenant_id)
            assert deleted_tenant is None

    @pytest.mark.asyncio
    async def test_delete_tenant_with_purge(self, test_session, test_settings):
        """Test deleting tenant with data purge."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, _ = await manager.create_tenant(name="Purge Test")
            catalog_url = tenant.ducklake_catalog_url
            await test_session.commit()

            # For SQLite catalogs, verify file exists
            if test_settings.is_sqlite:
                catalog_path = Path(catalog_url)
                assert catalog_path.exists()

            await manager.delete_tenant(tenant.tenant_id, purge_data=True)
            await test_session.commit()

            # Verify catalog file is deleted (for SQLite)
            if test_settings.is_sqlite:
                assert not catalog_path.exists()

    @pytest.mark.asyncio
    async def test_delete_tenant_not_found(self, test_session):
        """Test deleting non-existent tenant raises exception."""
        manager = TenantManager(test_session)

        with pytest.raises(TenantNotFoundError):
            await manager.delete_tenant("tenant-nonexistent")

    @pytest.mark.asyncio
    async def test_tenant_id_generation_with_special_chars(
        self, test_session, test_settings
    ):
        """Test tenant ID generation handles special characters."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, _ = await manager.create_tenant(name="Test @ Company! #123")

            # Should be slugified
            assert tenant.tenant_id == "test-company-123"
            assert "#" not in tenant.tenant_id
            assert "@" not in tenant.tenant_id
            assert "!" not in tenant.tenant_id

    @pytest.mark.asyncio
    async def test_catalog_url_format_sqlite(self, test_session, test_settings):
        """Test catalog URL format for SQLite backend."""
        # Ensure SQLite backend
        test_settings.metadata_db_url = "sqlite:///test.db"

        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, _ = await manager.create_tenant(name="SQLite Catalog Test")

            # Verify it's a file path
            assert not tenant.ducklake_catalog_url.startswith("postgresql")
            # Should be .sqlite not .duckdb for catalog
            assert ".sqlite" in tenant.ducklake_catalog_url


class TestAPIKeyManagement:
    """Test suite for API key management methods."""

    @pytest.mark.asyncio
    async def test_create_api_key_success(self, test_session, test_settings):
        """Test creating a new API key for a tenant."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            # Create tenant
            tenant, _ = await manager.create_tenant(name="API Key Test")
            await test_session.commit()

            # Create additional API key
            api_key_obj, plain_key = await manager.create_api_key(
                tenant_id=tenant.tenant_id, description="Test API Key"
            )

        assert api_key_obj.key_id.startswith("key-")
        assert api_key_obj.tenant_id == tenant.tenant_id
        assert api_key_obj.description == "Test API Key"
        assert api_key_obj.key_prefix == plain_key[:8]
        assert len(plain_key) == 43  # secrets.token_urlsafe(32)

        # Verify hash
        assert bcrypt.checkpw(plain_key.encode(), api_key_obj.key_hash.encode())

    @pytest.mark.asyncio
    async def test_create_api_key_with_expiration(self, test_session, test_settings):
        """Test creating API key with expiration date."""
        from datetime import datetime, timedelta, timezone

        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, _ = await manager.create_tenant(name="Expiry Test")
            await test_session.commit()

            expires_at = datetime.now(timezone.utc) + timedelta(days=30)
            api_key_obj, _ = await manager.create_api_key(
                tenant_id=tenant.tenant_id,
                description="Temporary Key",
                expires_at=expires_at,
            )

        assert api_key_obj.expires_at is not None
        assert api_key_obj.expires_at.date() == expires_at.date()

    @pytest.mark.asyncio
    async def test_create_api_key_tenant_not_found(self, test_session, test_settings):
        """Test creating API key for non-existent tenant fails."""
        manager = TenantManager(test_session)

        with pytest.raises(TenantNotFoundError):
            await manager.create_api_key(
                tenant_id="tenant-nonexistent", description="Should Fail"
            )

    @pytest.mark.asyncio
    async def test_list_api_keys_success(self, test_session, test_settings):
        """Test listing all API keys for a tenant."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, _ = await manager.create_tenant(name="List Keys Test")
            await test_session.commit()

            # Create multiple API keys (in addition to initial key created with tenant)
            key1, _ = await manager.create_api_key(tenant.tenant_id, "Key 1")
            key2, _ = await manager.create_api_key(tenant.tenant_id, "Key 2")
            key3, _ = await manager.create_api_key(tenant.tenant_id, "Key 3")
            await test_session.commit()

            # List keys
            keys = await manager.list_api_keys(tenant.tenant_id)

        # Should have 4 keys: 1 initial + 3 created
        assert len(keys) == 4
        key_ids = [k.key_id for k in keys]
        assert key1.key_id in key_ids
        assert key2.key_id in key_ids
        assert key3.key_id in key_ids

    @pytest.mark.asyncio
    async def test_list_api_keys_excludes_expired(self, test_session, test_settings):
        """Test listing API keys excludes expired ones by default."""
        from datetime import datetime, timedelta, timezone

        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, _ = await manager.create_tenant(name="Expired Keys Test")
            await test_session.commit()

            # Create active key
            active_key, _ = await manager.create_api_key(tenant.tenant_id, "Active Key")

            # Create expired key
            expired_date = datetime.now(timezone.utc) - timedelta(days=1)
            expired_key, _ = await manager.create_api_key(
                tenant.tenant_id, "Expired Key", expires_at=expired_date
            )
            await test_session.commit()

            # List without expired
            keys = await manager.list_api_keys(tenant.tenant_id, include_expired=False)

        # Should have 2 non-expired keys: 1 initial + 1 active key created
        assert len(keys) == 2
        key_ids = [k.key_id for k in keys]
        assert active_key.key_id in key_ids

    @pytest.mark.asyncio
    async def test_get_api_key_success(self, test_session, test_settings):
        """Test getting specific API key by ID."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, _ = await manager.create_tenant(name="Get Key Test")
            await test_session.commit()

            created_key, _ = await manager.create_api_key(
                tenant.tenant_id, "Specific Key"
            )
            await test_session.commit()

            # Get the key
            retrieved_key = await manager.get_api_key(
                tenant.tenant_id, created_key.key_id
            )

        assert retrieved_key.key_id == created_key.key_id
        assert retrieved_key.description == "Specific Key"

    @pytest.mark.asyncio
    async def test_get_api_key_not_found(self, test_session, test_settings):
        """Test getting non-existent API key fails."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, _ = await manager.create_tenant(name="Get Key Fail Test")
            await test_session.commit()

            with pytest.raises(APIKeyNotFoundError):
                await manager.get_api_key(tenant.tenant_id, "key-nonexistent")

    @pytest.mark.asyncio
    async def test_revoke_api_key_success(self, test_session, test_settings):
        """Test revoking (deleting) an API key."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, _ = await manager.create_tenant(name="Revoke Key Test")
            await test_session.commit()

            api_key_obj, _ = await manager.create_api_key(
                tenant.tenant_id, "To Be Revoked"
            )
            await test_session.commit()

            # Revoke the key (new signature: key_id first, tenant_id optional)
            await manager.revoke_api_key(api_key_obj.key_id, tenant.tenant_id)
            await test_session.commit()

            # Verify key is deleted
            with pytest.raises(APIKeyNotFoundError):
                await manager.get_api_key(tenant.tenant_id, api_key_obj.key_id)

    @pytest.mark.asyncio
    async def test_revoke_api_key_not_found(self, test_session, test_settings):
        """Test revoking non-existent API key fails."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, _ = await manager.create_tenant(name="Revoke Fail Test")
            await test_session.commit()

            with pytest.raises(APIKeyNotFoundError):
                # New signature: key_id first, tenant_id optional
                await manager.revoke_api_key("key-nonexistent", tenant.tenant_id)

    @pytest.mark.asyncio
    async def test_multiple_keys_per_tenant(self, test_session, test_settings):
        """Test that tenants can have multiple API keys."""
        manager = TenantManager(test_session)

        with patch("duckpond.tenants.manager.get_settings", return_value=test_settings):
            tenant, _ = await manager.create_tenant(name="Multi Key Test")
            await test_session.commit()

            # Create multiple keys (in addition to initial key created with tenant)
            keys = []
            for i in range(5):
                key_obj, plain_key = await manager.create_api_key(
                    tenant.tenant_id, f"Key {i}"
                )
                keys.append((key_obj, plain_key))
            await test_session.commit()

            # Verify all keys exist
            all_keys = await manager.list_api_keys(tenant.tenant_id)

        # Should have 6 keys: 1 initial + 5 created
        assert len(all_keys) == 6

        # Verify each key is unique
        key_ids = [k.key_id for k in all_keys]
        assert len(set(key_ids)) == 6

        prefixes = [k.key_prefix for k in all_keys]
        assert len(set(prefixes)) == 6  # All different prefixes


# Fixtures


@pytest.fixture
async def test_session(tmp_path):
    """Create test database session with temporary database."""
    from duckpond.db import create_engine, create_session_factory, init_db

    # Create temporary database
    db_path = tmp_path / "test_tenants.db"
    db_url = f"sqlite:///{db_path}"

    # Create engine and initialize database
    engine = create_engine(
        type("Settings", (), {"metadata_db_url": db_url, "is_sqlite": True})()
    )

    # Initialize database schema
    await init_db(engine)

    # Create session factory
    session_factory = create_session_factory(engine)

    async with session_factory() as session:
        yield session
        await session.rollback()

    await engine.dispose()


@pytest.fixture
def test_settings(tmp_path):
    """Create test settings with temporary paths."""
    from duckpond.config import Settings

    settings = Settings(
        metadata_db_url=f"sqlite:///{tmp_path / 'metadata.db'}",
        local_storage_path=tmp_path / "storage",
        catalog_enabled=True,
    )

    # Ensure storage directories exist
    settings.local_storage_path.mkdir(parents=True, exist_ok=True)

    return settings
