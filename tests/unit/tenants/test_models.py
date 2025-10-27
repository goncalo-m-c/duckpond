"""Unit tests for tenant SQLAlchemy models."""
import pytest
from datetime import datetime, timedelta, UTC
from sqlalchemy import event, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import selectinload

from duckpond.db.base import Base
from duckpond.tenants.models import APIKey, Tenant, TenantStatus


@pytest.fixture
async def async_engine():
    """Create in-memory SQLite engine for testing."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
    )
    
    # Enable foreign key constraints for SQLite
    @event.listens_for(engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()
    
    # Create all tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    yield engine
    
    # Cleanup
    await engine.dispose()


@pytest.fixture
async def session(async_engine):
    """Create async session for testing."""
    from sqlalchemy.ext.asyncio import async_sessionmaker
    
    async_session = async_sessionmaker(
        async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    
    async with async_session() as session:
        yield session


@pytest.fixture
def sample_tenant_data():
    """Sample tenant data for tests."""
    return {
        "tenant_id": "tenant-acme",
        "name": "Acme Corporation",
        "api_key_hash": "hashed_key_12345",
        "ducklake_catalog_url": "http://localhost:8181/api/v1",
        "storage_backend": "s3",
        "storage_config": {
            "bucket": "acme-data",
            "region": "us-east-1",
            "endpoint": "https://s3.amazonaws.com",
        },
        "max_storage_gb": 500,
        "max_query_memory_gb": 8,
        "max_concurrent_queries": 20,
    }


@pytest.fixture
def sample_api_key_data():
    """Sample API key data for tests."""
    return {
        "key_id": "key-abc123",
        "tenant_id": "tenant-acme",
        "key_prefix": "dp_test_",
        "key_hash": "hashed_api_key_xyz789",
        "description": "Production API key",
        "expires_at": datetime.now(UTC) + timedelta(days=365),
    }


class TestTenantModel:
    """Test cases for Tenant model."""
    
    async def test_create_tenant_with_required_fields(self, session, sample_tenant_data):
        """Test creating a tenant with all required fields."""
        tenant = Tenant(**sample_tenant_data)
        session.add(tenant)
        await session.commit()
        await session.refresh(tenant)
        
        assert tenant.tenant_id == "tenant-acme"
        assert tenant.name == "Acme Corporation"
        assert tenant.api_key_hash == "hashed_key_12345"
        assert tenant.ducklake_catalog_url == "http://localhost:8181/api/v1"
        assert tenant.storage_backend == "s3"
        assert tenant.storage_config["bucket"] == "acme-data"
        assert tenant.max_storage_gb == 500
        assert tenant.max_query_memory_gb == 8
        assert tenant.max_concurrent_queries == 20
        assert isinstance(tenant.created_at, datetime)
        assert isinstance(tenant.updated_at, datetime)
    
    async def test_create_tenant_with_minimal_fields(self, session):
        """Test creating tenant with only required fields."""
        tenant = Tenant(
            tenant_id="tenant-minimal",
            name="Minimal Tenant",
            api_key_hash="hash123",
            ducklake_catalog_url="http://localhost:8181",
            storage_backend="local",
        )
        session.add(tenant)
        await session.commit()
        await session.refresh(tenant)
        
        assert tenant.tenant_id == "tenant-minimal"
        assert tenant.storage_config is None
        assert tenant.max_storage_gb == 100  # Default
        assert tenant.max_query_memory_gb == 4  # Default
        assert tenant.max_concurrent_queries == 10  # Default
    
    async def test_tenant_unique_name_constraint(self, session, sample_tenant_data):
        """Test that tenant names must be unique."""
        tenant1 = Tenant(**sample_tenant_data)
        session.add(tenant1)
        await session.commit()
        
        # Try to create another tenant with same name
        tenant2_data = sample_tenant_data.copy()
        tenant2_data["tenant_id"] = "tenant-acme2"
        tenant2 = Tenant(**tenant2_data)
        session.add(tenant2)
        
        with pytest.raises(IntegrityError):
            await session.commit()
    
    async def test_tenant_json_storage_config(self, session):
        """Test JSON serialization for storage_config field."""
        config = {
            "bucket": "test-bucket",
            "region": "eu-west-1",
            "access_key": "AKIAIOSFODNN7EXAMPLE",
            "nested": {
                "key1": "value1",
                "key2": [1, 2, 3],
            },
        }
        
        tenant = Tenant(
            tenant_id="tenant-json-test",
            name="JSON Test Tenant",
            api_key_hash="hash456",
            ducklake_catalog_url="http://localhost:8181",
            storage_backend="s3",
            storage_config=config,
        )
        session.add(tenant)
        await session.commit()
        await session.refresh(tenant)
        
        assert tenant.storage_config == config
        assert tenant.storage_config["nested"]["key2"] == [1, 2, 3]
    
    async def test_tenant_timestamps(self, session, sample_tenant_data):
        """Test that timestamps are automatically set."""
        tenant = Tenant(**sample_tenant_data)
        session.add(tenant)
        await session.commit()
        await session.refresh(tenant)
        
        assert tenant.created_at is not None
        assert tenant.updated_at is not None
        assert isinstance(tenant.created_at, datetime)
        assert isinstance(tenant.updated_at, datetime)
        
        # Verify timestamps are recent (within last minute)
        now = datetime.now(UTC)
        # Compare naive datetime from DB with UTC-aware datetime
        created_delta = (now.replace(tzinfo=None) - tenant.created_at).total_seconds()
        updated_delta = (now.replace(tzinfo=None) - tenant.updated_at).total_seconds()
        assert created_delta < 60
        assert updated_delta < 60
    
    async def test_tenant_repr(self, sample_tenant_data):
        """Test string representation of Tenant."""
        tenant = Tenant(**sample_tenant_data)
        repr_str = repr(tenant)
        
        assert "Tenant" in repr_str
        assert "tenant-acme" in repr_str
        assert "Acme Corporation" in repr_str
        assert "s3" in repr_str
    
    async def test_tenant_storage_backend_index(self, session, sample_tenant_data):
        """Test that storage_backend index allows efficient queries."""
        # Create multiple tenants with different backends
        for i, backend in enumerate(["s3", "azure", "gcs", "local", "s3"]):
            data = sample_tenant_data.copy()
            data["tenant_id"] = f"tenant-{i}"
            data["name"] = f"Tenant {i}"
            data["storage_backend"] = backend
            tenant = Tenant(**data)
            session.add(tenant)
        
        await session.commit()
        
        # Query by storage_backend (should use index)
        result = await session.execute(
            select(Tenant).where(Tenant.storage_backend == "s3")
        )
        s3_tenants = result.scalars().all()
        
        assert len(s3_tenants) == 2
        assert all(t.storage_backend == "s3" for t in s3_tenants)


class TestAPIKeyModel:
    """Test cases for APIKey model."""
    
    async def test_create_api_key_with_all_fields(
        self, session, sample_tenant_data, sample_api_key_data
    ):
        """Test creating an API key with all fields."""
        # First create a tenant
        tenant = Tenant(**sample_tenant_data)
        session.add(tenant)
        await session.commit()
        
        # Create API key
        api_key = APIKey(**sample_api_key_data)
        session.add(api_key)
        await session.commit()
        await session.refresh(api_key)
        
        assert api_key.key_id == "key-abc123"
        assert api_key.tenant_id == "tenant-acme"
        assert api_key.key_hash == "hashed_api_key_xyz789"
        assert api_key.description == "Production API key"
        assert isinstance(api_key.created_at, datetime)
        assert api_key.last_used is None
        assert api_key.expires_at is not None
    
    async def test_create_api_key_minimal(self, session, sample_tenant_data):
        """Test creating API key with minimal required fields."""
        tenant = Tenant(**sample_tenant_data)
        session.add(tenant)
        await session.commit()
        
        api_key = APIKey(
            key_id="key-minimal",
            tenant_id="tenant-acme",
            key_prefix="dp_mini_",
            key_hash="hash_minimal_123",
        )
        session.add(api_key)
        await session.commit()
        await session.refresh(api_key)
        
        assert api_key.key_id == "key-minimal"
        assert api_key.description is None
        assert api_key.last_used is None
        assert api_key.expires_at is None
    
    async def test_api_key_unique_hash_constraint(
        self, session, sample_tenant_data, sample_api_key_data
    ):
        """Test that API key hashes must be unique."""
        tenant = Tenant(**sample_tenant_data)
        session.add(tenant)
        await session.commit()
        
        api_key1 = APIKey(**sample_api_key_data)
        session.add(api_key1)
        await session.commit()
        
        # Try to create another key with same hash
        api_key2_data = sample_api_key_data.copy()
        api_key2_data["key_id"] = "key-different"
        api_key2 = APIKey(**api_key2_data)
        session.add(api_key2)
        
        with pytest.raises(IntegrityError):
            await session.commit()
    
    async def test_api_key_foreign_key_constraint(self, session, sample_api_key_data):
        """Test that API key requires valid tenant_id."""
        api_key = APIKey(**sample_api_key_data)
        session.add(api_key)
        
        with pytest.raises(IntegrityError):
            await session.commit()
    
    async def test_api_key_repr(self, sample_api_key_data):
        """Test string representation of APIKey."""
        api_key = APIKey(**sample_api_key_data)
        repr_str = repr(api_key)
        
        assert "APIKey" in repr_str
        assert "key-abc123" in repr_str
        assert "tenant-acme" in repr_str


class TestTenantAPIKeyRelationship:
    """Test cases for Tenant-APIKey relationship."""
    
    async def test_tenant_has_api_keys_relationship(
        self, session, sample_tenant_data, sample_api_key_data
    ):
        """Test that tenant can access its API keys."""
        tenant = Tenant(**sample_tenant_data)
        session.add(tenant)
        await session.commit()
        
        # Create multiple API keys
        for i in range(3):
            api_key_data = sample_api_key_data.copy()
            api_key_data["key_id"] = f"key-{i}"
            api_key_data["key_hash"] = f"hash-{i}"
            api_key = APIKey(**api_key_data)
            session.add(api_key)
        
        await session.commit()
        
        # Refresh and load relationships
        await session.refresh(tenant, ["api_keys"])
        
        assert len(tenant.api_keys) == 3
        assert all(isinstance(key, APIKey) for key in tenant.api_keys)
        assert all(key.tenant_id == "tenant-acme" for key in tenant.api_keys)
    
    async def test_api_key_has_tenant_relationship(
        self, session, sample_tenant_data, sample_api_key_data
    ):
        """Test that API key can access its tenant."""
        tenant = Tenant(**sample_tenant_data)
        session.add(tenant)
        await session.commit()
        
        api_key = APIKey(**sample_api_key_data)
        session.add(api_key)
        await session.commit()
        
        # Refresh and load relationship
        await session.refresh(api_key, ["tenant"])
        
        assert api_key.tenant is not None
        assert isinstance(api_key.tenant, Tenant)
        assert api_key.tenant.tenant_id == "tenant-acme"
        assert api_key.tenant.name == "Acme Corporation"
    
    async def test_cascade_delete_api_keys(
        self, session, sample_tenant_data, sample_api_key_data
    ):
        """Test that deleting tenant cascades to API keys."""
        tenant = Tenant(**sample_tenant_data)
        session.add(tenant)
        await session.commit()
        
        # Create multiple API keys
        key_ids = []
        for i in range(3):
            api_key_data = sample_api_key_data.copy()
            api_key_data["key_id"] = f"key-cascade-{i}"
            api_key_data["key_hash"] = f"hash-cascade-{i}"
            api_key = APIKey(**api_key_data)
            session.add(api_key)
            key_ids.append(api_key_data["key_id"])
        
        await session.commit()
        
        # Verify keys exist
        result = await session.execute(select(APIKey))
        assert len(result.scalars().all()) == 3
        
        # Delete tenant
        await session.delete(tenant)
        await session.commit()
        
        # Verify all API keys were deleted
        result = await session.execute(select(APIKey))
        assert len(result.scalars().all()) == 0
    
    async def test_relationship_eager_loading(
        self, session, sample_tenant_data, sample_api_key_data
    ):
        """Test that relationships use selectin loading strategy."""
        tenant = Tenant(**sample_tenant_data)
        session.add(tenant)
        await session.commit()
        
        api_key = APIKey(**sample_api_key_data)
        session.add(api_key)
        await session.commit()
        
        # Query tenant with eager loading
        result = await session.execute(
            select(Tenant)
            .where(Tenant.tenant_id == "tenant-acme")
            .options(selectinload(Tenant.api_keys))
        )
        tenant_loaded = result.scalar_one()
        
        # Should be able to access api_keys without additional query
        assert len(tenant_loaded.api_keys) == 1
        assert tenant_loaded.api_keys[0].key_id == "key-abc123"


class TestTenantStatusEnum:
    """Test cases for TenantStatus enum (legacy compatibility)."""
    
    def test_tenant_status_constants(self):
        """Test that TenantStatus has expected constants."""
        assert TenantStatus.ACTIVE == "active"
        assert TenantStatus.SUSPENDED == "suspended"
        assert TenantStatus.DELETED == "deleted"


class TestModelIndexes:
    """Test cases for model indexes."""
    
    async def test_tenant_indexes_created(self, async_engine):
        """Test that tenant indexes are created."""
        from sqlalchemy import inspect
        
        async with async_engine.connect() as conn:
            indexes = await conn.run_sync(
                lambda sync_conn: inspect(sync_conn).get_indexes("tenants")
            )
        
        index_names = [idx["name"] for idx in indexes]
        assert "idx_tenants_storage_backend" in index_names
        assert "idx_tenants_name" in index_names
    
    async def test_api_key_indexes_created(self, async_engine):
        """Test that API key indexes are created."""
        from sqlalchemy import inspect
        
        async with async_engine.connect() as conn:
            indexes = await conn.run_sync(
                lambda sync_conn: inspect(sync_conn).get_indexes("api_keys")
            )
        
        index_names = [idx["name"] for idx in indexes]
        assert "idx_api_keys_tenant" in index_names
        assert "idx_api_keys_hash" in index_names
        assert "idx_api_keys_expires" in index_names


class TestModelValidation:
    """Test cases for model field validation."""
    
    async def test_tenant_requires_tenant_id(self, session):
        """Test that tenant_id is required."""
        tenant = Tenant(
            name="Test",
            api_key_hash="hash",
            ducklake_catalog_url="http://test",
            storage_backend="s3",
        )
        session.add(tenant)
        
        with pytest.raises(IntegrityError):
            await session.commit()
    
    async def test_tenant_requires_name(self, session):
        """Test that name is required."""
        tenant = Tenant(
            tenant_id="tenant-test",
            api_key_hash="hash",
            ducklake_catalog_url="http://test",
            storage_backend="s3",
        )
        session.add(tenant)
        
        with pytest.raises(IntegrityError):
            await session.commit()
    
    async def test_api_key_requires_key_id(self, session, sample_tenant_data):
        """Test that key_id is required."""
        tenant = Tenant(**sample_tenant_data)
        session.add(tenant)
        await session.commit()
        
        api_key = APIKey(
            tenant_id="tenant-acme",
            key_hash="hash123",
        )
        session.add(api_key)
        
        with pytest.raises(IntegrityError):
            await session.commit()
    
    async def test_api_key_requires_key_hash(self, session, sample_tenant_data):
        """Test that key_hash is required."""
        tenant = Tenant(**sample_tenant_data)
        session.add(tenant)
        await session.commit()
        
        api_key = APIKey(
            key_id="key-test",
            tenant_id="tenant-acme",
        )
        session.add(api_key)
        
        with pytest.raises(IntegrityError):
            await session.commit()
