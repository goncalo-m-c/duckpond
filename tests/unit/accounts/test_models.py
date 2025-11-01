"""Unit tests for account SQLAlchemy models."""
import pytest
from datetime import datetime, timedelta, UTC
from sqlalchemy import event, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import selectinload

from duckpond.db.base import Base
from duckpond.accounts.models import APIKey, Account, AccountStatus


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
def sample_account_data():
    """Sample account data for tests."""
    return {
        "account_id": "account-acme",
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
        "account_id": "account-acme",
        "key_prefix": "dp_test_",
        "key_hash": "hashed_api_key_xyz789",
        "description": "Production API key",
        "expires_at": datetime.now(UTC) + timedelta(days=365),
    }


class TestAccountModel:
    """Test cases for Account model."""
    
    async def test_create_account_with_required_fields(self, session, sample_account_data):
        """Test creating a account with all required fields."""
        account = Account(**sample_account_data)
        session.add(account)
        await session.commit()
        await session.refresh(account)
        
        assert account.account_id == "account-acme"
        assert account.name == "Acme Corporation"
        assert account.api_key_hash == "hashed_key_12345"
        assert account.ducklake_catalog_url == "http://localhost:8181/api/v1"
        assert account.storage_backend == "s3"
        assert account.storage_config["bucket"] == "acme-data"
        assert account.max_storage_gb == 500
        assert account.max_query_memory_gb == 8
        assert account.max_concurrent_queries == 20
        assert isinstance(account.created_at, datetime)
        assert isinstance(account.updated_at, datetime)
    
    async def test_create_account_with_minimal_fields(self, session):
        """Test creating account with only required fields."""
        account = Account(
            account_id="account-minimal",
            name="Minimal Account",
            api_key_hash="hash123",
            ducklake_catalog_url="http://localhost:8181",
            storage_backend="local",
        )
        session.add(account)
        await session.commit()
        await session.refresh(account)
        
        assert account.account_id == "account-minimal"
        assert account.storage_config is None
        assert account.max_storage_gb == 100  # Default
        assert account.max_query_memory_gb == 4  # Default
        assert account.max_concurrent_queries == 10  # Default
    
    async def test_account_unique_name_constraint(self, session, sample_account_data):
        """Test that account names must be unique."""
        account1 = Account(**sample_account_data)
        session.add(account1)
        await session.commit()
        
        # Try to create another account with same name
        account2_data = sample_account_data.copy()
        account2_data["account_id"] = "account-acme2"
        account2 = Account(**account2_data)
        session.add(account2)
        
        with pytest.raises(IntegrityError):
            await session.commit()
    
    async def test_account_json_storage_config(self, session):
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
        
        account = Account(
            account_id="account-json-test",
            name="JSON Test Account",
            api_key_hash="hash456",
            ducklake_catalog_url="http://localhost:8181",
            storage_backend="s3",
            storage_config=config,
        )
        session.add(account)
        await session.commit()
        await session.refresh(account)
        
        assert account.storage_config == config
        assert account.storage_config["nested"]["key2"] == [1, 2, 3]
    
    async def test_account_timestamps(self, session, sample_account_data):
        """Test that timestamps are automatically set."""
        account = Account(**sample_account_data)
        session.add(account)
        await session.commit()
        await session.refresh(account)
        
        assert account.created_at is not None
        assert account.updated_at is not None
        assert isinstance(account.created_at, datetime)
        assert isinstance(account.updated_at, datetime)
        
        # Verify timestamps are recent (within last minute)
        now = datetime.now(UTC)
        # Compare naive datetime from DB with UTC-aware datetime
        created_delta = (now.replace(tzinfo=None) - account.created_at).total_seconds()
        updated_delta = (now.replace(tzinfo=None) - account.updated_at).total_seconds()
        assert created_delta < 60
        assert updated_delta < 60
    
    async def test_account_repr(self, sample_account_data):
        """Test string representation of Account."""
        account = Account(**sample_account_data)
        repr_str = repr(account)
        
        assert "Account" in repr_str
        assert "account-acme" in repr_str
        assert "Acme Corporation" in repr_str
        assert "s3" in repr_str
    
    async def test_account_storage_backend_index(self, session, sample_account_data):
        """Test that storage_backend index allows efficient queries."""
        # Create multiple accounts with different backends
        for i, backend in enumerate(["s3", "azure", "gcs", "local", "s3"]):
            data = sample_account_data.copy()
            data["account_id"] = f"account-{i}"
            data["name"] = f"Account {i}"
            data["storage_backend"] = backend
            account = Account(**data)
            session.add(account)
        
        await session.commit()
        
        # Query by storage_backend (should use index)
        result = await session.execute(
            select(Account).where(Account.storage_backend == "s3")
        )
        s3_accounts = result.scalars().all()
        
        assert len(s3_accounts) == 2
        assert all(t.storage_backend == "s3" for t in s3_accounts)


class TestAPIKeyModel:
    """Test cases for APIKey model."""
    
    async def test_create_api_key_with_all_fields(
        self, session, sample_account_data, sample_api_key_data
    ):
        """Test creating an API key with all fields."""
        # First create a account
        account = Account(**sample_account_data)
        session.add(account)
        await session.commit()
        
        # Create API key
        api_key = APIKey(**sample_api_key_data)
        session.add(api_key)
        await session.commit()
        await session.refresh(api_key)
        
        assert api_key.key_id == "key-abc123"
        assert api_key.account_id == "account-acme"
        assert api_key.key_hash == "hashed_api_key_xyz789"
        assert api_key.description == "Production API key"
        assert isinstance(api_key.created_at, datetime)
        assert api_key.last_used is None
        assert api_key.expires_at is not None
    
    async def test_create_api_key_minimal(self, session, sample_account_data):
        """Test creating API key with minimal required fields."""
        account = Account(**sample_account_data)
        session.add(account)
        await session.commit()
        
        api_key = APIKey(
            key_id="key-minimal",
            account_id="account-acme",
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
        self, session, sample_account_data, sample_api_key_data
    ):
        """Test that API key hashes must be unique."""
        account = Account(**sample_account_data)
        session.add(account)
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
        """Test that API key requires valid account_id."""
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
        assert "account-acme" in repr_str


class TestAccountAPIKeyRelationship:
    """Test cases for Account-APIKey relationship."""
    
    async def test_account_has_api_keys_relationship(
        self, session, sample_account_data, sample_api_key_data
    ):
        """Test that account can access its API keys."""
        account = Account(**sample_account_data)
        session.add(account)
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
        await session.refresh(account, ["api_keys"])
        
        assert len(account.api_keys) == 3
        assert all(isinstance(key, APIKey) for key in account.api_keys)
        assert all(key.account_id == "account-acme" for key in account.api_keys)
    
    async def test_api_key_has_account_relationship(
        self, session, sample_account_data, sample_api_key_data
    ):
        """Test that API key can access its account."""
        account = Account(**sample_account_data)
        session.add(account)
        await session.commit()
        
        api_key = APIKey(**sample_api_key_data)
        session.add(api_key)
        await session.commit()
        
        # Refresh and load relationship
        await session.refresh(api_key, ["account"])
        
        assert api_key.account is not None
        assert isinstance(api_key.account, Account)
        assert api_key.account.account_id == "account-acme"
        assert api_key.account.name == "Acme Corporation"
    
    async def test_cascade_delete_api_keys(
        self, session, sample_account_data, sample_api_key_data
    ):
        """Test that deleting account cascades to API keys."""
        account = Account(**sample_account_data)
        session.add(account)
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
        
        # Delete account
        await session.delete(account)
        await session.commit()
        
        # Verify all API keys were deleted
        result = await session.execute(select(APIKey))
        assert len(result.scalars().all()) == 0
    
    async def test_relationship_eager_loading(
        self, session, sample_account_data, sample_api_key_data
    ):
        """Test that relationships use selectin loading strategy."""
        account = Account(**sample_account_data)
        session.add(account)
        await session.commit()
        
        api_key = APIKey(**sample_api_key_data)
        session.add(api_key)
        await session.commit()
        
        # Query account with eager loading
        result = await session.execute(
            select(Account)
            .where(Account.account_id == "account-acme")
            .options(selectinload(Account.api_keys))
        )
        account_loaded = result.scalar_one()
        
        # Should be able to access api_keys without additional query
        assert len(account_loaded.api_keys) == 1
        assert account_loaded.api_keys[0].key_id == "key-abc123"


class TestAccountStatusEnum:
    """Test cases for AccountStatus enum (legacy compatibility)."""
    
    def test_account_status_constants(self):
        """Test that AccountStatus has expected constants."""
        assert AccountStatus.ACTIVE == "active"
        assert AccountStatus.SUSPENDED == "suspended"
        assert AccountStatus.DELETED == "deleted"


class TestModelIndexes:
    """Test cases for model indexes."""
    
    async def test_account_indexes_created(self, async_engine):
        """Test that account indexes are created."""
        from sqlalchemy import inspect
        
        async with async_engine.connect() as conn:
            indexes = await conn.run_sync(
                lambda sync_conn: inspect(sync_conn).get_indexes("accounts")
            )
        
        index_names = [idx["name"] for idx in indexes]
        assert "idx_accounts_storage_backend" in index_names
        assert "idx_accounts_name" in index_names
    
    async def test_api_key_indexes_created(self, async_engine):
        """Test that API key indexes are created."""
        from sqlalchemy import inspect
        
        async with async_engine.connect() as conn:
            indexes = await conn.run_sync(
                lambda sync_conn: inspect(sync_conn).get_indexes("api_keys")
            )
        
        index_names = [idx["name"] for idx in indexes]
        assert "idx_api_keys_account" in index_names
        assert "idx_api_keys_hash" in index_names
        assert "idx_api_keys_expires" in index_names


class TestModelValidation:
    """Test cases for model field validation."""
    
    async def test_account_requires_account_id(self, session):
        """Test that account_id is required."""
        account = Account(
            name="Test",
            api_key_hash="hash",
            ducklake_catalog_url="http://test",
            storage_backend="s3",
        )
        session.add(account)
        
        with pytest.raises(IntegrityError):
            await session.commit()
    
    async def test_account_requires_name(self, session):
        """Test that name is required."""
        account = Account(
            account_id="account-test",
            api_key_hash="hash",
            ducklake_catalog_url="http://test",
            storage_backend="s3",
        )
        session.add(account)
        
        with pytest.raises(IntegrityError):
            await session.commit()
    
    async def test_api_key_requires_key_id(self, session, sample_account_data):
        """Test that key_id is required."""
        account = Account(**sample_account_data)
        session.add(account)
        await session.commit()
        
        api_key = APIKey(
            account_id="account-acme",
            key_hash="hash123",
        )
        session.add(api_key)
        
        with pytest.raises(IntegrityError):
            await session.commit()
    
    async def test_api_key_requires_key_hash(self, session, sample_account_data):
        """Test that key_hash is required."""
        account = Account(**sample_account_data)
        session.add(account)
        await session.commit()
        
        api_key = APIKey(
            key_id="key-test",
            account_id="account-acme",
        )
        session.add(api_key)
        
        with pytest.raises(IntegrityError):
            await session.commit()
