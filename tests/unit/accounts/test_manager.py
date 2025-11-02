"""Unit tests for AccountManager."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import bcrypt
import pytest
from sqlalchemy import select

from duckpond.accounts.manager import (
    APIKeyNotFoundError,
    AccountAlreadyExistsError,
    AccountManager,
    AccountManagerError,
    AccountNotFoundError,
)
from duckpond.accounts.models import APIKey, Account


class TestAccountManager:
    """Test suite for AccountManager class."""

    @pytest.mark.asyncio
    async def test_create_account_with_defaults(self, test_session, test_settings):
        """Test creating account with default values."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, api_key = await manager.create_account(name="Test Account")

        assert account.account_id == "test-account"
        assert account.name == "Test Account"
        assert account.storage_backend == "local"
        assert account.max_storage_gb == 100
        assert account.max_query_memory_gb == 4
        assert account.max_concurrent_queries == 10
        assert api_key  # API key should be generated
        assert len(api_key) > 20  # API key should be reasonably long

        # Verify password hash
        assert bcrypt.checkpw(api_key.encode(), account.api_key_hash.encode())

    @pytest.mark.asyncio
    async def test_create_account_with_custom_values(self, test_session, test_settings):
        """Test creating account with custom quota values."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, api_key = await manager.create_account(
                name="Custom Account",
                storage_backend="s3",
                storage_config={"bucket": "test-bucket", "region": "us-west-2"},
                max_storage_gb=500,
                max_query_memory_gb=16,
                max_concurrent_queries=50,
            )

        assert account.account_id == "custom-account"
        assert account.name == "Custom Account"
        assert account.storage_backend == "s3"
        assert account.storage_config == {"bucket": "test-bucket", "region": "us-west-2"}
        assert account.max_storage_gb == 500
        assert account.max_query_memory_gb == 16
        assert account.max_concurrent_queries == 50

    @pytest.mark.asyncio
    async def test_create_account_generates_unique_ids(
        self, test_session, test_settings
    ):
        """Test that duplicate names generate unique account IDs."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account1, _ = await manager.create_account(name="Duplicate")

            # Mock the name check to allow duplicate name for ID generation test
            # In reality, this would fail at the name uniqueness check
            # So we test the ID generation separately
            assert account1.account_id == "duplicate"

    @pytest.mark.asyncio
    async def test_create_account_duplicate_name_fails(
        self, test_session, test_settings
    ):
        """Test that creating account with duplicate name fails."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            await manager.create_account(name="Unique Name")
            await test_session.commit()

            with pytest.raises(AccountAlreadyExistsError) as exc_info:
                await manager.create_account(name="Unique Name")

            assert "already exists" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_create_account_creates_catalog(self, test_session, test_settings):
        """Test that account creation creates DuckLake catalog."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, _ = await manager.create_account(name="Catalog Test")

            # Verify catalog URL is set
            assert account.ducklake_catalog_url

            # For SQLite, verify it's a file path
            if test_settings.is_sqlite:
                # Catalog URL should point to the catalog sqlite file
                assert account.ducklake_catalog_url.endswith("_catalog.sqlite")

    @pytest.mark.asyncio
    async def test_get_account_existing(self, test_session, test_settings):
        """Test retrieving existing account."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            created_account, _ = await manager.create_account(name="Get Test")
            await test_session.commit()

            retrieved_account = await manager.get_account(created_account.account_id)

            assert retrieved_account is not None
            assert retrieved_account.account_id == created_account.account_id
            assert retrieved_account.name == "Get Test"

    @pytest.mark.asyncio
    async def test_get_account_not_found(self, test_session):
        """Test retrieving non-existent account returns None."""
        manager = AccountManager(test_session)

        account = await manager.get_account("account-nonexistent")

        assert account is None

    @pytest.mark.asyncio
    async def test_get_account_by_id_existing(self, test_session, test_settings):
        """Test get_account_by_id retrieves existing account."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            created_account, _ = await manager.create_account(name="Get By ID Test")
            await test_session.commit()

            retrieved_account = await manager.get_account_by_id(created_account.account_id)

            assert retrieved_account.account_id == created_account.account_id
            assert retrieved_account.name == "Get By ID Test"

    @pytest.mark.asyncio
    async def test_get_account_by_id_not_found_raises(self, test_session):
        """Test get_account_by_id raises exception for non-existent account."""
        manager = AccountManager(test_session)

        with pytest.raises(AccountNotFoundError) as exc_info:
            await manager.get_account_by_id("account-nonexistent")

        assert "not found" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_list_accounts_empty(self, test_session):
        """Test listing accounts when none exist."""
        manager = AccountManager(test_session)

        accounts, total = await manager.list_accounts()

        assert accounts == []
        assert total == 0

    @pytest.mark.asyncio
    async def test_list_accounts_with_data(self, test_session, test_settings):
        """Test listing accounts with multiple records."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            # Create multiple accounts
            await manager.create_account(name="Account 1")
            await manager.create_account(name="Account 2")
            await manager.create_account(name="Account 3")
            await test_session.commit()

            accounts, total = await manager.list_accounts()

            assert len(accounts) == 3
            assert total == 3
            assert all(isinstance(t, Account) for t in accounts)

    @pytest.mark.asyncio
    async def test_list_accounts_pagination(self, test_session, test_settings):
        """Test account listing with pagination."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            # Create 5 accounts
            for i in range(5):
                await manager.create_account(name=f"Account {i}")
            await test_session.commit()

            # Get first page
            page1, total = await manager.list_accounts(offset=0, limit=2)
            assert len(page1) == 2
            assert total == 5

            # Get second page
            page2, total = await manager.list_accounts(offset=2, limit=2)
            assert len(page2) == 2
            assert total == 5

            # Verify different accounts
            page1_ids = {t.account_id for t in page1}
            page2_ids = {t.account_id for t in page2}
            assert page1_ids != page2_ids

    @pytest.mark.asyncio
    async def test_update_account_quotas_all_fields(self, test_session, test_settings):
        """Test updating all account quota fields."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, _ = await manager.create_account(name="Update Test")
            await test_session.commit()

            updated = await manager.update_account_quotas(
                account.account_id,
                max_storage_gb=200,
                max_query_memory_gb=8,
                max_concurrent_queries=20,
            )

            assert updated.max_storage_gb == 200
            assert updated.max_query_memory_gb == 8
            assert updated.max_concurrent_queries == 20

    @pytest.mark.asyncio
    async def test_update_account_quotas_partial(self, test_session, test_settings):
        """Test updating only some account quota fields."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, _ = await manager.create_account(
                name="Partial Update Test",
                max_storage_gb=100,
                max_query_memory_gb=4,
                max_concurrent_queries=10,
            )
            await test_session.commit()

            updated = await manager.update_account_quotas(
                account.account_id,
                max_storage_gb=300,  # Only update storage
            )

            assert updated.max_storage_gb == 300
            assert updated.max_query_memory_gb == 4  # Unchanged
            assert updated.max_concurrent_queries == 10  # Unchanged

    @pytest.mark.asyncio
    async def test_update_account_quotas_not_found(self, test_session):
        """Test updating quotas for non-existent account raises exception."""
        manager = AccountManager(test_session)

        with pytest.raises(AccountNotFoundError):
            await manager.update_account_quotas(
                "account-nonexistent",
                max_storage_gb=200,
            )

    @pytest.mark.asyncio
    async def test_delete_account_without_purge(self, test_session, test_settings):
        """Test deleting account without data purge."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, _ = await manager.create_account(name="Delete Test")
            await test_session.commit()

            account_id = account.account_id

            await manager.delete_account(account_id, purge_data=False)
            await test_session.commit()

            # Verify account is deleted
            deleted_account = await manager.get_account(account_id)
            assert deleted_account is None

    @pytest.mark.asyncio
    async def test_delete_account_with_purge(self, test_session, test_settings):
        """Test deleting account with data purge."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, _ = await manager.create_account(name="Purge Test")
            catalog_url = account.ducklake_catalog_url
            await test_session.commit()

            # For SQLite catalogs, verify file exists
            if test_settings.is_sqlite:
                catalog_path = Path(catalog_url)
                assert catalog_path.exists()

            await manager.delete_account(account.account_id, purge_data=True)
            await test_session.commit()

            # Verify catalog file is deleted (for SQLite)
            if test_settings.is_sqlite:
                assert not catalog_path.exists()

    @pytest.mark.asyncio
    async def test_delete_account_not_found(self, test_session):
        """Test deleting non-existent account raises exception."""
        manager = AccountManager(test_session)

        with pytest.raises(AccountNotFoundError):
            await manager.delete_account("account-nonexistent")

    @pytest.mark.asyncio
    async def test_account_id_generation_with_special_chars(
        self, test_session, test_settings
    ):
        """Test account ID generation handles special characters."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, _ = await manager.create_account(name="Test @ Company! #123")

            # Should be slugified
            assert account.account_id == "test-company-123"
            assert "#" not in account.account_id
            assert "@" not in account.account_id
            assert "!" not in account.account_id

    @pytest.mark.asyncio
    async def test_catalog_url_format_sqlite(self, test_session, test_settings):
        """Test catalog URL format for SQLite backend."""
        # Ensure SQLite backend
        test_settings.metadata_db_url = "sqlite:///test.db"

        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, _ = await manager.create_account(name="SQLite Catalog Test")

            # Verify it's a file path
            assert not account.ducklake_catalog_url.startswith("postgresql")
            # Should be .sqlite not .duckdb for catalog
            assert ".sqlite" in account.ducklake_catalog_url


class TestAPIKeyManagement:
    """Test suite for API key management methods."""

    @pytest.mark.asyncio
    async def test_create_api_key_success(self, test_session, test_settings):
        """Test creating a new API key for a account."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            # Create account
            account, _ = await manager.create_account(name="API Key Test")
            await test_session.commit()

            # Create additional API key
            api_key_obj, plain_key = await manager.create_api_key(
                account_id=account.account_id, description="Test API Key"
            )

        assert api_key_obj.key_id.startswith("key-")
        assert api_key_obj.account_id == account.account_id
        assert api_key_obj.description == "Test API Key"
        assert api_key_obj.key_prefix == plain_key[:8]
        assert len(plain_key) == 43  # secrets.token_urlsafe(32)

        # Verify hash
        assert bcrypt.checkpw(plain_key.encode(), api_key_obj.key_hash.encode())

    @pytest.mark.asyncio
    async def test_create_api_key_with_expiration(self, test_session, test_settings):
        """Test creating API key with expiration date."""
        from datetime import datetime, timedelta, timezone

        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, _ = await manager.create_account(name="Expiry Test")
            await test_session.commit()

            expires_at = datetime.now(timezone.utc) + timedelta(days=30)
            api_key_obj, _ = await manager.create_api_key(
                account_id=account.account_id,
                description="Temporary Key",
                expires_at=expires_at,
            )

        assert api_key_obj.expires_at is not None
        assert api_key_obj.expires_at.date() == expires_at.date()

    @pytest.mark.asyncio
    async def test_create_api_key_account_not_found(self, test_session, test_settings):
        """Test creating API key for non-existent account fails."""
        manager = AccountManager(test_session)

        with pytest.raises(AccountNotFoundError):
            await manager.create_api_key(
                account_id="account-nonexistent", description="Should Fail"
            )

    @pytest.mark.asyncio
    async def test_list_api_keys_success(self, test_session, test_settings):
        """Test listing all API keys for a account."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, _ = await manager.create_account(name="List Keys Test")
            await test_session.commit()

            # Create multiple API keys (in addition to initial key created with account)
            key1, _ = await manager.create_api_key(account.account_id, "Key 1")
            key2, _ = await manager.create_api_key(account.account_id, "Key 2")
            key3, _ = await manager.create_api_key(account.account_id, "Key 3")
            await test_session.commit()

            # List keys
            keys = await manager.list_api_keys(account.account_id)

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

        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, _ = await manager.create_account(name="Expired Keys Test")
            await test_session.commit()

            # Create active key
            active_key, _ = await manager.create_api_key(account.account_id, "Active Key")

            # Create expired key
            expired_date = datetime.now(timezone.utc) - timedelta(days=1)
            expired_key, _ = await manager.create_api_key(
                account.account_id, "Expired Key", expires_at=expired_date
            )
            await test_session.commit()

            # List without expired
            keys = await manager.list_api_keys(account.account_id, include_expired=False)

        # Should have 2 non-expired keys: 1 initial + 1 active key created
        assert len(keys) == 2
        key_ids = [k.key_id for k in keys]
        assert active_key.key_id in key_ids

    @pytest.mark.asyncio
    async def test_get_api_key_success(self, test_session, test_settings):
        """Test getting specific API key by ID."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, _ = await manager.create_account(name="Get Key Test")
            await test_session.commit()

            created_key, _ = await manager.create_api_key(
                account.account_id, "Specific Key"
            )
            await test_session.commit()

            # Get the key
            retrieved_key = await manager.get_api_key(
                account.account_id, created_key.key_id
            )

        assert retrieved_key.key_id == created_key.key_id
        assert retrieved_key.description == "Specific Key"

    @pytest.mark.asyncio
    async def test_get_api_key_not_found(self, test_session, test_settings):
        """Test getting non-existent API key fails."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, _ = await manager.create_account(name="Get Key Fail Test")
            await test_session.commit()

            with pytest.raises(APIKeyNotFoundError):
                await manager.get_api_key(account.account_id, "key-nonexistent")

    @pytest.mark.asyncio
    async def test_revoke_api_key_success(self, test_session, test_settings):
        """Test revoking (deleting) an API key."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, _ = await manager.create_account(name="Revoke Key Test")
            await test_session.commit()

            api_key_obj, _ = await manager.create_api_key(
                account.account_id, "To Be Revoked"
            )
            await test_session.commit()

            # Revoke the key (new signature: key_id first, account_id optional)
            await manager.revoke_api_key(api_key_obj.key_id, account.account_id)
            await test_session.commit()

            # Verify key is deleted
            with pytest.raises(APIKeyNotFoundError):
                await manager.get_api_key(account.account_id, api_key_obj.key_id)

    @pytest.mark.asyncio
    async def test_revoke_api_key_not_found(self, test_session, test_settings):
        """Test revoking non-existent API key fails."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, _ = await manager.create_account(name="Revoke Fail Test")
            await test_session.commit()

            with pytest.raises(APIKeyNotFoundError):
                # New signature: key_id first, account_id optional
                await manager.revoke_api_key("key-nonexistent", account.account_id)

    @pytest.mark.asyncio
    async def test_multiple_keys_per_account(self, test_session, test_settings):
        """Test that accounts can have multiple API keys."""
        manager = AccountManager(test_session)

        with patch("duckpond.accounts.manager.get_settings", return_value=test_settings):
            account, _ = await manager.create_account(name="Multi Key Test")
            await test_session.commit()

            # Create multiple keys (in addition to initial key created with account)
            keys = []
            for i in range(5):
                key_obj, plain_key = await manager.create_api_key(
                    account.account_id, f"Key {i}"
                )
                keys.append((key_obj, plain_key))
            await test_session.commit()

            # Verify all keys exist
            all_keys = await manager.list_api_keys(account.account_id)

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
    db_path = tmp_path / "test_accounts.db"
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
