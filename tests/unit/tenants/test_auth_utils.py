"""Tests for API key authentication and management utilities."""
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import bcrypt
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from duckpond.tenants.auth import (
    APIKeyAuthenticator,
    CachedAuthResult,
    generate_api_key,
    get_authenticator,
    hash_api_key,
    verify_api_key,
)
from duckpond.tenants.models import APIKey, Tenant


class TestCachedAuthResult:
    """Test CachedAuthResult caching functionality."""

    def test_cache_result_creation(self):
        """Test creating a cache result."""
        tenant = MagicMock(spec=Tenant)
        tenant.tenant_id = "tenant-test"
        api_key = MagicMock(spec=APIKey)
        api_key.key_id = "key-123"

        cached = CachedAuthResult(tenant, api_key)

        assert cached.tenant == tenant
        assert cached.api_key == api_key
        assert isinstance(cached.timestamp, float)
        assert cached.timestamp <= time.time()

    def test_cache_not_expired_within_ttl(self):
        """Test cache entry is not expired within TTL."""
        tenant = MagicMock(spec=Tenant)
        api_key = MagicMock(spec=APIKey)

        cached = CachedAuthResult(tenant, api_key)

        # Should not be expired immediately
        assert not cached.is_expired(ttl=30)

        # Should not be expired after 1 second
        time.sleep(1)
        assert not cached.is_expired(ttl=30)

    def test_cache_expired_after_ttl(self):
        """Test cache entry expires after TTL."""
        tenant = MagicMock(spec=Tenant)
        api_key = MagicMock(spec=APIKey)

        cached = CachedAuthResult(tenant, api_key)

        # Manually set timestamp to past
        cached.timestamp = time.time() - 31

        # Should be expired with 30s TTL
        assert cached.is_expired(ttl=30)

    def test_cache_result_repr(self):
        """Test string representation."""
        tenant = MagicMock(spec=Tenant)
        tenant.tenant_id = "tenant-test"
        api_key = MagicMock(spec=APIKey)

        cached = CachedAuthResult(tenant, api_key)
        repr_str = repr(cached)

        assert "CachedAuthResult" in repr_str
        assert "tenant-test" in repr_str


class TestAPIKeyAuthenticator:
    """Test APIKeyAuthenticator with caching."""

    @pytest.fixture
    def authenticator(self):
        """Create authenticator instance."""
        return APIKeyAuthenticator(cache_size=100, cache_ttl=30)

    @pytest.fixture
    def mock_session(self):
        """Create mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def sample_tenant(self):
        """Create sample tenant."""
        tenant = MagicMock(spec=Tenant)
        tenant.tenant_id = "tenant-test"
        tenant.name = "Test Tenant"
        return tenant

    @pytest.fixture
    def sample_api_key_model(self, sample_tenant):
        """Create sample API key model."""
        api_key = MagicMock(spec=APIKey)
        api_key.key_id = "key-123"
        api_key.tenant_id = "tenant-test"
        api_key.key_prefix = "testkey1"
        api_key.key_hash = hash_api_key("testkey123456789")
        api_key.tenant = sample_tenant
        return api_key

    @pytest.mark.asyncio
    async def test_authenticate_cache_miss_success(
        self, authenticator, mock_session, sample_tenant, sample_api_key_model
    ):
        """Test successful authentication with cache miss."""
        api_key = "testkey123456789"

        # Mock database query
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = sample_api_key_model
        mock_session.execute.return_value = mock_result

        # Authenticate
        result = await authenticator.authenticate(api_key, mock_session)

        assert result is not None
        tenant, db_key = result
        assert tenant == sample_tenant
        assert db_key == sample_api_key_model

        # Verify cache was populated
        assert api_key in authenticator._cache

    @pytest.mark.asyncio
    async def test_authenticate_cache_hit(
        self, authenticator, mock_session, sample_tenant, sample_api_key_model
    ):
        """Test authentication with cache hit."""
        api_key = "testkey123456789"

        # Populate cache
        authenticator._put_in_cache(api_key, sample_tenant, sample_api_key_model)

        # Authenticate (should not query database)
        result = await authenticator.authenticate(api_key, mock_session)

        assert result is not None
        tenant, db_key = result
        assert tenant == sample_tenant
        assert db_key == sample_api_key_model

        # Verify no database query
        mock_session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_authenticate_invalid_key_prefix(
        self, authenticator, mock_session
    ):
        """Test authentication with invalid key prefix."""
        api_key = "invalidkey123"

        # Mock database query - no key found
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        # Authenticate
        result = await authenticator.authenticate(api_key, mock_session)

        assert result is None

    @pytest.mark.asyncio
    async def test_authenticate_invalid_key_hash(
        self, authenticator, mock_session, sample_api_key_model
    ):
        """Test authentication with invalid key hash."""
        api_key = "wrongkey123456789"

        # Mock database query
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = sample_api_key_model
        mock_session.execute.return_value = mock_result

        # Authenticate (wrong key)
        result = await authenticator.authenticate(api_key, mock_session)

        assert result is None

    @pytest.mark.asyncio
    async def test_authenticate_cache_expiry(
        self, authenticator, mock_session, sample_tenant, sample_api_key_model
    ):
        """Test cache entry expiration."""
        api_key = "testkey123456789"

        # Populate cache with expired entry
        cached = CachedAuthResult(sample_tenant, sample_api_key_model)
        cached.timestamp = time.time() - 31  # Expired
        authenticator._cache[api_key] = cached

        # Mock database query for fresh data
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = sample_api_key_model
        mock_session.execute.return_value = mock_result

        # Authenticate (should query database due to expired cache)
        result = await authenticator.authenticate(api_key, mock_session)

        assert result is not None
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_authenticate_database_error(
        self, authenticator, mock_session
    ):
        """Test authentication with database error."""
        api_key = "testkey123456789"

        # Mock database error
        mock_session.execute.side_effect = Exception("Database error")

        # Authenticate
        result = await authenticator.authenticate(api_key, mock_session)

        assert result is None

    def test_invalidate_specific_key(self, authenticator, sample_tenant, sample_api_key_model):
        """Test invalidating specific API key."""
        api_key = "testkey123456789"

        # Populate cache
        authenticator._put_in_cache(api_key, sample_tenant, sample_api_key_model)
        assert api_key in authenticator._cache

        # Invalidate
        authenticator.invalidate(api_key)
        assert api_key not in authenticator._cache

    def test_invalidate_tenant(self, authenticator, sample_tenant):
        """Test invalidating all keys for a tenant."""
        # Create multiple keys for same tenant
        keys = ["key1", "key2", "key3"]
        for key in keys:
            api_key_model = MagicMock(spec=APIKey)
            api_key_model.tenant_id = "tenant-test"
            authenticator._put_in_cache(key, sample_tenant, api_key_model)

        # Add key for different tenant
        other_tenant = MagicMock(spec=Tenant)
        other_tenant.tenant_id = "tenant-other"
        other_key = MagicMock(spec=APIKey)
        authenticator._put_in_cache("otherkey", other_tenant, other_key)

        # Invalidate tenant
        authenticator.invalidate_tenant("tenant-test")

        # Verify tenant keys removed but other tenant remains
        for key in keys:
            assert key not in authenticator._cache
        assert "otherkey" in authenticator._cache

    def test_clear_cache(self, authenticator, sample_tenant, sample_api_key_model):
        """Test clearing entire cache."""
        # Populate cache
        for i in range(5):
            api_key = f"key{i}"
            authenticator._put_in_cache(api_key, sample_tenant, sample_api_key_model)

        assert len(authenticator._cache) == 5

        # Clear cache
        authenticator.clear_cache()
        assert len(authenticator._cache) == 0

    def test_cache_lru_eviction(self, authenticator, sample_tenant):
        """Test LRU cache eviction when full."""
        # Set small cache size
        authenticator.cache_size = 3

        # Add entries
        for i in range(4):
            api_key = f"key{i}"
            api_key_model = MagicMock(spec=APIKey)
            time.sleep(0.01)  # Ensure different timestamps
            authenticator._put_in_cache(api_key, sample_tenant, api_key_model)

        # Cache should only have 3 entries (oldest evicted)
        assert len(authenticator._cache) == 3
        assert "key0" not in authenticator._cache  # Oldest removed
        assert "key3" in authenticator._cache  # Newest present

    def test_get_cache_stats(self, authenticator, sample_tenant, sample_api_key_model):
        """Test getting cache statistics."""
        # Add some entries
        for i in range(3):
            authenticator._put_in_cache(f"key{i}", sample_tenant, sample_api_key_model)

        stats = authenticator.get_cache_stats()

        assert stats["size"] == 3
        assert stats["max_size"] == 100
        assert stats["ttl"] == 30


class TestAPIKeyUtilities:
    """Test API key utility functions."""

    def test_generate_api_key(self):
        """Test generating API key."""
        api_key = generate_api_key()

        assert isinstance(api_key, str)
        assert len(api_key) == 43  # secrets.token_urlsafe(32) produces 43 chars
        
        # Verify uniqueness
        api_key2 = generate_api_key()
        assert api_key != api_key2

    def test_hash_api_key(self):
        """Test hashing API key."""
        api_key = "test-api-key-123"
        
        key_hash = hash_api_key(api_key)

        assert isinstance(key_hash, str)
        assert len(key_hash) == 60  # bcrypt hash length
        assert key_hash.startswith("$2b$")  # bcrypt prefix

    def test_hash_api_key_custom_rounds(self):
        """Test hashing with custom bcrypt rounds."""
        api_key = "test-api-key-123"
        
        # Hash with 10 rounds
        key_hash = hash_api_key(api_key, rounds=10)
        
        assert "$2b$10$" in key_hash

    def test_verify_api_key_success(self):
        """Test verifying correct API key."""
        api_key = "test-api-key-123"
        key_hash = hash_api_key(api_key)

        assert verify_api_key(api_key, key_hash) is True

    def test_verify_api_key_failure(self):
        """Test verifying incorrect API key."""
        api_key = "test-api-key-123"
        wrong_key = "wrong-api-key-456"
        key_hash = hash_api_key(api_key)

        assert verify_api_key(wrong_key, key_hash) is False

    def test_hash_deterministic(self):
        """Test that same key produces different hashes (salt)."""
        api_key = "test-api-key-123"
        
        hash1 = hash_api_key(api_key)
        hash2 = hash_api_key(api_key)

        # Different hashes due to different salts
        assert hash1 != hash2
        
        # But both verify the same key
        assert verify_api_key(api_key, hash1)
        assert verify_api_key(api_key, hash2)


class TestGetAuthenticator:
    """Test global authenticator singleton."""

    def test_get_authenticator_creates_singleton(self):
        """Test get_authenticator creates singleton."""
        # Clear any existing singleton
        import duckpond.tenants.auth as auth_module
        auth_module._authenticator = None

        auth1 = get_authenticator()
        auth2 = get_authenticator()

        assert auth1 is auth2

    def test_get_authenticator_with_params(self):
        """Test get_authenticator with custom parameters."""
        # Clear singleton
        import duckpond.tenants.auth as auth_module
        auth_module._authenticator = None

        auth = get_authenticator(cache_size=500, cache_ttl=60)

        assert auth.cache_size == 500
        assert auth.cache_ttl == 60

    def test_get_authenticator_ignores_subsequent_params(self):
        """Test that subsequent calls ignore parameters."""
        # Clear singleton
        import duckpond.tenants.auth as auth_module
        auth_module._authenticator = None

        auth1 = get_authenticator(cache_size=100, cache_ttl=30)
        auth2 = get_authenticator(cache_size=500, cache_ttl=60)

        # Should be same instance with original params
        assert auth1 is auth2
        assert auth2.cache_size == 100
        assert auth2.cache_ttl == 30
