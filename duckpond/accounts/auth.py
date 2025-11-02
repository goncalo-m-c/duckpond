"""API key authentication with caching and management utilities."""

import secrets
import time
from typing import Optional

import bcrypt
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from duckpond.accounts.models import APIKey, Account

logger = structlog.get_logger(__name__)

CACHE_TTL = 30


class CachedAuthResult:
    """Container for cached authentication results with TTL."""

    def __init__(self, account: Account, api_key: APIKey):
        """
        Initialize cached auth result.

        Args:
            account: Account model instance
            api_key: APIKey model instance
        """
        self.account = account
        self.api_key = api_key
        self.timestamp = time.time()

    def is_expired(self, ttl: int = CACHE_TTL) -> bool:
        """
        Check if cache entry has expired.

        Args:
            ttl: Time to live in seconds

        Returns:
            True if expired, False otherwise
        """
        return (time.time() - self.timestamp) > ttl

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"<CachedAuthResult account_id={self.account.account_id} "
            f"age={time.time() - self.timestamp:.1f}s>"
        )


class APIKeyAuthenticator:
    """
    API key authenticator with LRU cache and TTL.

    This class provides high-performance API key authentication by:
    1. Using LRU cache to avoid repeated database queries
    2. Implementing TTL to ensure cache freshness
    3. Thread-safe operation for concurrent requests
    4. Proper bcrypt verification of API keys

    Cache strategy:
    - LRU cache with 1000 entry maximum
    - 30-second TTL for each entry
    - Cache key is the full API key
    - Cache stores account and API key models
    """

    def __init__(self, cache_size: int = 1000, cache_ttl: int = CACHE_TTL):
        """
        Initialize authenticator with cache configuration.

        Args:
            cache_size: Maximum number of cached entries (default 1000)
            cache_ttl: Time to live for cache entries in seconds (default 30)
        """
        self.cache_size = cache_size
        self.cache_ttl = cache_ttl
        self._cache: dict[str, CachedAuthResult] = {}
        logger.info(
            "APIKeyAuthenticator initialized",
            cache_size=cache_size,
            cache_ttl=cache_ttl,
        )

    async def authenticate(
        self, api_key: str, session: AsyncSession
    ) -> tuple[Account, APIKey] | None:
        """
        Authenticate API key and return account and API key models.

        This method:
        1. Checks cache for valid entry
        2. If cache miss or expired, queries database
        3. Verifies API key with bcrypt
        4. Updates cache on successful authentication

        Args:
            api_key: Plain text API key
            session: Database session

        Returns:
            Tuple of (Account, APIKey) if authenticated, None otherwise
        """
        cached = self._get_from_cache(api_key)
        if cached:
            logger.debug(
                "api_key_cache_hit",
                account_id=cached.account.account_id,
                age=time.time() - cached.timestamp,
            )
            return (cached.account, cached.api_key)

        logger.debug("api_key_cache_miss", key_prefix=api_key[:8])

        result = await self._authenticate_from_db(api_key, session)

        if result:
            account, db_key = result
            self._put_in_cache(api_key, account, db_key)
            logger.info(
                "authentication_success",
                account_id=account.account_id,
                key_id=db_key.key_id,
            )
            return result

        logger.warning("authentication_failed", key_prefix=api_key[:8])
        return None

    async def _authenticate_from_db(
        self, api_key: str, session: AsyncSession
    ) -> tuple[Account, APIKey] | None:
        """
        Authenticate API key against database.

        Args:
            api_key: Plain text API key
            session: Database session

        Returns:
            Tuple of (Account, APIKey) if authenticated, None otherwise
        """
        try:
            key_prefix = api_key[:8]
            stmt = select(APIKey).where(APIKey.key_prefix == key_prefix)
            result = await session.execute(stmt)
            db_key = result.scalar_one_or_none()

            if not db_key:
                logger.debug("key_not_found", key_prefix=key_prefix)
                return None

            if not bcrypt.checkpw(api_key.encode(), db_key.key_hash.encode()):
                logger.warning(
                    "key_hash_mismatch",
                    key_id=db_key.key_id,
                    account_id=db_key.account_id,
                )
                return None

            account = db_key.account
            if not account:
                stmt = select(Account).where(Account.account_id == db_key.account_id)
                result = await session.execute(stmt)
                account = result.scalar_one_or_none()

                if not account:
                    logger.error(
                        "account_not_found",
                        account_id=db_key.account_id,
                        key_id=db_key.key_id,
                    )
                    return None

            return (account, db_key)

        except Exception as e:
            logger.error("authentication_error", error=str(e), exc_info=True)
            return None

    def _get_from_cache(self, api_key: str) -> Optional[CachedAuthResult]:
        """
        Get entry from cache if valid and not expired.

        Args:
            api_key: API key to look up

        Returns:
            CachedAuthResult if found and valid, None otherwise
        """
        cached = self._cache.get(api_key)

        if cached and not cached.is_expired(self.cache_ttl):
            return cached

        if cached:
            logger.debug("cache_entry_expired", account_id=cached.account.account_id)
            del self._cache[api_key]

        return None

    def _put_in_cache(self, api_key: str, account: Account, db_key: APIKey) -> None:
        """
        Store authentication result in cache with LRU eviction.

        Args:
            api_key: API key
            account: Account model
            db_key: APIKey model
        """
        if len(self._cache) >= self.cache_size:
            oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k].timestamp)
            logger.debug(
                "cache_eviction",
                evicted_account=self._cache[oldest_key].account.account_id,
            )
            del self._cache[oldest_key]

        self._cache[api_key] = CachedAuthResult(account, db_key)
        logger.debug(
            "cache_entry_added", account_id=account.account_id, cache_size=len(self._cache)
        )

    def invalidate(self, api_key: str) -> None:
        """
        Invalidate specific API key in cache.

        Use this when an API key is revoked or modified.

        Args:
            api_key: API key to invalidate
        """
        if api_key in self._cache:
            account_id = self._cache[api_key].account.account_id
            del self._cache[api_key]
            logger.info("cache_invalidated", account_id=account_id)

    def invalidate_account(self, account_id: str) -> None:
        """
        Invalidate all cache entries for a account.

        Use this when account is deleted or all keys are revoked.

        Args:
            account_id: Account ID to invalidate
        """
        keys_to_remove = [
            key
            for key, cached in self._cache.items()
            if cached.account.account_id == account_id
        ]

        for key in keys_to_remove:
            del self._cache[key]

        if keys_to_remove:
            logger.info(
                "account_cache_invalidated",
                account_id=account_id,
                keys_removed=len(keys_to_remove),
            )

    def clear_cache(self) -> None:
        """Clear entire cache."""
        cache_size = len(self._cache)
        self._cache.clear()
        logger.info("cache_cleared", entries_removed=cache_size)

    def get_cache_stats(self) -> dict:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        return {
            "size": len(self._cache),
            "max_size": self.cache_size,
            "ttl": self.cache_ttl,
        }


def generate_api_key() -> str:
    """
    Generate a secure API key.

    Uses secrets.token_urlsafe(32) to generate 256-bit URL-safe key.

    Returns:
        URL-safe API key string (43 characters)
    """
    return secrets.token_urlsafe(32)


def hash_api_key(api_key: str, rounds: int = 12) -> str:
    """
    Hash API key with bcrypt.

    Args:
        api_key: Plain text API key
        rounds: bcrypt cost factor (default 12)

    Returns:
        bcrypt hash string
    """
    return bcrypt.hashpw(api_key.encode(), bcrypt.gensalt(rounds=rounds)).decode()


def verify_api_key(api_key: str, key_hash: str) -> bool:
    """
    Verify API key against bcrypt hash.

    Args:
        api_key: Plain text API key
        key_hash: bcrypt hash

    Returns:
        True if verified, False otherwise
    """
    return bcrypt.checkpw(api_key.encode(), key_hash.encode())


_authenticator: Optional[APIKeyAuthenticator] = None


def get_authenticator(
    cache_size: int = 1000, cache_ttl: int = CACHE_TTL
) -> APIKeyAuthenticator:
    """
    Get or create global authenticator instance.

    Args:
        cache_size: Cache size (only used on first call)
        cache_ttl: Cache TTL (only used on first call)

    Returns:
        APIKeyAuthenticator singleton instance
    """
    global _authenticator

    if _authenticator is None:
        _authenticator = APIKeyAuthenticator(cache_size=cache_size, cache_ttl=cache_ttl)

    return _authenticator
