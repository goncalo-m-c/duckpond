"""Tests for API dependencies including authentication."""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from duckpond.api.dependencies import (
    get_api_key,
    get_current_account,
    validate_account_access,
)
from duckpond.api.exceptions import ForbiddenException, UnauthorizedException
from duckpond.accounts.models import APIKey, Account
from duckpond.accounts.auth import hash_api_key


class TestGetAPIKey:
    """Test API key extraction from headers."""

    async def test_get_api_key_from_x_api_key_header(self):
        """Test extracting API key from X-API-Key header."""
        api_key = await get_api_key(x_api_key="test-api-key-123", authorization=None)
        assert api_key == "test-api-key-123"

    async def test_get_api_key_from_bearer_token(self):
        """Test extracting API key from Authorization Bearer token."""
        api_key = await get_api_key(
            x_api_key=None, authorization="Bearer test-token-456"
        )
        assert api_key == "test-token-456"

    async def test_get_api_key_prefers_x_api_key(self):
        """Test that X-API-Key header takes precedence over Authorization."""
        api_key = await get_api_key(
            x_api_key="primary-key", authorization="Bearer secondary-key"
        )
        assert api_key == "primary-key"

    async def test_get_api_key_missing_raises_unauthorized(self):
        """Test that missing API key raises UnauthorizedException."""
        with pytest.raises(UnauthorizedException) as exc_info:
            await get_api_key(x_api_key=None, authorization=None)
        assert "API key required" in str(exc_info.value.detail)

    async def test_get_api_key_invalid_bearer_format(self):
        """Test that invalid Bearer format raises UnauthorizedException."""
        with pytest.raises(UnauthorizedException):
            await get_api_key(x_api_key=None, authorization="InvalidFormat")


class TestGetCurrentAccount:
    """Test account authentication via database."""

    @pytest.fixture
    def mock_session(self):
        """Create mock database session."""
        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()
        session.rollback = AsyncMock()
        return session

    @pytest.fixture
    def mock_account(self):
        """Create mock account."""
        account = MagicMock(spec=Account)
        account.account_id = "account-test-123"
        account.name = "Test Account"
        account.storage_backend = "local"
        return account

    @pytest.fixture
    def mock_api_key_obj(self):
        """Create mock API key object."""
        api_key_obj = MagicMock(spec=APIKey)
        api_key_obj.key_id = "key-abc123"
        api_key_obj.account_id = "account-test-123"
        api_key_obj.key_prefix = "_AKCJyQF"
        api_key_obj.expires_at = None
        api_key_obj.last_used = None
        return api_key_obj

    async def test_get_current_account_valid_key(
        self, mock_session, mock_account, mock_api_key_obj
    ):
        """Test successful authentication with valid API key."""
        api_key = "_AKCJyQFBOtsJNvN-DqVVciTj0T6g_5vj9iL_ymTAws"

        with patch("duckpond.api.dependencies.get_authenticator") as mock_get_auth:
            mock_authenticator = MagicMock()
            mock_authenticator.authenticate = AsyncMock(
                return_value=(mock_account, mock_api_key_obj)
            )
            mock_get_auth.return_value = mock_authenticator

            account_id = await get_current_account(api_key, mock_session)

            assert account_id == "account-test-123"
            mock_authenticator.authenticate.assert_called_once_with(
                api_key, mock_session
            )
            mock_session.commit.assert_called_once()

    async def test_get_current_account_invalid_key(self, mock_session):
        """Test authentication failure with invalid API key."""
        api_key = "invalid-key"

        with patch("duckpond.api.dependencies.get_authenticator") as mock_get_auth:
            mock_authenticator = MagicMock()
            mock_authenticator.authenticate = AsyncMock(return_value=None)
            mock_get_auth.return_value = mock_authenticator

            with pytest.raises(UnauthorizedException) as exc_info:
                await get_current_account(api_key, mock_session)

            assert "Invalid or expired API key" in str(exc_info.value.detail)

    async def test_get_current_account_expired_key(
        self, mock_session, mock_account, mock_api_key_obj
    ):
        """Test authentication failure with expired API key."""
        api_key = "_AKCJyQFBOtsJNvN-DqVVciTj0T6g_5vj9iL_ymTAws"

        # Set expiration to yesterday
        mock_api_key_obj.expires_at = datetime.now(timezone.utc) - timedelta(days=1)

        with patch("duckpond.api.dependencies.get_authenticator") as mock_get_auth:
            mock_authenticator = MagicMock()
            mock_authenticator.authenticate = AsyncMock(
                return_value=(mock_account, mock_api_key_obj)
            )
            mock_get_auth.return_value = mock_authenticator

            with pytest.raises(UnauthorizedException) as exc_info:
                await get_current_account(api_key, mock_session)

            assert "API key has expired" in str(exc_info.value.detail)

    async def test_get_current_account_updates_last_used(
        self, mock_session, mock_account, mock_api_key_obj
    ):
        """Test that last_used timestamp is updated on successful auth."""
        api_key = "_AKCJyQFBOtsJNvN-DqVVciTj0T6g_5vj9iL_ymTAws"

        with patch("duckpond.api.dependencies.get_authenticator") as mock_get_auth:
            mock_authenticator = MagicMock()
            mock_authenticator.authenticate = AsyncMock(
                return_value=(mock_account, mock_api_key_obj)
            )
            mock_get_auth.return_value = mock_authenticator

            await get_current_account(api_key, mock_session)

            # Verify last_used was updated
            assert mock_api_key_obj.last_used is not None
            assert isinstance(mock_api_key_obj.last_used, datetime)
            mock_session.commit.assert_called_once()

    async def test_get_current_account_handles_commit_error(
        self, mock_session, mock_account, mock_api_key_obj
    ):
        """Test that commit errors when updating last_used don't fail the request."""
        api_key = "_AKCJyQFBOtsJNvN-DqVVciTj0T6g_5vj9iL_ymTAws"
        mock_session.commit = AsyncMock(side_effect=Exception("DB error"))

        with patch("duckpond.api.dependencies.get_authenticator") as mock_get_auth:
            mock_authenticator = MagicMock()
            mock_authenticator.authenticate = AsyncMock(
                return_value=(mock_account, mock_api_key_obj)
            )
            mock_get_auth.return_value = mock_authenticator

            # Should succeed despite commit error
            account_id = await get_current_account(api_key, mock_session)

            assert account_id == "account-test-123"
            mock_session.rollback.assert_called_once()


class TestValidateAccountAccess:
    """Test account access validation."""

    async def test_validate_account_access_same_account(self):
        """Test validation succeeds when account matches."""
        result = await validate_account_access(
            account_id="account-123", requested_account="account-123"
        )
        assert result == "account-123"

    async def test_validate_account_access_no_requested_account(self):
        """Test validation succeeds when no requested account specified."""
        result = await validate_account_access(
            account_id="account-123", requested_account=None
        )
        assert result == "account-123"

    async def test_validate_account_access_different_account(self):
        """Test validation fails when account doesn't match."""
        with pytest.raises(ForbiddenException) as exc_info:
            await validate_account_access(
                account_id="account-123", requested_account="account-456"
            )
        assert "cannot access resources" in str(exc_info.value.detail)


class TestAuthenticationIntegration:
    """Integration tests for full authentication flow."""

    async def test_full_authentication_flow(self):
        """Test complete authentication flow from header to account ID."""
        # This test demonstrates the full dependency chain
        api_key = "_AKCJyQFBOtsJNvN-DqVVciTj0T6g_5vj9iL_ymTAws"
        mock_session = AsyncMock(spec=AsyncSession)
        mock_account = MagicMock(spec=Account)
        mock_account.account_id = "account-production"
        mock_api_key_obj = MagicMock(spec=APIKey)
        mock_api_key_obj.expires_at = None

        with patch("duckpond.api.dependencies.get_authenticator") as mock_get_auth:
            mock_authenticator = MagicMock()
            mock_authenticator.authenticate = AsyncMock(
                return_value=(mock_account, mock_api_key_obj)
            )
            mock_get_auth.return_value = mock_authenticator

            # Step 1: Extract API key from header
            extracted_key = await get_api_key(x_api_key=api_key, authorization=None)
            assert extracted_key == api_key

            # Step 2: Authenticate and get account ID
            account_id = await get_current_account(extracted_key, mock_session)
            assert account_id == "account-production"

            # Step 3: Validate access
            validated = await validate_account_access(
                account_id=account_id, requested_account=account_id
            )
            assert validated == account_id


class TestAPIKeyFormats:
    """Test various API key formats for backward compatibility."""

    @pytest.fixture
    def mock_session(self):
        """Create mock database session."""
        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()
        session.rollback = AsyncMock()
        return session

    async def test_url_safe_base64_format(self, mock_session):
        """Test API key with URL-safe base64 format (current standard)."""
        # This is the format generated by secrets.token_urlsafe(32)
        api_key = "_AKCJyQFBOtsJNvN-DqVVciTj0T6g_5vj9iL_ymTAws"

        mock_account = MagicMock(spec=Account)
        mock_account.account_id = "account-test"
        mock_api_key_obj = MagicMock(spec=APIKey)
        mock_api_key_obj.expires_at = None

        with patch("duckpond.api.dependencies.get_authenticator") as mock_get_auth:
            mock_authenticator = MagicMock()
            mock_authenticator.authenticate = AsyncMock(
                return_value=(mock_account, mock_api_key_obj)
            )
            mock_get_auth.return_value = mock_authenticator

            account_id = await get_current_account(api_key, mock_session)
            assert account_id == "account-test"

    async def test_rejects_old_account_format(self, mock_session):
        """Test that old account_xxx_xxx format is rejected."""
        # Old format that was used in the mock implementation
        api_key = "account_test-123_secret"

        with patch("duckpond.api.dependencies.get_authenticator") as mock_get_auth:
            mock_authenticator = MagicMock()
            # Old format won't be found in database
            mock_authenticator.authenticate = AsyncMock(return_value=None)
            mock_get_auth.return_value = mock_authenticator

            with pytest.raises(UnauthorizedException):
                await get_current_account(api_key, mock_session)
