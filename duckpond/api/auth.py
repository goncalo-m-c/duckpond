"""API authentication using account API keys."""

import structlog
from fastapi import Depends, HTTPException, Query, Request, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from duckpond.db.session import get_db_session
from duckpond.accounts.auth import get_authenticator
from duckpond.accounts.models import APIKey, Account

logger = structlog.get_logger(__name__)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class AuthenticatedAccount:
    """Container for authenticated account information."""

    def __init__(self, account: Account, api_key: APIKey):
        """
        Initialize authenticated account.

        Args:
            account: Account model instance
            api_key: APIKey model instance
        """
        self.account = account
        self.api_key = api_key

    @property
    def account_id(self) -> str:
        """Get account ID."""
        return self.account.account_id

    @property
    def name(self) -> str:
        """Get account name."""
        return self.account.name

    def __repr__(self) -> str:
        """String representation."""
        return f"<AuthenticatedAccount account_id={self.account_id} name={self.name}>"


async def get_current_account(
    api_key_header_value: str | None = Security(api_key_header),
    api_key_query: str | None = Query(default=None, alias="X-API-KEY"),
    session: AsyncSession = Depends(get_db_session),
) -> AuthenticatedAccount:
    """
    Validate API key and return authenticated account.

    This dependency performs API key authentication by:
    1. Extracting API key from X-API-Key header OR X-API-KEY query parameter
    2. Using cached authenticator for fast validation
    3. Verifying the key using bcrypt (cached for 30s)
    4. Loading the associated account (from cache if available)
    5. Checking account is active

    Args:
        request: FastAPI request
        api_key_header_value: API key from request header
        api_key_query: API key from query parameter (for browser access)
        session: Database session

    Returns:
        AuthenticatedAccount with account and api_key models

    Raises:
        HTTPException: 401 if authentication fails
    """
    # Try header first, then query parameter
    api_key = api_key_header_value or api_key_query
    if not api_key:
        logger.warning("authentication_failed", reason="missing_api_key")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    try:
        authenticator = get_authenticator()
        result = await authenticator.authenticate(api_key, session)

        if not result:
            logger.warning("authentication_failed", reason="invalid_api_key")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key",
                headers={"WWW-Authenticate": "ApiKey"},
            )

        account, db_key = result
        logger.info("authentication_success", account_id=account.account_id)
        return AuthenticatedAccount(account=account, api_key=db_key)

    except HTTPException:
        raise
    except Exception as e:
        logger.error("authentication_error", error=str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication error",
        ) from e


async def get_current_account_optional(
    api_key_header_value: str | None = Security(api_key_header),
    api_key_query: str | None = Query(default=None, alias="X-API-KEY"),
    session: AsyncSession = Depends(get_db_session),
) -> AuthenticatedAccount | None:
    """
    Optionally validate API key and return authenticated account.

    Same as get_current_account but returns None instead of raising
    401 if API key is missing. Still raises 401 if key is present but invalid.

    Args:
        request: FastAPI request
        api_key_header_value: API key from request header
        api_key_query: API key from query parameter (for browser access)
        session: Database session

    Returns:
        AuthenticatedAccount if authenticated, None otherwise

    Raises:
        HTTPException: 401 if authentication fails with present but invalid key
    """
    # Try header first, then query parameter
    api_key = api_key_header_value or api_key_query

    if not api_key:
        return None

    return await get_current_account(
        api_key_header_value=api_key_header_value,
        api_key_query=api_key_query,
        session=session,
    )
