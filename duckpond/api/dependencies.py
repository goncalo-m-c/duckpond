"""FastAPI dependencies for DuckPond API.

This module provides dependency injection for:
- API key extraction and validation
- Account authentication via database lookup
- Database session management
- Catalog manager access
"""

from typing import Annotated, AsyncGenerator

from fastapi import Cookie, Depends, Header, Query
from sqlalchemy.ext.asyncio import AsyncSession

from duckpond.api.exceptions import ForbiddenException, UnauthorizedException
from duckpond.config import get_settings
from duckpond.db.session import get_db_session
from duckpond.accounts.auth import get_authenticator


async def get_api_key(
    x_api_key: Annotated[str | None, Header()] = None,
    authorization: Annotated[str | None, Header()] = None,
    api_key: Annotated[str | None, Query()] = None,
    x_api_key_query: Annotated[str | None, Query(alias="X-API-KEY")] = None,
    notebook_api_key: Annotated[str | None, Cookie()] = None,
) -> str:
    """Extract API key from headers, query parameters, or cookies.

    Supports X-API-Key header, Authorization: Bearer token, api_key/X-API-KEY query parameters,
    and notebook_api_key cookie.

    Args:
        x_api_key: API key from X-API-Key header
        authorization: Bearer token from Authorization header
        api_key: API key from query parameter
        x_api_key_query: API key from X-API-KEY query parameter
        notebook_api_key: API key from notebook_api_key cookie

    Returns:
        API key string

    Raises:
        UnauthorizedException: If no API key is provided

    Example:
        @app.get("/protected")
        async def protected(api_key: str = Depends(get_api_key)):
            return {"api_key": api_key[:8] + "..."}
    """
    # Priority: header > query > cookie
    key = x_api_key or api_key or x_api_key_query or notebook_api_key
    if not key and authorization:
        if authorization.startswith("Bearer "):
            key = authorization[7:]

    if not key:
        raise UnauthorizedException("API key required")

    return key


async def get_current_account(
    api_key: Annotated[str, Depends(get_api_key)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> str:
    """Validate API key and return account ID.

    This validates the API key against the database using bcrypt hash verification
    and returns the authenticated account ID.

    Args:
        api_key: API key from headers
        session: Database session

    Returns:
        Account ID string

    Raises:
        UnauthorizedException: If API key is invalid or expired
        ForbiddenException: If account is not active

    Example:
        @app.get("/account-info")
        async def info(account_id: str = Depends(get_current_account)):
            return {"account_id": account_id}
    """
    authenticator = get_authenticator()

    result = await authenticator.authenticate(api_key, session)

    if not result:
        raise UnauthorizedException("Invalid or expired API key")

    account, api_key_obj = result

    # Check if API key is expired
    if api_key_obj.expires_at:
        from datetime import datetime, timezone

        if api_key_obj.expires_at < datetime.now(timezone.utc):
            raise UnauthorizedException("API key has expired")

    # Update last_used timestamp (async, don't wait for it)
    # This is done in the background to avoid slowing down requests
    from datetime import datetime, timezone

    api_key_obj.last_used = datetime.now(timezone.utc)
    try:
        await session.commit()
    except Exception:
        # Ignore errors when updating last_used - it's not critical
        await session.rollback()

    return account.account_id


async def validate_account_access(
    account_id: Annotated[str, Depends(get_current_account)],
    requested_account: str | None = None,
) -> str:
    """Validate that authenticated account can access requested resource.

    Args:
        account_id: Authenticated account ID
        requested_account: Account ID being requested (optional)

    Returns:
        Validated account ID

    Raises:
        ForbiddenException: If account doesn't have access

    Example:
        @app.get("/accounts/{account_id}/data")
        async def get_data(
            account_id: str,
            validated: str = Depends(validate_account_access)
        ):
            return {"account": validated}
    """
    if requested_account and requested_account != account_id:
        raise ForbiddenException(
            f"Account {account_id} cannot access resources for {requested_account}"
        )

    return account_id


def get_settings_dependency():
    """Get application settings.

    Returns:
        Application settings

    Example:
        @app.get("/config")
        async def config(settings = Depends(get_settings_dependency)):
            return {"backend": settings.default_storage_backend}
    """
    return get_settings()


# Type aliases for cleaner dependency injection
CurrentAccount = Annotated[str, Depends(get_current_account)]
APIKey = Annotated[str, Depends(get_api_key)]
Settings = Annotated[object, Depends(get_settings_dependency)]
DatabaseSession = Annotated[AsyncSession, Depends(get_db_session)]
