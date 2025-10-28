"""FastAPI dependencies for DuckPond API.

This module provides dependency injection for:
- API key extraction and validation
- Tenant authentication via database lookup
- Database session management
- Catalog manager access
"""

from typing import Annotated, AsyncGenerator

from fastapi import Depends, Header
from sqlalchemy.ext.asyncio import AsyncSession

from duckpond.api.exceptions import ForbiddenException, UnauthorizedException
from duckpond.config import get_settings
from duckpond.db.session import get_db_session
from duckpond.tenants.auth import get_authenticator


async def get_api_key(
    x_api_key: Annotated[str | None, Header()] = None,
    authorization: Annotated[str | None, Header()] = None,
) -> str:
    """Extract API key from headers.

    Supports both X-API-Key header and Authorization: Bearer token.

    Args:
        x_api_key: API key from X-API-Key header
        authorization: Bearer token from Authorization header

    Returns:
        API key string

    Raises:
        UnauthorizedException: If no API key is provided

    Example:
        @app.get("/protected")
        async def protected(api_key: str = Depends(get_api_key)):
            return {"api_key": api_key[:8] + "..."}
    """
    api_key = x_api_key

    if not api_key and authorization:
        if authorization.startswith("Bearer "):
            api_key = authorization[7:]

    if not api_key:
        raise UnauthorizedException("API key required")

    return api_key


async def get_current_tenant(
    api_key: Annotated[str, Depends(get_api_key)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> str:
    """Validate API key and return tenant ID.

    This validates the API key against the database using bcrypt hash verification
    and returns the authenticated tenant ID.

    Args:
        api_key: API key from headers
        session: Database session

    Returns:
        Tenant ID string

    Raises:
        UnauthorizedException: If API key is invalid or expired
        ForbiddenException: If tenant is not active

    Example:
        @app.get("/tenant-info")
        async def info(tenant_id: str = Depends(get_current_tenant)):
            return {"tenant_id": tenant_id}
    """
    authenticator = get_authenticator()

    result = await authenticator.authenticate(api_key, session)

    if not result:
        raise UnauthorizedException("Invalid or expired API key")

    tenant, api_key_obj = result

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

    return tenant.tenant_id


async def validate_tenant_access(
    tenant_id: Annotated[str, Depends(get_current_tenant)],
    requested_tenant: str | None = None,
) -> str:
    """Validate that authenticated tenant can access requested resource.

    Args:
        tenant_id: Authenticated tenant ID
        requested_tenant: Tenant ID being requested (optional)

    Returns:
        Validated tenant ID

    Raises:
        ForbiddenException: If tenant doesn't have access

    Example:
        @app.get("/tenants/{tenant_id}/data")
        async def get_data(
            tenant_id: str,
            validated: str = Depends(validate_tenant_access)
        ):
            return {"tenant": validated}
    """
    if requested_tenant and requested_tenant != tenant_id:
        raise ForbiddenException(
            f"Tenant {tenant_id} cannot access resources for {requested_tenant}"
        )

    return tenant_id


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
CurrentTenant = Annotated[str, Depends(get_current_tenant)]
APIKey = Annotated[str, Depends(get_api_key)]
Settings = Annotated[object, Depends(get_settings_dependency)]
DatabaseSession = Annotated[AsyncSession, Depends(get_db_session)]
