"""FastAPI dependencies for DuckPond API.

This module provides dependency injection for:
- API key extraction and validation
- Tenant authentication
- Database session management
- Catalog manager access
"""

from typing import Annotated, AsyncGenerator

from fastapi import Depends, Header

from duckpond.api.exceptions import ForbiddenException, UnauthorizedException
from duckpond.config import get_settings


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
) -> str:
    """Validate API key and return tenant ID.

    For now, this is a simplified implementation that extracts tenant ID
    from the API key format. In production, this should validate against
    a database of API keys.

    API key format: "tenant_<tenant_id>_<random_secret>"

    Args:
        api_key: Validated API key

    Returns:
        Tenant ID string

    Raises:
        UnauthorizedException: If API key is invalid
        ForbiddenException: If tenant is not active

    Example:
        @app.get("/tenant-info")
        async def info(tenant_id: str = Depends(get_current_tenant)):
            return {"tenant_id": tenant_id}
    """
    if not api_key.startswith("tenant_"):
        raise UnauthorizedException("Invalid API key format")

    parts = api_key.split("_")
    if len(parts) < 3:
        raise UnauthorizedException("Invalid API key format")

    tenant_id = parts[1]

    if not tenant_id:
        raise UnauthorizedException("Invalid tenant ID in API key")

    return tenant_id


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


CurrentTenant = Annotated[str, Depends(get_current_tenant)]
APIKey = Annotated[str, Depends(get_api_key)]
Settings = Annotated[object, Depends(get_settings_dependency)]


async def get_db_session() -> AsyncGenerator[None, None]:
    """Get database session (placeholder).

    This is a placeholder for future database integration.
    Will be implemented when metadata database is added.

    Yields:
        Database session
    """
    yield None
