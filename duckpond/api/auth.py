"""API authentication using tenant API keys."""

import structlog
from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from duckpond.db.session import get_db_session
from duckpond.tenants.auth import get_authenticator
from duckpond.tenants.models import APIKey, Tenant

logger = structlog.get_logger(__name__)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class AuthenticatedTenant:
    """Container for authenticated tenant information."""

    def __init__(self, tenant: Tenant, api_key: APIKey):
        """
        Initialize authenticated tenant.

        Args:
            tenant: Tenant model instance
            api_key: APIKey model instance
        """
        self.tenant = tenant
        self.api_key = api_key

    @property
    def tenant_id(self) -> str:
        """Get tenant ID."""
        return self.tenant.tenant_id

    @property
    def name(self) -> str:
        """Get tenant name."""
        return self.tenant.name

    def __repr__(self) -> str:
        """String representation."""
        return f"<AuthenticatedTenant tenant_id={self.tenant_id} name={self.name}>"


async def get_current_tenant(
    api_key: str | None = Security(api_key_header),
    session: AsyncSession = Depends(get_db_session),
) -> AuthenticatedTenant:
    """
    Validate API key and return authenticated tenant.

    This dependency performs API key authentication by:
    1. Extracting API key from X-API-Key header
    2. Using cached authenticator for fast validation
    3. Verifying the key using bcrypt (cached for 30s)
    4. Loading the associated tenant (from cache if available)
    5. Checking tenant is active

    Args:
        api_key: API key from request header
        session: Database session

    Returns:
        AuthenticatedTenant with tenant and api_key models

    Raises:
        HTTPException: 401 if authentication fails
    """
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

        tenant, db_key = result
        logger.info("authentication_success", tenant_id=tenant.tenant_id)
        return AuthenticatedTenant(tenant=tenant, api_key=db_key)

    except HTTPException:
        raise
    except Exception as e:
        logger.error("authentication_error", error=str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication error",
        ) from e


async def get_current_tenant_optional(
    api_key: str | None = Security(api_key_header),
    session: AsyncSession = Depends(get_db_session),
) -> AuthenticatedTenant | None:
    """
    Optionally validate API key and return authenticated tenant.

    Same as get_current_tenant but returns None instead of raising
    401 if API key is missing. Still raises 401 if key is present but invalid.

    Args:
        api_key: API key from request header
        session: Database session

    Returns:
        AuthenticatedTenant if authenticated, None otherwise

    Raises:
        HTTPException: 401 if authentication fails with present but invalid key
    """
    if not api_key:
        return None

    return await get_current_tenant(api_key=api_key, session=session)
