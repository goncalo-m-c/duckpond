"""Authentication endpoints for web UI."""

from datetime import datetime, timezone
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from duckpond.accounts.auth import get_authenticator
from duckpond.api.dependencies import get_api_key
from duckpond.config import get_settings
from duckpond.db.session import get_db_session

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/auth", tags=["auth"])


class LoginRequest(BaseModel):
    """Login request with API key."""

    api_key: str = Field(..., min_length=1, description="Account API key")
    remember_me: bool = Field(default=False, description="Remember login session")


class UserInfo(BaseModel):
    """User information returned after login."""

    account_id: str = Field(..., description="Account ID")
    name: str = Field(..., description="Account name")


class TenantInfo(BaseModel):
    """Tenant/account information."""

    account_id: str = Field(..., description="Account ID")
    name: str = Field(..., description="Account name")
    storage_backend: str = Field(..., description="Storage backend type")
    max_storage_gb: int = Field(..., description="Storage quota in GB")
    max_query_memory_gb: int = Field(..., description="Query memory quota in GB")
    max_concurrent_queries: int = Field(..., description="Concurrent query quota")


class LoginResponse(BaseModel):
    """Login response with user and tenant information."""

    user: UserInfo
    tenant: TenantInfo


class MeResponse(BaseModel):
    """Current user information response."""

    tenant_id: str = Field(..., description="Account/tenant ID")
    api_keys: list[dict] = Field(..., description="List of API keys (masked)")
    quotas: dict = Field(..., description="Account quotas")


class LogoutResponse(BaseModel):
    """Logout response."""

    success: bool = Field(default=True, description="Logout success status")


@router.post(
    "/login",
    response_model=LoginResponse,
    status_code=status.HTTP_200_OK,
    summary="Login with API key",
    description="Authenticate user with API key and create session",
)
async def login(
    login_data: LoginRequest,
    response: Response,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> LoginResponse:
    """
    Login with API key.

    Validates the API key, creates a session cookie, and returns user info.

    Args:
        login_data: Login request with API key
        response: FastAPI response for setting cookies
        session: Database session

    Returns:
        LoginResponse with user and tenant information

    Raises:
        HTTPException: 401 if authentication fails
    """
    try:
        authenticator = get_authenticator()
        result = await authenticator.authenticate(login_data.api_key, session)

        if not result:
            logger.warning("login_failed", reason="invalid_api_key")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key",
            )

        account, api_key_obj = result

        # Check if API key is expired
        if api_key_obj.expires_at:
            if api_key_obj.expires_at < datetime.now(timezone.utc):
                logger.warning(
                    "login_failed",
                    reason="expired_api_key",
                    account_id=account.account_id,
                )
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="API key has expired",
                )

        # Update last_used timestamp
        api_key_obj.last_used = datetime.now(timezone.utc)
        try:
            await session.commit()
        except Exception:
            await session.rollback()

        # Set session cookie
        settings = get_settings()
        max_age = 30 * 24 * 60 * 60 if login_data.remember_me else 24 * 60 * 60

        response.set_cookie(
            key="notebook_api_key",
            value=login_data.api_key,
            max_age=max_age,
            httponly=True,
            secure=settings.duckpond_host != "localhost",
            samesite="lax",
        )

        logger.info("login_success", account_id=account.account_id, name=account.name)

        return LoginResponse(
            user=UserInfo(
                account_id=account.account_id,
                name=account.name,
            ),
            tenant=TenantInfo(
                account_id=account.account_id,
                name=account.name,
                storage_backend=account.storage_backend,
                max_storage_gb=account.max_storage_gb,
                max_query_memory_gb=account.max_query_memory_gb,
                max_concurrent_queries=account.max_concurrent_queries,
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("login_error", error=str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication error",
        ) from e


@router.post(
    "/logout",
    response_model=LogoutResponse,
    status_code=status.HTTP_200_OK,
    summary="Logout current session",
    description="Clear session cookie and logout user",
)
async def logout(response: Response) -> LogoutResponse:
    """
    Logout current user.

    Clears the session cookie.

    Args:
        response: FastAPI response for clearing cookies

    Returns:
        LogoutResponse with success status
    """
    response.delete_cookie(key="notebook_api_key", httponly=True, samesite="lax")
    logger.info("logout_success")
    return LogoutResponse(success=True)


@router.get(
    "/me",
    response_model=MeResponse,
    status_code=status.HTTP_200_OK,
    summary="Get current user information",
    description="Returns information about the currently authenticated user",
)
async def get_current_user(
    api_key: Annotated[str, Depends(get_api_key)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> MeResponse:
    """
    Get current user information.

    Returns account ID, API keys (masked), and quotas.

    Args:
        api_key: API key from cookie/header
        session: Database session

    Returns:
        MeResponse with user information

    Raises:
        HTTPException: 401 if not authenticated
    """
    try:
        authenticator = get_authenticator()
        result = await authenticator.authenticate(api_key, session)

        if not result:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired API key",
            )

        account, api_key_obj = result

        # Get all API keys for this account (masked)
        masked_keys = [
            {
                "key_id": key.key_id,
                "key_preview": f"{key.key_id[:8]}...",
                "created_at": key.created_at.isoformat(),
                "expires_at": key.expires_at.isoformat() if key.expires_at else None,
                "last_used": key.last_used.isoformat() if key.last_used else None,
            }
            for key in account.api_keys
        ]

        quotas = {
            "max_storage_gb": account.max_storage_gb,
            "max_query_memory_gb": account.max_query_memory_gb,
            "max_concurrent_queries": account.max_concurrent_queries,
        }

        return MeResponse(
            tenant_id=account.account_id,
            api_keys=masked_keys,
            quotas=quotas,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_me_error", error=str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve user information",
        ) from e
