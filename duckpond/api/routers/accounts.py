"""Account management and API key operations."""

from datetime import datetime, timezone
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from duckpond.accounts.auth import get_authenticator
from duckpond.accounts.models import APIKey
from duckpond.api.dependencies import get_api_key
from duckpond.db.session import get_db_session

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/accounts", tags=["accounts"])


class AccountInfo(BaseModel):
    """Account information response."""

    account_id: str = Field(..., description="Account ID")
    name: str = Field(..., description="Account name")
    storage_backend: str = Field(..., description="Storage backend type")
    max_storage_gb: int = Field(..., description="Storage quota in GB")
    max_query_memory_gb: int = Field(..., description="Query memory quota in GB")
    max_concurrent_queries: int = Field(..., description="Concurrent query quota")
    created_at: str = Field(..., description="Account creation timestamp")
    status: str = Field(default="active", description="Account status")


class APIKeyInfo(BaseModel):
    """API key information (masked)."""

    key_id: str = Field(..., description="API key ID")
    key_preview: str = Field(..., description="Masked key preview")
    description: str | None = Field(None, description="Key description")
    created_at: str = Field(..., description="Creation timestamp")
    last_used: str | None = Field(None, description="Last used timestamp")
    expires_at: str | None = Field(None, description="Expiration timestamp")


class CreateAPIKeyRequest(BaseModel):
    """Request to create a new API key."""

    name: str | None = Field(
        None, max_length=512, description="Optional description/name for the key"
    )
    expires_in_days: int | None = Field(
        None, ge=1, le=365, description="Days until expiration (1-365)"
    )


class CreateAPIKeyResponse(BaseModel):
    """Response with newly created API key."""

    key_id: str = Field(..., description="API key ID")
    api_key: str = Field(..., description="Full API key (shown only once)")
    description: str | None = Field(None, description="Key description")
    created_at: str = Field(..., description="Creation timestamp")
    expires_at: str | None = Field(None, description="Expiration timestamp")
    warning: str = Field(
        default="Save this key now - it won't be shown again!",
        description="Warning message",
    )


class UsageStats(BaseModel):
    """Current usage statistics."""

    notebooks: int = Field(..., description="Number of notebooks")
    active_sessions: int = Field(..., description="Number of active sessions")
    storage_gb: float = Field(..., description="Storage used in GB")


class QuotaInfo(BaseModel):
    """Quota information."""

    max_notebooks: int | None = Field(None, description="Max notebooks (null = unlimited)")
    max_storage_gb: int = Field(..., description="Storage quota in GB")
    max_query_memory_gb: int = Field(..., description="Query memory quota in GB")
    max_concurrent_queries: int = Field(..., description="Concurrent query quota")


class AccountDetailsResponse(BaseModel):
    """Complete account details with usage and quotas."""

    account: AccountInfo
    usage: UsageStats
    quotas: QuotaInfo
    api_keys: list[APIKeyInfo]


class DeleteAPIKeyResponse(BaseModel):
    """Response after deleting an API key."""

    success: bool = Field(default=True, description="Deletion success")
    message: str = Field(default="API key revoked successfully", description="Message")


@router.get(
    "/me",
    response_model=AccountDetailsResponse,
    status_code=status.HTTP_200_OK,
    summary="Get current account details",
    description="Returns complete account information including usage, quotas, and API keys",
)
async def get_account_info(
    api_key: Annotated[str, Depends(get_api_key)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> AccountDetailsResponse:
    """
    Get current account information.

    Returns account details, usage statistics, quotas, and API keys.

    Args:
        api_key: API key from cookie/header
        session: Database session

    Returns:
        AccountDetailsResponse with complete account information

    Raises:
        HTTPException: 401 if not authenticated, 500 on error
    """
    try:
        authenticator = get_authenticator()
        result = await authenticator.authenticate(api_key, session)

        if not result:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired API key",
            )

        account, _ = result

        # Build account info
        account_info = AccountInfo(
            account_id=account.account_id,
            name=account.name,
            storage_backend=account.storage_backend,
            max_storage_gb=account.max_storage_gb,
            max_query_memory_gb=account.max_query_memory_gb,
            max_concurrent_queries=account.max_concurrent_queries,
            created_at=account.created_at.isoformat(),
            status="active",  # Always active if they can authenticate
        )

        # Build API keys list (masked)
        api_keys_info = [
            APIKeyInfo(
                key_id=key.key_id,
                key_preview=f"{key.key_prefix}{'*' * 24}",
                description=key.description,
                created_at=key.created_at.isoformat(),
                last_used=key.last_used.isoformat() if key.last_used else None,
                expires_at=key.expires_at.isoformat() if key.expires_at else None,
            )
            for key in account.api_keys
        ]

        # Calculate usage statistics
        # TODO: Implement actual storage calculation
        usage_stats = UsageStats(
            notebooks=0,  # Will be calculated from notebooks table
            active_sessions=0,  # Will be calculated from active sessions
            storage_gb=0.0,  # Will be calculated from file sizes
        )

        # Build quotas
        quotas = QuotaInfo(
            max_notebooks=None,  # Unlimited by default
            max_storage_gb=account.max_storage_gb,
            max_query_memory_gb=account.max_query_memory_gb,
            max_concurrent_queries=account.max_concurrent_queries,
        )

        logger.info(
            "account_info_retrieved",
            account_id=account.account_id,
            api_keys_count=len(api_keys_info),
        )

        return AccountDetailsResponse(
            account=account_info,
            usage=usage_stats,
            quotas=quotas,
            api_keys=api_keys_info,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_account_info_error", error=str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve account information",
        ) from e


@router.get(
    "/me/api-keys",
    response_model=list[APIKeyInfo],
    status_code=status.HTTP_200_OK,
    summary="List API keys",
    description="Returns list of API keys for the current account (masked)",
)
async def list_api_keys(
    api_key: Annotated[str, Depends(get_api_key)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> list[APIKeyInfo]:
    """
    List all API keys for current account.

    Returns masked API keys with metadata.

    Args:
        api_key: API key from cookie/header
        session: Database session

    Returns:
        List of APIKeyInfo objects

    Raises:
        HTTPException: 401 if not authenticated, 500 on error
    """
    try:
        authenticator = get_authenticator()
        result = await authenticator.authenticate(api_key, session)

        if not result:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired API key",
            )

        account, _ = result

        # Build API keys list
        api_keys_info = [
            APIKeyInfo(
                key_id=key.key_id,
                key_preview=f"{key.key_prefix}{'*' * 24}",
                description=key.description,
                created_at=key.created_at.isoformat(),
                last_used=key.last_used.isoformat() if key.last_used else None,
                expires_at=key.expires_at.isoformat() if key.expires_at else None,
            )
            for key in account.api_keys
        ]

        logger.info("api_keys_listed", account_id=account.account_id, count=len(api_keys_info))

        return api_keys_info

    except HTTPException:
        raise
    except Exception as e:
        logger.error("list_api_keys_error", error=str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list API keys",
        ) from e


@router.post(
    "/me/api-keys",
    response_model=CreateAPIKeyResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create API key",
    description="Creates a new API key for the current account",
)
async def create_api_key(
    request: CreateAPIKeyRequest,
    api_key: Annotated[str, Depends(get_api_key)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> CreateAPIKeyResponse:
    """
    Create a new API key.

    Generates a new API key for the account. The full key is returned
    only once - it should be saved by the user immediately.

    Args:
        request: Create API key request
        api_key: API key from cookie/header
        session: Database session

    Returns:
        CreateAPIKeyResponse with the new API key

    Raises:
        HTTPException: 401 if not authenticated, 500 on error
    """
    try:
        authenticator = get_authenticator()
        result = await authenticator.authenticate(api_key, session)

        if not result:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired API key",
            )

        account, _ = result

        # Generate new API key
        import hashlib
        import secrets

        # Generate key: duck_<32 random hex chars>
        new_key = f"duck_{secrets.token_hex(16)}"
        key_hash = hashlib.sha256(new_key.encode()).hexdigest()
        key_prefix = new_key[:8]
        key_id = f"key-{secrets.token_urlsafe(8)}"

        # Calculate expiration if provided
        expires_at = None
        if request.expires_in_days:
            from datetime import timedelta

            expires_at = datetime.now(timezone.utc) + timedelta(days=request.expires_in_days)

        # Create new API key
        new_api_key = APIKey(
            key_id=key_id,
            account_id=account.account_id,
            key_prefix=key_prefix,
            key_hash=key_hash,
            description=request.name,
            created_at=datetime.now(timezone.utc),
            expires_at=expires_at,
        )

        session.add(new_api_key)
        await session.commit()

        logger.info(
            "api_key_created",
            account_id=account.account_id,
            key_id=key_id,
            description=request.name,
        )

        return CreateAPIKeyResponse(
            key_id=key_id,
            api_key=new_key,
            description=request.name,
            created_at=new_api_key.created_at.isoformat(),
            expires_at=expires_at.isoformat() if expires_at else None,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("create_api_key_error", error=str(e), exc_info=True)
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create API key",
        ) from e


@router.delete(
    "/me/api-keys/{key_id}",
    response_model=DeleteAPIKeyResponse,
    status_code=status.HTTP_200_OK,
    summary="Revoke API key",
    description="Revokes (deletes) an API key for the current account",
)
async def delete_api_key(
    key_id: str,
    api_key: Annotated[str, Depends(get_api_key)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> DeleteAPIKeyResponse:
    """
    Revoke an API key.

    Deletes the specified API key. Cannot delete the currently used key
    if it's the only key for the account.

    Args:
        key_id: API key ID to revoke
        api_key: API key from cookie/header
        session: Database session

    Returns:
        DeleteAPIKeyResponse with success status

    Raises:
        HTTPException: 401 if not authenticated, 400 if invalid, 500 on error
    """
    try:
        authenticator = get_authenticator()
        result = await authenticator.authenticate(api_key, session)

        if not result:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired API key",
            )

        account, current_key = result

        # Check if key exists and belongs to this account
        stmt = select(APIKey).where(
            APIKey.key_id == key_id, APIKey.account_id == account.account_id
        )
        result = await session.execute(stmt)
        key_to_delete = result.scalar_one_or_none()

        if not key_to_delete:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="API key not found",
            )

        # Prevent deleting the current key if it's the last one
        total_keys = len(account.api_keys)
        if total_keys == 1 and key_to_delete.key_id == current_key.key_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot revoke your only API key. Create another key first.",
            )

        # Delete the key
        await session.delete(key_to_delete)
        await session.commit()

        logger.info(
            "api_key_revoked",
            account_id=account.account_id,
            key_id=key_id,
        )

        return DeleteAPIKeyResponse(
            success=True,
            message="API key revoked successfully",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("delete_api_key_error", error=str(e), exc_info=True)
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to revoke API key",
        ) from e
