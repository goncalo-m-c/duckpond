"""FastAPI router for account management endpoints."""

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from duckpond.db.session import get_db_session
from duckpond.accounts import (
    AccountAlreadyExistsError,
    AccountCreate,
    AccountCreateResponse,
    AccountListResponse,
    AccountManager,
    AccountManagerError,
    AccountNotFoundError,
    AccountResponse,
    AccountUpdate,
)

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/accounts", tags=["accounts"])


def get_account_manager(
    session: AsyncSession = Depends(get_db_session),
) -> AccountManager:
    """Dependency to get AccountManager instance."""
    return AccountManager(session=session)


@router.post(
    "",
    response_model=AccountCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new account",
    description="Creates a new account with isolated storage and generates an API key",
)
async def create_account(
    account_data: AccountCreate,
    manager: AccountManager = Depends(get_account_manager),
) -> AccountCreateResponse:
    """
    Create a new account.

    Args:
        account_data: Account creation parameters
        manager: AccountManager dependency

    Returns:
        AccountCreateResponse with account details and one-time API key

    Raises:
        HTTPException: 409 if account already exists, 500 for other errors
    """
    try:
        logger.info("creating_account", name=account_data.name)
        account, api_key = await manager.create_account(
            name=account_data.name,
            storage_backend=account_data.storage_backend,
            storage_config=account_data.storage_config,
            max_storage_gb=account_data.max_storage_gb,
            max_query_memory_gb=account_data.max_query_memory_gb,
            max_concurrent_queries=account_data.max_concurrent_queries,
        )
        logger.info(
            "account_created",
            account_id=account.account_id,
            name=account.name,
        )
        return AccountCreateResponse(
            account=AccountResponse.model_validate(account),
            api_key=api_key,
        )
    except AccountAlreadyExistsError as e:
        logger.warning("account_already_exists", name=account_data.name, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Account '{account_data.name}' already exists",
        ) from e
    except AccountManagerError as e:
        logger.error("account_creation_failed", name=account_data.name, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create account",
        ) from e


@router.get(
    "/{account_id}",
    response_model=AccountResponse,
    summary="Get account by ID",
    description="Retrieves account details by account ID",
)
async def get_account(
    account_id: str,
    manager: AccountManager = Depends(get_account_manager),
) -> AccountResponse:
    """
    Get account by ID.

    Args:
        account_id: Unique account identifier
        manager: AccountManager dependency

    Returns:
        AccountResponse with account details

    Raises:
        HTTPException: 404 if account not found, 500 for other errors
    """
    try:
        logger.info("fetching_account", account_id=account_id)
        account = await manager.get_account_by_id(account_id)
        return AccountResponse.model_validate(account)
    except AccountNotFoundError as e:
        logger.warning("account_not_found", account_id=account_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Account '{account_id}' not found",
        ) from e
    except AccountManagerError as e:
        logger.error("account_fetch_failed", account_id=account_id, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve account",
        ) from e


@router.get(
    "",
    response_model=AccountListResponse,
    summary="List accounts",
    description="Lists all accounts with pagination support",
)
async def list_accounts(
    offset: int = Query(default=0, ge=0, description="Number of records to skip"),
    limit: int = Query(
        default=100, ge=1, le=1000, description="Maximum number of records"
    ),
    manager: AccountManager = Depends(get_account_manager),
) -> AccountListResponse:
    """
    List accounts with pagination.

    Args:
        offset: Number of records to skip
        limit: Maximum number of records to return
        manager: AccountManager dependency

    Returns:
        AccountListResponse with paginated account list

    Raises:
        HTTPException: 500 for errors
    """
    try:
        logger.info("listing_accounts", offset=offset, limit=limit)
        accounts, total = await manager.list_accounts(offset=offset, limit=limit)
        return AccountListResponse(
            accounts=[AccountResponse.model_validate(t) for t in accounts],
            total=total,
            offset=offset,
            limit=limit,
        )
    except AccountManagerError as e:
        logger.error("account_list_failed", offset=offset, limit=limit, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list accounts",
        ) from e


@router.patch(
    "/{account_id}/quotas",
    response_model=AccountResponse,
    summary="Update account quotas",
    description="Updates account resource quotas (storage, memory, concurrency)",
)
async def update_account_quotas(
    account_id: str,
    quota_updates: AccountUpdate,
    manager: AccountManager = Depends(get_account_manager),
) -> AccountResponse:
    """
    Update account quotas.

    Args:
        account_id: Unique account identifier
        quota_updates: Quota values to update
        manager: AccountManager dependency

    Returns:
        AccountResponse with updated account details

    Raises:
        HTTPException: 404 if account not found, 500 for other errors
    """
    try:
        logger.info(
            "updating_account_quotas",
            account_id=account_id,
            updates=quota_updates.model_dump(),
        )
        account = await manager.update_account_quotas(
            account_id=account_id,
            max_storage_gb=quota_updates.max_storage_gb,
            max_query_memory_gb=quota_updates.max_query_memory_gb,
            max_concurrent_queries=quota_updates.max_concurrent_queries,
        )
        logger.info("account_quotas_updated", account_id=account_id)
        return AccountResponse.model_validate(account)
    except AccountNotFoundError as e:
        logger.warning("account_not_found", account_id=account_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Account '{account_id}' not found",
        ) from e
    except AccountManagerError as e:
        logger.error("account_update_failed", account_id=account_id, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update account quotas",
        ) from e


@router.delete(
    "/{account_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete account",
    description="Deletes a account and optionally purges all data",
)
async def delete_account(
    account_id: str,
    purge_data: bool = Query(
        default=False,
        description="If true, permanently delete all account data",
    ),
    manager: AccountManager = Depends(get_account_manager),
) -> None:
    """
    Delete account.

    Args:
        account_id: Unique account identifier
        purge_data: Whether to permanently delete account data
        manager: AccountManager dependency

    Raises:
        HTTPException: 404 if account not found, 500 for other errors
    """
    try:
        logger.info("deleting_account", account_id=account_id, purge_data=purge_data)
        await manager.delete_account(account_id, purge_data=purge_data)
        logger.info("account_deleted", account_id=account_id)
    except AccountNotFoundError as e:
        logger.warning("account_not_found", account_id=account_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Account '{account_id}' not found",
        ) from e
    except AccountManagerError as e:
        logger.error("account_delete_failed", account_id=account_id, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete account",
        ) from e
