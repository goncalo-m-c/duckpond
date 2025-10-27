"""FastAPI router for tenant management endpoints."""

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from duckpond.db.session import get_db_session
from duckpond.tenants import (
    TenantAlreadyExistsError,
    TenantCreate,
    TenantCreateResponse,
    TenantListResponse,
    TenantManager,
    TenantManagerError,
    TenantNotFoundError,
    TenantResponse,
    TenantUpdate,
)

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/tenants", tags=["tenants"])


def get_tenant_manager(
    session: AsyncSession = Depends(get_db_session),
) -> TenantManager:
    """Dependency to get TenantManager instance."""
    return TenantManager(session=session)


@router.post(
    "",
    response_model=TenantCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new tenant",
    description="Creates a new tenant with isolated storage and generates an API key",
)
async def create_tenant(
    tenant_data: TenantCreate,
    manager: TenantManager = Depends(get_tenant_manager),
) -> TenantCreateResponse:
    """
    Create a new tenant.

    Args:
        tenant_data: Tenant creation parameters
        manager: TenantManager dependency

    Returns:
        TenantCreateResponse with tenant details and one-time API key

    Raises:
        HTTPException: 409 if tenant already exists, 500 for other errors
    """
    try:
        logger.info("creating_tenant", name=tenant_data.name)
        tenant, api_key = await manager.create_tenant(
            name=tenant_data.name,
            storage_backend=tenant_data.storage_backend,
            storage_config=tenant_data.storage_config,
            max_storage_gb=tenant_data.max_storage_gb,
            max_query_memory_gb=tenant_data.max_query_memory_gb,
            max_concurrent_queries=tenant_data.max_concurrent_queries,
        )
        logger.info(
            "tenant_created",
            tenant_id=tenant.tenant_id,
            name=tenant.name,
        )
        return TenantCreateResponse(
            tenant=TenantResponse.model_validate(tenant),
            api_key=api_key,
        )
    except TenantAlreadyExistsError as e:
        logger.warning("tenant_already_exists", name=tenant_data.name, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Tenant '{tenant_data.name}' already exists",
        ) from e
    except TenantManagerError as e:
        logger.error("tenant_creation_failed", name=tenant_data.name, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create tenant",
        ) from e


@router.get(
    "/{tenant_id}",
    response_model=TenantResponse,
    summary="Get tenant by ID",
    description="Retrieves tenant details by tenant ID",
)
async def get_tenant(
    tenant_id: str,
    manager: TenantManager = Depends(get_tenant_manager),
) -> TenantResponse:
    """
    Get tenant by ID.

    Args:
        tenant_id: Unique tenant identifier
        manager: TenantManager dependency

    Returns:
        TenantResponse with tenant details

    Raises:
        HTTPException: 404 if tenant not found, 500 for other errors
    """
    try:
        logger.info("fetching_tenant", tenant_id=tenant_id)
        tenant = await manager.get_tenant_by_id(tenant_id)
        return TenantResponse.model_validate(tenant)
    except TenantNotFoundError as e:
        logger.warning("tenant_not_found", tenant_id=tenant_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant '{tenant_id}' not found",
        ) from e
    except TenantManagerError as e:
        logger.error("tenant_fetch_failed", tenant_id=tenant_id, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve tenant",
        ) from e


@router.get(
    "",
    response_model=TenantListResponse,
    summary="List tenants",
    description="Lists all tenants with pagination support",
)
async def list_tenants(
    offset: int = Query(default=0, ge=0, description="Number of records to skip"),
    limit: int = Query(
        default=100, ge=1, le=1000, description="Maximum number of records"
    ),
    manager: TenantManager = Depends(get_tenant_manager),
) -> TenantListResponse:
    """
    List tenants with pagination.

    Args:
        offset: Number of records to skip
        limit: Maximum number of records to return
        manager: TenantManager dependency

    Returns:
        TenantListResponse with paginated tenant list

    Raises:
        HTTPException: 500 for errors
    """
    try:
        logger.info("listing_tenants", offset=offset, limit=limit)
        tenants, total = await manager.list_tenants(offset=offset, limit=limit)
        return TenantListResponse(
            tenants=[TenantResponse.model_validate(t) for t in tenants],
            total=total,
            offset=offset,
            limit=limit,
        )
    except TenantManagerError as e:
        logger.error("tenant_list_failed", offset=offset, limit=limit, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list tenants",
        ) from e


@router.patch(
    "/{tenant_id}/quotas",
    response_model=TenantResponse,
    summary="Update tenant quotas",
    description="Updates tenant resource quotas (storage, memory, concurrency)",
)
async def update_tenant_quotas(
    tenant_id: str,
    quota_updates: TenantUpdate,
    manager: TenantManager = Depends(get_tenant_manager),
) -> TenantResponse:
    """
    Update tenant quotas.

    Args:
        tenant_id: Unique tenant identifier
        quota_updates: Quota values to update
        manager: TenantManager dependency

    Returns:
        TenantResponse with updated tenant details

    Raises:
        HTTPException: 404 if tenant not found, 500 for other errors
    """
    try:
        logger.info(
            "updating_tenant_quotas",
            tenant_id=tenant_id,
            updates=quota_updates.model_dump(),
        )
        tenant = await manager.update_tenant_quotas(
            tenant_id=tenant_id,
            max_storage_gb=quota_updates.max_storage_gb,
            max_query_memory_gb=quota_updates.max_query_memory_gb,
            max_concurrent_queries=quota_updates.max_concurrent_queries,
        )
        logger.info("tenant_quotas_updated", tenant_id=tenant_id)
        return TenantResponse.model_validate(tenant)
    except TenantNotFoundError as e:
        logger.warning("tenant_not_found", tenant_id=tenant_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant '{tenant_id}' not found",
        ) from e
    except TenantManagerError as e:
        logger.error("tenant_update_failed", tenant_id=tenant_id, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update tenant quotas",
        ) from e


@router.delete(
    "/{tenant_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete tenant",
    description="Deletes a tenant and optionally purges all data",
)
async def delete_tenant(
    tenant_id: str,
    purge_data: bool = Query(
        default=False,
        description="If true, permanently delete all tenant data",
    ),
    manager: TenantManager = Depends(get_tenant_manager),
) -> None:
    """
    Delete tenant.

    Args:
        tenant_id: Unique tenant identifier
        purge_data: Whether to permanently delete tenant data
        manager: TenantManager dependency

    Raises:
        HTTPException: 404 if tenant not found, 500 for other errors
    """
    try:
        logger.info("deleting_tenant", tenant_id=tenant_id, purge_data=purge_data)
        await manager.delete_tenant(tenant_id, purge_data=purge_data)
        logger.info("tenant_deleted", tenant_id=tenant_id)
    except TenantNotFoundError as e:
        logger.warning("tenant_not_found", tenant_id=tenant_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant '{tenant_id}' not found",
        ) from e
    except TenantManagerError as e:
        logger.error("tenant_delete_failed", tenant_id=tenant_id, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete tenant",
        ) from e
