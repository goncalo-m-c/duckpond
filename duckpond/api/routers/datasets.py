"""Dataset router for DuckPond API.

This module provides REST endpoints for dataset management including:
- Create new datasets
- List datasets with filtering
- Get dataset metadata
- Update dataset metadata
- Delete datasets
"""

from typing import Optional

from fastapi import APIRouter, status

from duckpond.api.dependencies import CurrentTenant
from duckpond.api.exceptions import ConflictException, NotFoundException
from duckpond.catalog.manager import create_catalog_manager
from duckpond.catalog.schemas import (
    CreateDatasetRequest,
    DatasetListResponse,
    DatasetMetadata,
    UpdateDatasetRequest,
)
from duckpond.exceptions import DatasetNotFoundError

router = APIRouter(prefix="/api/v1/datasets", tags=["datasets"])


@router.get(
    "",
    response_model=DatasetListResponse,
    status_code=status.HTTP_200_OK,
    summary="List datasets",
)
async def list_datasets(
    tenant_id: CurrentTenant,
    dataset_type: Optional[str] = None,
    pattern: Optional[str] = None,
):
    """List all datasets for the authenticated tenant.

    Supports filtering by dataset type and name pattern.

    Args:
        tenant_id: Authenticated tenant ID
        dataset_type: Filter by type (table, view, external)
        pattern: SQL LIKE pattern for name filtering (e.g., "sales%")

    Returns:
        List of datasets with metadata

    Example:
        GET /api/v1/datasets
        GET /api/v1/datasets?dataset_type=table
        GET /api/v1/datasets?pattern=sales%
    """
    catalog = await create_catalog_manager(tenant_id)

    try:
        datasets = await catalog.list_datasets(
            dataset_type=dataset_type,
            pattern=pattern,
        )
        return datasets
    except Exception:
        return DatasetListResponse(datasets=[], total=0)


@router.get(
    "/{dataset_name}",
    response_model=DatasetMetadata,
    status_code=status.HTTP_200_OK,
    summary="Get dataset",
)
async def get_dataset(
    dataset_name: str,
    tenant_id: CurrentTenant,
):
    """Get metadata for a specific dataset.

    Args:
        dataset_name: Name of the dataset
        tenant_id: Authenticated tenant ID

    Returns:
        Dataset metadata

    Raises:
        NotFoundException: If dataset does not exist

    Example:
        GET /api/v1/datasets/sales
    """
    catalog = await create_catalog_manager(tenant_id)

    try:
        dataset = await catalog.get_dataset_metadata(dataset_name)
        return dataset
    except DatasetNotFoundError:
        raise NotFoundException(f"Dataset {dataset_name} not found")
    except Exception as e:
        error_msg = str(e).lower()
        if "not found" in error_msg or "does not exist" in error_msg:
            raise NotFoundException(f"Dataset {dataset_name} not found")
        raise
