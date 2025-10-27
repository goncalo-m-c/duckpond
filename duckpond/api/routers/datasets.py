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


@router.post(
    "",
    response_model=DatasetMetadata,
    status_code=status.HTTP_201_CREATED,
    summary="Create dataset",
)
async def create_dataset(
    request: CreateDatasetRequest,
    tenant_id: CurrentTenant,
):
    """Create a new dataset.

    Creates a new table or view in the catalog with the specified schema
    and configuration.

    Args:
        request: Dataset creation request
        tenant_id: Authenticated tenant ID

    Returns:
        Created dataset metadata

    Raises:
        ConflictException: If dataset already exists
        ValidationException: If schema is invalid

    Example:
        POST /api/v1/datasets
        {
            "name": "sales",
            "type": "table",
            "format": "parquet",
            "schema": {
                "columns": [
                    {"name": "order_id", "type": "BIGINT", "nullable": false},
                    {"name": "customer_id", "type": "BIGINT", "nullable": false},
                    {"name": "amount", "type": "DECIMAL", "nullable": false}
                ]
            },
            "description": "Sales transactions"
        }
    """
    catalog = await create_catalog_manager(tenant_id)

    try:
        dataset = await catalog.create_dataset(request)
        return dataset
    except Exception as e:
        error_msg = str(e).lower()
        if "already exists" in error_msg or "conflict" in error_msg:
            raise ConflictException(f"Dataset {request.name} already exists")
        raise


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


@router.patch(
    "/{dataset_name}",
    response_model=DatasetMetadata,
    status_code=status.HTTP_200_OK,
    summary="Update dataset",
)
async def update_dataset(
    dataset_name: str,
    request: UpdateDatasetRequest,
    tenant_id: CurrentTenant,
):
    """Update dataset metadata.

    Updates the description and/or properties of an existing dataset.
    Schema changes should use the schema evolution endpoint.

    Args:
        dataset_name: Name of the dataset to update
        request: Update request with new metadata
        tenant_id: Authenticated tenant ID

    Returns:
        Updated dataset metadata

    Raises:
        NotFoundException: If dataset does not exist

    Example:
        PATCH /api/v1/datasets/sales
        {
            "description": "Updated sales data with 2024 records",
            "properties": {
                "owner": "analytics_team",
                "retention_days": "365"
            }
        }
    """
    catalog = await create_catalog_manager(tenant_id)

    try:
        dataset = await catalog.update_dataset(dataset_name, request)
        return dataset
    except DatasetNotFoundError:
        raise NotFoundException(f"Dataset {dataset_name} not found")
    except Exception as e:
        error_msg = str(e).lower()
        if "not found" in error_msg or "does not exist" in error_msg:
            raise NotFoundException(f"Dataset {dataset_name} not found")
        raise


@router.delete(
    "/{dataset_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete dataset",
)
async def delete_dataset(
    dataset_name: str,
    tenant_id: CurrentTenant,
    if_exists: bool = False,
):
    """Delete a dataset.

    Permanently removes the dataset and all its data from the catalog.

    Args:
        dataset_name: Name of the dataset to delete
        tenant_id: Authenticated tenant ID
        if_exists: If True, don't error if dataset doesn't exist

    Raises:
        NotFoundException: If dataset does not exist and if_exists=False

    Example:
        DELETE /api/v1/datasets/sales
        DELETE /api/v1/datasets/sales?if_exists=true
    """
    catalog = await create_catalog_manager(tenant_id)

    try:
        await catalog.delete_dataset(dataset_name, if_exists=if_exists)
    except DatasetNotFoundError:
        if not if_exists:
            raise NotFoundException(f"Dataset {dataset_name} not found")
    except Exception as e:
        error_msg = str(e).lower()
        if not if_exists and (
            "not found" in error_msg or "does not exist" in error_msg
        ):
            raise NotFoundException(f"Dataset {dataset_name} not found")
        if if_exists and ("not found" in error_msg or "does not exist" in error_msg):
            pass
        else:
            raise
