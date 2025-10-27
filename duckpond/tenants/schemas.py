"""Pydantic schemas for tenant operations."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class TenantCreate(BaseModel):
    """Schema for creating a new tenant."""

    name: str = Field(
        ..., min_length=1, max_length=255, description="Unique tenant name"
    )
    storage_backend: Literal["local", "s3"] = Field(
        default="local", description="Storage backend type"
    )
    storage_config: dict[str, str] = Field(
        default_factory=dict, description="Storage backend configuration"
    )
    max_storage_gb: int = Field(
        default=100, ge=1, description="Maximum storage quota in gigabytes"
    )
    max_query_memory_gb: int = Field(
        default=4, ge=1, description="Maximum query memory in gigabytes"
    )
    max_concurrent_queries: int = Field(
        default=10, ge=1, description="Maximum number of concurrent queries"
    )


class TenantUpdate(BaseModel):
    """Schema for updating tenant quotas."""

    max_storage_gb: int | None = Field(
        default=None, ge=1, description="Maximum storage quota in gigabytes"
    )
    max_query_memory_gb: int | None = Field(
        default=None, ge=1, description="Maximum query memory in gigabytes"
    )
    max_concurrent_queries: int | None = Field(
        default=None, ge=1, description="Maximum number of concurrent queries"
    )


class TenantResponse(BaseModel):
    """Schema for tenant response."""

    tenant_id: str = Field(..., description="Unique tenant identifier")
    name: str = Field(..., description="Tenant name")
    storage_backend: str = Field(..., description="Storage backend type")
    ducklake_catalog_url: str = Field(..., description="DuckLake catalog URL")
    storage_config: dict[str, str] | None = Field(
        default=None, description="Storage backend configuration"
    )
    max_storage_gb: int = Field(..., description="Maximum storage quota")
    max_query_memory_gb: int = Field(..., description="Maximum query memory")
    max_concurrent_queries: int = Field(..., description="Maximum concurrent queries")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    model_config = ConfigDict(from_attributes=True)


class TenantCreateResponse(BaseModel):
    """Schema for tenant creation response with API key."""

    tenant: TenantResponse = Field(..., description="Created tenant details")
    api_key: str = Field(..., description="Plain text API key (only shown once)")


class TenantListResponse(BaseModel):
    """Schema for paginated tenant list response."""

    tenants: list[TenantResponse] = Field(..., description="List of tenants")
    total: int = Field(..., description="Total number of tenants")
    offset: int = Field(..., description="Current offset")
    limit: int = Field(..., description="Current limit")
