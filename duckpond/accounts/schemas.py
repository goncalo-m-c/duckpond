"""Pydantic schemas for account operations."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class AccountCreate(BaseModel):
    """Schema for creating a new account."""

    name: str = Field(..., min_length=1, max_length=255, description="Unique account name")
    storage_backend: Literal["local", "s3"] = Field(
        default="local", description="Storage backend type"
    )
    storage_config: dict[str, str] = Field(
        default_factory=dict, description="Storage backend configuration"
    )
    max_storage_gb: int = Field(default=100, ge=1, description="Maximum storage quota in gigabytes")
    max_query_memory_gb: int = Field(
        default=4, ge=1, description="Maximum query memory in gigabytes"
    )
    max_concurrent_queries: int = Field(
        default=10, ge=1, description="Maximum number of concurrent queries"
    )


class AccountUpdate(BaseModel):
    """Schema for updating account quotas."""

    max_storage_gb: int | None = Field(
        default=None, ge=1, description="Maximum storage quota in gigabytes"
    )
    max_query_memory_gb: int | None = Field(
        default=None, ge=1, description="Maximum query memory in gigabytes"
    )
    max_concurrent_queries: int | None = Field(
        default=None, ge=1, description="Maximum number of concurrent queries"
    )


class AccountResponse(BaseModel):
    """Schema for account response."""

    account_id: str = Field(..., description="Unique account identifier")
    name: str = Field(..., description="Account name")
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


class AccountCreateResponse(BaseModel):
    """Schema for account creation response with API key."""

    account: AccountResponse = Field(..., description="Created account details")
    api_key: str = Field(..., description="Plain text API key (only shown once)")


class AccountListResponse(BaseModel):
    """Schema for paginated account list response."""

    accounts: list[AccountResponse] = Field(..., description="List of accounts")
    total: int = Field(..., description="Total number of accounts")
    offset: int = Field(..., description="Current offset")
    limit: int = Field(..., description="Current limit")


# Backward compatibility aliases (deprecated)
AccountCreate = AccountCreate
AccountUpdate = AccountUpdate
AccountResponse = AccountResponse
AccountCreateResponse = AccountCreateResponse
AccountListResponse = AccountListResponse
