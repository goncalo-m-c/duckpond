"""SQLAlchemy ORM models for tenant management."""

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from duckpond.db.base import Base


class Tenant(Base):
    """
    Tenant model representing a DuckPond tenant with isolated resources.

    Each tenant has:
    - Unique identifier and name
    - API key authentication
    - DuckLake catalog configuration
    - Storage backend configuration
    - Resource quotas
    - Multiple API keys (one-to-many relationship)
    """

    __tablename__ = "tenants"

    tenant_id: Mapped[str] = mapped_column(
        String(64),
        primary_key=True,
        nullable=False,
        comment="Unique tenant identifier with format tenant-{slug}",
    )

    name: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False, comment="Unique tenant name"
    )

    api_key_hash: Mapped[str] = mapped_column(
        String(255), nullable=False, comment="Hashed API key for tenant authentication"
    )

    ducklake_catalog_url: Mapped[str] = mapped_column(
        String(512), nullable=False, comment="URL for DuckLake catalog REST API"
    )

    storage_backend: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="Storage backend type (s3, azure, gcs, local)",
    )

    storage_config: Mapped[Optional[dict]] = mapped_column(
        JSON, nullable=True, comment="JSON configuration for storage backend"
    )

    max_storage_gb: Mapped[int] = mapped_column(
        Integer,
        default=100,
        nullable=False,
        comment="Maximum storage quota in gigabytes",
    )

    max_query_memory_gb: Mapped[int] = mapped_column(
        Integer, default=4, nullable=False, comment="Maximum query memory in gigabytes"
    )

    max_concurrent_queries: Mapped[int] = mapped_column(
        Integer,
        default=10,
        nullable=False,
        comment="Maximum number of concurrent queries",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=func.now(),
        nullable=False,
        comment="Timestamp when tenant was created",
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=func.now(),
        onupdate=func.now(),
        nullable=False,
        comment="Timestamp when tenant was last updated",
    )

    api_keys: Mapped[list["APIKey"]] = relationship(
        "APIKey", back_populates="tenant", cascade="all, delete-orphan", lazy="selectin"
    )

    __table_args__ = (
        Index("idx_tenants_storage_backend", "storage_backend"),
        Index("idx_tenants_name", "name"),
    )

    def __repr__(self) -> str:
        """String representation of Tenant."""
        return (
            f"<Tenant(tenant_id='{self.tenant_id}', "
            f"name='{self.name}', "
            f"storage_backend='{self.storage_backend}')>"
        )


class APIKey(Base):
    """
    API Key model for tenant authentication.

    Each tenant can have multiple API keys with:
    - Unique key identifier and hash
    - Optional description
    - Expiration date
    - Last used timestamp
    - Foreign key relationship to tenant (with cascade delete)
    """

    __tablename__ = "api_keys"

    key_id: Mapped[str] = mapped_column(
        String(64),
        primary_key=True,
        nullable=False,
        comment="Unique API key identifier",
    )

    tenant_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("tenants.tenant_id", ondelete="CASCADE"),
        nullable=False,
        comment="Tenant this API key belongs to",
    )

    key_prefix: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        index=True,
        comment="First 8 characters of API key for quick lookup",
    )

    key_hash: Mapped[str] = mapped_column(
        String(255),
        unique=True,
        nullable=False,
        comment="Hashed API key for authentication",
    )

    description: Mapped[Optional[str]] = mapped_column(
        String(512), nullable=True, comment="Optional description of API key purpose"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=func.now(),
        nullable=False,
        comment="Timestamp when API key was created",
    )

    last_used: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True, comment="Timestamp when API key was last used"
    )

    expires_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True, comment="Timestamp when API key expires"
    )

    tenant: Mapped["Tenant"] = relationship(
        "Tenant", back_populates="api_keys", lazy="selectin"
    )

    __table_args__ = (
        Index("idx_api_keys_tenant", "tenant_id"),
        Index("idx_api_keys_hash", "key_hash"),
        Index("idx_api_keys_expires", "expires_at"),
    )

    def __repr__(self) -> str:
        """String representation of APIKey."""
        return (
            f"<APIKey(key_id='{self.key_id}', "
            f"tenant_id='{self.tenant_id}', "
            f"expires_at={self.expires_at})>"
        )


class TenantStatus:
    """Legacy tenant status constants for CLI compatibility."""

    ACTIVE = "active"
    SUSPENDED = "suspended"
    DELETED = "deleted"
