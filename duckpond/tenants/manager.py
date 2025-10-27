"""Tenant lifecycle management implementation."""

import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import bcrypt
import structlog
from slugify import slugify
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from duckpond.config import get_settings
from duckpond.exceptions import DuckPondError
from duckpond.storage.utils import (
    clear_storage_backend_cache,
    get_storage_backend_for_tenant,
    validate_storage_config,
)
from duckpond.tenants.auth import get_authenticator
from duckpond.tenants.models import APIKey, Tenant
from duckpond.catalog.manager import create_catalog_manager

logger = structlog.get_logger()


class TenantManagerError(DuckPondError):
    """Base exception for tenant manager errors."""

    pass


class TenantAlreadyExistsError(TenantManagerError):
    """Tenant with given name already exists."""

    pass


class TenantNotFoundError(TenantManagerError):
    """Tenant not found."""

    pass


class APIKeyNotFoundError(TenantManagerError):
    """API key not found."""

    pass


class TenantManager:
    """
    Manages tenant lifecycle and operations.

    Handles:
    - Tenant creation with DuckLake catalog initialization
    - Tenant retrieval and listing
    - Quota updates
    - Tenant deletion with optional data purge
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize TenantManager with database session.

        Args:
            session: Async SQLAlchemy session for database operations
        """
        self.session = session
        self.settings = get_settings()

    async def create_tenant(
        self,
        name: str,
        storage_backend: str = "local",
        storage_config: Optional[dict[str, str]] = None,
        max_storage_gb: int = 100,
        max_query_memory_gb: int = 4,
        max_concurrent_queries: int = 10,
    ) -> tuple[Tenant, str]:
        """
        Create new tenant with DuckLake catalog.

        Args:
            name: Unique tenant name
            storage_backend: Storage backend type (local, s3)
            storage_config: Storage backend configuration dict
            max_storage_gb: Maximum storage quota in GB
            max_query_memory_gb: Maximum query memory in GB
            max_concurrent_queries: Maximum concurrent queries

        Returns:
            Tuple of (Tenant object, plain text API key)

        Raises:
            TenantAlreadyExistsError: If tenant name already exists
            TenantManagerError: If tenant creation fails
        """
        logger.info("Creating tenant", tenant_name=name)

        existing = await self._get_tenant_by_name(name)
        if existing:
            raise TenantAlreadyExistsError(
                f"Tenant with name '{name}' already exists",
                context={"tenant_name": name},
            )

        valid, error = await validate_storage_config(
            storage_backend, storage_config or {}
        )
        if not valid:
            raise TenantManagerError(
                f"Invalid storage configuration: {error}",
                context={"tenant_name": name, "storage_backend": storage_backend},
            )

        try:
            tenant_id = await self._generate_tenant_id(name)
            api_key = secrets.token_urlsafe(32)
            api_key_hash = bcrypt.hashpw(api_key.encode(), bcrypt.gensalt()).decode()
            key_prefix = api_key[:8]
            key_id = f"key-{secrets.token_urlsafe(16)}"

            logger.debug(
                "Generated tenant credentials",
                tenant_id=tenant_id,
                api_key_length=len(api_key),
            )

            catalog_manager = await create_catalog_manager(tenant_id)
            logger.debug(
                "Created DuckLake catalog", catalog_url=catalog_manager.catalog_url
            )

            data_dirs = await self._create_data_dirs(tenant_id)
            logger.debug("Created data directories", data_dirs=data_dirs)

            tenant = Tenant(
                tenant_id=tenant_id,
                name=name,
                api_key_hash=api_key_hash,
                ducklake_catalog_url=str(catalog_manager.catalog_url),
                storage_backend=storage_backend,
                storage_config=storage_config or {},
                max_storage_gb=max_storage_gb,
                max_query_memory_gb=max_query_memory_gb,
                max_concurrent_queries=max_concurrent_queries,
            )

            self.session.add(tenant)
            await self.session.flush()

            api_key_obj = APIKey(
                key_id=key_id,
                tenant_id=tenant_id,
                key_prefix=key_prefix,
                key_hash=api_key_hash,
                description="Initial API key created with tenant",
                expires_at=None,
            )

            self.session.add(api_key_obj)
            await self.session.flush()

            logger.info(
                "Tenant created successfully",
                tenant_id=tenant_id,
                tenant_name=name,
                storage_backend=storage_backend,
            )

            return tenant, api_key

        except TenantAlreadyExistsError:
            raise
        except Exception as e:
            logger.error("Failed to create tenant", error=str(e), tenant_name=name)
            raise TenantManagerError(
                f"Failed to create tenant: {str(e)}",
                context={"tenant_name": name, "error": str(e)},
            ) from e

    async def get_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """
        Retrieve tenant by ID.

        Args:
            tenant_id: Unique tenant identifier

        Returns:
            Tenant object if found, None otherwise
        """
        logger.debug("Retrieving tenant", tenant_id=tenant_id)

        stmt = select(Tenant).where(Tenant.tenant_id == f"{tenant_id}")
        result = await self.session.execute(stmt)
        tenant = result.scalar_one_or_none()

        if tenant:
            logger.debug("Tenant found", tenant_id=tenant_id)
        else:
            logger.debug("Tenant not found", tenant_id=tenant_id)

        return tenant

    async def get_tenant_by_id(self, tenant_id: str) -> Tenant:
        """
        Retrieve tenant by ID, raising exception if not found.

        Args:
            tenant_id: Unique tenant identifier

        Returns:
            Tenant object

        Raises:
            TenantNotFoundError: If tenant not found
        """
        tenant = await self.get_tenant(tenant_id)
        if not tenant:
            raise TenantNotFoundError(
                f"Tenant not found: {tenant_id}", context={"tenant_id": tenant_id}
            )
        return tenant

    async def list_tenants(
        self,
        offset: int = 0,
        limit: int = 100,
    ) -> tuple[list[Tenant], int]:
        """
        List tenants with pagination.

        Args:
            offset: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            Tuple of (list of tenants, total count)
        """
        logger.debug("Listing tenants", offset=offset, limit=limit)

        count_stmt = select(func.count()).select_from(Tenant)
        count_result = await self.session.execute(count_stmt)
        total = count_result.scalar_one()

        stmt = (
            select(Tenant)
            .order_by(Tenant.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        tenants = list(result.scalars().all())

        logger.debug("Tenants retrieved", count=len(tenants), total=total)

        return tenants, total

    async def update_tenant_quotas(
        self,
        tenant_id: str,
        max_storage_gb: Optional[int] = None,
        max_query_memory_gb: Optional[int] = None,
        max_concurrent_queries: Optional[int] = None,
    ) -> Tenant:
        """
        Update tenant resource quotas.

        Args:
            tenant_id: Unique tenant identifier
            max_storage_gb: New storage quota (optional)
            max_query_memory_gb: New query memory quota (optional)
            max_concurrent_queries: New concurrent query limit (optional)

        Returns:
            Updated Tenant object

        Raises:
            TenantNotFoundError: If tenant not found
        """
        logger.info("Updating tenant quotas", tenant_id=tenant_id)

        tenant = await self.get_tenant_by_id(tenant_id)

        if max_storage_gb is not None:
            tenant.max_storage_gb = max_storage_gb
        if max_query_memory_gb is not None:
            tenant.max_query_memory_gb = max_query_memory_gb
        if max_concurrent_queries is not None:
            tenant.max_concurrent_queries = max_concurrent_queries

        await self.session.flush()
        await self.session.refresh(tenant)

        logger.info(
            "Tenant quotas updated",
            tenant_id=tenant_id,
            max_storage_gb=tenant.max_storage_gb,
            max_query_memory_gb=tenant.max_query_memory_gb,
            max_concurrent_queries=tenant.max_concurrent_queries,
        )

        return tenant

    async def delete_tenant(
        self,
        tenant_id: str,
        purge_data: bool = False,
    ) -> None:
        """
        Delete tenant and optionally purge all data.

        Args:
            tenant_id: Unique tenant identifier
            purge_data: If True, purge catalog and data files

        Raises:
            TenantNotFoundError: If tenant not found
        """
        logger.info("Deleting tenant", tenant_id=tenant_id, purge_data=purge_data)

        tenant = await self.get_tenant_by_id(tenant_id)

        if purge_data:
            await self._purge_tenant_data(tenant)

        stmt = delete(Tenant).where(Tenant.tenant_id == tenant_id)
        await self.session.execute(stmt)

        clear_storage_backend_cache(tenant_id)

        get_authenticator().invalidate_tenant(tenant_id)

        logger.info("Tenant deleted", tenant_id=tenant_id)

    async def create_api_key(
        self,
        tenant_id: str,
        description: Optional[str] = None,
        expires_at: Optional[datetime] = None,
    ) -> tuple[APIKey, str]:
        """
        Create a new API key for a tenant.

        Args:
            tenant_id: Unique tenant identifier
            description: Optional description of API key purpose
            expires_at: Optional expiration datetime

        Returns:
            Tuple of (APIKey object, plain text API key)

        Raises:
            TenantNotFoundError: If tenant not found
        """
        logger.info("Creating API key", tenant_id=tenant_id, description=description)

        _ = await self.get_tenant_by_id(tenant_id)

        api_key = secrets.token_urlsafe(32)
        key_prefix = api_key[:8]
        key_hash = bcrypt.hashpw(api_key.encode(), bcrypt.gensalt(rounds=12)).decode()

        key_id = f"key-{secrets.token_urlsafe(16)}"

        api_key_obj = APIKey(
            key_id=key_id,
            tenant_id=tenant_id,
            key_prefix=key_prefix,
            key_hash=key_hash,
            description=description,
            expires_at=expires_at,
        )

        self.session.add(api_key_obj)
        await self.session.flush()
        await self.session.refresh(api_key_obj)

        logger.info(
            "API key created", tenant_id=tenant_id, key_id=key_id, expires_at=expires_at
        )

        return api_key_obj, api_key

    async def revoke_api_key(
        self,
        key_id: str,
        tenant_id: Optional[str] = None,
    ) -> None:
        """
        Revoke (delete) an API key.

        Args:
            key_id: API key identifier to revoke
            tenant_id: Optional tenant identifier (for backward compatibility)

        Raises:
            APIKeyNotFoundError: If API key not found
        """
        logger.info("Revoking API key", key_id=key_id, tenant_id=tenant_id)

        if tenant_id:
            stmt = select(APIKey).where(
                APIKey.tenant_id == tenant_id, APIKey.key_id == key_id
            )
        else:
            stmt = select(APIKey).where(APIKey.key_id == key_id)

        result = await self.session.execute(stmt)
        api_key = result.scalar_one_or_none()

        if not api_key:
            raise APIKeyNotFoundError(
                f"API key not found: {key_id}",
                context={"tenant_id": tenant_id, "key_id": key_id},
            )

        actual_tenant_id = api_key.tenant_id

        stmt = delete(APIKey).where(APIKey.key_id == key_id)
        await self.session.execute(stmt)

        get_authenticator().invalidate_tenant(actual_tenant_id)

        logger.info("API key revoked", tenant_id=actual_tenant_id, key_id=key_id)

    async def list_api_keys(
        self,
        tenant_id: str,
        include_expired: bool = False,
    ) -> list[APIKey]:
        """
        List all API keys for a tenant.

        Args:
            tenant_id: Unique tenant identifier
            include_expired: If False, exclude expired keys (default False)

        Returns:
            List of APIKey objects

        Raises:
            TenantNotFoundError: If tenant not found
        """
        logger.debug(
            "Listing API keys", tenant_id=tenant_id, include_expired=include_expired
        )

        await self.get_tenant_by_id(tenant_id)

        stmt = select(APIKey).where(APIKey.tenant_id == tenant_id)

        if not include_expired:
            now = datetime.now(timezone.utc)
            stmt = stmt.where((APIKey.expires_at.is_(None)) | (APIKey.expires_at > now))

        stmt = stmt.order_by(APIKey.created_at.desc())

        result = await self.session.execute(stmt)
        api_keys = list(result.scalars().all())

        logger.debug("API keys retrieved", tenant_id=tenant_id, count=len(api_keys))

        return api_keys

    async def get_api_key(
        self,
        tenant_id: str,
        key_id: str,
    ) -> APIKey:
        """
        Get specific API key by ID.

        Args:
            tenant_id: Unique tenant identifier
            key_id: API key identifier

        Returns:
            APIKey object

        Raises:
            TenantNotFoundError: If tenant not found
            APIKeyNotFoundError: If API key not found
        """
        logger.debug("Getting API key", tenant_id=tenant_id, key_id=key_id)

        await self.get_tenant_by_id(tenant_id)

        stmt = select(APIKey).where(
            APIKey.tenant_id == tenant_id, APIKey.key_id == key_id
        )
        result = await self.session.execute(stmt)
        api_key = result.scalar_one_or_none()

        if not api_key:
            raise APIKeyNotFoundError(
                f"API key not found: {key_id}",
                context={"tenant_id": tenant_id, "key_id": key_id},
            )

        return api_key

    async def calculate_storage_usage(
        self,
        tenant_id: str,
    ) -> int:
        """
        Calculate total storage usage for a tenant in bytes.

        This scans all files in the tenant's storage and sums their sizes.

        Args:
            tenant_id: Unique tenant identifier

        Returns:
            Total storage usage in bytes

        Raises:
            TenantNotFoundError: If tenant not found
            StorageBackendError: If storage operations fail

        Examples:
            usage_bytes = await manager.calculate_storage_usage("tenant-123")
            usage_gb = usage_bytes / (1024 ** 3)
        """
        from duckpond.storage.utils import calculate_tenant_storage_usage

        logger.info("Calculating storage usage", tenant_id=tenant_id)

        tenant = await self.get_tenant_by_id(tenant_id)

        usage = await calculate_tenant_storage_usage(tenant)

        logger.info(
            "Storage usage calculated",
            tenant_id=tenant_id,
            usage_bytes=usage,
            usage_gb=round(usage / (1024**3), 2),
        )

        return usage

    async def _get_tenant_by_name(self, name: str) -> Optional[Tenant]:
        """Get tenant by name."""
        stmt = select(Tenant).where(Tenant.name == name)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def _generate_tenant_id(self, name: str) -> str:
        """
        Generate unique tenant ID from name.

        Args:
            name: Tenant name

        Returns:
            Unique tenant ID with format {slug}
        """
        base_slug = slugify(name, max_length=50)
        tenant_id = f"{base_slug}"

        counter = 1
        while await self._tenant_id_exists(tenant_id):
            tenant_id = f"{base_slug}-{counter}"
            counter += 1

        return tenant_id

    async def _tenant_id_exists(self, tenant_id: str) -> bool:
        """Check if tenant ID already exists."""
        stmt = (
            select(func.count())
            .select_from(Tenant)
            .where(Tenant.tenant_id == tenant_id)
        )
        result = await self.session.execute(stmt)
        count = result.scalar_one()
        return count > 0

    async def _create_data_dirs(self, tenant_id: str) -> str:
        """
        Create data directories for tenant.

        Args:
            tenant_id: Unique tenant identifier

        Returns:
            list of data directories

        Raises:
            TenantManagerError: If catalog creation fails
        """
        try:
            streams_dir = (
                self.settings.local_storage_path / "tenants" / tenant_id / "streams"
            )
            streams_dir.mkdir(parents=True, exist_ok=True)
            streams_dir_path = str(streams_dir)

            tables_dir = (
                self.settings.local_storage_path / "tenants" / tenant_id / "tables"
            )
            tables_dir.mkdir(parents=True, exist_ok=True)
            tables_dir_path = str(tables_dir)

            logger.info(
                "Data dirs created",
                tenant_id=tenant_id,
                streams_dir_path=streams_dir_path,
                tables_dir_path=tables_dir_path,
            )

            return streams_dir_path, tables_dir_path

        except Exception as e:
            logger.error("Failed to create DuckLake catalog", error=str(e))
            raise TenantManagerError(
                f"Failed to create DuckLake catalog: {str(e)}",
                context={"tenant_id": tenant_id, "error": str(e)},
            ) from e

    async def _purge_tenant_data(self, tenant: Tenant) -> None:
        """
        Purge tenant's catalog and data files.

        Args:
            tenant: Tenant object
        """
        try:
            catalog_url = tenant.ducklake_catalog_url

            if not catalog_url.startswith("postgresql"):
                catalog_path = Path(catalog_url)
                if catalog_path.exists():
                    catalog_path.unlink()
                    logger.debug("Deleted catalog file", path=str(catalog_path))

            try:
                backend = get_storage_backend_for_tenant(tenant, cache=False)

                files = await backend.list_files(
                    prefix="", tenant_id=tenant.tenant_id, recursive=True
                )

                deleted_count = 0
                for file_path in files:
                    try:
                        await backend.delete_file(file_path, tenant_id=tenant.tenant_id)
                        deleted_count += 1
                    except Exception as e:
                        logger.warning(
                            "Failed to delete file during purge",
                            file=file_path,
                            error=str(e),
                        )

                logger.info(
                    "Purged tenant data files",
                    tenant_id=tenant.tenant_id,
                    files_deleted=deleted_count,
                )
            except Exception as e:
                logger.warning(
                    "Failed to purge data files from storage backend",
                    error=str(e),
                    tenant_id=tenant.tenant_id,
                )

            clear_storage_backend_cache(tenant.tenant_id)

            logger.info("Purged tenant data", tenant_id=tenant.tenant_id)

        except Exception as e:
            logger.warning(
                "Failed to purge tenant data", error=str(e), tenant_id=tenant.tenant_id
            )
