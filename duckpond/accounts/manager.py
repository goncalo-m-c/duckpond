"""Account lifecycle management implementation."""

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
    get_storage_backend_for_account,
    validate_storage_config,
)
from duckpond.accounts.auth import get_authenticator
from duckpond.accounts.models import APIKey, Account
from duckpond.catalog.manager import create_catalog_manager

logger = structlog.get_logger()


class AccountManagerError(DuckPondError):
    """Base exception for account manager errors."""

    pass


class AccountAlreadyExistsError(AccountManagerError):
    """Account with given name already exists."""

    pass


class AccountNotFoundError(AccountManagerError):
    """Account not found."""

    pass


class APIKeyNotFoundError(AccountManagerError):
    """API key not found."""

    pass


class AccountManager:
    """
    Manages account lifecycle and operations.

    Handles:
    - Account creation with DuckLake catalog initialization
    - Account retrieval and listing
    - Quota updates
    - Account deletion with optional data purge
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize AccountManager with database session.

        Args:
            session: Async SQLAlchemy session for database operations
        """
        self.session = session
        self.settings = get_settings()

    async def create_account(
        self,
        name: str,
        storage_backend: str = "local",
        storage_config: Optional[dict[str, str]] = None,
        max_storage_gb: int = 100,
        max_query_memory_gb: int = 4,
        max_concurrent_queries: int = 10,
    ) -> tuple[Account, str]:
        """
        Create new account with DuckLake catalog.

        Args:
            name: Unique account name
            storage_backend: Storage backend type (local, s3)
            storage_config: Storage backend configuration dict
            max_storage_gb: Maximum storage quota in GB
            max_query_memory_gb: Maximum query memory in GB
            max_concurrent_queries: Maximum concurrent queries

        Returns:
            Tuple of (Account object, plain text API key)

        Raises:
            AccountAlreadyExistsError: If account name already exists
            AccountManagerError: If account creation fails
        """
        logger.info("Creating account", account_name=name)

        existing = await self._get_account_by_name(name)
        if existing:
            raise AccountAlreadyExistsError(
                f"Account with name '{name}' already exists",
                context={"account_name": name},
            )

        valid, error = await validate_storage_config(
            storage_backend, storage_config or {}
        )
        if not valid:
            raise AccountManagerError(
                f"Invalid storage configuration: {error}",
                context={"account_name": name, "storage_backend": storage_backend},
            )

        try:
            account_id = await self._generate_account_id(name)
            api_key = secrets.token_urlsafe(32)
            api_key_hash = bcrypt.hashpw(api_key.encode(), bcrypt.gensalt()).decode()
            key_prefix = api_key[:8]
            key_id = f"key-{secrets.token_urlsafe(16)}"

            logger.debug(
                "Generated account credentials",
                account_id=account_id,
                api_key_length=len(api_key),
            )

            catalog_manager = await create_catalog_manager(account_id)
            logger.debug(
                "Created DuckLake catalog", catalog_url=catalog_manager.catalog_url
            )

            data_dirs = await self._create_data_dirs(account_id)
            logger.debug("Created data directories", data_dirs=data_dirs)

            account = Account(
                account_id=account_id,
                name=name,
                api_key_hash=api_key_hash,
                ducklake_catalog_url=str(catalog_manager.catalog_url),
                storage_backend=storage_backend,
                storage_config=storage_config or {},
                max_storage_gb=max_storage_gb,
                max_query_memory_gb=max_query_memory_gb,
                max_concurrent_queries=max_concurrent_queries,
            )

            self.session.add(account)
            await self.session.flush()

            api_key_obj = APIKey(
                key_id=key_id,
                account_id=account_id,
                key_prefix=key_prefix,
                key_hash=api_key_hash,
                description="Initial API key created with account",
                expires_at=None,
            )

            self.session.add(api_key_obj)
            await self.session.flush()

            logger.info(
                "Account created successfully",
                account_id=account_id,
                account_name=name,
                storage_backend=storage_backend,
            )

            return account, api_key

        except AccountAlreadyExistsError:
            raise
        except Exception as e:
            logger.error("Failed to create account", error=str(e), account_name=name)
            raise AccountManagerError(
                f"Failed to create account: {str(e)}",
                context={"account_name": name, "error": str(e)},
            ) from e

    async def get_account(self, account_id: str) -> Optional[Account]:
        """
        Retrieve account by ID.

        Args:
            account_id: Unique account identifier

        Returns:
            Account object if found, None otherwise
        """
        logger.debug("Retrieving account", account_id=account_id)

        stmt = select(Account).where(Account.account_id == f"{account_id}")
        result = await self.session.execute(stmt)
        account = result.scalar_one_or_none()

        if account:
            logger.debug("Account found", account_id=account_id)
        else:
            logger.debug("Account not found", account_id=account_id)

        return account

    async def get_account_by_id(self, account_id: str) -> Account:
        """
        Retrieve account by ID, raising exception if not found.

        Args:
            account_id: Unique account identifier

        Returns:
            Account object

        Raises:
            AccountNotFoundError: If account not found
        """
        account = await self.get_account(account_id)
        if not account:
            raise AccountNotFoundError(
                f"Account not found: {account_id}", context={"account_id": account_id}
            )
        return account

    async def list_accounts(
        self,
        offset: int = 0,
        limit: int = 100,
    ) -> tuple[list[Account], int]:
        """
        List accounts with pagination.

        Args:
            offset: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            Tuple of (list of accounts, total count)
        """
        logger.debug("Listing accounts", offset=offset, limit=limit)

        count_stmt = select(func.count()).select_from(Account)
        count_result = await self.session.execute(count_stmt)
        total = count_result.scalar_one()

        stmt = (
            select(Account)
            .order_by(Account.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        accounts = list(result.scalars().all())

        logger.debug("Accounts retrieved", count=len(accounts), total=total)

        return accounts, total

    async def update_account_quotas(
        self,
        account_id: str,
        max_storage_gb: Optional[int] = None,
        max_query_memory_gb: Optional[int] = None,
        max_concurrent_queries: Optional[int] = None,
    ) -> Account:
        """
        Update account resource quotas.

        Args:
            account_id: Unique account identifier
            max_storage_gb: New storage quota (optional)
            max_query_memory_gb: New query memory quota (optional)
            max_concurrent_queries: New concurrent query limit (optional)

        Returns:
            Updated Account object

        Raises:
            AccountNotFoundError: If account not found
        """
        logger.info("Updating account quotas", account_id=account_id)

        account = await self.get_account_by_id(account_id)

        if max_storage_gb is not None:
            account.max_storage_gb = max_storage_gb
        if max_query_memory_gb is not None:
            account.max_query_memory_gb = max_query_memory_gb
        if max_concurrent_queries is not None:
            account.max_concurrent_queries = max_concurrent_queries

        await self.session.flush()
        await self.session.refresh(account)

        logger.info(
            "Account quotas updated",
            account_id=account_id,
            max_storage_gb=account.max_storage_gb,
            max_query_memory_gb=account.max_query_memory_gb,
            max_concurrent_queries=account.max_concurrent_queries,
        )

        return account

    async def delete_account(
        self,
        account_id: str,
        purge_data: bool = False,
    ) -> None:
        """
        Delete account and optionally purge all data.

        Args:
            account_id: Unique account identifier
            purge_data: If True, purge catalog and data files

        Raises:
            AccountNotFoundError: If account not found
        """
        logger.info("Deleting account", account_id=account_id, purge_data=purge_data)

        account = await self.get_account_by_id(account_id)

        if purge_data:
            await self._purge_account_data(account)

        stmt = delete(Account).where(Account.account_id == account_id)
        await self.session.execute(stmt)

        clear_storage_backend_cache(account_id)

        get_authenticator().invalidate_account(account_id)

        logger.info("Account deleted", account_id=account_id)

    async def create_api_key(
        self,
        account_id: str,
        description: Optional[str] = None,
        expires_at: Optional[datetime] = None,
    ) -> tuple[APIKey, str]:
        """
        Create a new API key for a account.

        Args:
            account_id: Unique account identifier
            description: Optional description of API key purpose
            expires_at: Optional expiration datetime

        Returns:
            Tuple of (APIKey object, plain text API key)

        Raises:
            AccountNotFoundError: If account not found
        """
        logger.info("Creating API key", account_id=account_id, description=description)

        _ = await self.get_account_by_id(account_id)

        api_key = secrets.token_urlsafe(32)
        key_prefix = api_key[:8]
        key_hash = bcrypt.hashpw(api_key.encode(), bcrypt.gensalt(rounds=12)).decode()

        key_id = f"key-{secrets.token_urlsafe(16)}"

        api_key_obj = APIKey(
            key_id=key_id,
            account_id=account_id,
            key_prefix=key_prefix,
            key_hash=key_hash,
            description=description,
            expires_at=expires_at,
        )

        self.session.add(api_key_obj)
        await self.session.flush()
        await self.session.refresh(api_key_obj)

        logger.info(
            "API key created", account_id=account_id, key_id=key_id, expires_at=expires_at
        )

        return api_key_obj, api_key

    async def revoke_api_key(
        self,
        key_id: str,
        account_id: Optional[str] = None,
    ) -> None:
        """
        Revoke (delete) an API key.

        Args:
            key_id: API key identifier to revoke
            account_id: Optional account identifier (for backward compatibility)

        Raises:
            APIKeyNotFoundError: If API key not found
        """
        logger.info("Revoking API key", key_id=key_id, account_id=account_id)

        if account_id:
            stmt = select(APIKey).where(
                APIKey.account_id == account_id, APIKey.key_id == key_id
            )
        else:
            stmt = select(APIKey).where(APIKey.key_id == key_id)

        result = await self.session.execute(stmt)
        api_key = result.scalar_one_or_none()

        if not api_key:
            raise APIKeyNotFoundError(
                f"API key not found: {key_id}",
                context={"account_id": account_id, "key_id": key_id},
            )

        actual_account_id = api_key.account_id

        stmt = delete(APIKey).where(APIKey.key_id == key_id)
        await self.session.execute(stmt)

        get_authenticator().invalidate_account(actual_account_id)

        logger.info("API key revoked", account_id=actual_account_id, key_id=key_id)

    async def list_api_keys(
        self,
        account_id: str,
        include_expired: bool = False,
    ) -> list[APIKey]:
        """
        List all API keys for a account.

        Args:
            account_id: Unique account identifier
            include_expired: If False, exclude expired keys (default False)

        Returns:
            List of APIKey objects

        Raises:
            AccountNotFoundError: If account not found
        """
        logger.debug(
            "Listing API keys", account_id=account_id, include_expired=include_expired
        )

        await self.get_account_by_id(account_id)

        stmt = select(APIKey).where(APIKey.account_id == account_id)

        if not include_expired:
            now = datetime.now(timezone.utc)
            stmt = stmt.where((APIKey.expires_at.is_(None)) | (APIKey.expires_at > now))

        stmt = stmt.order_by(APIKey.created_at.desc())

        result = await self.session.execute(stmt)
        api_keys = list(result.scalars().all())

        logger.debug("API keys retrieved", account_id=account_id, count=len(api_keys))

        return api_keys

    async def get_api_key(
        self,
        account_id: str,
        key_id: str,
    ) -> APIKey:
        """
        Get specific API key by ID.

        Args:
            account_id: Unique account identifier
            key_id: API key identifier

        Returns:
            APIKey object

        Raises:
            AccountNotFoundError: If account not found
            APIKeyNotFoundError: If API key not found
        """
        logger.debug("Getting API key", account_id=account_id, key_id=key_id)

        await self.get_account_by_id(account_id)

        stmt = select(APIKey).where(
            APIKey.account_id == account_id, APIKey.key_id == key_id
        )
        result = await self.session.execute(stmt)
        api_key = result.scalar_one_or_none()

        if not api_key:
            raise APIKeyNotFoundError(
                f"API key not found: {key_id}",
                context={"account_id": account_id, "key_id": key_id},
            )

        return api_key

    async def calculate_storage_usage(
        self,
        account_id: str,
    ) -> int:
        """
        Calculate total storage usage for a account in bytes.

        This scans all files in the account's storage and sums their sizes.

        Args:
            account_id: Unique account identifier

        Returns:
            Total storage usage in bytes

        Raises:
            AccountNotFoundError: If account not found
            StorageBackendError: If storage operations fail

        Examples:
            usage_bytes = await manager.calculate_storage_usage("account-123")
            usage_gb = usage_bytes / (1024 ** 3)
        """
        from duckpond.storage.utils import calculate_account_storage_usage

        logger.info("Calculating storage usage", account_id=account_id)

        account = await self.get_account_by_id(account_id)

        usage = await calculate_account_storage_usage(account)

        logger.info(
            "Storage usage calculated",
            account_id=account_id,
            usage_bytes=usage,
            usage_gb=round(usage / (1024**3), 2),
        )

        return usage

    async def _get_account_by_name(self, name: str) -> Optional[Account]:
        """Get account by name."""
        stmt = select(Account).where(Account.name == name)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def _generate_account_id(self, name: str) -> str:
        """
        Generate unique account ID from name.

        Args:
            name: Account name

        Returns:
            Unique account ID with format {slug}
        """
        base_slug = slugify(name, max_length=50)
        account_id = f"{base_slug}"

        counter = 1
        while await self._account_id_exists(account_id):
            account_id = f"{base_slug}-{counter}"
            counter += 1

        return account_id

    async def _account_id_exists(self, account_id: str) -> bool:
        """Check if account ID already exists."""
        stmt = (
            select(func.count())
            .select_from(Account)
            .where(Account.account_id == account_id)
        )
        result = await self.session.execute(stmt)
        count = result.scalar_one()
        return count > 0

    async def _create_data_dirs(self, account_id: str) -> str:
        """
        Create data directories for account.

        Args:
            account_id: Unique account identifier

        Returns:
            list of data directories

        Raises:
            AccountManagerError: If catalog creation fails
        """
        try:
            streams_dir = (
                self.settings.local_storage_path / "accounts" / account_id / "streams"
            )
            streams_dir.mkdir(parents=True, exist_ok=True)
            streams_dir_path = str(streams_dir)

            tables_dir = (
                self.settings.local_storage_path / "accounts" / account_id / "tables"
            )
            tables_dir.mkdir(parents=True, exist_ok=True)
            tables_dir_path = str(tables_dir)

            logger.info(
                "Data dirs created",
                account_id=account_id,
                streams_dir_path=streams_dir_path,
                tables_dir_path=tables_dir_path,
            )

            return streams_dir_path, tables_dir_path

        except Exception as e:
            logger.error("Failed to create DuckLake catalog", error=str(e))
            raise AccountManagerError(
                f"Failed to create DuckLake catalog: {str(e)}",
                context={"account_id": account_id, "error": str(e)},
            ) from e

    async def _purge_account_data(self, account: Account) -> None:
        """
        Purge account's catalog and data files.

        Args:
            account: Account object
        """
        try:
            catalog_url = account.ducklake_catalog_url

            if not catalog_url.startswith("postgresql"):
                catalog_path = Path(catalog_url)
                if catalog_path.exists():
                    catalog_path.unlink()
                    logger.debug("Deleted catalog file", path=str(catalog_path))

            try:
                backend = get_storage_backend_for_account(account, cache=False)

                files = await backend.list_files(
                    prefix="", account_id=account.account_id, recursive=True
                )

                deleted_count = 0
                for file_path in files:
                    try:
                        await backend.delete_file(file_path, account_id=account.account_id)
                        deleted_count += 1
                    except Exception as e:
                        logger.warning(
                            "Failed to delete file during purge",
                            file=file_path,
                            error=str(e),
                        )

                logger.info(
                    "Purged account data files",
                    account_id=account.account_id,
                    files_deleted=deleted_count,
                )
            except Exception as e:
                logger.warning(
                    "Failed to purge data files from storage backend",
                    error=str(e),
                    account_id=account.account_id,
                )

            clear_storage_backend_cache(account.account_id)

            logger.info("Purged account data", account_id=account.account_id)

        except Exception as e:
            logger.warning(
                "Failed to purge account data", error=str(e), account_id=account.account_id
            )
