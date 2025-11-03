"""Storage backend utility functions and helpers."""

from pathlib import Path
from typing import Optional

import structlog

from duckpond.accounts.models import Account
from duckpond.exceptions import StorageBackendError
from duckpond.storage.backend import StorageBackend
from duckpond.storage.local_backend import LocalBackend
from duckpond.storage.s3_backend import S3Backend

logger = structlog.get_logger()

_storage_backend_cache: dict[str, StorageBackend] = {}


def get_storage_backend(
    backend_type: str,
    config: Optional[dict] = None,
) -> StorageBackend:
    """
    Get storage backend instance by type and configuration.

    Factory function that creates appropriate storage backend
    based on the backend type string.

    Args:
        backend_type: Storage backend type (local, s3)
        config: Storage backend configuration dictionary

    Returns:
        StorageBackend instance

    Raises:
        ValueError: If backend_type is unsupported
        StorageBackendError: If backend initialization fails

    Examples:
        backend = get_storage_backend("local", {"base_path": "/data"})

        backend = get_storage_backend("s3", {
            "bucket": "my-bucket",
            "region": "us-east-1"
        })
    """
    config = config or {}

    try:
        if backend_type == "local":
            base_path = config.get("base_path", "./data")
            return LocalBackend(base_path=Path(base_path))

        elif backend_type == "s3":
            bucket = config.get("bucket")
            if not bucket:
                raise ValueError("S3 backend requires 'bucket' in config")

            return S3Backend(
                bucket=bucket,
                region=config.get("region", "us-east-1"),
                endpoint_url=config.get("endpoint_url"),
                aws_access_key_id=config.get("aws_access_key_id"),
                aws_secret_access_key=config.get("aws_secret_access_key"),
            )

        else:
            raise ValueError(
                f"Unsupported storage backend: {backend_type}. Supported types: local, s3"
            )

    except Exception as e:
        logger.error("Failed to create storage backend", backend_type=backend_type, error=str(e))
        raise StorageBackendError(
            backend_type, "init", f"Failed to initialize storage backend: {str(e)}"
        ) from e


def get_storage_backend_for_account(
    account: Account,
    cache: bool = True,
) -> StorageBackend:
    """
    Get storage backend instance for a account.

    Retrieves the appropriate storage backend based on account configuration.
    Caches backend instances by account_id for performance.

    Args:
        account: Account object with storage configuration
        cache: Whether to use cached backend instance (default True)

    Returns:
        StorageBackend instance configured for account

    Raises:
        StorageBackendError: If backend creation fails

    Examples:
        account = await manager.get_account_by_id("account-123")
        backend = get_storage_backend_for_account(account)

        await backend.upload_file(
            local_path="/tmp/file.txt",
            remote_key="data/file.txt",
            account_id=account.account_id
        )
    """
    if cache and account.account_id in _storage_backend_cache:
        return _storage_backend_cache[account.account_id]

    backend = get_storage_backend(
        backend_type=account.storage_backend, config=account.storage_config or {}
    )

    if cache:
        _storage_backend_cache[account.account_id] = backend

    logger.debug(
        "Created storage backend for account",
        account_id=account.account_id,
        backend_type=account.storage_backend,
        cached=cache,
    )

    return backend


def clear_storage_backend_cache(account_id: Optional[str] = None) -> None:
    """
    Clear storage backend cache.

    Useful when account configuration changes or for testing.

    Args:
        account_id: Specific account ID to clear. If None, clears entire cache.

    Examples:
        clear_storage_backend_cache("account-123")

        clear_storage_backend_cache()
    """
    if account_id:
        _storage_backend_cache.pop(account_id, None)
        logger.debug("Cleared storage backend cache", account_id=account_id)
    else:
        _storage_backend_cache.clear()
        logger.debug("Cleared all storage backend cache")


async def calculate_account_storage_usage(
    account: Account,
    storage_backend: Optional[StorageBackend] = None,
) -> int:
    """
    Calculate total storage usage for a account in bytes.

    Sums up file sizes for all files under the account's prefix.

    Args:
        account: Account object
        storage_backend: Optional pre-initialized storage backend.
                        If None, will create one from account config.

    Returns:
        Total storage usage in bytes

    Raises:
        StorageBackendError: If storage operations fail

    Examples:
        account = await manager.get_account_by_id("account-123")

        usage_bytes = await calculate_account_storage_usage(account)
        usage_gb = usage_bytes / (1024 ** 3)

        print(f"Account uses {usage_gb:.2f} GB")
    """
    if storage_backend is None:
        storage_backend = get_storage_backend_for_account(account)

    try:
        usage = await storage_backend.get_storage_usage(account.account_id)

        logger.info(
            "Calculated account storage usage",
            account_id=account.account_id,
            usage_bytes=usage,
            usage_gb=round(usage / (1024**3), 2),
        )

        return usage

    except Exception as e:
        logger.error(
            "Failed to calculate storage usage",
            account_id=account.account_id,
            error=str(e),
        )
        raise StorageBackendError(
            account.storage_backend,
            "calculate_usage",
            f"Failed to calculate storage usage: {str(e)}",
        ) from e


async def check_storage_connectivity(
    account: Account,
    storage_backend: Optional[StorageBackend] = None,
) -> tuple[bool, Optional[str]]:
    """
    Check storage backend connectivity for a account.

    Performs a basic connectivity test to ensure the storage backend
    is accessible and properly configured.

    Args:
        account: Account object
        storage_backend: Optional pre-initialized storage backend.
                        If None, will create one from account config.

    Returns:
        Tuple of (success: bool, error_message: Optional[str])

    Examples:
        account = await manager.get_account_by_id("account-123")

        success, error = await check_storage_connectivity(account)
        if success:
            print("Storage is accessible")
        else:
            print(f"Storage error: {error}")
    """
    if storage_backend is None:
        try:
            storage_backend = get_storage_backend_for_account(account)
        except Exception as e:
            return False, f"Failed to create storage backend: {str(e)}"

    try:
        await storage_backend.list_files(prefix="", account_id=account.account_id, recursive=False)

        logger.info(
            "Storage connectivity check passed",
            account_id=account.account_id,
            backend_type=account.storage_backend,
        )

        return True, None

    except Exception as e:
        error_msg = f"Storage connectivity check failed: {str(e)}"
        logger.warning(
            "Storage connectivity check failed",
            account_id=account.account_id,
            backend_type=account.storage_backend,
            error=str(e),
        )
        return False, error_msg


async def validate_storage_config(
    backend_type: str,
    config: dict,
) -> tuple[bool, Optional[str]]:
    """
    Validate storage backend configuration.

    Checks that required configuration parameters are present
    and valid for the specified backend type.

    Args:
        backend_type: Storage backend type (local, s3)
        config: Configuration dictionary to validate

    Returns:
        Tuple of (valid: bool, error_message: Optional[str])

    Examples:
        config = {"bucket": "my-bucket", "region": "us-east-1"}
        valid, error = await validate_storage_config("s3", config)

        if not valid:
            print(f"Invalid config: {error}")
    """
    try:
        if backend_type == "local":
            return True, None

        elif backend_type == "s3":
            if not config.get("bucket"):
                return False, "S3 backend requires 'bucket' parameter"

            bucket = config["bucket"]
            if not bucket or not isinstance(bucket, str):
                return False, "S3 bucket name must be a non-empty string"

            region = config.get("region", "us-east-1")
            if not isinstance(region, str):
                return False, "S3 region must be a string"

            return True, None

        else:
            return False, f"Unsupported backend type: {backend_type}"

    except Exception as e:
        return False, f"Configuration validation error: {str(e)}"


def format_storage_size(bytes_value: int) -> str:
    """
    Format byte size as human-readable string.

    Args:
        bytes_value: Size in bytes

    Returns:
        Formatted string (e.g., "1.5 GB", "512 MB")

    Examples:
        >>> format_storage_size(1024)
        '1.0 KB'
        >>> format_storage_size(1536 * 1024 * 1024)
        '1.5 GB'
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"
