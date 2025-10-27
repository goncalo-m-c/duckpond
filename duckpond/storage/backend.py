"""Storage backend interface for DuckPond.

This module provides the abstract interface for storage backends with support
for local filesystem and S3-compatible storage.

Key Design Principles:
- Tenant isolation through automatic key prefixing ({tenant_id}/{key})
- All operations are async for consistent API
- Callers never specify tenant_id in keys (automatic prefixing)
- Support for both local filesystem and cloud storage patterns
- Presigned URLs only supported for S3-compatible backends
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class StorageBackend(ABC):
    """Abstract base class for storage backends.

    All concrete implementations must support tenant isolation through
    automatic key prefixing. The tenant_id is passed to each method
    but should never appear in the remote_key parameter - it's added
    automatically by the backend.

    Example:
        upload_file(local_path, "data/file.csv", "tenant-123")
        -> Actually stored at: tenant-123/data/file.csv
    """

    @abstractmethod
    async def upload_file(
        self,
        local_path: Path,
        remote_key: str,
        tenant_id: str,
        metadata: dict[str, str] | None = None,
        convert_to_parquet: bool = True,
    ) -> str:
        """Upload a file to storage.

        Args:
            local_path: Local file path to upload
            remote_key: Remote key WITHOUT tenant prefix (e.g., "data/file.csv")
            tenant_id: Tenant ID for automatic prefixing
            metadata: Optional metadata dictionary to attach to file
            convert_to_parquet: Whether to convert the file to Parquet format using DuckDB

        Returns:
            Full remote path WITH tenant prefix (e.g., "tenant-123/data/file.parquet")

        Raises:
            FileNotFoundError: If local file doesn't exist
            StorageBackendError: If upload fails
            InsufficientStorageError: If storage quota would be exceeded
        """
        pass

    @abstractmethod
    async def download_file(
        self,
        remote_key: str,
        local_path: Path,
        tenant_id: str,
    ) -> None:
        """Download a file from storage.

        Args:
            remote_key: Remote key WITHOUT tenant prefix
            local_path: Local file path to download to
            tenant_id: Tenant ID for automatic prefixing

        Raises:
            FileNotFoundError: If remote file doesn't exist
            StorageBackendError: If download fails
        """
        pass

    @abstractmethod
    async def delete_file(self, remote_key: str, tenant_id: str) -> None:
        """Delete a file from storage.

        Args:
            remote_key: Remote key WITHOUT tenant prefix
            tenant_id: Tenant ID for automatic prefixing

        Raises:
            FileNotFoundError: If file doesn't exist
            StorageBackendError: If deletion fails
        """
        pass

    @abstractmethod
    async def delete_prefix(self, tenant_id: str) -> int:
        """Delete all files with a tenant prefix.

        This is typically used when deleting a tenant to clean up all their data.

        Args:
            tenant_id: Tenant ID - all files matching this prefix will be deleted

        Returns:
            Number of files deleted

        Raises:
            StorageBackendError: If deletion fails
        """
        pass

    @abstractmethod
    async def list_files(
        self,
        prefix: str,
        tenant_id: str,
        recursive: bool = True,
    ) -> list[str]:
        """List files matching a prefix.

        Args:
            prefix: Prefix to match (WITHOUT tenant prefix)
            tenant_id: Tenant ID for automatic prefixing
            recursive: If True, list all files recursively; if False, only immediate children

        Returns:
            List of file paths WITHOUT tenant prefix (e.g., ["data/file1.csv", "data/file2.csv"])

        Raises:
            StorageBackendError: If listing fails
        """
        pass

    @abstractmethod
    async def file_exists(self, remote_key: str, tenant_id: str) -> bool:
        """Check if a file exists in storage.

        Args:
            remote_key: Remote key WITHOUT tenant prefix
            tenant_id: Tenant ID for automatic prefixing

        Returns:
            True if file exists, False otherwise

        Raises:
            StorageBackendError: If check fails
        """
        pass

    @abstractmethod
    async def get_file_size(self, remote_key: str, tenant_id: str) -> int:
        """Get file size in bytes.

        Args:
            remote_key: Remote key WITHOUT tenant prefix
            tenant_id: Tenant ID for automatic prefixing

        Returns:
            File size in bytes

        Raises:
            FileNotFoundError: If file doesn't exist
            StorageBackendError: If operation fails
        """
        pass

    @abstractmethod
    async def get_file_metadata(
        self, remote_key: str, tenant_id: str
    ) -> dict[str, Any]:
        """Get file metadata.

        Args:
            remote_key: Remote key WITHOUT tenant prefix
            tenant_id: Tenant ID for automatic prefixing

        Returns:
            Dictionary containing metadata (size, modified_time, content_type, etc.)

        Raises:
            FileNotFoundError: If file doesn't exist
            StorageBackendError: If operation fails
        """
        pass

    @abstractmethod
    async def get_storage_usage(self, tenant_id: str) -> int:
        """Calculate total storage usage for a tenant.

        Args:
            tenant_id: Tenant identifier

        Returns:
            Total storage usage in bytes

        Raises:
            StorageBackendError: If calculation fails
        """
        pass

    @abstractmethod
    async def generate_presigned_url(
        self,
        remote_key: str,
        tenant_id: str,
        expires_seconds: int = 3600,
        method: str = "GET",
    ) -> str:
        """Generate a presigned URL for direct file access.

        Note: This method is only supported for S3-compatible backends.
        Local filesystem backends will raise NotImplementedError.

        Args:
            remote_key: Remote key WITHOUT tenant prefix
            tenant_id: Tenant ID for automatic prefixing
            expires_seconds: URL expiration time in seconds (default: 1 hour)
            method: HTTP method (GET, PUT, etc.)

        Returns:
            Presigned URL string

        Raises:
            NotImplementedError: If backend doesn't support presigned URLs (e.g., local)
            FileNotFoundError: If file doesn't exist (for GET URLs)
            StorageBackendError: If URL generation fails
        """
        pass

    def _build_tenant_key(self, tenant_id: str, remote_key: str) -> str:
        """Build full storage key with tenant prefix.

        This is a helper method used internally by backends to construct
        the actual storage path with tenant isolation.

        Args:
            tenant_id: Tenant identifier
            remote_key: Key without tenant prefix

        Returns:
            Full key with tenant prefix: "{tenant_id}/{remote_key}"
        """
        remote_key = remote_key.lstrip("/")
        return f"{tenant_id}/{remote_key}"

    def _build_uploads_tenant_key(self, tenant_id: str, remote_key: str) -> str:
        """Build full uplaods storage key with tenant prefix.

        This is a helper method used internally by backends to construct
        the actual storage path with tenant isolation.

        Args:
            tenant_id: Tenant identifier
            remote_key: Key without tenant prefix

        Returns:
            Full key with tenant prefix: "{tenant_id}/{remote_key}"
        """
        remote_key = remote_key.lstrip("/")
        return f"{tenant_id}/uploads/{remote_key}"

    def _build_tables_tenant_key(self, tenant_id: str, remote_key: str) -> str:
        """Build full uplaods storage key with tenant prefix.

        This is a helper method used internally by backends to construct
        the actual storage path with tenant isolation.

        Args:
            tenant_id: Tenant identifier
            remote_key: Key without tenant prefix

        Returns:
            Full key with tenant prefix: "{tenant_id}/{remote_key}"
        """
        remote_key = remote_key.lstrip("/")
        return f"{tenant_id}/tables/{remote_key}"


class MockStorageBackend(StorageBackend):
    """Mock storage backend for testing and development.

    This implementation stores files in memory and is useful for:
    - Unit tests that don't need real storage
    - Development environments
    - Testing storage logic without external dependencies
    """

    def __init__(self) -> None:
        """Initialize mock storage backend."""
        self._files: dict[str, bytes] = {}
        self._metadata: dict[str, dict[str, Any]] = {}

    async def upload_file(
        self,
        local_path: Path,
        remote_key: str,
        tenant_id: str,
        metadata: dict[str, str] | None = None,
        convert_to_parquet: bool = True,
    ) -> str:
        """Upload file to mock storage."""
        from duckpond.exceptions import FileNotFoundError as DuckPondFileNotFoundError

        if not local_path.exists():
            raise DuckPondFileNotFoundError(str(local_path))

        final_remote_key = remote_key
        original_extension = local_path.suffix.lower()

        if convert_to_parquet and original_extension != ".parquet":
            if final_remote_key.endswith(original_extension):
                final_remote_key = (
                    final_remote_key[: -len(original_extension)] + ".parquet"
                )
            elif "." not in final_remote_key:
                final_remote_key = final_remote_key + ".parquet"
            else:
                final_remote_key = (
                    Path(final_remote_key).with_suffix(".parquet").as_posix()
                )

        full_key = self._build_tenant_key(tenant_id, final_remote_key)
        print(f"full_key: {full_key}")
        with open(local_path, "rb") as f:
            self._files[full_key] = f.read()

        self._metadata[full_key] = {
            "size": local_path.stat().st_size,
            "content_type": "application/octet-stream",
            "original_filename": local_path.name,
            "original_extension": original_extension,
            "converted_to_parquet": str(
                convert_to_parquet and original_extension != ".parquet"
            ),
            **(metadata or {}),
        }

        return full_key

    async def download_file(
        self, remote_key: str, local_path: Path, tenant_id: str
    ) -> None:
        """Download file from mock storage."""
        from duckpond.exceptions import FileNotFoundError as DuckPondFileNotFoundError

        full_key = self._build_tenant_key(tenant_id, remote_key)
        if full_key not in self._files:
            raise DuckPondFileNotFoundError(full_key)

        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(self._files[full_key])

    async def delete_file(self, remote_key: str, tenant_id: str) -> None:
        """Delete file from mock storage."""
        from duckpond.exceptions import FileNotFoundError as DuckPondFileNotFoundError

        full_key = self._build_tenant_key(tenant_id, remote_key)
        if full_key not in self._files:
            raise DuckPondFileNotFoundError(full_key)

        del self._files[full_key]
        if full_key in self._metadata:
            del self._metadata[full_key]

    async def delete_prefix(self, tenant_id: str) -> int:
        """Delete all files with tenant prefix."""
        prefix = f"{tenant_id}/"
        keys_to_delete = [k for k in self._files.keys() if k.startswith(prefix)]

        for key in keys_to_delete:
            del self._files[key]
            if key in self._metadata:
                del self._metadata[key]

        return len(keys_to_delete)

    async def list_files(
        self, prefix: str, tenant_id: str, recursive: bool = True
    ) -> list[str]:
        """List files in mock storage."""
        full_prefix = self._build_tenant_key(tenant_id, prefix)
        matching_files = []

        for key in self._files.keys():
            if key.startswith(full_prefix):
                relative_key = key[len(f"{tenant_id}/") :]

                if recursive:
                    matching_files.append(relative_key)
                else:
                    remainder = relative_key[len(prefix) :].lstrip("/")
                    if "/" not in remainder:
                        matching_files.append(relative_key)

        return sorted(matching_files)

    async def file_exists(self, remote_key: str, tenant_id: str) -> bool:
        """Check if file exists in mock storage."""
        full_key = self._build_tenant_key(tenant_id, remote_key)
        return full_key in self._files

    async def get_file_size(self, remote_key: str, tenant_id: str) -> int:
        """Get file size from mock storage."""
        from duckpond.exceptions import FileNotFoundError as DuckPondFileNotFoundError

        full_key = self._build_tenant_key(tenant_id, remote_key)
        if full_key not in self._files:
            raise DuckPondFileNotFoundError(full_key)

        return len(self._files[full_key])

    async def get_file_metadata(
        self, remote_key: str, tenant_id: str
    ) -> dict[str, Any]:
        """Get file metadata from mock storage."""
        from duckpond.exceptions import FileNotFoundError as DuckPondFileNotFoundError

        full_key = self._build_tenant_key(tenant_id, remote_key)
        if full_key not in self._files:
            raise DuckPondFileNotFoundError(full_key)

        return self._metadata.get(full_key, {})

    async def get_storage_usage(self, tenant_id: str) -> int:
        """Calculate total storage usage for tenant."""
        prefix = f"{tenant_id}/"
        total_size = 0

        for key, content in self._files.items():
            if key.startswith(prefix):
                if key in self._metadata and len(content) == 0:
                    total_size += self._metadata[key].get("size", 0)
                else:
                    total_size += len(content)

        return total_size

    async def generate_presigned_url(
        self,
        remote_key: str,
        tenant_id: str,
        expires_seconds: int = 3600,
        method: str = "GET",
    ) -> str:
        """Generate mock presigned URL."""
        full_key = self._build_tenant_key(tenant_id, remote_key)
        return f"mock://presigned/{full_key}?expires={expires_seconds}&method={method}"

    def add_file(
        self,
        remote_key: str,
        tenant_id: str,
        content: bytes | int,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Add a file to mock storage (for testing).

        Args:
            remote_key: Remote key without tenant prefix
            tenant_id: Tenant ID
            content: File content as bytes, or integer size in bytes
            metadata: Optional metadata dictionary
        """
        full_key = self._build_tenant_key(tenant_id, remote_key)

        if isinstance(content, int):
            self._files[full_key] = b""
            self._metadata[full_key] = metadata or {"size": content}
        else:
            self._files[full_key] = content
            self._metadata[full_key] = metadata or {"size": len(content)}

    def clear_tenant(self, tenant_id: str) -> None:
        """Clear all files for a tenant (for testing)."""
        prefix = f"{tenant_id}/"
        keys_to_delete = [k for k in self._files.keys() if k.startswith(prefix)]

        for key in keys_to_delete:
            del self._files[key]
            if key in self._metadata:
                del self._metadata[key]

    def clear_all(self) -> None:
        """Clear all files (for testing)."""
        self._files.clear()
        self._metadata.clear()
