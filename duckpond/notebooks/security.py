"""Security utilities for notebook operations."""

from pathlib import Path

import structlog

from duckpond.notebooks.exceptions import PathSecurityException

logger = structlog.get_logger(__name__)


def validate_notebook_path(notebook_path: str | Path, account_notebook_dir: Path) -> Path:
    """
    Validate notebook path for security.

    This function prevents path traversal attacks by ensuring:
    1. The path resolves to a location within the tenant's notebook directory
    2. No parent directory references (../) escape the tenant directory
    3. The path is normalized and absolute

    Args:
        notebook_path: User-provided notebook path (relative or absolute)
        account_notebook_dir: Absolute path to tenant's notebook directory

    Returns:
        Validated absolute path within tenant directory

    Raises:
        PathSecurityException: If path validation fails
    """
    try:
        notebook_path = Path(notebook_path)

        if notebook_path.is_absolute():
            resolved_path = notebook_path.resolve()
        else:
            resolved_path = (account_notebook_dir / notebook_path).resolve()

        account_dir_resolved = account_notebook_dir.resolve()

        if not str(resolved_path).startswith(str(account_dir_resolved)):
            logger.warning(
                "path_traversal_attempt",
                notebook_path=str(notebook_path),
                resolved_path=str(resolved_path),
                account_dir=str(account_dir_resolved),
            )
            raise PathSecurityException(
                str(notebook_path),
                "Path escapes tenant notebook directory",
            )

        if ".." in notebook_path.parts:
            logger.warning(
                "suspicious_path_pattern",
                notebook_path=str(notebook_path),
            )
            raise PathSecurityException(
                str(notebook_path),
                "Path contains parent directory references",
            )

        logger.debug(
            "path_validated",
            notebook_path=str(notebook_path),
            resolved_path=str(resolved_path),
        )

        return resolved_path

    except PathSecurityException:
        raise
    except Exception as e:
        logger.error(
            "path_validation_error",
            notebook_path=str(notebook_path),
            error=str(e),
            exc_info=True,
        )
        raise PathSecurityException(
            str(notebook_path),
            f"Path validation failed: {str(e)}",
        )


def get_account_notebook_directory(account_id: str, storage_path: Path) -> Path:
    """
    Get the notebook directory for a tenant.

    Creates the directory if it doesn't exist with secure permissions.

    Args:
        account_id: Tenant identifier
        storage_path: Base storage path from configuration

    Returns:
        Absolute path to tenant's notebook directory
    """
    account_dir = storage_path / "accounts" / account_id / "notebooks"

    account_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    logger.debug(
        "account_notebook_directory",
        account_id=account_id,
        notebook_dir=str(account_dir),
    )

    return account_dir


def get_account_data_directory(account_id: str, storage_path: Path) -> Path:
    """
    Get the data directory for a tenant.

    This is used as the working directory for marimo processes.

    Args:
        account_id: Tenant identifier
        storage_path: Base storage path from configuration

    Returns:
        Absolute path to tenant's data directory
    """
    account_data_dir = storage_path / "accounts" / account_id

    account_data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    return account_data_dir


def validate_filename(filename: str) -> None:
    """
    Validate filename for security.

    Ensures filename doesn't contain path separators or other dangerous characters.

    Args:
        filename: Filename to validate

    Raises:
        PathSecurityException: If filename is invalid
    """
    if not filename:
        raise PathSecurityException(filename, "Filename cannot be empty")

    if "/" in filename or "\\" in filename:
        raise PathSecurityException(
            filename,
            "Filename cannot contain path separators",
        )

    if filename.startswith("."):
        raise PathSecurityException(
            filename,
            "Filename cannot start with dot",
        )

    dangerous_chars = ["<", ">", ":", '"', "|", "?", "*"]
    for char in dangerous_chars:
        if char in filename:
            raise PathSecurityException(
                filename,
                f"Filename contains dangerous character: {char}",
            )

    if not filename.endswith(".py"):
        raise PathSecurityException(
            filename,
            "Notebook filename must end with .py",
        )
