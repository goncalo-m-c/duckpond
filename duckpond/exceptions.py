"""Custom exceptions for DuckPond."""

from typing import Any


class DuckPondError(Exception):
    """Base exception for all DuckPond errors."""

    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.message = message
        self.context = context

    def to_dict(self) -> dict[str, Any]:
        """Convert exception to dictionary."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "context": self.context,
        }


class ConfigurationError(DuckPondError):
    """Configuration-related errors."""

    pass


class AccountError(DuckPondError):
    """Base class for account-related errors."""

    pass


class AccountNotFoundError(AccountError):
    """Account does not exist."""

    def __init__(self, account_id: str) -> None:
        super().__init__(f"Account not found: {account_id}", account_id=account_id)


class AccountAlreadyExistsError(AccountError):
    """Account already exists."""

    def __init__(self, account_id: str) -> None:
        super().__init__(f"Account already exists: {account_id}", account_id=account_id)


class QuotaExceededError(AccountError):
    """Account quota exceeded."""

    def __init__(self, account_id: str, quota_type: str, limit: Any, current: Any) -> None:
        super().__init__(
            f"Quota exceeded for {quota_type}: {current}/{limit}",
            account_id=account_id,
            quota_type=quota_type,
            limit=limit,
            current=current,
        )


class AuthenticationError(DuckPondError):
    """Authentication-related errors."""

    pass


class InvalidAPIKeyError(AuthenticationError):
    """Invalid or expired API key."""

    def __init__(self) -> None:
        super().__init__("Invalid or expired API key")


class StorageError(DuckPondError):
    """Storage-related errors."""

    pass


class FileNotFoundError(StorageError):
    """File not found in storage."""

    def __init__(self, path: str) -> None:
        super().__init__(f"File not found: {path}", path=path)


class StorageBackendError(StorageError):
    """Storage backend operation failed."""

    def __init__(self, backend: str, operation: str, details: str) -> None:
        super().__init__(
            f"Storage backend error ({backend}): {operation} - {details}",
            backend=backend,
            operation=operation,
            details=details,
        )


class InsufficientStorageError(StorageError):
    """Insufficient storage available for operation."""

    def __init__(self, account_id: str, required: int, available: int) -> None:
        super().__init__(
            f"Insufficient storage for account {account_id}: required {required} bytes, available {available} bytes",
            account_id=account_id,
            required=required,
            available=available,
        )


class QueryError(DuckPondError):
    """Query execution errors."""

    pass


class QueryTimeoutError(QueryError):
    """Query execution timeout."""

    def __init__(self, timeout_seconds: int) -> None:
        super().__init__(f"Query timeout after {timeout_seconds}s", timeout_seconds=timeout_seconds)


class QueryMemoryLimitError(QueryError):
    """Query exceeded memory limit."""

    def __init__(self, limit: str) -> None:
        super().__init__(f"Query exceeded memory limit: {limit}", limit=limit)


class ConcurrentQueryLimitError(QueryError):
    """Concurrent query limit exceeded."""

    def __init__(self, account_id: str, limit: int) -> None:
        super().__init__(
            f"Concurrent query limit exceeded for account {account_id}: {limit}",
            account_id=account_id,
            limit=limit,
        )


class ConnectionPoolExhaustedError(QueryError):
    """Connection pool exhausted."""

    def __init__(self, max_connections: int) -> None:
        super().__init__(
            f"Connection pool exhausted: all {max_connections} connections in use",
            max_connections=max_connections,
        )


class SQLValidationError(QueryError):
    """SQL validation failed."""

    def __init__(self, message: str) -> None:
        super().__init__(f"SQL validation error: {message}")


class QueryExecutionError(QueryError):
    """Query execution failed."""

    def __init__(self, message: str, query: str | None = None) -> None:
        super().__init__(
            message,
            query=query,
        )


class CatalogError(DuckPondError):
    """Catalog-related errors."""

    pass


class DatasetNotFoundError(CatalogError):
    """Dataset not found in catalog."""

    def __init__(self, dataset: str) -> None:
        super().__init__(f"Dataset not found: {dataset}", dataset=dataset)


class SchemaIncompatibleError(CatalogError):
    """Schema incompatibility detected."""

    def __init__(self, details: str) -> None:
        super().__init__(f"Schema incompatible: {details}", details=details)


class IngestionError(DuckPondError):
    """Data ingestion errors."""

    pass


class UnsupportedFormatError(IngestionError):
    """Unsupported file or data format."""

    def __init__(self, format_name: str) -> None:
        super().__init__(f"Unsupported format: {format_name}", format_name=format_name)


class SchemaInferenceError(IngestionError):
    """Failed to infer schema."""

    def __init__(self, details: str) -> None:
        super().__init__(f"Schema inference failed: {details}", details=details)


def get_http_status(error: Exception) -> int:
    """Map exception to HTTP status code."""
    status_map = {
        AccountNotFoundError: 404,
        DatasetNotFoundError: 404,
        FileNotFoundError: 404,
        AccountAlreadyExistsError: 409,
        InvalidAPIKeyError: 401,
        AuthenticationError: 401,
        QuotaExceededError: 429,
        ConcurrentQueryLimitError: 429,
        InsufficientStorageError: 507,
        ConfigurationError: 500,
        StorageBackendError: 500,
        QueryTimeoutError: 504,
        QueryMemoryLimitError: 507,
        UnsupportedFormatError: 400,
        SchemaInferenceError: 400,
        SQLValidationError: 400,
        QueryExecutionError: 500,
    }

    for exc_type, status in status_map.items():
        if isinstance(error, exc_type):
            return status

    return 500
