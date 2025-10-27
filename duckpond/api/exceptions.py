"""FastAPI exception hierarchy for DuckPond API.

This module defines custom exception classes that map to HTTP status codes
and provide consistent error responses across the API.
"""

from fastapi import HTTPException, status


class DuckPondAPIException(HTTPException):
    """Base API exception for DuckPond.

    All custom API exceptions should inherit from this class.
    Automatically maps to HTTP status codes.
    """

    def __init__(
        self,
        status_code: int,
        detail: str,
        headers: dict | None = None,
    ):
        """Initialize API exception.

        Args:
            status_code: HTTP status code
            detail: Error message detail
            headers: Optional HTTP headers
        """
        super().__init__(status_code=status_code, detail=detail, headers=headers)


class UnauthorizedException(DuckPondAPIException):
    """Unauthorized access (401).

    Raised when authentication credentials are missing or invalid.
    """

    def __init__(self, detail: str = "Not authenticated"):
        """Initialize unauthorized exception.

        Args:
            detail: Error message (default: "Not authenticated")
        """
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
            headers={"WWW-Authenticate": "Bearer"},
        )


class ForbiddenException(DuckPondAPIException):
    """Forbidden access (403).

    Raised when user is authenticated but doesn't have permission.
    """

    def __init__(self, detail: str = "Forbidden"):
        """Initialize forbidden exception.

        Args:
            detail: Error message (default: "Forbidden")
        """
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=detail,
        )


class NotFoundException(DuckPondAPIException):
    """Resource not found (404).

    Raised when requested resource doesn't exist.
    """

    def __init__(self, detail: str = "Resource not found"):
        """Initialize not found exception.

        Args:
            detail: Error message (default: "Resource not found")
        """
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=detail,
        )


class ConflictException(DuckPondAPIException):
    """Resource conflict (409).

    Raised when operation conflicts with existing resource state.
    """

    def __init__(self, detail: str = "Resource conflict"):
        """Initialize conflict exception.

        Args:
            detail: Error message (default: "Resource conflict")
        """
        super().__init__(
            status_code=status.HTTP_409_CONFLICT,
            detail=detail,
        )


class ValidationException(DuckPondAPIException):
    """Validation error (422).

    Raised when request validation fails.
    """

    def __init__(self, detail: str = "Validation error"):
        """Initialize validation exception.

        Args:
            detail: Error message (default: "Validation error")
        """
        super().__init__(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=detail,
        )


class BadRequestException(DuckPondAPIException):
    """Bad request (400).

    Raised when request is malformed or invalid.
    """

    def __init__(self, detail: str = "Bad request"):
        """Initialize bad request exception.

        Args:
            detail: Error message (default: "Bad request")
        """
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail,
        )


class InternalServerException(DuckPondAPIException):
    """Internal server error (500).

    Raised when an unexpected error occurs.
    """

    def __init__(self, detail: str = "Internal server error"):
        """Initialize internal server exception.

        Args:
            detail: Error message (default: "Internal server error")
        """
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail,
        )


class ServiceUnavailableException(DuckPondAPIException):
    """Service unavailable (503).

    Raised when service is temporarily unavailable.
    """

    def __init__(self, detail: str = "Service temporarily unavailable"):
        """Initialize service unavailable exception.

        Args:
            detail: Error message (default: "Service temporarily unavailable")
        """
        super().__init__(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=detail,
        )
