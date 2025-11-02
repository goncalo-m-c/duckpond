"""Exceptions for notebook operations."""


class NotebookException(Exception):
    """Base exception for notebook-related errors."""

    pass


class SessionNotFoundException(NotebookException):
    """Raised when a notebook session is not found."""

    def __init__(self, session_id: str):
        """Initialize exception with session ID."""
        self.session_id = session_id
        super().__init__(f"Notebook session not found: {session_id}")


class PortExhaustedException(NotebookException):
    """Raised when no ports are available for new sessions."""

    def __init__(self, message: str = "No available ports for notebook session"):
        """Initialize exception with message."""
        super().__init__(message)


class ProcessStartupException(NotebookException):
    """Raised when marimo process fails to start."""

    def __init__(self, details: str):
        """Initialize exception with error details."""
        self.details = details
        super().__init__(f"Failed to start marimo process: {details}")


class ProcessHealthException(NotebookException):
    """Raised when marimo process health check fails."""

    def __init__(self, session_id: str, details: str):
        """Initialize exception with session ID and details."""
        self.session_id = session_id
        self.details = details
        super().__init__(
            f"Notebook process health check failed for {session_id}: {details}"
        )


class PathSecurityException(NotebookException):
    """Raised when path validation fails for security reasons."""

    def __init__(self, path: str, reason: str):
        """Initialize exception with path and reason."""
        self.path = path
        self.reason = reason
        super().__init__(f"Path security validation failed for {path}: {reason}")


class SessionLimitException(NotebookException):
    """Raised when maximum concurrent sessions limit is reached."""

    def __init__(self, current: int, maximum: int):
        """Initialize exception with current and maximum session counts."""
        self.current = current
        self.maximum = maximum
        super().__init__(f"Maximum concurrent sessions reached: {current}/{maximum}")


class NotebookNotFoundException(NotebookException):
    """Raised when a notebook file is not found."""

    def __init__(self, notebook_path: str):
        """Initialize exception with notebook path."""
        self.notebook_path = notebook_path
        super().__init__(f"Notebook file not found: {notebook_path}")
