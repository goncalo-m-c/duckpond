"""Notebook session data structures."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from duckpond.notebooks.process import MarimoProcess


class SessionStatus(str, Enum):
    """Notebook session status."""

    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    CRASHED = "crashed"
    UNHEALTHY = "unhealthy"


@dataclass
class NotebookSession:
    """
    Represents an active marimo notebook session.

    Attributes:
        session_id: Unique session identifier
        tenant_id: Tenant owning this session
        notebook_path: Absolute path to notebook file
        process: MarimoProcess wrapper managing the container
        port: Port marimo is listening on
        status: Current session status
        created_at: Timestamp when session was created
        last_accessed: Timestamp of last activity
        pid: Container ID for Docker-based execution
        failed_health_checks: Counter for consecutive failed health checks
    """

    session_id: str
    tenant_id: str
    notebook_path: Path
    process: "MarimoProcess"
    port: int
    status: SessionStatus = SessionStatus.STARTING
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_accessed: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    pid: Optional[str] = None  # Container ID instead of PID
    failed_health_checks: int = 0

    def __post_init__(self) -> None:
        """Set container ID from process after initialization."""
        if self.process and hasattr(self.process, "container_id"):
            self.pid = self.process.container_id

    def update_last_accessed(self) -> None:
        """Update last accessed timestamp to current time."""
        self.last_accessed = datetime.now(timezone.utc)

    def is_idle(self, timeout_seconds: int) -> bool:
        """
        Check if session has been idle longer than timeout.

        Args:
            timeout_seconds: Idle timeout threshold

        Returns:
            True if session is idle beyond timeout, False otherwise
        """
        now = datetime.now(timezone.utc)
        idle_seconds = (now - self.last_accessed).total_seconds()
        return idle_seconds > timeout_seconds

    def is_process_alive(self) -> bool:
        """
        Check if the marimo container is still running.

        Returns:
            True if container is running, False otherwise
        """
        if not self.process:
            return False
        return self.process.is_alive()

    def increment_failed_health_checks(self) -> int:
        """
        Increment failed health check counter.

        Returns:
            New failed health check count
        """
        self.failed_health_checks += 1
        return self.failed_health_checks

    def reset_health_checks(self) -> None:
        """Reset failed health check counter to zero."""
        self.failed_health_checks = 0

    def to_dict(self) -> dict:
        """
        Convert session to dictionary representation.

        Returns:
            Dictionary with session information
        """
        return {
            "session_id": self.session_id,
            "tenant_id": self.tenant_id,
            "notebook_path": str(self.notebook_path),
            "port": self.port,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "last_accessed": self.last_accessed.isoformat(),
            "pid": self.pid,
            "is_alive": self.is_process_alive(),
        }
