"""Notebook session data structures."""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional


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
        process: Subprocess running marimo
        port: Port marimo is listening on
        status: Current session status
        created_at: Timestamp when session was created
        last_accessed: Timestamp of last activity
        pid: Process ID of marimo process
        failed_health_checks: Counter for consecutive failed health checks
    """

    session_id: str
    tenant_id: str
    notebook_path: Path
    process: asyncio.subprocess.Process
    port: int
    status: SessionStatus = SessionStatus.STARTING
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_accessed: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    pid: Optional[int] = None
    failed_health_checks: int = 0

    def __post_init__(self) -> None:
        """Set PID from process after initialization."""
        if self.process and self.process.pid:
            self.pid = self.process.pid

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
        Check if the marimo process is still running.

        Returns:
            True if process is running, False otherwise
        """
        if not self.process:
            return False
        return self.process.returncode is None

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
