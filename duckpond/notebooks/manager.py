"""Notebook session manager."""

import asyncio
import secrets
from pathlib import Path
from typing import Optional

import structlog

from duckpond.config import Settings
from duckpond.notebooks.exceptions import (
    PortExhaustedException,
    SessionLimitException,
    SessionNotFoundException,
)
from duckpond.notebooks.process import MarimoProcess
from duckpond.notebooks.security import (
    get_tenant_data_directory,
    get_tenant_notebook_directory,
    validate_notebook_path,
)
from duckpond.notebooks.session import NotebookSession, SessionStatus

logger = structlog.get_logger(__name__)


class NotebookManager:
    """
    Manages notebook sessions across all tenants.

    Handles:
    - Session creation and lifecycle
    - Port allocation from configurable range
    - Session cleanup on timeout
    - Health monitoring
    """

    def __init__(self, settings: Settings):
        """
        Initialize notebook manager.

        Args:
            settings: Application settings
        """
        self.settings = settings
        self.sessions: dict[str, NotebookSession] = {}
        self.available_ports: set[int] = set(
            range(
                settings.notebook_port_range_start,
                settings.notebook_port_range_end + 1,
            )
        )
        self.cleanup_task: Optional[asyncio.Task] = None
        self.health_check_task: Optional[asyncio.Task] = None

        logger.info(
            "notebook_manager_initialized",
            port_range=f"{settings.notebook_port_range_start}-{settings.notebook_port_range_end}",
            max_sessions=settings.notebook_max_concurrent_sessions,
        )

    async def start(self) -> None:
        """Start background tasks for cleanup and health monitoring."""
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        self.health_check_task = asyncio.create_task(self._health_check_loop())

        logger.info("notebook_manager_background_tasks_started")

    async def stop(self) -> None:
        """Stop all sessions and background tasks."""
        logger.info("notebook_manager_stopping")

        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass

        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass

        session_ids = list(self.sessions.keys())
        for session_id in session_ids:
            await self.terminate_session(session_id)

        logger.info("notebook_manager_stopped")

    async def create_session(
        self,
        tenant_id: str,
        notebook_path: str | Path,
    ) -> NotebookSession:
        """
        Create a new notebook session.

        Args:
            tenant_id: Tenant identifier
            notebook_path: Path to notebook file (relative to tenant notebook dir)

        Returns:
            Created notebook session

        Raises:
            SessionLimitException: If max concurrent sessions reached
            PortExhaustedException: If no ports available
            NotebookNotFoundException: If notebook file doesn't exist
            PathSecurityException: If path validation fails
        """
        if len(self.sessions) >= self.settings.notebook_max_concurrent_sessions:
            raise SessionLimitException(
                len(self.sessions),
                self.settings.notebook_max_concurrent_sessions,
            )

        tenant_notebook_dir = get_tenant_notebook_directory(
            tenant_id, self.settings.local_storage_path
        )
        tenant_data_dir = get_tenant_data_directory(
            tenant_id, self.settings.local_storage_path
        )

        validated_path = validate_notebook_path(notebook_path, tenant_notebook_dir)

        # Create notebook if it doesn't exist
        if not validated_path.exists():
            from duckpond.notebooks.files import create_notebook

            logger.info(
                "creating_missing_notebook",
                tenant_id=tenant_id,
                notebook=str(validated_path),
            )

            # Ensure notebook directory exists
            tenant_notebook_dir.mkdir(parents=True, exist_ok=True)

            # Create notebook with default template
            await create_notebook(
                filename=validated_path.name,
                tenant_notebook_dir=tenant_notebook_dir,
            )

        port = self._allocate_port()

        session_id = secrets.token_urlsafe(16)

        logger.info(
            "creating_notebook_session",
            session_id=session_id,
            tenant_id=tenant_id,
            notebook=str(validated_path),
            port=port,
        )

        try:
            marimo_process = MarimoProcess(
                notebook_path=validated_path,
                port=port,
                tenant_data_dir=tenant_data_dir,
                tenant_id=tenant_id,
                docker_image=self.settings.notebook_docker_image,
                memory_limit_mb=self.settings.notebook_max_memory_mb,
                cpu_limit=self.settings.notebook_cpu_limit,
                startup_timeout=self.settings.notebook_startup_timeout_seconds,
            )

            container_id = await marimo_process.start()

            session = NotebookSession(
                session_id=session_id,
                tenant_id=tenant_id,
                notebook_path=validated_path,
                process=marimo_process,  # Store the MarimoProcess wrapper
                port=port,
                status=SessionStatus.RUNNING,
            )

            self.sessions[session_id] = session

            logger.info(
                "notebook_session_created",
                session_id=session_id,
                tenant_id=tenant_id,
                port=port,
                container_id=container_id[:12],
            )

            return session

        except Exception as e:
            self._release_port(port)
            logger.error(
                "notebook_session_creation_failed",
                session_id=session_id,
                tenant_id=tenant_id,
                error=str(e),
                exc_info=True,
            )
            raise

    async def get_session(self, session_id: str) -> NotebookSession:
        """
        Get session by ID.

        Args:
            session_id: Session identifier

        Returns:
            Notebook session

        Raises:
            SessionNotFoundException: If session doesn't exist
        """
        session = self.sessions.get(session_id)
        if not session:
            raise SessionNotFoundException(session_id)

        session.update_last_accessed()
        return session

    async def list_sessions(
        self, tenant_id: Optional[str] = None
    ) -> list[NotebookSession]:
        """
        List all sessions, optionally filtered by tenant.

        Args:
            tenant_id: Optional tenant filter

        Returns:
            List of notebook sessions
        """
        if tenant_id:
            return [s for s in self.sessions.values() if s.tenant_id == tenant_id]
        return list(self.sessions.values())

    async def terminate_session(self, session_id: str) -> None:
        """
        Terminate a session and clean up resources.

        Args:
            session_id: Session identifier

        Raises:
            SessionNotFoundException: If session doesn't exist
        """
        session = self.sessions.get(session_id)
        if not session:
            raise SessionNotFoundException(session_id)

        logger.info(
            "terminating_notebook_session",
            session_id=session_id,
            tenant_id=session.tenant_id,
            port=session.port,
        )

        session.status = SessionStatus.STOPPING

        # Use the MarimoProcess stored in the session
        await session.process.stop()

        self._release_port(session.port)

        del self.sessions[session_id]

        logger.info(
            "notebook_session_terminated",
            session_id=session_id,
            tenant_id=session.tenant_id,
        )

    def _allocate_port(self) -> int:
        """
        Allocate a port from the available pool.

        Returns:
            Allocated port number

        Raises:
            PortExhaustedException: If no ports available
        """
        if not self.available_ports:
            raise PortExhaustedException()

        port = self.available_ports.pop()
        logger.debug("port_allocated", port=port, remaining=len(self.available_ports))
        return port

    def _release_port(self, port: int) -> None:
        """
        Release a port back to the available pool.

        Args:
            port: Port number to release
        """
        self.available_ports.add(port)
        logger.debug("port_released", port=port, available=len(self.available_ports))

    async def _cleanup_loop(self) -> None:
        """Background task to clean up idle sessions."""
        logger.info("cleanup_loop_started")

        while True:
            try:
                await asyncio.sleep(60)

                idle_sessions = []
                for session_id, session in self.sessions.items():
                    if session.is_idle(self.settings.notebook_session_timeout_seconds):
                        idle_sessions.append((session_id, session))

                for session_id, session in idle_sessions:
                    logger.info(
                        "cleaning_up_idle_session",
                        session_id=session_id,
                        tenant_id=session.tenant_id,
                        idle_seconds=int(
                            (session.last_accessed - session.created_at).total_seconds()
                        ),
                    )
                    await self.terminate_session(session_id)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "cleanup_loop_error",
                    error=str(e),
                    exc_info=True,
                )

    async def _health_check_loop(self) -> None:
        """Background task to monitor session health."""
        logger.info("health_check_loop_started")

        while True:
            try:
                await asyncio.sleep(
                    self.settings.notebook_health_check_interval_seconds
                )

                unhealthy_sessions = []

                for session_id, session in self.sessions.items():
                    if not session.is_process_alive():
                        logger.warning(
                            "session_process_dead",
                            session_id=session_id,
                            tenant_id=session.tenant_id,
                        )
                        session.status = SessionStatus.CRASHED
                        unhealthy_sessions.append(session_id)
                        continue

                    # Use the MarimoProcess stored in the session
                    is_healthy = await session.process.check_health()

                    if is_healthy:
                        session.reset_health_checks()
                    else:
                        failed_count = session.increment_failed_health_checks()
                        logger.warning(
                            "session_health_check_failed",
                            session_id=session_id,
                            tenant_id=session.tenant_id,
                            failed_count=failed_count,
                        )

                        if failed_count >= 3:
                            logger.error(
                                "session_unhealthy",
                                session_id=session_id,
                                tenant_id=session.tenant_id,
                            )
                            session.status = SessionStatus.UNHEALTHY
                            unhealthy_sessions.append(session_id)

                for session_id in unhealthy_sessions:
                    await self.terminate_session(session_id)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "health_check_loop_error",
                    error=str(e),
                    exc_info=True,
                )

    def get_status(self) -> dict:
        """
        Get manager status for monitoring.

        Returns:
            Dictionary with manager status
        """
        return {
            "enabled": self.settings.notebook_enabled,
            "active_sessions": len(self.sessions),
            "max_sessions": self.settings.notebook_max_concurrent_sessions,
            "available_ports": len(self.available_ports),
            "sessions_by_status": {
                status.value: len(
                    [s for s in self.sessions.values() if s.status == status]
                )
                for status in SessionStatus
            },
        }
