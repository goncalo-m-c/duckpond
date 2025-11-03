"""Core Docker container management for DuckPond.

This module provides a generic Docker container wrapper that handles:
- Container lifecycle (start, stop, cleanup)
- Health monitoring
- Log access
- Async subprocess management
"""

import asyncio
import os
from typing import Optional

import httpx
import structlog

from duckpond.docker.config import ContainerConfig
from duckpond.docker.exceptions import (
    ContainerExecutionException,
    ContainerNotFoundException,
    ContainerStartupException,
    ContainerStopException,
)

logger = structlog.get_logger(__name__)


class DockerContainer:
    """
    Generic Docker container wrapper with lifecycle management.

    Provides high-level interface for running isolated Docker containers
    with health monitoring, resource limits, and graceful shutdown.

    Features:
    - Async container lifecycle management
    - Configurable startup health checks
    - HTTP-based health monitoring
    - Container log access
    - Graceful shutdown with timeout
    - Automatic cleanup on failure

    Usage:
        config = ContainerConfig(
            image="duckpond:25.1",
            command=["python", "script.py"],
            name="my-container",
        )
        container = DockerContainer(config)
        await container.start()
        is_healthy = await container.check_health()
        await container.stop()
    """

    def __init__(self, config: ContainerConfig):
        """
        Initialize Docker container wrapper.

        Args:
            config: Container configuration
        """
        self.config = config
        self.container_id: Optional[str] = None
        self._startup_process: Optional[asyncio.subprocess.Process] = None

    async def start(self, health_check_url: Optional[str] = None) -> str:
        """
        Start Docker container and optionally wait for health.

        Args:
            health_check_url: Optional HTTP URL to check for health
                            (e.g., "http://127.0.0.1:8080/health")
                            If provided, will wait for URL to respond before returning

        Returns:
            The container ID

        Raises:
            ContainerStartupException: If container fails to start or become healthy
        """
        logger.info(
            "starting_docker_container",
            container_name=self.config.name,
            image=self.config.image,
            detach=self.config.detach,
        )

        try:
            # Build docker run command
            command = self.config.to_docker_run_args()

            logger.info(
                "docker_run_command",
                command=" ".join(command),
                container_name=self.config.name,
            )

            # Start container
            self._startup_process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            # Wait for process to complete and get output
            stdout, stderr = await self._startup_process.communicate()

            if self._startup_process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                raise ContainerStartupException(f"Failed to start container: {error_msg}")

            # Extract container ID
            self.container_id = stdout.decode().strip()

            logger.info(
                "docker_container_started",
                container_id=self.container_id[:12],
                container_name=self.config.name,
            )

            # Wait for health check if URL provided
            if health_check_url:
                await self._wait_for_http_health(health_check_url)

            logger.info(
                "docker_container_ready",
                container_id=self.container_id[:12],
                container_name=self.config.name,
            )

            return self.container_id

        except ContainerStartupException:
            await self._cleanup_failed_start()
            raise
        except Exception as e:
            logger.error(
                "docker_container_start_failed",
                container_name=self.config.name,
                error=str(e),
                exc_info=True,
            )
            await self._cleanup_failed_start()
            raise ContainerStartupException(f"Unexpected error: {str(e)}") from e

    async def _wait_for_http_health(self, health_url: str) -> None:
        """
        Wait for HTTP health check to succeed.

        Polls the health endpoint until it responds successfully or timeout is reached.

        Args:
            health_url: HTTP URL to check

        Raises:
            ContainerStartupException: If container doesn't become healthy in time
        """
        start_time = asyncio.get_event_loop().time()
        timeout = self.config.startup_timeout_seconds
        check_interval = self.config.health_check_interval_seconds

        logger.debug(
            "waiting_for_health",
            health_url=health_url,
            timeout=timeout,
            container_id=self.container_id[:12] if self.container_id else None,
        )

        async with httpx.AsyncClient() as client:
            while True:
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed > timeout:
                    logs = await self.get_logs(tail=50)
                    raise ContainerStartupException(
                        f"Health check timeout after {timeout}s. Container logs:\n{logs}"
                    )

                # Check if container is still running
                is_running = await self.is_running()
                if not is_running:
                    logs = await self.get_logs(tail=50)
                    raise ContainerStartupException(
                        f"Container stopped unexpectedly. Logs:\n{logs}"
                    )

                try:
                    response = await client.get(health_url, timeout=2.0)
                    if response.status_code == 200:
                        logger.debug(
                            "health_check_passed",
                            container_id=self.container_id[:12] if self.container_id else None,
                            elapsed=f"{elapsed:.2f}s",
                        )
                        return
                except (httpx.RequestError, httpx.TimeoutException):
                    pass

                await asyncio.sleep(check_interval)

    async def is_running(self) -> bool:
        """
        Check if container is currently running.

        Returns:
            True if container is running, False otherwise
        """
        if not self.container_id:
            return False

        try:
            process = await asyncio.create_subprocess_exec(
                "docker",
                "inspect",
                "-f",
                "{{.State.Running}}",
                self.container_id,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, _ = await process.communicate()
            is_running = stdout.decode().strip() == "true"

            logger.debug(
                "container_running_check",
                container_id=self.container_id[:12],
                is_running=is_running,
            )

            return is_running

        except Exception as e:
            logger.warning(
                "container_check_failed",
                container_id=self.container_id[:12],
                error=str(e),
            )
            return False

    async def check_health(self, health_url: Optional[str] = None) -> bool:
        """
        Check if container is healthy.

        Performs both container running check and optional HTTP health check.

        Args:
            health_url: Optional HTTP URL to check for health

        Returns:
            True if healthy, False otherwise
        """
        if not self.container_id:
            return False

        # Check container is running
        is_running = await self.is_running()
        if not is_running:
            logger.warning(
                "container_not_running",
                container_id=self.container_id[:12],
            )
            return False

        # Check HTTP health if URL provided
        if health_url:
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(health_url, timeout=5.0)
                    is_healthy = response.status_code == 200

                    if is_healthy:
                        logger.debug(
                            "health_check_ok",
                            container_id=self.container_id[:12],
                        )
                    else:
                        logger.warning(
                            "health_check_failed",
                            container_id=self.container_id[:12],
                            status=response.status_code,
                        )

                    return is_healthy

            except Exception as e:
                logger.warning(
                    "health_check_error",
                    container_id=self.container_id[:12],
                    error=str(e),
                )
                return False

        # No HTTP check, just return running status
        return True

    async def get_logs(self, tail: int = 100) -> str:
        """
        Read container logs.

        Args:
            tail: Number of lines to read from end of logs

        Returns:
            Container logs as string (limited to 2000 characters)
        """
        if not self.container_id:
            return ""

        try:
            process = await asyncio.create_subprocess_exec(
                "docker",
                "logs",
                "--tail",
                str(tail),
                self.container_id,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=5.0)
            logs = stdout.decode("utf-8", errors="replace")
            if stderr:
                logs += "\n" + stderr.decode("utf-8", errors="replace")
            return logs[:2000]  # Limit log size

        except asyncio.TimeoutError:
            return "(log read timeout)"
        except Exception as e:
            return f"(error reading logs: {str(e)})"

    async def execute(self, command: list[str], timeout: int = 30) -> tuple[str, str, int]:
        """
        Execute command inside running container.

        Args:
            command: Command and arguments to execute
            timeout: Timeout in seconds

        Returns:
            Tuple of (stdout, stderr, return_code)

        Raises:
            ContainerNotFoundException: If container is not running
            ContainerExecutionException: If execution fails
        """
        if not self.container_id:
            raise ContainerNotFoundException("Container not started")

        is_running = await self.is_running()
        if not is_running:
            raise ContainerNotFoundException("Container is not running")

        try:
            exec_command = ["docker", "exec", self.container_id] + command

            process = await asyncio.create_subprocess_exec(
                *exec_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)

            return (
                stdout.decode("utf-8", errors="replace"),
                stderr.decode("utf-8", errors="replace"),
                process.returncode or 0,
            )

        except asyncio.TimeoutError:
            raise ContainerExecutionException(f"Command execution timeout after {timeout}s")
        except Exception as e:
            raise ContainerExecutionException(f"Command execution failed: {str(e)}") from e

    async def stop(self, timeout: Optional[int] = None) -> None:
        """
        Stop container gracefully.

        Sends docker stop which gives the container time to shut down gracefully.
        If timeout is reached, Docker will forcefully kill the container.

        Args:
            timeout: Seconds to wait for graceful shutdown
                    (defaults to config.stop_timeout_seconds)

        Raises:
            ContainerStopException: If stop fails
        """
        if not self.container_id:
            return

        stop_timeout = timeout or self.config.stop_timeout_seconds

        logger.info(
            "stopping_docker_container",
            container_id=self.container_id[:12],
            container_name=self.config.name,
            timeout=stop_timeout,
        )

        try:
            process = await asyncio.create_subprocess_exec(
                "docker",
                "stop",
                "-t",
                str(stop_timeout),
                self.container_id,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            await process.wait()

            logger.info(
                "docker_container_stopped",
                container_id=self.container_id[:12],
                container_name=self.config.name,
            )

        except Exception as e:
            logger.error(
                "docker_container_stop_error",
                container_id=self.container_id[:12],
                container_name=self.config.name,
                error=str(e),
                exc_info=True,
            )
            raise ContainerStopException(f"Failed to stop container: {str(e)}") from e

    async def kill(self) -> None:
        """
        Forcefully kill container immediately.

        Use this when graceful shutdown fails or immediate termination is needed.
        """
        if not self.container_id:
            return

        logger.warning(
            "killing_docker_container",
            container_id=self.container_id[:12],
            container_name=self.config.name,
        )

        try:
            process = await asyncio.create_subprocess_exec(
                "docker",
                "kill",
                self.container_id,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            await process.wait()

            logger.info(
                "docker_container_killed",
                container_id=self.container_id[:12],
                container_name=self.config.name,
            )

        except Exception as e:
            logger.error(
                "docker_container_kill_error",
                container_id=self.container_id[:12],
                error=str(e),
            )

    async def _cleanup_failed_start(self) -> None:
        """Clean up resources after failed startup."""
        if self.container_id:
            try:
                await self.stop(timeout=5)
            except Exception as e:
                logger.warning(
                    "cleanup_failed_start_error",
                    container_id=self.container_id[:12],
                    error=str(e),
                )

    def get_container_id(self) -> Optional[str]:
        """
        Get container ID.

        Returns:
            Container ID if started, None otherwise
        """
        return self.container_id

    def is_alive(self) -> bool:
        """
        Quick check if container exists (synchronous).

        Returns:
            True if container ID exists, False otherwise

        Note: This only checks if container was started. Use is_running()
              or check_health() for actual runtime status.
        """
        return self.container_id is not None

    @staticmethod
    def get_aws_credentials_env() -> dict[str, str]:
        """
        Get AWS credentials from environment variables.

        Returns:
            Dictionary of AWS environment variables found in current environment
        """
        env_vars = {}
        aws_keys = [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_REGION",
            "AWS_DEFAULT_REGION",
        ]

        for key in aws_keys:
            if key in os.environ:
                env_vars[key] = os.environ[key]

        return env_vars

    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()
