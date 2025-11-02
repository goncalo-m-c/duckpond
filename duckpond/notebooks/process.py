"""Marimo process management with Docker isolation."""

import asyncio
from pathlib import Path
from typing import Optional

import httpx
import structlog

from duckpond.notebooks.exceptions import (
    ProcessStartupException,
)

logger = structlog.get_logger(__name__)


class MarimoProcess:
    """
    Wrapper for marimo container with health monitoring.

    Manages the lifecycle of a marimo editor process running in Docker including:
    - Container spawning with proper isolation and resource limits
    - Startup health checks
    - Ongoing health monitoring
    - Graceful shutdown
    """

    def __init__(
        self,
        notebook_path: Path,
        port: int,
        tenant_data_dir: Path,
        account_id: str,
        docker_image: str = "python:3.12-slim",
        memory_limit_mb: int = 2048,
        cpu_limit: float = 2.0,
        startup_timeout: int = 30,
    ):
        """
        Initialize marimo container wrapper.

        Args:
            notebook_path: Absolute path to notebook file
            port: Port for marimo to listen on
            tenant_data_dir: Working directory for marimo process
            account_id: Tenant identifier for container naming
            docker_image: Docker image to use
            memory_limit_mb: Memory limit in megabytes
            cpu_limit: CPU limit (1.0 = 1 core)
            startup_timeout: Maximum seconds to wait for startup
        """
        self.notebook_path = notebook_path
        self.port = port
        self.tenant_data_dir = tenant_data_dir
        self.account_id = account_id
        self.docker_image = docker_image
        self.memory_limit_mb = memory_limit_mb
        self.cpu_limit = cpu_limit
        self.startup_timeout = startup_timeout
        self.process: Optional[asyncio.subprocess.Process] = None
        self.container_id: Optional[str] = None

    async def start(self) -> str:
        """
        Start marimo container and wait for it to be healthy.

        Returns:
            The container ID

        Raises:
            ProcessStartupException: If container fails to start or become healthy
        """
        import os

        # Generate container name
        container_name = f"marimo-{self.account_id}-{self.port}"

        # Relative path within container
        notebook_rel_path = self.notebook_path.relative_to(self.tenant_data_dir)

        # Build docker run command
        command = [
            "docker",
            "run",
            "--rm",  # Auto-remove on stop
            "--detach",  # Run in background
            "--name",
            container_name,
            # Resource limits
            f"--memory={self.memory_limit_mb}m",
            f"--cpus={self.cpu_limit}",
            # Network
            "--network=host",  # Use host network for simplicity
            # Volume mounts
            "-v",
            f"{self.tenant_data_dir}:/workspace",
            # Working directory
            "-w",
            "/workspace",
        ]

        # Add S3 credentials if available
        if "AWS_ACCESS_KEY_ID" in os.environ:
            command.extend(["-e", f"AWS_ACCESS_KEY_ID={os.environ['AWS_ACCESS_KEY_ID']}"])
        if "AWS_SECRET_ACCESS_KEY" in os.environ:
            command.extend(["-e", f"AWS_SECRET_ACCESS_KEY={os.environ['AWS_SECRET_ACCESS_KEY']}"])
        if "AWS_SESSION_TOKEN" in os.environ:
            command.extend(["-e", f"AWS_SESSION_TOKEN={os.environ['AWS_SESSION_TOKEN']}"])
        if "AWS_REGION" in os.environ:
            command.extend(["-e", f"AWS_REGION={os.environ['AWS_REGION']}"])

        # Image and command
        command.extend(
            [
                self.docker_image,
                "sh",
                "-c",
                f"pip install --quiet marimo duckdb && "
                f"python -m marimo edit --headless --no-token "
                f"--host 0.0.0.0 --port {self.port} {notebook_rel_path}",
            ]
        )

        logger.info(
            "starting_marimo_container",
            container_name=container_name,
            image=self.docker_image,
            port=self.port,
            memory_mb=self.memory_limit_mb,
            cpu_limit=self.cpu_limit,
            notebook=str(notebook_rel_path),
        )

        try:
            # Start container and capture container ID
            self.process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            # Wait for process to complete and get output
            stdout, stderr = await self.process.communicate()

            if self.process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                raise ProcessStartupException(f"Failed to start container: {error_msg}")

            self.container_id = stdout.decode().strip()

            logger.info(
                "marimo_container_started",
                container_id=self.container_id[:12],
                container_name=container_name,
                port=self.port,
            )

            # Wait for container and marimo to be healthy
            await self._wait_for_health()

            logger.info(
                "marimo_container_ready",
                container_id=self.container_id[:12],
                port=self.port,
            )

            return self.container_id

        except ProcessStartupException:
            await self._cleanup_failed_start()
            raise
        except Exception as e:
            logger.error(
                "marimo_container_start_failed",
                error=str(e),
                exc_info=True,
            )
            await self._cleanup_failed_start()
            raise ProcessStartupException(f"Unexpected error: {str(e)}")

    async def _wait_for_health(self) -> None:
        """
        Wait for marimo container to become healthy.

        Polls the health endpoint until it responds or timeout is reached.

        Raises:
            ProcessStartupException: If container doesn't become healthy in time
        """
        health_url = f"http://127.0.0.1:{self.port}/health"
        start_time = asyncio.get_event_loop().time()

        async with httpx.AsyncClient() as client:
            while True:
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed > self.startup_timeout:
                    logs = await self._read_container_logs()
                    raise ProcessStartupException(
                        f"Startup timeout after {self.startup_timeout}s. Container logs: {logs}"
                    )

                # Check if container is still running
                is_running = await self._check_container_running()
                if not is_running:
                    logs = await self._read_container_logs()
                    raise ProcessStartupException(f"Container stopped unexpectedly. Logs: {logs}")

                try:
                    response = await client.get(health_url, timeout=2.0)
                    if response.status_code == 200:
                        logger.debug(
                            "health_check_passed",
                            container_id=self.container_id[:12] if self.container_id else None,
                            port=self.port,
                            elapsed=f"{elapsed:.2f}s",
                        )
                        return
                except (httpx.RequestError, httpx.TimeoutException):
                    pass

                await asyncio.sleep(0.5)

    async def _check_container_running(self) -> bool:
        """
        Check if container is still running.

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
            return stdout.decode().strip() == "true"

        except Exception as e:
            logger.warning("container_check_failed", error=str(e))
            return False

    async def _read_container_logs(self) -> str:
        """
        Read container logs for error diagnostics.

        Returns:
            Container logs as string (last 50 lines)
        """
        if not self.container_id:
            return ""

        try:
            process = await asyncio.create_subprocess_exec(
                "docker",
                "logs",
                "--tail",
                "50",
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

    async def _cleanup_failed_start(self) -> None:
        """Clean up resources after failed startup."""
        if self.container_id:
            try:
                await asyncio.create_subprocess_exec("docker", "stop", "-t", "5", self.container_id)
            except Exception as e:
                logger.warning("cleanup_failed_start_error", error=str(e))

    async def check_health(self) -> bool:
        """
        Check if marimo container is healthy.

        Returns:
            True if healthy, False otherwise
        """
        if not self.container_id:
            return False

        # Check container is running
        is_running = await self._check_container_running()
        if not is_running:
            logger.warning(
                "container_not_running",
                container_id=self.container_id[:12],
                port=self.port,
            )
            return False

        # Check marimo health endpoint
        health_url = f"http://127.0.0.1:{self.port}/health"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(health_url, timeout=5.0)
                is_healthy = response.status_code == 200

                if is_healthy:
                    logger.debug(
                        "health_check_ok",
                        container_id=self.container_id[:12],
                        port=self.port,
                    )
                else:
                    logger.warning(
                        "health_check_failed",
                        container_id=self.container_id[:12],
                        port=self.port,
                        status=response.status_code,
                    )

                return is_healthy

        except Exception as e:
            logger.warning(
                "health_check_error",
                container_id=self.container_id[:12],
                port=self.port,
                error=str(e),
            )
            return False

    async def stop(self, timeout: int = 10) -> None:
        """
        Stop marimo container gracefully.

        Sends docker stop which gives the container time to shut down gracefully.
        If timeout is reached, Docker will forcefully kill the container.

        Args:
            timeout: Seconds to wait for graceful shutdown
        """
        if not self.container_id:
            return

        logger.info(
            "stopping_marimo_container",
            container_id=self.container_id[:12],
            port=self.port,
        )

        try:
            process = await asyncio.create_subprocess_exec(
                "docker",
                "stop",
                "-t",
                str(timeout),
                self.container_id,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            await process.wait()

            logger.info(
                "marimo_container_stopped",
                container_id=self.container_id[:12],
                port=self.port,
            )

        except Exception as e:
            logger.error(
                "marimo_container_stop_error",
                container_id=self.container_id[:12],
                error=str(e),
                exc_info=True,
            )

    def is_alive(self) -> bool:
        """
        Check if container is still running (synchronous check).

        Returns:
            True if container exists, False otherwise

        Note: This is a quick check. Use check_health() for full health validation.
        """
        return self.container_id is not None
