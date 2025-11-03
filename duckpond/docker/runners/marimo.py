"""Marimo notebook Docker runner.

This module provides a specialized Docker runner for marimo notebooks
with proper isolation, resource limits, and health monitoring.
"""

from pathlib import Path
from typing import Optional

import structlog

from duckpond.docker.config import ContainerConfig
from duckpond.docker.container import DockerContainer

logger = structlog.get_logger(__name__)


class MarimoRunner:
    """
    Docker runner for marimo notebook processes.

    Provides a high-level interface for running marimo notebooks in isolated
    Docker containers with automatic health monitoring and lifecycle management.

    Features:
    - Isolated execution environment
    - Configurable resource limits
    - Automatic AWS credentials passing
    - Health check integration
    - Graceful shutdown

    Usage:
        runner = MarimoRunner(
            notebook_path=Path("/path/to/notebook.py"),
            port=8080,
            account_data_dir=Path("/data/account123"),
            account_id="account123",
        )
        container_id = await runner.start()
        is_healthy = await runner.check_health()
        await runner.stop()
    """

    def __init__(
        self,
        notebook_path: Path,
        port: int,
        account_data_dir: Path,
        account_id: str,
        docker_image: str = "duckpond:25.1",
        memory_limit_mb: int = 2048,
        cpu_limit: float = 2.0,
        startup_timeout: int = 30,
    ):
        """
        Initialize marimo notebook runner.

        Args:
            notebook_path: Absolute path to notebook file
            port: Port for marimo to listen on
            account_data_dir: Working directory for marimo process
            account_id: Tenant identifier for container naming
            docker_image: Docker image to use
            memory_limit_mb: Memory limit in megabytes
            cpu_limit: CPU limit (1.0 = 1 core)
            startup_timeout: Maximum seconds to wait for startup
        """
        self.notebook_path = notebook_path
        self.port = port
        self.account_data_dir = account_data_dir
        self.account_id = account_id
        self.docker_image = docker_image
        self.memory_limit_mb = memory_limit_mb
        self.cpu_limit = cpu_limit
        self.startup_timeout = startup_timeout

        # Build container configuration
        self.config = self._build_config()
        self.container = DockerContainer(self.config)

    def _build_config(self) -> ContainerConfig:
        """
        Build Docker container configuration for marimo.

        Returns:
            ContainerConfig with marimo-specific settings
        """
        # Generate container name
        container_name = f"marimo-{self.account_id}-{self.port}"

        # Relative path within container
        notebook_rel_path = self.notebook_path.relative_to(self.account_data_dir)

        # Build marimo command
        marimo_command = [
            "sh",
            "-c",
            f"pip install --quiet marimo duckdb && "
            f"python -m marimo edit --headless --no-token "
            f"--host 0.0.0.0 --port {self.port} {notebook_rel_path}",
        ]

        # Create base configuration
        config = ContainerConfig(
            image=self.docker_image,
            command=marimo_command,
            name=container_name,
            startup_timeout_seconds=self.startup_timeout,
        )

        # Add volume mount
        config.add_volume(
            host_path=self.account_data_dir,
            container_path="/workspace",
            read_only=False,
        )

        # Set working directory
        config.working_dir = "/workspace"

        # Use host network for simplicity
        config.use_host_network()

        # Set resource limits
        config.set_resources(
            memory_mb=self.memory_limit_mb,
            cpu_limit=self.cpu_limit,
        )

        # Add AWS credentials from environment
        aws_env = DockerContainer.get_aws_credentials_env()
        if aws_env:
            config.add_env_from_dict(aws_env)
            logger.debug(
                "added_aws_credentials",
                container_name=container_name,
                keys=list(aws_env.keys()),
            )

        return config

    async def start(self) -> str:
        """
        Start marimo container and wait for it to be healthy.

        Returns:
            The container ID

        Raises:
            ContainerStartupException: If container fails to start or become healthy
        """
        logger.info(
            "starting_marimo_runner",
            account_id=self.account_id,
            port=self.port,
            notebook=str(self.notebook_path.name),
            memory_mb=self.memory_limit_mb,
            cpu_limit=self.cpu_limit,
        )

        # Start container with health check
        health_url = f"http://127.0.0.1:{self.port}/health"
        container_id = await self.container.start(health_check_url=health_url)

        logger.info(
            "marimo_runner_started",
            container_id=container_id[:12],
            account_id=self.account_id,
            port=self.port,
        )

        return container_id

    async def check_health(self) -> bool:
        """
        Check if marimo container is healthy.

        Returns:
            True if healthy, False otherwise
        """
        health_url = f"http://127.0.0.1:{self.port}/health"
        return await self.container.check_health(health_url=health_url)

    async def stop(self, timeout: int = 10) -> None:
        """
        Stop marimo container gracefully.

        Args:
            timeout: Seconds to wait for graceful shutdown
        """
        logger.info(
            "stopping_marimo_runner",
            account_id=self.account_id,
            port=self.port,
        )

        await self.container.stop(timeout=timeout)

        logger.info(
            "marimo_runner_stopped",
            account_id=self.account_id,
            port=self.port,
        )

    async def get_logs(self, tail: int = 100) -> str:
        """
        Get container logs.

        Args:
            tail: Number of lines to read from end

        Returns:
            Container logs as string
        """
        return await self.container.get_logs(tail=tail)

    def is_alive(self) -> bool:
        """
        Check if container exists (synchronous check).

        Returns:
            True if container was started, False otherwise
        """
        return self.container.is_alive()

    async def is_running(self) -> bool:
        """
        Check if container is currently running.

        Returns:
            True if container is running, False otherwise
        """
        return await self.container.is_running()

    def get_container_id(self) -> Optional[str]:
        """
        Get container ID.

        Returns:
            Container ID if started, None otherwise
        """
        return self.container.get_container_id()

    def get_url(self) -> str:
        """
        Get marimo notebook URL.

        Returns:
            URL to access marimo notebook
        """
        return f"http://127.0.0.1:{self.port}"
