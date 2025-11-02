"""Marimo process management."""

import asyncio
import signal
from pathlib import Path
from typing import Optional

import httpx
import structlog

from duckpond.notebooks.exceptions import (
    ProcessHealthException,
    ProcessStartupException,
)

logger = structlog.get_logger(__name__)


class MarimoProcess:
    """
    Wrapper for marimo subprocess with health monitoring.

    Manages the lifecycle of a marimo editor process including:
    - Process spawning with proper environment
    - Startup health checks
    - Ongoing health monitoring
    - Graceful shutdown
    """

    def __init__(
        self,
        notebook_path: Path,
        port: int,
        tenant_data_dir: Path,
        startup_timeout: int = 30,
    ):
        """
        Initialize marimo process wrapper.

        Args:
            notebook_path: Absolute path to notebook file
            port: Port for marimo to listen on
            tenant_data_dir: Working directory for marimo process
            startup_timeout: Maximum seconds to wait for startup
        """
        self.notebook_path = notebook_path
        self.port = port
        self.tenant_data_dir = tenant_data_dir
        self.startup_timeout = startup_timeout
        self.process: Optional[asyncio.subprocess.Process] = None

    async def start(self) -> asyncio.subprocess.Process:
        """
        Start marimo process and wait for it to be healthy.

        Returns:
            The started subprocess

        Raises:
            ProcessStartupException: If process fails to start or become healthy
        """
        import os
        import sys

        # Inherit current environment and add DuckDB path
        env = os.environ.copy()

        # Ensure marimo uses the same Python for its kernel
        # This prevents marimo from using system Python instead of virtualenv Python
        # We prepend the virtualenv bin directory to PATH so that when marimo
        # spawns the kernel, it finds our virtualenv Python first
        venv_bin = os.path.dirname(sys.executable)
        if "PATH" in env:
            env["PATH"] = f"{venv_bin}:{env['PATH']}"
        else:
            env["PATH"] = venv_bin

        # Use sys.executable to run marimo with the same Python interpreter
        command = [
            sys.executable,
            "-m",
            "marimo",
            "edit",
            "--headless",
            "--no-token",
            "--host",
            "127.0.0.1",
            "--port",
            str(self.port),
            str(self.notebook_path),
        ]

        logger.info(
            "starting_marimo_process",
            command=" ".join(command),
            cwd=str(self.tenant_data_dir),
            port=self.port,
            notebook=str(self.notebook_path),
        )

        try:
            # Create log files for marimo output
            import tempfile

            log_dir = tempfile.gettempdir()
            stdout_log = f"{log_dir}/marimo_{self.port}_stdout.log"
            stderr_log = f"{log_dir}/marimo_{self.port}_stderr.log"

            stdout_file = open(stdout_log, "w")
            stderr_file = open(stderr_log, "w")

            logger.info(
                "marimo_logs_created",
                stdout_log=stdout_log,
                stderr_log=stderr_log,
            )

            self.process = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(self.tenant_data_dir),
                env=env,
                stdout=stdout_file,
                stderr=stderr_file,
            )

            logger.info(
                "marimo_process_spawned",
                pid=self.process.pid,
                port=self.port,
            )

            await self._wait_for_health()

            logger.info(
                "marimo_process_ready",
                pid=self.process.pid,
                port=self.port,
            )

            return self.process

        except ProcessStartupException:
            await self._cleanup_failed_start()
            raise
        except Exception as e:
            logger.error(
                "marimo_process_start_failed",
                error=str(e),
                exc_info=True,
            )
            await self._cleanup_failed_start()
            raise ProcessStartupException(f"Unexpected error: {str(e)}")

    async def _wait_for_health(self) -> None:
        """
        Wait for marimo process to become healthy.

        Polls the health endpoint until it responds or timeout is reached.

        Raises:
            ProcessStartupException: If process doesn't become healthy in time
        """
        health_url = f"http://127.0.0.1:{self.port}/health"
        start_time = asyncio.get_event_loop().time()

        async with httpx.AsyncClient() as client:
            while True:
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed > self.startup_timeout:
                    stderr = await self._read_stderr()
                    raise ProcessStartupException(
                        f"Startup timeout after {self.startup_timeout}s. Stderr: {stderr}"
                    )

                if self.process and self.process.returncode is not None:
                    stderr = await self._read_stderr()
                    raise ProcessStartupException(
                        f"Process exited with code {self.process.returncode}. Stderr: {stderr}"
                    )

                try:
                    response = await client.get(health_url, timeout=2.0)
                    if response.status_code == 200:
                        logger.debug(
                            "health_check_passed",
                            port=self.port,
                            elapsed=f"{elapsed:.2f}s",
                        )
                        return
                except (httpx.RequestError, httpx.TimeoutException):
                    pass

                await asyncio.sleep(0.5)

    async def _read_stderr(self) -> str:
        """
        Read stderr from process for error diagnostics.

        Returns:
            Stderr content as string (up to 1KB)
        """
        if not self.process or not self.process.stderr:
            return ""

        try:
            stderr_bytes = await asyncio.wait_for(
                self.process.stderr.read(1024), timeout=1.0
            )
            return stderr_bytes.decode("utf-8", errors="replace")
        except asyncio.TimeoutError:
            return "(stderr read timeout)"
        except Exception as e:
            return f"(error reading stderr: {str(e)})"

    async def _cleanup_failed_start(self) -> None:
        """Clean up resources after failed startup."""
        if self.process:
            try:
                self.process.terminate()
                await asyncio.wait_for(self.process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                if self.process:
                    self.process.kill()
            except Exception as e:
                logger.warning(
                    "cleanup_failed_start_error",
                    error=str(e),
                )

    async def check_health(self) -> bool:
        """
        Check if marimo process is healthy.

        Returns:
            True if healthy, False otherwise
        """
        if not self.process or self.process.returncode is not None:
            logger.warning(
                "process_not_running",
                port=self.port,
                returncode=self.process.returncode if self.process else None,
            )
            return False

        health_url = f"http://127.0.0.1:{self.port}/health"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(health_url, timeout=5.0)
                is_healthy = response.status_code == 200

                if is_healthy:
                    logger.debug("health_check_ok", port=self.port)
                else:
                    logger.warning(
                        "health_check_failed",
                        port=self.port,
                        status=response.status_code,
                    )

                return is_healthy

        except Exception as e:
            logger.warning(
                "health_check_error",
                port=self.port,
                error=str(e),
            )
            return False

    async def stop(self, timeout: int = 5) -> None:
        """
        Stop marimo process gracefully.

        Sends SIGTERM and waits for graceful shutdown. If timeout is reached,
        sends SIGKILL to force termination.

        Args:
            timeout: Seconds to wait for graceful shutdown
        """
        if not self.process:
            return

        pid = self.process.pid
        logger.info("stopping_marimo_process", pid=pid, port=self.port)

        try:
            self.process.send_signal(signal.SIGTERM)

            try:
                await asyncio.wait_for(self.process.wait(), timeout=timeout)
                logger.info(
                    "marimo_process_stopped_gracefully",
                    pid=pid,
                    port=self.port,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "marimo_process_timeout_killing",
                    pid=pid,
                    port=self.port,
                )
                self.process.kill()
                await self.process.wait()
                logger.info("marimo_process_killed", pid=pid, port=self.port)

        except ProcessLookupError:
            logger.debug("marimo_process_already_stopped", pid=pid)
        except Exception as e:
            logger.error(
                "marimo_process_stop_error",
                pid=pid,
                error=str(e),
                exc_info=True,
            )

    def is_alive(self) -> bool:
        """
        Check if process is still running.

        Returns:
            True if process is running, False otherwise
        """
        if not self.process:
            return False
        return self.process.returncode is None
