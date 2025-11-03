"""Docker container management for DuckPond.

This module provides Docker-based isolation for running workloads
such as notebooks and query execution.

Key components:
- DockerContainer: Generic container lifecycle management
- ContainerConfig: Container configuration models
- Runners: Specialized runners for marimo notebooks and queries
"""

from duckpond.docker.config import (
    ContainerConfig,
    NetworkConfig,
    PortMapping,
    ResourceLimits,
    VolumeMount,
)
from duckpond.docker.container import DockerContainer
from duckpond.docker.exceptions import (
    ContainerExecutionException,
    ContainerHealthCheckException,
    ContainerNotFoundException,
    ContainerStartupException,
    ContainerStopException,
    DockerException,
    DockerImageException,
    DockerNetworkException,
    DockerVolumeException,
)

__all__ = [
    # Core classes
    "DockerContainer",
    "ContainerConfig",
    # Configuration models
    "VolumeMount",
    "PortMapping",
    "ResourceLimits",
    "NetworkConfig",
    # Exceptions
    "DockerException",
    "ContainerStartupException",
    "ContainerHealthCheckException",
    "ContainerStopException",
    "ContainerNotFoundException",
    "ContainerExecutionException",
    "DockerImageException",
    "DockerVolumeException",
    "DockerNetworkException",
]
