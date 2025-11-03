"""Docker-specific exceptions for DuckPond."""


class DockerException(Exception):
    """Base exception for Docker operations."""

    pass


class ContainerStartupException(DockerException):
    """Raised when container fails to start."""

    pass


class ContainerHealthCheckException(DockerException):
    """Raised when container health check fails."""

    pass


class ContainerStopException(DockerException):
    """Raised when container fails to stop gracefully."""

    pass


class ContainerNotFoundException(DockerException):
    """Raised when container is not found."""

    pass


class ContainerExecutionException(DockerException):
    """Raised when command execution in container fails."""

    pass


class DockerImageException(DockerException):
    """Raised when Docker image is invalid or unavailable."""

    pass


class DockerVolumeException(DockerException):
    """Raised when volume mounting fails."""

    pass


class DockerNetworkException(DockerException):
    """Raised when network configuration fails."""

    pass
