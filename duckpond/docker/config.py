"""Docker container configuration models."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class VolumeMount:
    """Docker volume mount configuration."""

    host_path: Path
    container_path: str
    read_only: bool = False

    def to_docker_arg(self) -> str:
        """Convert to Docker -v argument."""
        mount_str = f"{self.host_path}:{self.container_path}"
        if self.read_only:
            mount_str += ":ro"
        return mount_str


@dataclass
class PortMapping:
    """Docker port mapping configuration."""

    host_port: int
    container_port: int
    protocol: str = "tcp"

    def to_docker_arg(self) -> str:
        """Convert to Docker -p argument."""
        return f"{self.host_port}:{self.container_port}/{self.protocol}"


@dataclass
class ResourceLimits:
    """Docker resource limits configuration."""

    memory_mb: Optional[int] = None
    cpu_limit: Optional[float] = None
    cpu_shares: Optional[int] = None

    def to_docker_args(self) -> List[str]:
        """Convert to Docker resource limit arguments."""
        args = []
        if self.memory_mb:
            args.extend([f"--memory={self.memory_mb}m"])
        if self.cpu_limit:
            args.extend([f"--cpus={self.cpu_limit}"])
        if self.cpu_shares:
            args.extend([f"--cpu-shares={self.cpu_shares}"])
        return args


@dataclass
class NetworkConfig:
    """Docker network configuration."""

    mode: str = "bridge"  # bridge, host, none, container:<name|id>
    hostname: Optional[str] = None
    dns: List[str] = field(default_factory=list)

    def to_docker_args(self) -> List[str]:
        """Convert to Docker network arguments."""
        args = [f"--network={self.mode}"]
        if self.hostname:
            args.extend([f"--hostname={self.hostname}"])
        for dns_server in self.dns:
            args.extend([f"--dns={dns_server}"])
        return args


@dataclass
class ContainerConfig:
    """Complete Docker container configuration.

    This configuration model provides all settings needed to run
    a Docker container with proper isolation, resource limits,
    and environment configuration.
    """

    # Required fields
    image: str
    command: List[str]
    name: str

    # Optional fields
    volumes: List[VolumeMount] = field(default_factory=list)
    ports: List[PortMapping] = field(default_factory=list)
    environment: Dict[str, str] = field(default_factory=dict)
    working_dir: Optional[str] = None
    resources: ResourceLimits = field(default_factory=ResourceLimits)
    network: NetworkConfig = field(default_factory=NetworkConfig)

    # Container behavior
    detach: bool = True
    auto_remove: bool = True
    interactive: bool = False
    tty: bool = False

    # Timeouts
    startup_timeout_seconds: int = 30
    stop_timeout_seconds: int = 10
    health_check_interval_seconds: float = 0.5

    def to_docker_run_args(self) -> List[str]:
        """Convert configuration to docker run command arguments.

        Returns:
            List of command-line arguments for docker run
        """
        args = ["docker", "run"]

        # Basic flags
        if self.detach:
            args.append("--detach")
        if self.auto_remove:
            args.append("--rm")
        if self.interactive:
            args.append("--interactive")
        if self.tty:
            args.append("--tty")

        # Container name
        args.extend(["--name", self.name])

        # Resource limits
        args.extend(self.resources.to_docker_args())

        # Network configuration
        args.extend(self.network.to_docker_args())

        # Volumes
        for volume in self.volumes:
            args.extend(["-v", volume.to_docker_arg()])

        # Ports
        for port in self.ports:
            args.extend(["-p", port.to_docker_arg()])

        # Environment variables
        for key, value in self.environment.items():
            args.extend(["-e", f"{key}={value}"])

        # Working directory
        if self.working_dir:
            args.extend(["-w", self.working_dir])

        # Image
        args.append(self.image)

        # Command
        args.extend(self.command)

        return args

    def add_volume(self, host_path: Path, container_path: str, read_only: bool = False) -> None:
        """Add a volume mount to the configuration."""
        self.volumes.append(VolumeMount(host_path, container_path, read_only))

    def add_port(self, host_port: int, container_port: int, protocol: str = "tcp") -> None:
        """Add a port mapping to the configuration."""
        self.ports.append(PortMapping(host_port, container_port, protocol))

    def add_env(self, key: str, value: str) -> None:
        """Add an environment variable to the configuration."""
        self.environment[key] = value

    def add_env_from_dict(self, env_vars: Dict[str, str]) -> None:
        """Add multiple environment variables from a dictionary."""
        self.environment.update(env_vars)

    def set_resources(
        self,
        memory_mb: Optional[int] = None,
        cpu_limit: Optional[float] = None,
        cpu_shares: Optional[int] = None,
    ) -> None:
        """Set resource limits for the container."""
        if memory_mb is not None:
            self.resources.memory_mb = memory_mb
        if cpu_limit is not None:
            self.resources.cpu_limit = cpu_limit
        if cpu_shares is not None:
            self.resources.cpu_shares = cpu_shares

    def use_host_network(self) -> None:
        """Configure container to use host network mode."""
        self.network.mode = "host"

    def use_bridge_network(self) -> None:
        """Configure container to use bridge network mode."""
        self.network.mode = "bridge"
