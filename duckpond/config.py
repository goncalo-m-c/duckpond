"""Configuration management using pydantic-settings."""

from pathlib import Path
from typing import Any, Literal, Tuple, Type

import yaml
from pydantic import Field, field_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


def load_yaml_config(config_path: Path | None = None) -> dict[str, Any]:
    """
    Load configuration from YAML file.

    Priority:
    1. Explicitly provided config_path
    2. ~/.duckpond/config.yaml (default location)
    3. Empty dict if no file exists

    Args:
        config_path: Optional path to config file

    Returns:
        Dictionary of configuration values (flattened from nested YAML)
    """
    if config_path is None:
        config_path = Path.home() / ".duckpond" / "config.yaml"

    if not config_path.exists():
        return {}

    try:
        with open(config_path) as f:
            yaml_data = yaml.safe_load(f) or {}

        flattened = {}

        if "server" in yaml_data:
            server = yaml_data["server"]
            if "host" in server:
                flattened["duckpond_host"] = server["host"]
            if "port" in server:
                flattened["duckpond_port"] = server["port"]
            if "workers" in server:
                flattened["duckpond_workers"] = server["workers"]

        if "database" in yaml_data:
            db = yaml_data["database"]
            if "url" in db:
                flattened["metadata_db_url"] = db["url"]
            if "pool_size" in db:
                flattened["db_pool_size"] = db["pool_size"]
            if "max_overflow" in db:
                flattened["db_max_overflow"] = db["max_overflow"]
            if "pool_timeout" in db:
                flattened["db_pool_timeout"] = db["pool_timeout"]
            if "pool_recycle" in db:
                flattened["db_pool_recycle"] = db["pool_recycle"]

        if "storage" in yaml_data:
            storage = yaml_data["storage"]
            if "default_backend" in storage:
                flattened["default_storage_backend"] = storage["default_backend"]
            if "local_path" in storage:
                flattened["local_storage_path"] = storage["local_path"]
            if "s3_bucket" in storage:
                flattened["s3_bucket"] = storage["s3_bucket"]
            if "s3_region" in storage:
                flattened["s3_region"] = storage["s3_region"]
            if "s3_endpoint_url" in storage:
                flattened["s3_endpoint_url"] = storage["s3_endpoint_url"]

        if "duckdb" in yaml_data:
            duckdb = yaml_data["duckdb"]
            if "memory_limit" in duckdb:
                flattened["duckdb_memory_limit"] = duckdb["memory_limit"]
            if "threads" in duckdb:
                flattened["duckdb_threads"] = duckdb["threads"]
            if "pool_size" in duckdb:
                flattened["duckdb_pool_size"] = duckdb["pool_size"]

        if "limits" in yaml_data:
            limits = yaml_data["limits"]
            if "max_file_size_mb" in limits:
                flattened["max_file_size_mb"] = limits["max_file_size_mb"]
            if "default_max_storage_gb" in limits:
                flattened["default_max_storage_gb"] = limits["default_max_storage_gb"]
            if "default_max_query_memory_gb" in limits:
                flattened["default_max_query_memory_gb"] = limits[
                    "default_max_query_memory_gb"
                ]
            if "default_max_concurrent_queries" in limits:
                flattened["default_max_concurrent_queries"] = limits[
                    "default_max_concurrent_queries"
                ]
            if "api_key_cache_ttl_seconds" in limits:
                flattened["api_key_cache_ttl_seconds"] = limits[
                    "api_key_cache_ttl_seconds"
                ]

        if "streaming" in yaml_data:
            streaming = yaml_data["streaming"]
            if "buffer_size" in streaming:
                flattened["stream_buffer_size"] = streaming["buffer_size"]
            if "flush_interval_seconds" in streaming:
                flattened["stream_flush_interval_seconds"] = streaming[
                    "flush_interval_seconds"
                ]

        if "upload" in yaml_data:
            upload = yaml_data["upload"]
            if "temp_dir" in upload:
                flattened["temp_upload_dir"] = upload["temp_dir"]

        if "wal" in yaml_data:
            wal = yaml_data["wal"]
            if "enabled" in wal:
                flattened["wal_enabled"] = wal["enabled"]
            if "sync_mode" in wal:
                flattened["wal_sync_mode"] = wal["sync_mode"]
            if "directory" in wal:
                flattened["wal_directory"] = wal["directory"]

        if "catalog" in yaml_data:
            catalog = yaml_data["catalog"]
            if "enabled" in catalog:
                flattened["catalog_enabled"] = catalog["enabled"]

        if "logging" in yaml_data:
            logging = yaml_data["logging"]
            if "level" in logging:
                flattened["log_level"] = logging["level"]
            if "format" in logging:
                flattened["log_format"] = logging["format"]

        return flattened

    except Exception as e:
        import warnings

        warnings.warn(f"Failed to load config from {config_path}: {e}")
        return {}


_config_path: Path | None = None


class YamlSettingsSource(PydanticBaseSettingsSource):
    """
    Custom settings source that loads configuration from YAML file.

    This allows YAML config to be loaded with proper priority in the settings chain.
    """

    def get_field_value(self, field: Any, field_name: str) -> Tuple[Any, str, bool]:
        """Not used since we override __call__."""
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Load configuration from YAML file."""
        return load_yaml_config(_config_path)


class Settings(BaseSettings):
    """
    DuckPond configuration settings.

    Configuration priority (highest to lowest):
    1. Environment variables (e.g., DUCKPOND_PORT=9000)
    2. YAML configuration file (~/.duckpond/config.yaml)
    3. Default values defined in this class
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    duckpond_host: str = Field(default="0.0.0.0", description="Server bind address")
    duckpond_port: int = Field(default=8000, ge=1, le=65535, description="Server port")
    duckpond_workers: int = Field(
        default=4, ge=1, description="Number of worker processes"
    )

    metadata_db_url: str = Field(
        default="sqlite:///~/.duckpond/metadata.db",
        description="SQLAlchemy database URL",
    )
    db_pool_size: int = Field(
        default=10,
        ge=1,
        description="Database connection pool size (PostgreSQL only)",
    )
    db_max_overflow: int = Field(
        default=20,
        ge=0,
        description="Max overflow connections beyond pool_size",
    )
    db_pool_timeout: int = Field(
        default=30,
        ge=1,
        description="Timeout in seconds for getting connection from pool",
    )
    db_pool_recycle: int = Field(
        default=3600,
        ge=0,
        description="Recycle connections after this many seconds",
    )

    default_storage_backend: Literal["local", "s3"] = Field(
        default="local",
        description="Default storage backend",
    )
    local_storage_path: Path = Field(
        default=Path("~/.duckpond/data"),
        description="Local storage root path",
    )
    s3_bucket: str | None = Field(default=None, description="S3 bucket name")
    s3_region: str = Field(default="us-east-1", description="S3 region")
    s3_endpoint_url: str | None = Field(
        default=None,
        description="Custom S3 endpoint (for MinIO, etc.)",
    )

    catalog_enabled: bool = Field(default=True, description="Enable DuckLake catalog")

    default_max_storage_gb: int = Field(
        default=100, ge=1, description="Default storage quota"
    )
    default_max_query_memory_gb: int = Field(
        default=4,
        ge=1,
        description="Default query memory limit",
    )
    default_max_concurrent_queries: int = Field(
        default=10,
        ge=1,
        description="Default concurrent query limit",
    )
    api_key_cache_ttl_seconds: int = Field(
        default=30,
        ge=0,
        description="API key cache TTL",
    )

    max_file_size_mb: int = Field(
        default=1000, ge=1, description="Max file upload size"
    )
    temp_upload_dir: Path = Field(
        default=Path("/tmp/duckpond/uploads"),
        description="Temporary upload directory",
    )

    stream_buffer_size: int = Field(
        default=100000,
        ge=1000,
        description="Stream buffer size in records",
    )
    stream_flush_interval_seconds: int = Field(
        default=60,
        ge=1,
        description="Stream flush interval",
    )

    wal_enabled: bool = Field(default=False, description="Enable Write-Ahead Log")
    wal_sync_mode: Literal["fsync", "fdatasync", "async"] = Field(
        default="fdatasync",
        description="WAL sync mode",
    )
    wal_directory: Path = Field(
        default=Path("~/.duckpond/wal"),
        description="WAL directory",
    )

    duckdb_memory_limit: str = Field(default="4GB", description="DuckDB memory limit")
    duckdb_threads: int = Field(default=4, ge=1, description="DuckDB thread count")
    duckdb_pool_size: int = Field(
        default=10, ge=1, description="DuckDB connection pool size"
    )

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging level",
    )
    log_format: Literal["json", "text"] = Field(
        default="json", description="Log format"
    )

    notebook_session_timeout_seconds: int = Field(
        default=3600,
        ge=300,
        description="Notebook session inactivity timeout (default 1 hour, min 5 minutes)",
    )
    notebook_enabled: bool = Field(
        default=True,
        description="Enable marimo notebook integration",
    )
    notebook_port_range_start: int = Field(
        default=10000,
        ge=1024,
        le=65535,
        description="Start of port range for marimo processes",
    )
    notebook_port_range_end: int = Field(
        default=20000,
        ge=1024,
        le=65535,
        description="End of port range for marimo processes",
    )
    notebook_max_concurrent_sessions: int = Field(
        default=100,
        ge=1,
        description="Maximum concurrent notebook sessions across all tenants",
    )
    notebook_max_memory_mb: int = Field(
        default=2048,
        ge=256,
        description="Maximum memory per notebook session in MB",
    )
    notebook_startup_timeout_seconds: int = Field(
        default=30,
        ge=5,
        description="Timeout for marimo process startup",
    )
    notebook_health_check_interval_seconds: int = Field(
        default=30,
        ge=10,
        description="Interval for notebook process health checks",
    )
    notebook_docker_image: str = Field(
        default="python:3.12-slim",
        description="Docker image to use for marimo containers",
    )
    notebook_cpu_limit: float = Field(
        default=2.0,
        ge=0.1,
        le=16.0,
        description="CPU limit per notebook container (1.0 = 1 core)",
    )

    @field_validator("local_storage_path", "temp_upload_dir", "wal_directory")
    @classmethod
    def validate_paths(cls, v: Path) -> Path:
        """Ensure paths are absolute."""
        if not v.is_absolute():
            v = v.expanduser().resolve()
        return v

    @field_validator("metadata_db_url")
    @classmethod
    def validate_db_url(cls, v: str) -> str:
        """Validate database URL format and expand ~ in SQLite paths."""
        if not (v.startswith("sqlite://") or v.startswith("postgresql")):
            raise ValueError("Database URL must be SQLite or PostgreSQL")

        if v.startswith("sqlite:///"):
            path_part = v[10:]

            if path_part.startswith("~"):
                from pathlib import Path

                expanded_path = Path(path_part).expanduser()
                v = f"sqlite:///{expanded_path}"

        return v

    @property
    def is_sqlite(self) -> bool:
        """Check if using SQLite backend."""
        return self.metadata_db_url.startswith("sqlite")

    @property
    def is_postgresql(self) -> bool:
        """Check if using PostgreSQL backend."""
        return self.metadata_db_url.startswith("postgresql")

    @property
    def max_file_size_bytes(self) -> int:
        """Get max file size in bytes."""
        return self.max_file_size_mb * 1024 * 1024

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """
        Customize settings sources priority.

        Priority order (highest to lowest):
        1. Explicit kwargs (init_settings) - for testing and programmatic config
        2. Environment variables
        3. YAML configuration file
        4. .env file
        5. Field defaults
        """
        return (
            init_settings,
            env_settings,
            YamlSettingsSource(settings_cls),
            dotenv_settings,
        )


_settings: Settings | None = None


def get_settings(config_path: Path | None = None, reload: bool = False) -> Settings:
    """
    Get global settings instance.

    Configuration loading order (highest to lowest priority):
    1. Environment variables (e.g., DUCKPOND_PORT=9000)
    2. YAML configuration file (~/.duckpond/config.yaml)
    3. .env file
    4. Default values defined in Settings class

    Args:
        config_path: Optional path to YAML config file (defaults to ~/.duckpond/config.yaml)
        reload: If True, force reload settings (useful for testing)

    Returns:
        Settings instance with merged configuration
    """
    global _settings, _config_path
    if _settings is None or reload:
        _config_path = config_path

        _settings = Settings()
    return _settings


def reset_settings() -> None:
    """Reset settings singleton (useful for testing)."""
    global _settings
    _settings = None


settings = get_settings()
