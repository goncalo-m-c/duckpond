"""Database engine configuration and initialization."""

from pathlib import Path

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import AsyncAdaptedQueuePool, NullPool

from duckpond.config import Settings

logger = structlog.get_logger()


class Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""

    pass


def create_engine(settings: Settings) -> AsyncEngine:
    """
    Create async SQLAlchemy engine based on configuration.

    Args:
        settings: Application settings with database URL

    Returns:
        Configured async engine

    Raises:
        ValueError: If database URL is invalid or unsupported
    """
    db_url = settings.metadata_db_url
    logger.info(
        "Creating database engine",
        database_type="sqlite" if settings.is_sqlite else "postgresql",
        url=db_url.split("@")[-1] if "@" in db_url else db_url,
    )

    if settings.is_sqlite:
        if not db_url.startswith("sqlite+aiosqlite://"):
            db_url = db_url.replace("sqlite://", "sqlite+aiosqlite://", 1)

        db_path_str = db_url.split("///", 1)[-1] if "///" in db_url else None
        if db_path_str:
            db_path = Path(db_path_str)
            db_dir = db_path.parent
            if not db_dir.exists():
                logger.info("Creating database directory", path=str(db_dir))
                db_dir.mkdir(parents=True, exist_ok=True)

        engine = create_async_engine(
            db_url,
            echo=False,
            poolclass=NullPool,
            connect_args={
                "check_same_thread": False,
            },
        )
        logger.debug("SQLite engine created", url=db_url)

    elif settings.is_postgresql:
        if not db_url.startswith("postgresql+asyncpg://"):
            db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

        pool_size = settings.db_pool_size
        max_overflow = settings.db_max_overflow
        pool_timeout = settings.db_pool_timeout
        pool_recycle = settings.db_pool_recycle

        engine = create_async_engine(
            db_url,
            echo=False,
            poolclass=AsyncAdaptedQueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            pool_pre_ping=True,
        )
        logger.debug(
            "PostgreSQL engine created",
            pool_size=pool_size,
            max_overflow=max_overflow,
        )

    else:
        raise ValueError(f"Unsupported database URL: {db_url}")

    return engine


async def check_connection(engine: AsyncEngine) -> bool:
    """
    Check if database connection is healthy.

    Args:
        engine: SQLAlchemy async engine

    Returns:
        True if connection is healthy, False otherwise
    """
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        logger.debug("Database connection check successful")
        return True
    except Exception as e:
        logger.error("Database connection check failed", error=str(e))
        return False


async def init_db(engine: AsyncEngine) -> None:
    """
    Initialize database schema.

    Args:
        engine: SQLAlchemy async engine
    """
    logger.info("Initializing database schema")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database schema initialized successfully")


async def dispose_engine(engine: AsyncEngine) -> None:
    """
    Dispose of engine and close all connections.

    Args:
        engine: SQLAlchemy async engine
    """
    logger.info("Disposing database engine")
    await engine.dispose()
    logger.debug("Database engine disposed")
