"""Database session management and context managers."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

logger = structlog.get_logger()


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """
    Create async session factory.

    Args:
        engine: SQLAlchemy async engine

    Returns:
        Configured async session maker
    """
    return async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )


@asynccontextmanager
async def get_session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager for database sessions with automatic transaction handling.

    Args:
        session_factory: Async session maker

    Yields:
        Database session

    Example:
        ```python
        async with get_session(session_factory) as session:
            result = await session.execute(select(Tenant))
            tenants = result.scalars().all()
        ```
    """
    session = session_factory()
    try:
        yield session
        await session.commit()
        logger.debug("Session committed successfully")
    except Exception as e:
        await session.rollback()
        logger.error("Session rolled back due to error", error=str(e))
        raise
    finally:
        await session.close()
        logger.debug("Session closed")


@asynccontextmanager
async def get_session_no_commit(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager for read-only sessions without automatic commit.

    Use this for read-only operations or when manual transaction control is needed.

    Args:
        session_factory: Async session maker

    Yields:
        Database session

    Example:
        ```python
        async with get_session_no_commit(session_factory) as session:
            result = await session.execute(select(Tenant))
            tenants = result.scalars().all()
        ```
    """
    session = session_factory()
    try:
        yield session
    except Exception as e:
        await session.rollback()
        logger.error("Session rolled back due to error", error=str(e))
        raise
    finally:
        await session.close()
        logger.debug("Session closed")


class DatabaseSession:
    """
    Database session manager with connection pooling.

    This class manages the lifecycle of database connections and sessions,
    providing both context managers and direct session access.
    """

    def __init__(self, engine: AsyncEngine):
        """
        Initialize session manager.

        Args:
            engine: SQLAlchemy async engine
        """
        self.engine = engine
        self.session_factory = create_session_factory(engine)
        logger.info("Database session manager initialized")

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Get a database session with automatic transaction handling.

        Yields:
            Database session
        """
        async with get_session(self.session_factory) as session:
            yield session

    @asynccontextmanager
    async def session_no_commit(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Get a read-only database session without automatic commit.

        Yields:
            Database session
        """
        async with get_session_no_commit(self.session_factory) as session:
            yield session

    async def close(self) -> None:
        """Close all connections and dispose of the engine."""
        logger.info("Closing database session manager")
        await self.engine.dispose()
        logger.debug("Database engine disposed")


_global_engine: AsyncEngine | None = None


def get_engine() -> AsyncEngine:
    """
    Get global database engine.

    This is used for FastAPI dependency injection.

    Returns:
        Global AsyncEngine instance

    Raises:
        RuntimeError: If engine is not initialized
    """
    from duckpond.config import get_settings

    global _global_engine
    if _global_engine is None:
        from duckpond.db import base

        settings = get_settings()
        _global_engine = base.create_engine(settings)

    assert _global_engine is not None
    return _global_engine


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency for database sessions.

    Yields:
        Database session for request handling

    Example:
        ```python
        @app.get("/items")
        async def get_items(session: AsyncSession = Depends(get_db_session)):
            result = await session.execute(select(Item))
            return result.scalars().all()
        ```
    """
    engine = get_engine()
    session_factory = create_session_factory(engine)
    async with get_session(session_factory) as session:
        yield session
