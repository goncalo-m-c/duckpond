"""Alembic migration environment configuration for async SQLAlchemy."""
import asyncio
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context

# Import our application's base and settings
from duckpond.config import settings
from duckpond.db.base import Base

# Import all models so Alembic can detect them
from duckpond.tenants.models import APIKey, Tenant  # noqa: F401

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set the database URL from our application settings
# This overrides the placeholder in alembic.ini
# But only if a URL hasn't been explicitly set already
# (e.g., by programmatic migration functions)
db_url = config.get_main_option("sqlalchemy.url")
if not db_url or db_url == "":
    db_url = settings.metadata_db_url
    
    # Convert to async driver URLs
    if db_url.startswith("sqlite://") and "aiosqlite" not in db_url:
        db_url = db_url.replace("sqlite://", "sqlite+aiosqlite://", 1)
    elif db_url.startswith("postgresql://") and "asyncpg" not in db_url:
        db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    
    config.set_main_option("sqlalchemy.url", db_url)

# Add your model's MetaData object here for 'autogenerate' support
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Execute migrations within a connection context."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Run migrations in 'online' mode with async engine.

    In this scenario we need to create an async Engine
    and associate a connection with the context.
    """
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
        # Commit the transaction to ensure changes are persisted
        await connection.commit()

    await connectable.dispose()


def run_migrations_online() -> None:
    """Entry point for online migrations - dispatches to async handler."""
    try:
        # Check if an event loop is already running
        asyncio.get_running_loop()
        # If we get here, we're in a running event loop (like pytest-asyncio)
        # Since we're being called from a thread pool (via run_in_executor),
        # we can safely use asyncio.run() as we're in a separate thread
        asyncio.run(run_async_migrations())
    except RuntimeError:
        # No event loop running, create a new one with asyncio.run()
        asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
