"""Database migration utilities for programmatic migration control."""

import asyncio
from pathlib import Path

import structlog
from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.ext.asyncio import AsyncEngine


logger = structlog.get_logger()


def get_alembic_config() -> Config:
    """
    Get Alembic configuration object.

    Returns:
        Alembic Config instance
    """
    project_root = Path(__file__).parent.parent.parent
    alembic_ini = project_root / "alembic.ini"

    if not alembic_ini.exists():
        raise FileNotFoundError(f"alembic.ini not found at {alembic_ini}")

    alembic_cfg = Config(str(alembic_ini))
    return alembic_cfg


async def get_current_revision(engine: AsyncEngine) -> str | None:
    """
    Get the current database revision.

    Args:
        engine: SQLAlchemy async engine

    Returns:
        Current revision hash, or None if no migrations applied
    """

    def _get_current_revision(connection):
        """Sync function to get current revision."""
        context = MigrationContext.configure(connection)
        return context.get_current_revision()

    async with engine.connect() as conn:
        current_rev = await conn.run_sync(_get_current_revision)

    logger.debug("Retrieved current database revision", revision=current_rev)
    return current_rev


async def run_migrations(
    engine: AsyncEngine,
    revision: str = "head",
    sql: bool = False,
) -> None:
    """
    Run database migrations to specified revision.

    Args:
        engine: SQLAlchemy async engine
        revision: Target revision (default: "head" for latest)
        sql: If True, generate SQL instead of executing

    Raises:
        RuntimeError: If migration fails
    """
    logger.info("Running database migrations", target_revision=revision, sql_mode=sql)

    alembic_cfg = get_alembic_config()

    db_url = str(engine.url)
    alembic_cfg.set_main_option("sqlalchemy.url", db_url)

    def _run_sync():
        """Run migrations synchronously in a thread."""
        if sql:
            command.upgrade(alembic_cfg, revision, sql=True)
        else:
            command.upgrade(alembic_cfg, revision)

    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _run_sync)

        if not sql:
            current_rev = await get_current_revision(engine)
            logger.info(
                "Database migrations completed successfully",
                current_revision=current_rev,
            )
        else:
            logger.info("Migration SQL generated successfully")
    except Exception as e:
        logger.error("Database migration failed", error=str(e), revision=revision)
        raise RuntimeError(f"Migration to {revision} failed: {e}") from e


async def downgrade_migrations(
    engine: AsyncEngine,
    revision: str = "-1",
) -> None:
    """
    Downgrade database to specified revision.

    Args:
        engine: SQLAlchemy async engine
        revision: Target revision (default: "-1" for one step back, "base" for all down)

    Raises:
        RuntimeError: If downgrade fails
    """
    logger.warning("Downgrading database migrations", target_revision=revision)

    alembic_cfg = get_alembic_config()

    db_url = str(engine.url)
    alembic_cfg.set_main_option("sqlalchemy.url", db_url)

    def _run_sync():
        """Run downgrade synchronously in a thread."""
        command.downgrade(alembic_cfg, revision)

    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _run_sync)

        current_rev = await get_current_revision(engine)
        logger.info(
            "Database downgrade completed successfully",
            current_revision=current_rev,
        )
    except Exception as e:
        logger.error("Database downgrade failed", error=str(e), revision=revision)
        raise RuntimeError(f"Downgrade to {revision} failed: {e}") from e


async def get_migration_history(engine: AsyncEngine) -> list[dict]:
    """
    Get migration history with current status.

    Args:
        engine: SQLAlchemy async engine

    Returns:
        List of migration info dicts with revision, description, and is_current flag
    """
    alembic_cfg = get_alembic_config()
    script = ScriptDirectory.from_config(alembic_cfg)

    current_rev = await get_current_revision(engine)

    history = []
    for revision in script.walk_revisions():
        history.append(
            {
                "revision": revision.revision,
                "down_revision": revision.down_revision,
                "description": revision.doc,
                "is_current": revision.revision == current_rev,
            }
        )

    logger.debug("Retrieved migration history", count=len(history))
    return history


async def check_migration_status(engine: AsyncEngine) -> dict:
    """
    Check if database is up to date with migrations.

    Args:
        engine: SQLAlchemy async engine

    Returns:
        Status dict with current_revision, latest_revision, and is_up_to_date
    """
    alembic_cfg = get_alembic_config()
    script = ScriptDirectory.from_config(alembic_cfg)

    current_rev = await get_current_revision(engine)
    latest_rev = script.get_current_head()

    status = {
        "current_revision": current_rev,
        "latest_revision": latest_rev,
        "is_up_to_date": current_rev == latest_rev,
    }

    logger.info("Checked migration status", **status)
    return status


def generate_migration(message: str, autogenerate: bool = True) -> str:
    """
    Generate a new migration file.

    Args:
        message: Migration description
        autogenerate: If True, auto-detect schema changes

    Returns:
        Path to generated migration file

    Raises:
        RuntimeError: If migration generation fails
    """
    logger.info("Generating new migration", message=message, autogenerate=autogenerate)

    alembic_cfg = get_alembic_config()

    try:
        if autogenerate:
            command.revision(alembic_cfg, message=message, autogenerate=True)
        else:
            command.revision(alembic_cfg, message=message)

        logger.info("Migration generated successfully", message=message)
        return f"Migration '{message}' generated"
    except Exception as e:
        logger.error("Migration generation failed", error=str(e), message=message)
        raise RuntimeError(f"Failed to generate migration: {e}") from e


async def stamp_database(engine: AsyncEngine, revision: str = "head") -> None:
    """
    Stamp database with a specific revision without running migrations.

    This is useful when you've manually created the schema and want to mark
    it as being at a specific migration version.

    Args:
        engine: SQLAlchemy async engine
        revision: Revision to stamp database with

    Raises:
        RuntimeError: If stamping fails
    """
    logger.warning("Stamping database with revision", revision=revision)

    alembic_cfg = get_alembic_config()

    db_url = str(engine.url)
    alembic_cfg.set_main_option("sqlalchemy.url", db_url)

    def _run_sync():
        """Run stamp synchronously in a thread."""
        command.stamp(alembic_cfg, revision)

    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _run_sync)

        current_rev = await get_current_revision(engine)
        logger.info("Database stamped successfully", current_revision=current_rev)
    except Exception as e:
        logger.error("Database stamping failed", error=str(e), revision=revision)
        raise RuntimeError(f"Failed to stamp database: {e}") from e
