"""Tests for database migration utilities."""
import pytest
from pathlib import Path

from duckpond.config import Settings
from duckpond.db import (
    check_migration_status,
    create_engine,
    downgrade_migrations,
    get_current_revision,
    get_migration_history,
    run_migrations,
)


@pytest.fixture
async def test_engine(tmp_path):
    """Create a test database engine with temporary database."""
    db_path = tmp_path / "test_migrations.db"
    test_settings = Settings(metadata_db_url=f"sqlite:///{db_path}")
    engine = create_engine(test_settings)

    yield engine

    await engine.dispose()


@pytest.mark.asyncio
async def test_run_migrations_upgrade_to_head(test_engine):
    """Test running migrations to head creates all tables."""
    # Initially, no revision should exist
    current_rev = await get_current_revision(test_engine)
    assert current_rev is None, "Expected no initial revision"

    # Run migrations
    await run_migrations(test_engine, revision="head")

    # Check that revision is now set
    current_rev = await get_current_revision(test_engine)
    assert current_rev is not None, "Expected revision after migration"

    # Verify tables exist by checking metadata
    from sqlalchemy import inspect

    async with test_engine.connect() as conn:

        def get_tables(connection):
            inspector = inspect(connection)
            return inspector.get_table_names()

        tables = await conn.run_sync(get_tables)

    assert "tenants" in tables, "Tenants table should exist"
    assert "api_keys" in tables, "API keys table should exist"
    assert "alembic_version" in tables, "Alembic version table should exist"


@pytest.mark.asyncio
async def test_downgrade_migrations_to_base(test_engine):
    """Test downgrading migrations removes all tables."""
    # First, run migrations
    await run_migrations(test_engine, revision="head")

    # Verify tables exist
    from sqlalchemy import inspect

    async with test_engine.connect() as conn:

        def get_tables(connection):
            inspector = inspect(connection)
            return inspector.get_table_names()

        tables = await conn.run_sync(get_tables)

    assert "tenants" in tables
    assert "api_keys" in tables

    # Now downgrade to base
    await downgrade_migrations(test_engine, revision="base")

    # Check tables are removed
    async with test_engine.connect() as conn:
        tables = await conn.run_sync(get_tables)

    assert "tenants" not in tables, "Tenants table should be removed"
    assert "api_keys" not in tables, "API keys table should be removed"
    assert "alembic_version" in tables, "Alembic version table should remain"


@pytest.mark.asyncio
async def test_get_current_revision(test_engine):
    """Test getting current database revision."""
    # Before migration
    current_rev = await get_current_revision(test_engine)
    assert current_rev is None

    # After migration
    await run_migrations(test_engine)
    current_rev = await get_current_revision(test_engine)
    assert current_rev is not None
    assert isinstance(current_rev, str)
    assert len(current_rev) > 0


@pytest.mark.asyncio
async def test_check_migration_status(test_engine):
    """Test checking migration status."""
    # Before migration - should not be up to date
    status = await check_migration_status(test_engine)
    assert status["current_revision"] is None
    assert status["latest_revision"] is not None
    assert status["is_up_to_date"] is False

    # After migration - should be up to date
    await run_migrations(test_engine)
    status = await check_migration_status(test_engine)
    assert status["current_revision"] is not None
    assert status["latest_revision"] is not None
    assert status["is_up_to_date"] is True


@pytest.mark.asyncio
async def test_get_migration_history(test_engine):
    """Test retrieving migration history."""
    await run_migrations(test_engine)

    history = await get_migration_history(test_engine)
    assert len(history) > 0, "Should have at least one migration"

    # Check structure of history entries
    for entry in history:
        assert "revision" in entry
        assert "down_revision" in entry
        assert "description" in entry
        assert "is_current" in entry

    # Check that exactly one migration is marked as current
    current_migrations = [m for m in history if m["is_current"]]
    assert len(current_migrations) == 1, "Exactly one migration should be current"


@pytest.mark.asyncio
async def test_migrations_idempotent(test_engine):
    """Test that running migrations multiple times is idempotent."""
    # Run migrations twice
    await run_migrations(test_engine)
    rev1 = await get_current_revision(test_engine)

    await run_migrations(test_engine)
    rev2 = await get_current_revision(test_engine)

    # Revision should be the same
    assert rev1 == rev2, "Running migrations twice should be idempotent"


@pytest.mark.asyncio
async def test_downgrade_and_upgrade_cycle(test_engine):
    """Test that downgrade and upgrade cycle works correctly."""
    # Initial upgrade
    await run_migrations(test_engine)
    initial_rev = await get_current_revision(test_engine)

    # Downgrade
    await downgrade_migrations(test_engine, revision="base")
    downgraded_rev = await get_current_revision(test_engine)
    assert downgraded_rev is None, "Revision should be None after downgrade to base"

    # Upgrade again
    await run_migrations(test_engine)
    final_rev = await get_current_revision(test_engine)

    # Should be back to the same revision
    assert final_rev == initial_rev, "Should return to same revision after cycle"


@pytest.mark.asyncio
async def test_verify_indexes_created(test_engine):
    """Test that all expected indexes are created."""
    await run_migrations(test_engine)

    from sqlalchemy import inspect

    async with test_engine.connect() as conn:

        def get_indexes(connection):
            inspector = inspect(connection)
            return {
                "tenants": inspector.get_indexes("tenants"),
                "api_keys": inspector.get_indexes("api_keys"),
            }

        indexes = await conn.run_sync(get_indexes)

    # Check tenants table indexes
    tenant_index_names = [idx["name"] for idx in indexes["tenants"]]
    assert "idx_tenants_name" in tenant_index_names
    assert "idx_tenants_storage_backend" in tenant_index_names

    # Check api_keys table indexes
    api_key_index_names = [idx["name"] for idx in indexes["api_keys"]]
    assert "idx_api_keys_tenant" in api_key_index_names
    assert "idx_api_keys_hash" in api_key_index_names
    assert "idx_api_keys_expires" in api_key_index_names


@pytest.mark.asyncio
async def test_verify_foreign_key_constraints(test_engine):
    """Test that foreign key constraints are properly created."""
    await run_migrations(test_engine)

    from sqlalchemy import inspect

    async with test_engine.connect() as conn:

        def get_foreign_keys(connection):
            inspector = inspect(connection)
            return inspector.get_foreign_keys("api_keys")

        fks = await conn.run_sync(get_foreign_keys)

    # Should have one foreign key from api_keys to tenants
    assert len(fks) >= 1, "Should have at least one foreign key"

    # Verify the foreign key references the correct table
    fk = fks[0]
    assert fk["referred_table"] == "tenants"
    assert "tenant_id" in fk["constrained_columns"]
    assert "tenant_id" in fk["referred_columns"]


@pytest.mark.asyncio
async def test_migration_with_invalid_revision(test_engine):
    """Test that migrating to invalid revision raises error."""
    with pytest.raises(RuntimeError, match="Migration to .* failed"):
        await run_migrations(test_engine, revision="invalid_revision_12345")


@pytest.mark.asyncio
async def test_alembic_config_file_exists():
    """Test that alembic.ini configuration file exists."""
    from duckpond.db.migrations import get_alembic_config

    config = get_alembic_config()
    assert config is not None

    # Verify alembic.ini exists
    project_root = Path(__file__).parent.parent.parent.parent
    alembic_ini = project_root / "alembic.ini"
    assert alembic_ini.exists(), "alembic.ini should exist in project root"


@pytest.mark.asyncio
async def test_verify_initial_migration_exists():
    """Test that initial_schema migration file exists."""
    project_root = Path(__file__).parent.parent.parent.parent
    versions_dir = project_root / "alembic" / "versions"

    assert versions_dir.exists(), "Alembic versions directory should exist"

    # Check for at least one migration file
    migration_files = list(versions_dir.glob("*.py"))
    migration_files = [
        f for f in migration_files if not f.name.startswith("__")
    ]  # Exclude __pycache__

    assert len(migration_files) > 0, "Should have at least one migration file"

    # Verify initial schema migration exists
    initial_migration = [f for f in migration_files if "initial_schema" in f.read_text()]
    assert len(initial_migration) > 0, "Should have initial_schema migration"
