"""Pytest fixtures for CLI tests."""
import asyncio
import os
import sys
import tempfile
from pathlib import Path

import pytest

import duckpond.db
from duckpond.config import Settings


@pytest.fixture(scope="function")
def test_db_path():
    """Create a temporary database for CLI tests."""
    # Create a temporary directory for test database
    tmpdir = tempfile.mkdtemp()
    db_path = Path(tmpdir) / "test_cli.db"
    
    # Set environment variable so CLI commands use this database
    # Note: pydantic-settings with case_sensitive=False looks for METADATA_DB_URL
    db_url = f"sqlite:///{db_path}"
    os.environ["METADATA_DB_URL"] = db_url
    
    # Clear settings cache to pick up new environment variable
    import duckpond.config
    duckpond.config._settings = None  # type: ignore
    
    # Reset global engine to None so it gets recreated with new settings
    import duckpond.db.session
    duckpond.db.session._global_engine = None  # type: ignore
    
    # Run migrations to set up schema
    settings = Settings(metadata_db_url=db_url)
    engine = duckpond.db.create_engine(settings)
    
    # Run migrations synchronously using asyncio.run
    async def setup_db():
        await duckpond.db.run_migrations(engine)
        await engine.dispose()
    
    asyncio.run(setup_db())
    
    yield db_path
    
    # Cleanup: dispose global engine if it was created
    import duckpond.db.session
    if duckpond.db.session._global_engine is not None:  # type: ignore
        async def cleanup_engine():
            await duckpond.db.session._global_engine.dispose()  # type: ignore
            duckpond.db.session._global_engine = None  # type: ignore
        asyncio.run(cleanup_engine())
    
    # Cleanup: remove database file
    if db_path.exists():
        db_path.unlink()
    try:
        os.rmdir(tmpdir)
    except OSError:
        pass  # Directory may not be empty due to catalog files
    
    if "METADATA_DB_URL" in os.environ:
        del os.environ["METADATA_DB_URL"]
    
    # Clear settings cache again
    import duckpond.config
    duckpond.config._settings = None  # type: ignore


@pytest.fixture(autouse=True)
def setup_test_db(test_db_path):
    """Automatically set up test database for all CLI tests."""
    # This fixture will run before each test
    # test_db_path fixture already set up the database
    yield
