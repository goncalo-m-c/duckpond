"""Database connection and session management.

This package provides async database connectivity for both SQLite and PostgreSQL,
with connection pooling, transaction management, and health checks.

Example usage:
    ```python
    from duckpond.config import settings
    from duckpond.db import create_engine, DatabaseSession

    engine = create_engine(settings)

    db = DatabaseSession(engine)

    async with db.session() as session:
        result = await session.execute(select(Account))
        accounts = result.scalars().all()

    await db.close()
    ```
"""

from duckpond.db.base import (
    Base,
    check_connection,
    create_engine,
    dispose_engine,
    init_db,
)
from duckpond.db.migrations import (
    check_migration_status,
    downgrade_migrations,
    generate_migration,
    get_alembic_config,
    get_current_revision,
    get_migration_history,
    run_migrations,
    stamp_database,
)
from duckpond.db.session import (
    DatabaseSession,
    create_session_factory,
    get_session,
    get_session_no_commit,
)

__all__ = [
    "Base",
    "create_engine",
    "init_db",
    "check_connection",
    "dispose_engine",
    "DatabaseSession",
    "create_session_factory",
    "get_session",
    "get_session_no_commit",
    "run_migrations",
    "downgrade_migrations",
    "get_current_revision",
    "get_migration_history",
    "check_migration_status",
    "generate_migration",
    "get_alembic_config",
    "stamp_database",
]
