"""DuckPond API routers package.

This package contains all API route handlers organized by domain.
"""

from duckpond.api.routers.auth import router as auth_router
from duckpond.api.routers.datasets import router as datasets_router
from duckpond.api.routers.health import router as health_router
from duckpond.api.routers.upload import router as upload_router
from duckpond.api.routers.notebooks import router as notebooks_router
from duckpond.api.routers.accounts import router as accounts_router

__all__ = [
    "auth_router",
    "health_router",
    "datasets_router",
    "upload_router",
    "notebooks_router",
    "accounts_router",
]
