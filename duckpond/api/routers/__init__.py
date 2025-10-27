"""DuckPond API routers package.

This package contains all API route handlers organized by domain.
"""

from duckpond.api.routers.datasets import router as datasets_router
from duckpond.api.routers.health import router as health_router
from duckpond.api.routers.upload import router as upload_router

__all__ = [
    "health_router",
    "datasets_router",
    "upload_router",
]
