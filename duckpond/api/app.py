"""FastAPI application factory and configuration."""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

import structlog
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from duckpond.api.exceptions import (
    DuckPondAPIException,
)
from duckpond.api.middleware import (
    CORSHeadersMiddleware,
    LoggingMiddleware,
    RequestIDMiddleware,
    TenantContextMiddleware,
)
from duckpond.api.routers import datasets_router, health_router, upload_router
from duckpond.api.routers.query import router as query_router
from duckpond.api.routers.streaming import router as streaming_router
from duckpond.config import get_settings
from duckpond.streaming.buffer_manager import BufferManager

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan manager.

    Handles startup and shutdown tasks:
    - Startup: Initialize application resources (buffer manager, storage)
    - Shutdown: Cleanup and close connections

    Args:
        app: FastAPI application instance

    Yields:
        None
    """
    settings = get_settings()
    logger.info(
        "application_starting",
        host=settings.duckpond_host,
        port=settings.duckpond_port,
        version="0.1.0",
    )

    try:
        buffer_size_mb = 128
        app.state.buffer_manager = BufferManager(
            max_buffer_size_bytes=buffer_size_mb * 1024 * 1024,
            max_queue_depth=100,
        )
        logger.info(
            "buffer_manager_initialized",
            max_buffer_size_mb=buffer_size_mb,
            max_queue_depth=100,
        )

        storage_path = Path(settings.local_storage_path).expanduser()
        storage_path.mkdir(parents=True, exist_ok=True)
        tenants_path = storage_path / "tenants"
        tenants_path.mkdir(parents=True, exist_ok=True)

        logger.info(
            "storage_initialized",
            storage_path=str(storage_path),
        )

        logger.info("application_initialized")
    except Exception as e:
        logger.error("initialization_failed", error=str(e), exc_info=True)
        raise

    yield

    logger.info("application_shutting_down")
    try:
        if hasattr(app.state, "buffer_manager"):
            await app.state.buffer_manager.close()
            logger.info("buffer_manager_closed")

        logger.info("cleanup_completed")
    except Exception as e:
        logger.error("shutdown_error", error=str(e), exc_info=True)


def create_app() -> FastAPI:
    """
    Create and configure FastAPI application.

    Returns:
        Configured FastAPI application instance
    """

    app = FastAPI(
        title="DuckPond API",
        description="Multi-tenant data platform with DuckDB and DuckLake",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    app.add_middleware(CORSHeadersMiddleware)
    app.add_middleware(LoggingMiddleware)
    app.add_middleware(RequestIDMiddleware)
    app.add_middleware(TenantContextMiddleware)

    register_exception_handlers(app)

    app.include_router(health_router)
    app.include_router(datasets_router)
    app.include_router(upload_router)
    app.include_router(query_router)
    app.include_router(streaming_router)

    @app.get("/", tags=["info"])
    async def root() -> dict[str, str]:
        """Root endpoint with API information."""
        return {
            "name": "DuckPond API",
            "version": "0.1.0",
            "docs": "/docs",
            "health": "/health",
        }

    logger.info("application_created", title=app.title, version=app.version)
    return app


def register_exception_handlers(app: FastAPI) -> None:
    """
    Register custom exception handlers.

    Args:
        app: FastAPI application instance
    """

    @app.exception_handler(DuckPondAPIException)
    async def duckpond_api_exception_handler(
        request: Request,
        exc: DuckPondAPIException,
    ) -> JSONResponse:
        """Handle DuckPondAPIException and all subclasses."""
        request_id = getattr(request.state, "request_id", "unknown")
        logger.warning(
            "api_exception",
            path=request.url.path,
            status_code=exc.status_code,
            detail=exc.detail,
            request_id=request_id,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "detail": exc.detail,
                "request_id": request_id,
            },
            headers=exc.headers,
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(
        request: Request,
        exc: Exception,
    ) -> JSONResponse:
        """Handle unexpected exceptions."""
        request_id = getattr(request.state, "request_id", "unknown")
        logger.error(
            "unexpected_error",
            path=request.url.path,
            error=str(exc),
            request_id=request_id,
            exc_info=True,
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "detail": "Internal server error",
                "request_id": request_id,
            },
        )


app = create_app()
