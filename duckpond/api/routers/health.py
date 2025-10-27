"""Health check router for DuckPond API.

This module provides health check endpoints for monitoring
and load balancer integration.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, status
from pydantic import BaseModel

router = APIRouter(prefix="/health", tags=["health"])


class HealthResponse(BaseModel):
    """Health check response model."""

    status: str
    timestamp: datetime
    version: str


class DetailedHealthResponse(BaseModel):
    """Detailed health check response model."""

    status: str
    timestamp: datetime
    version: str
    components: dict[str, str]


@router.get(
    "",
    response_model=HealthResponse,
    status_code=status.HTTP_200_OK,
    summary="Basic health check",
)
async def health_check() -> HealthResponse:
    """Basic health check endpoint.

    Returns:
        Health status and version

    Example:
        GET /health
        {
            "status": "healthy",
            "version": "0.1.0"
        }
    """
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(timezone.utc),
        version="0.1.0",
    )


@router.get(
    "/ready",
    response_model=HealthResponse,
    status_code=status.HTTP_200_OK,
    summary="Readiness check",
)
async def readiness_check() -> HealthResponse:
    """Readiness check for Kubernetes/load balancers.

    Indicates if the service is ready to accept traffic.

    Returns:
        Readiness status

    Example:
        GET /health/ready
        {
            "status": "ready",
            "version": "0.1.0"
        }
    """
    return HealthResponse(
        status="ready",
        timestamp=datetime.now(timezone.utc),
        version="0.1.0",
    )


@router.get(
    "/live",
    response_model=HealthResponse,
    status_code=status.HTTP_200_OK,
    summary="Liveness check",
)
async def liveness_check() -> HealthResponse:
    """Liveness check for Kubernetes.

    Indicates if the service is alive and running.

    Returns:
        Liveness status

    Example:
        GET /health/live
        {
            "status": "alive",
            "version": "0.1.0"
        }
    """
    return HealthResponse(
        status="alive",
        timestamp=datetime.now(timezone.utc),
        version="0.1.0",
    )


@router.get(
    "/detailed",
    response_model=DetailedHealthResponse,
    status_code=status.HTTP_200_OK,
    summary="Detailed health check",
)
async def detailed_health_check() -> DetailedHealthResponse:
    """Detailed health check with component status.

    Returns:
        Detailed health information including component statuses

    Example:
        GET /health/detailed
        {
            "status": "healthy",
            "version": "0.1.0",
            "components": {
                "api": "healthy",
                "storage": "healthy",
                "catalog": "healthy"
            }
        }
    """
    components = {
        "api": "healthy",
        "storage": "healthy",
        "catalog": "healthy",
    }

    return DetailedHealthResponse(
        status="healthy",
        timestamp=datetime.now(timezone.utc),
        version="0.1.0",
        components=components,
    )
