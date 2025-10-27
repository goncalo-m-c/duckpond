"""DuckPond REST API module.

This module provides the FastAPI application and related components
for the DuckPond REST API.
"""

from duckpond.api.app import app, create_app
from duckpond.api.dependencies import (
    APIKey,
    CurrentTenant,
    get_api_key,
    get_current_tenant,
)
from duckpond.api.exceptions import (
    BadRequestException,
    ConflictException,
    DuckPondAPIException,
    ForbiddenException,
    InternalServerException,
    NotFoundException,
    ServiceUnavailableException,
    UnauthorizedException,
    ValidationException,
)
from duckpond.api.middleware import (
    CORSHeadersMiddleware,
    LoggingMiddleware,
    RequestIDMiddleware,
    TenantContextMiddleware,
)

__all__ = [
    "app",
    "create_app",
    "get_current_tenant",
    "get_api_key",
    "CurrentTenant",
    "APIKey",
    "DuckPondAPIException",
    "UnauthorizedException",
    "ForbiddenException",
    "NotFoundException",
    "ConflictException",
    "ValidationException",
    "BadRequestException",
    "InternalServerException",
    "ServiceUnavailableException",
    "RequestIDMiddleware",
    "LoggingMiddleware",
    "CORSHeadersMiddleware",
    "TenantContextMiddleware",
]
