"""FastAPI middleware for DuckPond API.

This module provides middleware for:
- Request ID tracking
- Request/response logging
- CORS headers
- Error handling
"""

import logging
import time
from typing import Callable
from uuid import uuid4

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Add unique request ID to each request.

    The request ID is:
    - Stored in request.state.request_id
    - Added to response headers as X-Request-ID
    - Used for request correlation in logs

    Example:
        app.add_middleware(RequestIDMiddleware)
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request and add request ID.

        Args:
            request: Incoming HTTP request
            call_next: Next middleware/handler in chain

        Returns:
            HTTP response with X-Request-ID header
        """
        request_id = str(uuid4())
        request.state.request_id = request_id

        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id

        return response


class LoggingMiddleware(BaseHTTPMiddleware):
    """Log all requests with timing and status.

    Logs:
    - Request start: method, path, client IP
    - Request completion: status code, duration
    - Request ID for correlation

    Example:
        app.add_middleware(LoggingMiddleware)
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request with logging.

        Args:
            request: Incoming HTTP request
            call_next: Next middleware/handler in chain

        Returns:
            HTTP response
        """
        start_time = time.time()

        request_id = getattr(request.state, "request_id", "unknown")
        client_host = request.client.host if request.client else "unknown"

        logger.info(
            f"Request started: {request.method} {request.url.path}",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "client": client_host,
            },
        )

        response = await call_next(request)

        duration = time.time() - start_time

        logger.info(
            f"Request completed: {request.method} {request.url.path} "
            f"status={response.status_code} duration={duration:.3f}s",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_seconds": duration,
            },
        )

        return response


class CORSHeadersMiddleware(BaseHTTPMiddleware):
    """Add CORS headers for browser clients.

    Adds headers:
    - Access-Control-Allow-Origin: *
    - Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS
    - Access-Control-Allow-Headers: Content-Type, Authorization

    Note: For production, configure specific origins instead of *.

    Example:
        app.add_middleware(CORSHeadersMiddleware)
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request and add CORS headers.

        Args:
            request: Incoming HTTP request
            call_next: Next middleware/handler in chain

        Returns:
            HTTP response with CORS headers
        """
        response = await call_next(request)

        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = (
            "GET, POST, PUT, DELETE, OPTIONS"
        )
        response.headers["Access-Control-Allow-Headers"] = (
            "Content-Type, Authorization, X-API-Key"
        )

        return response


class AccountContextMiddleware(BaseHTTPMiddleware):
    """Add account context to request state.

    Extracts account_id from authenticated request and stores it
    in request.state for use by downstream handlers.

    Example:
        app.add_middleware(AccountContextMiddleware)
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request and add account context.

        Args:
            request: Incoming HTTP request
            call_next: Next middleware/handler in chain

        Returns:
            HTTP response
        """
        if not hasattr(request.state, "account_id"):
            request.state.account_id = None

        response = await call_next(request)
        return response
