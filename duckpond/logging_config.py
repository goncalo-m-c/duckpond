"""Logging configuration for DuckPond."""

import logging
import sys
from typing import Any

import structlog
from structlog.types import EventDict, Processor

from duckpond.config import get_settings


def add_timestamp(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    """Add timestamp to log entry."""
    from datetime import datetime, timezone

    event_dict["timestamp"] = datetime.now(timezone.utc).isoformat()
    return event_dict


def censor_sensitive_keys(
    logger: Any, method_name: str, event_dict: EventDict
) -> EventDict:
    """Remove sensitive data from logs."""
    sensitive_keys = {"api_key", "password", "secret", "token", "authorization"}

    for key in list(event_dict.keys()):
        if any(sensitive in key.lower() for sensitive in sensitive_keys):
            event_dict[key] = "***REDACTED***"

    return event_dict


def setup_logging() -> None:
    """Configure structured logging."""
    settings = get_settings()

    processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        add_timestamp,
        censor_sensitive_keys,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if settings.log_format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=sys.stderr.isatty(),
                exception_formatter=structlog.dev.plain_traceback,
            )
        )

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stderr,
        level=getattr(logging, settings.log_level),
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a logger instance."""
    return structlog.get_logger(name)


def log_operation(
    logger: structlog.stdlib.BoundLogger,
    operation: str,
    account_id: str | None = None,
    **kwargs: Any,
) -> None:
    """Log an operation with standard context."""
    context = {"operation": operation}
    if account_id:
        context["account_id"] = account_id
    context.update(kwargs)

    logger.info("operation", **context)


def log_error(
    logger: structlog.stdlib.BoundLogger,
    error: Exception,
    operation: str,
    account_id: str | None = None,
    **kwargs: Any,
) -> None:
    """Log an error with standard context."""
    context = {
        "operation": operation,
        "error_type": type(error).__name__,
        "error_message": str(error),
    }
    if account_id:
        context["account_id"] = account_id
    context.update(kwargs)

    logger.error("operation_failed", **context, exc_info=True)
