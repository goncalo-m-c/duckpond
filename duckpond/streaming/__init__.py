"""Streaming ingestion module for DuckPond.

This module provides functionality for streaming data ingestion using
Arrow IPC format with zero-copy performance and buffer management.
"""

from duckpond.streaming.arrow_ipc import ArrowIPCHandler
from duckpond.streaming.buffer_manager import BufferManager, BufferMetrics
from duckpond.streaming.exceptions import (
    BufferOverflowError,
    ProtocolError,
    SchemaValidationError,
    StreamingError,
)
from duckpond.streaming.ingestor import StreamingIngestor

__all__ = [
    "ArrowIPCHandler",
    "BufferManager",
    "BufferMetrics",
    "BufferOverflowError",
    "ProtocolError",
    "SchemaValidationError",
    "StreamingError",
    "StreamingIngestor",
]
