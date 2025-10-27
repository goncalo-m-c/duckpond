"""Exceptions for streaming operations."""


class StreamingError(Exception):
    """Base exception for streaming operations."""


class SchemaValidationError(StreamingError):
    """Arrow schema validation failed."""


class BufferOverflowError(StreamingError):
    """Buffer capacity exceeded."""


class ProtocolError(StreamingError):
    """Arrow IPC protocol violation."""
