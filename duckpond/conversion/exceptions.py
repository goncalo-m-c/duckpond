"""Exceptions for file conversion operations."""


class ConversionError(Exception):
    """Base exception for conversion operations."""


class UnsupportedFormatError(ConversionError):
    """File format is not supported for conversion."""


class FileSizeExceededError(ConversionError):
    """File exceeds maximum size limit."""


class ConversionTimeoutError(ConversionError):
    """Conversion operation timed out."""


class ValidationError(ConversionError):
    """Converted file failed validation checks."""


class SchemaInferenceError(ConversionError):
    """Failed to infer schema from source file."""
