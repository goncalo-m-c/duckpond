"""Tests for Phase 7.1: Conversion Module Architecture."""

import pytest
from pathlib import Path

from duckpond.conversion import (
    ConversionConfig,
    ConversionError,
    ConversionResult,
    ConversionTimeoutError,
    FileSizeExceededError,
    SchemaInferenceError,
    UnsupportedFormatError,
    ValidationError,
)
from duckpond.conversion.factory import ConverterFactory


class TestConversionConfig:
    """Tests for ConversionConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ConversionConfig()

        assert config.threads == 4
        assert config.memory_limit == "2GB"
        assert config.compression == "snappy"
        assert config.row_group_size == 122880
        assert config.max_file_size_bytes == 5 * 1024**3
        assert config.timeout_seconds == 300
        assert config.validate_row_count is True
        assert config.validate_schema is True
        assert config.executor_max_workers == 4

    def test_custom_config(self):
        """Test custom configuration values."""
        config = ConversionConfig(
            threads=8,
            memory_limit="4GB",
            compression="zstd",
            max_file_size_bytes=10 * 1024**3,
            timeout_seconds=600,
        )

        assert config.threads == 8
        assert config.memory_limit == "4GB"
        assert config.compression == "zstd"
        assert config.max_file_size_bytes == 10 * 1024**3
        assert config.timeout_seconds == 600

    def test_invalid_threads(self):
        """Test that invalid threads raises ValueError."""
        with pytest.raises(ValueError, match="threads must be >= 1"):
            ConversionConfig(threads=0)

    def test_invalid_executor_workers(self):
        """Test that invalid executor workers raises ValueError."""
        with pytest.raises(ValueError, match="executor_max_workers must be >= 1"):
            ConversionConfig(executor_max_workers=0)

    def test_invalid_file_size(self):
        """Test that invalid file size raises ValueError."""
        with pytest.raises(ValueError, match="max_file_size_bytes must be >= 1"):
            ConversionConfig(max_file_size_bytes=0)

    def test_invalid_timeout(self):
        """Test that invalid timeout raises ValueError."""
        with pytest.raises(ValueError, match="timeout_seconds must be >= 1"):
            ConversionConfig(timeout_seconds=0)


class TestConversionExceptions:
    """Tests for conversion exceptions."""

    def test_conversion_error(self):
        """Test base ConversionError."""
        error = ConversionError("Test error")
        assert str(error) == "Test error"
        assert isinstance(error, Exception)

    def test_unsupported_format_error(self):
        """Test UnsupportedFormatError."""
        error = UnsupportedFormatError("Unsupported format")
        assert isinstance(error, ConversionError)

    def test_file_size_exceeded_error(self):
        """Test FileSizeExceededError."""
        error = FileSizeExceededError("File too large")
        assert isinstance(error, ConversionError)

    def test_conversion_timeout_error(self):
        """Test ConversionTimeoutError."""
        error = ConversionTimeoutError("Timeout")
        assert isinstance(error, ConversionError)

    def test_validation_error(self):
        """Test ValidationError."""
        error = ValidationError("Validation failed")
        assert isinstance(error, ConversionError)

    def test_schema_inference_error(self):
        """Test SchemaInferenceError."""
        error = SchemaInferenceError("Schema inference failed")
        assert isinstance(error, ConversionError)


class TestConversionResult:
    """Tests for ConversionResult."""

    def test_successful_result(self):
        """Test successful conversion result."""
        result = ConversionResult(
            success=True,
            source_path=Path("/tmp/input.csv"),
            dest_path=Path("/tmp/output.parquet"),
            source_size_bytes=1000000,
            dest_size_bytes=500000,
            row_count=10000,
            duration_seconds=2.5,
            source_format="csv",
            dest_format="parquet",
            compression="snappy",
            schema_fingerprint="abc123",
        )

        assert result.success is True
        assert result.source_path == Path("/tmp/input.csv")
        assert result.dest_path == Path("/tmp/output.parquet")
        assert result.source_size_bytes == 1000000
        assert result.dest_size_bytes == 500000
        assert result.row_count == 10000
        assert result.duration_seconds == 2.5
        assert result.source_format == "csv"
        assert result.compression == "snappy"
        assert result.error_message is None

    def test_failed_result(self):
        """Test failed conversion result."""
        result = ConversionResult(
            success=False,
            source_path=Path("/tmp/input.csv"),
            error_message="Conversion failed",
        )

        assert result.success is False
        assert result.dest_path is None
        assert result.error_message == "Conversion failed"

    def test_compression_ratio(self):
        """Test compression ratio calculation."""
        result = ConversionResult(
            success=True,
            source_path=Path("/tmp/input.csv"),
            source_size_bytes=1000000,
            dest_size_bytes=500000,
        )

        assert result.compression_ratio == 0.5

    def test_compression_ratio_zero_source(self):
        """Test compression ratio with zero source size."""
        result = ConversionResult(
            success=True,
            source_path=Path("/tmp/input.csv"),
            source_size_bytes=0,
            dest_size_bytes=500000,
        )

        assert result.compression_ratio == 0.0

    def test_throughput_mbps(self):
        """Test throughput calculation."""
        result = ConversionResult(
            success=True,
            source_path=Path("/tmp/input.csv"),
            source_size_bytes=10 * 1024**2,  # 10 MB
            duration_seconds=2.0,
        )

        assert result.throughput_mbps == 5.0

    def test_throughput_mbps_zero_duration(self):
        """Test throughput with zero duration."""
        result = ConversionResult(
            success=True,
            source_path=Path("/tmp/input.csv"),
            source_size_bytes=10 * 1024**2,
            duration_seconds=0.0,
        )

        assert result.throughput_mbps == 0.0


class TestConverterFactory:
    """Tests for ConverterFactory."""

    def test_create_converter(self):
        """Test creating a converter."""
        config = ConversionConfig()
        csv_path = Path("test.csv")
        converter = ConverterFactory.create_converter(csv_path, config)

        assert converter is not None
        assert converter.config == config
        assert converter.strategy is not None

    def test_create_converter_default_config(self):
        """Test creating a converter with default config."""
        csv_path = Path("test.csv")
        converter = ConverterFactory.create_converter(csv_path)

        assert converter is not None
        assert converter.config is not None

    def test_create_converter_csv(self):
        """Test creating converter for CSV file."""
        from duckpond.conversion.strategies import CSVConversionStrategy

        converter = ConverterFactory.create_converter(Path("test.csv"))
        assert isinstance(converter.strategy, CSVConversionStrategy)

    def test_create_converter_json(self):
        """Test creating converter for JSON file."""
        from duckpond.conversion.strategies import JSONConversionStrategy

        converter = ConverterFactory.create_converter(Path("test.json"))
        assert isinstance(converter.strategy, JSONConversionStrategy)

    def test_create_converter_jsonl(self):
        """Test creating converter for JSONL file."""
        from duckpond.conversion.strategies import JSONConversionStrategy

        converter = ConverterFactory.create_converter(Path("test.jsonl"))
        assert isinstance(converter.strategy, JSONConversionStrategy)

    def test_create_converter_parquet(self):
        """Test creating converter for Parquet file."""
        from duckpond.conversion.strategies import ParquetCopyStrategy

        converter = ConverterFactory.create_converter(Path("test.parquet"))
        assert isinstance(converter.strategy, ParquetCopyStrategy)

    def test_create_converter_unsupported(self):
        """Test error for unsupported format."""
        with pytest.raises(UnsupportedFormatError, match="Unsupported file format"):
            ConverterFactory.create_converter(Path("test.txt"))

    def test_is_supported_csv(self):
        """Test that CSV files are supported."""
        assert ConverterFactory.is_supported(Path("test.csv")) is True
        assert ConverterFactory.is_supported(Path("test.CSV")) is True

    def test_is_supported_json(self):
        """Test that JSON files are supported."""
        assert ConverterFactory.is_supported(Path("test.json")) is True
        assert ConverterFactory.is_supported(Path("test.jsonl")) is True

    def test_is_supported_parquet(self):
        """Test that Parquet files are supported."""
        assert ConverterFactory.is_supported(Path("test.parquet")) is True

    def test_is_not_supported(self):
        """Test that unsupported formats return False."""
        assert ConverterFactory.is_supported(Path("test.txt")) is False
        assert ConverterFactory.is_supported(Path("test.xlsx")) is False
        assert ConverterFactory.is_supported(Path("test.pdf")) is False
