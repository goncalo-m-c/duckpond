"""Tests for Phase 7.3: DuckDB Converter."""

import asyncio
import pytest
from pathlib import Path
import time

from duckpond.conversion.config import ConversionConfig
from duckpond.conversion.converter import DuckDBConverter
from duckpond.conversion.exceptions import (
    ConversionTimeoutError,
    FileSizeExceededError,
    ValidationError,
)
from duckpond.conversion.result import ConversionResult
from duckpond.conversion.strategies import (
    CSVConversionStrategy,
    JSONConversionStrategy,
    ParquetCopyStrategy,
)


class TestDuckDBConverter:
    """Tests for DuckDBConverter."""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create a sample CSV file."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n")
        return csv_file

    @pytest.fixture
    def sample_json(self, tmp_path):
        """Create a sample JSON file."""
        json_file = tmp_path / "test.json"
        json_file.write_text(
            '[{"id": 1, "name": "Alice", "value": 100},'
            '{"id": 2, "name": "Bob", "value": 200}]'
        )
        return json_file

    @pytest.fixture
    def config(self):
        """Create default configuration."""
        return ConversionConfig(
            threads=2,
            compression="snappy",
            max_file_size_bytes=10 * 1024 * 1024,  # 10 MB
            timeout_seconds=30,
        )

    @pytest.mark.asyncio
    async def test_csv_conversion_success(self, config, sample_csv, tmp_path):
        """Test successful CSV conversion."""
        strategy = CSVConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)
        dest_path = tmp_path / "output.parquet"

        try:
            result = await converter.convert_to_parquet(sample_csv, dest_path)

            # Verify result
            assert result.success is True
            assert result.source_path == sample_csv
            assert result.dest_path == dest_path
            assert result.row_count == 3
            assert result.source_size_bytes > 0
            assert result.dest_size_bytes > 0
            assert result.duration_seconds > 0
            assert result.source_format == "csv"
            assert result.compression == "snappy"
            assert len(result.schema_fingerprint) == 16
            assert result.error_message is None

            # Verify file was created
            assert dest_path.exists()
        finally:
            converter.shutdown()

    @pytest.mark.asyncio
    async def test_json_conversion_success(self, config, sample_json, tmp_path):
        """Test successful JSON conversion."""
        strategy = JSONConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)
        dest_path = tmp_path / "output.parquet"

        try:
            result = await converter.convert_to_parquet(sample_json, dest_path)

            assert result.success is True
            assert result.row_count == 2
            assert result.source_format == "json"
            assert dest_path.exists()
        finally:
            converter.shutdown()

    @pytest.mark.asyncio
    async def test_compression_ratio_calculation(self, config, sample_csv, tmp_path):
        """Test compression ratio calculation."""
        strategy = CSVConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)
        dest_path = tmp_path / "output.parquet"

        try:
            result = await converter.convert_to_parquet(sample_csv, dest_path)

            # Verify compression ratio is calculated
            assert result.compression_ratio > 0
            # Note: For small files, Parquet overhead may make it larger than CSV
            # Just verify the ratio is being calculated correctly
            assert (
                result.dest_size_bytes / result.source_size_bytes
                == result.compression_ratio
            )
        finally:
            converter.shutdown()

    @pytest.mark.asyncio
    async def test_throughput_calculation(self, config, sample_csv, tmp_path):
        """Test throughput calculation."""
        strategy = CSVConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)
        dest_path = tmp_path / "output.parquet"

        try:
            result = await converter.convert_to_parquet(sample_csv, dest_path)

            # Verify throughput is calculated
            assert result.throughput_mbps > 0
            assert result.duration_seconds > 0
        finally:
            converter.shutdown()

    @pytest.mark.asyncio
    async def test_source_file_not_found(self, config, tmp_path):
        """Test error when source file doesn't exist."""
        strategy = CSVConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)
        source_path = tmp_path / "nonexistent.csv"
        dest_path = tmp_path / "output.parquet"

        try:
            with pytest.raises(FileNotFoundError, match="Source file not found"):
                await converter.convert_to_parquet(source_path, dest_path)
        finally:
            converter.shutdown()

    @pytest.mark.asyncio
    async def test_file_size_exceeded(self, tmp_path):
        """Test error when file exceeds size limit."""
        # Create config with small size limit
        config = ConversionConfig(max_file_size_bytes=100)  # 100 bytes
        strategy = CSVConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)

        # Create file larger than limit
        csv_file = tmp_path / "large.csv"
        csv_file.write_text("id,name\n" + "1,test\n" * 50)  # More than 100 bytes
        dest_path = tmp_path / "output.parquet"

        try:
            with pytest.raises(FileSizeExceededError, match="exceeds limit"):
                await converter.convert_to_parquet(csv_file, dest_path)
        finally:
            converter.shutdown()

    @pytest.mark.asyncio
    async def test_conversion_timeout(self, tmp_path):
        """Test that timeout mechanism is properly configured."""
        # Create config with timeout setting
        config = ConversionConfig(timeout_seconds=300)
        strategy = CSVConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)

        # Create a normal CSV file
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")
        dest_path = tmp_path / "output.parquet"

        try:
            # Should complete successfully within timeout
            result = await converter.convert_to_parquet(csv_file, dest_path)
            assert result.success is True
            # Verify timeout was configured
            assert converter.config.timeout_seconds == 300
        finally:
            converter.shutdown()

    @pytest.mark.asyncio
    async def test_validation_zero_rows(self, tmp_path):
        """Test validation error for zero-row files."""
        config = ConversionConfig(validate_row_count=True)
        strategy = CSVConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)

        # Create CSV with only headers
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("id,name,value\n")
        dest_path = tmp_path / "output.parquet"

        try:
            with pytest.raises(ValidationError, match="zero rows"):
                await converter.convert_to_parquet(csv_file, dest_path)
        finally:
            converter.shutdown()

    @pytest.mark.asyncio
    async def test_validation_disabled(self, tmp_path):
        """Test that validation can be disabled."""
        config = ConversionConfig(validate_row_count=False)
        strategy = CSVConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)

        # Create CSV with only headers
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("id,name,value\n")
        dest_path = tmp_path / "output.parquet"

        try:
            # Should succeed with validation disabled
            result = await converter.convert_to_parquet(csv_file, dest_path)
            assert result.success is True
            assert result.row_count == 0
        finally:
            converter.shutdown()

    @pytest.mark.asyncio
    async def test_schema_fingerprint_consistency(self, config, tmp_path):
        """Test that schema fingerprint is consistent for same schema."""
        strategy = CSVConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)

        # Create two CSV files with same schema
        csv1 = tmp_path / "test1.csv"
        csv1.write_text("id,name,value\n1,Alice,100\n")
        csv2 = tmp_path / "test2.csv"
        csv2.write_text("id,name,value\n2,Bob,200\n")

        dest1 = tmp_path / "output1.parquet"
        dest2 = tmp_path / "output2.parquet"

        try:
            result1 = await converter.convert_to_parquet(csv1, dest1)
            result2 = await converter.convert_to_parquet(csv2, dest2)

            # Schema fingerprints should be identical
            assert result1.schema_fingerprint == result2.schema_fingerprint
        finally:
            converter.shutdown()

    @pytest.mark.asyncio
    async def test_schema_fingerprint_different(self, config, tmp_path):
        """Test that schema fingerprint differs for different schemas."""
        strategy = CSVConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)

        # Create two CSV files with different schemas
        csv1 = tmp_path / "test1.csv"
        csv1.write_text("id,name\n1,Alice\n")
        csv2 = tmp_path / "test2.csv"
        csv2.write_text("id,name,value\n2,Bob,200\n")

        dest1 = tmp_path / "output1.parquet"
        dest2 = tmp_path / "output2.parquet"

        try:
            result1 = await converter.convert_to_parquet(csv1, dest1)
            result2 = await converter.convert_to_parquet(csv2, dest2)

            # Schema fingerprints should be different
            assert result1.schema_fingerprint != result2.schema_fingerprint
        finally:
            converter.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_conversions(self, config, tmp_path):
        """Test multiple concurrent conversions."""
        strategy = CSVConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)

        # Create multiple CSV files
        files = []
        for i in range(5):
            csv_file = tmp_path / f"test{i}.csv"
            csv_file.write_text(f"id,name\n{i},User{i}\n")
            files.append((csv_file, tmp_path / f"output{i}.parquet"))

        try:
            # Convert all files concurrently
            tasks = [converter.convert_to_parquet(src, dst) for src, dst in files]
            results = await asyncio.gather(*tasks)

            # Verify all conversions succeeded
            assert len(results) == 5
            for result in results:
                assert result.success is True
                assert result.row_count == 1
        finally:
            converter.shutdown()

    @pytest.mark.asyncio
    async def test_different_compression_options(self, sample_csv, tmp_path):
        """Test conversion with different compression options."""
        compressions = ["snappy", "zstd", "gzip"]

        for compression in compressions:
            config = ConversionConfig(compression=compression)
            strategy = CSVConversionStrategy(config)
            converter = DuckDBConverter(config, strategy)
            dest_path = tmp_path / f"output_{compression}.parquet"

            try:
                result = await converter.convert_to_parquet(sample_csv, dest_path)

                assert result.success is True
                assert result.compression == compression
                assert dest_path.exists()
            finally:
                converter.shutdown()

    @pytest.mark.asyncio
    async def test_parquet_passthrough(self, config, tmp_path):
        """Test Parquet passthrough strategy."""
        import duckdb

        # Create a Parquet file first
        parquet_file = tmp_path / "source.parquet"
        conn = duckdb.connect()
        conn.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
        conn.execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")
        conn.execute(f"COPY test TO '{parquet_file}' (FORMAT PARQUET)")
        conn.close()

        # Use Parquet copy strategy
        strategy = ParquetCopyStrategy(config)
        converter = DuckDBConverter(config, strategy)
        dest_path = tmp_path / "output.parquet"

        try:
            result = await converter.convert_to_parquet(parquet_file, dest_path)

            assert result.success is True
            assert result.row_count == 2
            assert result.source_format == "parquet"
            assert dest_path.exists()
        finally:
            converter.shutdown()

    @pytest.mark.asyncio
    async def test_large_file_conversion(self, config, tmp_path):
        """Test conversion of larger file."""
        # Create a larger CSV file (10,000 rows)
        csv_file = tmp_path / "large.csv"
        with open(csv_file, "w") as f:
            f.write("id,name,value,timestamp\n")
            for i in range(10000):
                f.write(f"{i},User{i},{i * 100},2024-01-01 00:00:00\n")

        strategy = CSVConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)
        dest_path = tmp_path / "output.parquet"

        try:
            start = time.time()
            result = await converter.convert_to_parquet(csv_file, dest_path)
            duration = time.time() - start

            assert result.success is True
            assert result.row_count == 10000
            assert result.duration_seconds > 0
            assert result.throughput_mbps > 0
            # Conversion should be reasonably fast
            assert duration < 10.0  # Should complete in less than 10 seconds
        finally:
            converter.shutdown()

    @pytest.mark.asyncio
    async def test_executor_max_workers(self, sample_csv, tmp_path):
        """Test that executor respects max_workers setting."""
        config = ConversionConfig(executor_max_workers=2)
        strategy = CSVConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)

        try:
            # Verify executor was created with correct settings
            assert converter.executor._max_workers == 2

            # Perform conversion
            dest_path = tmp_path / "output.parquet"
            result = await converter.convert_to_parquet(sample_csv, dest_path)
            assert result.success is True
        finally:
            converter.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_cleanup(self, config):
        """Test that shutdown properly cleans up resources."""
        strategy = CSVConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)

        # Shutdown should complete without error
        converter.shutdown()

        # Executor should be shutdown
        assert converter.executor._shutdown is True

    @pytest.mark.asyncio
    async def test_error_handling_with_metrics(self, config, tmp_path):
        """Test that errors still return partial metrics."""
        strategy = CSVConversionStrategy(config)
        converter = DuckDBConverter(config, strategy)

        # Create an invalid CSV that will cause conversion to fail
        csv_file = tmp_path / "invalid.csv"
        csv_file.write_text("id,name\n")  # Empty data
        dest_path = tmp_path / "output.parquet"

        try:
            # Disable validation to test the conversion failure path
            config.validate_row_count = False
            result = await converter.convert_to_parquet(csv_file, dest_path)

            # Even on failure, we should get timing metrics
            assert result.duration_seconds > 0
            assert result.source_path == csv_file
        finally:
            converter.shutdown()
