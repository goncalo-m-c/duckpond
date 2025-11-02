"""Tests for Phase 7.5: LocalBackend Integration with Conversion Module."""

import asyncio
import pytest
from pathlib import Path
import tempfile

from duckpond.conversion.config import ConversionConfig
from duckpond.storage.local_backend import LocalBackend


class TestLocalBackendIntegration:
    """Tests for LocalBackend integration with conversion module."""

    @pytest.fixture
    def temp_storage(self, tmp_path):
        """Create temporary storage directory."""
        storage_path = tmp_path / "storage"
        storage_path.mkdir()
        return storage_path

    @pytest.fixture
    def backend(self, temp_storage):
        """Create LocalBackend instance."""
        return LocalBackend(base_path=temp_storage)

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
    def sample_parquet(self, tmp_path):
        """Create a sample Parquet file."""
        import duckdb

        parquet_file = tmp_path / "test.parquet"
        conn = duckdb.connect()
        conn.execute("CREATE TABLE test (id INTEGER, name VARCHAR, value INTEGER)")
        conn.execute("INSERT INTO test VALUES (1, 'Alice', 100), (2, 'Bob', 200)")
        conn.execute(f"COPY test TO '{parquet_file}' (FORMAT PARQUET)")
        conn.close()
        return parquet_file

    @pytest.mark.asyncio
    async def test_upload_csv_with_conversion(self, backend, sample_csv):
        """Test CSV upload with automatic conversion."""
        result = await backend.upload_file(
            local_path=sample_csv,
            remote_key="data/test.csv",
            account_id="test-account",
            convert_to_parquet=True,
        )

        # Verify result structure
        assert isinstance(result, dict)
        assert "remote_path" in result
        assert "metrics" in result

        # Verify remote path
        assert result["remote_path"].endswith(".parquet")
        assert "test-account" in result["remote_path"]

        # Verify metrics
        metrics = result["metrics"]
        assert metrics is not None
        assert metrics["row_count"] == 3
        assert metrics["source_size_bytes"] > 0
        assert metrics["dest_size_bytes"] > 0
        assert metrics["compression_ratio"] > 0
        assert metrics["duration_seconds"] > 0
        assert metrics["throughput_mbps"] > 0
        assert "schema_fingerprint" in metrics

    @pytest.mark.asyncio
    async def test_upload_json_with_conversion(self, backend, sample_json):
        """Test JSON upload with automatic conversion."""
        result = await backend.upload_file(
            local_path=sample_json,
            remote_key="data/test.json",
            account_id="test-account",
            convert_to_parquet=True,
        )

        assert result["remote_path"].endswith(".parquet")
        assert result["metrics"]["row_count"] == 2

    @pytest.mark.asyncio
    async def test_upload_parquet_passthrough(self, backend, sample_parquet):
        """Test Parquet upload with passthrough."""
        result = await backend.upload_file(
            local_path=sample_parquet,
            remote_key="data/test.parquet",
            account_id="test-account",
            convert_to_parquet=True,
        )

        assert result["remote_path"].endswith(".parquet")
        # Parquet passthrough may not return metrics
        assert "metrics" in result

    @pytest.mark.asyncio
    async def test_upload_without_conversion(self, backend, sample_csv):
        """Test upload without conversion."""
        result = await backend.upload_file(
            local_path=sample_csv,
            remote_key="data/test.csv",
            account_id="test-account",
            convert_to_parquet=False,
        )

        assert result["remote_path"].endswith(".csv")
        assert result["metrics"] is None

    @pytest.mark.asyncio
    async def test_upload_with_custom_config(self, backend, sample_csv):
        """Test upload with custom conversion configuration."""
        config = ConversionConfig(
            threads=2,
            compression="zstd",
            max_file_size_bytes=1024 * 1024,  # 1 MB
            timeout_seconds=60,
        )

        result = await backend.upload_file(
            local_path=sample_csv,
            remote_key="data/test.csv",
            account_id="test-account",
            convert_to_parquet=True,
            conversion_config=config,
        )

        assert result["metrics"]["compression"] == "zstd"

    @pytest.mark.asyncio
    async def test_upload_with_metadata(self, backend, sample_csv):
        """Test upload with metadata."""
        metadata = {
            "dataset": "test-dataset",
            "version": "1.0",
            "uploaded_by": "test-user",
        }

        result = await backend.upload_file(
            local_path=sample_csv,
            remote_key="data/test.csv",
            account_id="test-account",
            metadata=metadata,
            convert_to_parquet=True,
        )

        assert result["remote_path"].endswith(".parquet")
        assert result["metrics"] is not None

    @pytest.mark.asyncio
    async def test_file_stored_in_correct_location(
        self, backend, sample_csv, temp_storage
    ):
        """Test that converted file is stored in tables/ directory."""
        result = await backend.upload_file(
            local_path=sample_csv,
            remote_key="data/test.csv",
            account_id="test-account",
            convert_to_parquet=True,
        )

        # Verify file exists in tables/ directory
        expected_path = (
            temp_storage / "test-account" / "tables" / "data" / "test.parquet"
        )
        assert expected_path.exists()

    @pytest.mark.asyncio
    async def test_original_file_removed_after_conversion(
        self, backend, sample_csv, temp_storage
    ):
        """Test that original file is removed from uploads/ after conversion."""
        result = await backend.upload_file(
            local_path=sample_csv,
            remote_key="data/test.csv",
            account_id="test-account",
            convert_to_parquet=True,
        )

        # Verify original file was removed from uploads/
        upload_path = temp_storage / "test-account" / "uploads" / "data" / "test.csv"
        assert not upload_path.exists()

    @pytest.mark.asyncio
    async def test_concurrent_uploads(self, backend, tmp_path):
        """Test multiple concurrent uploads."""
        # Create multiple CSV files
        files = []
        for i in range(3):
            csv_file = tmp_path / f"test{i}.csv"
            csv_file.write_text(f"id,name\n{i},User{i}\n")
            files.append(csv_file)

        # Upload concurrently
        tasks = [
            backend.upload_file(
                local_path=f,
                remote_key=f"data/{f.name}",
                account_id="test-account",
                convert_to_parquet=True,
            )
            for f in files
        ]
        results = await asyncio.gather(*tasks)

        # Verify all succeeded
        assert len(results) == 3
        for result in results:
            assert result["remote_path"].endswith(".parquet")
            assert result["metrics"]["row_count"] == 1

    @pytest.mark.asyncio
    async def test_large_csv_conversion(self, backend, tmp_path):
        """Test conversion of larger CSV file."""
        # Create a larger CSV file (1000 rows)
        csv_file = tmp_path / "large.csv"
        with open(csv_file, "w") as f:
            f.write("id,name,value\n")
            for i in range(1000):
                f.write(f"{i},User{i},{i * 100}\n")

        result = await backend.upload_file(
            local_path=csv_file,
            remote_key="data/large.csv",
            account_id="test-account",
            convert_to_parquet=True,
        )

        assert result["metrics"]["row_count"] == 1000
        assert result["metrics"]["throughput_mbps"] > 0

    @pytest.mark.asyncio
    async def test_schema_fingerprint_consistency(self, backend, tmp_path):
        """Test that schema fingerprints are consistent for same schema."""
        # Create two CSV files with same schema
        csv1 = tmp_path / "test1.csv"
        csv1.write_text("id,name\n1,Alice\n")
        csv2 = tmp_path / "test2.csv"
        csv2.write_text("id,name\n2,Bob\n")

        result1 = await backend.upload_file(
            local_path=csv1,
            remote_key="data/test1.csv",
            account_id="test-account",
            convert_to_parquet=True,
        )
        result2 = await backend.upload_file(
            local_path=csv2,
            remote_key="data/test2.csv",
            account_id="test-account",
            convert_to_parquet=True,
        )

        # Schema fingerprints should match
        assert (
            result1["metrics"]["schema_fingerprint"]
            == result2["metrics"]["schema_fingerprint"]
        )

    @pytest.mark.asyncio
    async def test_error_handling_unsupported_format(self, backend, tmp_path):
        """Test error handling for unsupported file format."""
        from duckpond.exceptions import StorageBackendError

        # Create an unsupported file
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("Some text content")

        with pytest.raises(StorageBackendError, match="Unsupported file format"):
            await backend.upload_file(
                local_path=txt_file,
                remote_key="data/test.txt",
                account_id="test-account",
                convert_to_parquet=True,
            )

    @pytest.mark.asyncio
    async def test_compression_ratio_calculation(self, backend, sample_csv):
        """Test that compression ratio is calculated correctly."""
        result = await backend.upload_file(
            local_path=sample_csv,
            remote_key="data/test.csv",
            account_id="test-account",
            convert_to_parquet=True,
        )

        metrics = result["metrics"]
        expected_ratio = metrics["dest_size_bytes"] / metrics["source_size_bytes"]
        assert abs(metrics["compression_ratio"] - expected_ratio) < 0.01

    @pytest.mark.asyncio
    async def test_metrics_structure(self, backend, sample_csv):
        """Test that metrics have all expected fields."""
        result = await backend.upload_file(
            local_path=sample_csv,
            remote_key="data/test.csv",
            account_id="test-account",
            convert_to_parquet=True,
        )

        metrics = result["metrics"]
        required_fields = [
            "row_count",
            "source_size_bytes",
            "dest_size_bytes",
            "compression_ratio",
            "duration_seconds",
            "throughput_mbps",
            "schema_fingerprint",
            "compression",
        ]

        for field in required_fields:
            assert field in metrics, f"Missing field: {field}"
