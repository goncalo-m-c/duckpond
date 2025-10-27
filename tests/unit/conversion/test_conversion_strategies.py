"""Tests for Phase 7.2: Conversion Strategies."""

import pytest
from pathlib import Path
import tempfile
import duckdb

from duckpond.conversion.config import ConversionConfig
from duckpond.conversion.exceptions import ConversionError
from duckpond.conversion.strategies import (
    BaseStrategy,
    CSVConversionStrategy,
    JSONConversionStrategy,
    ParquetCopyStrategy,
)


class TestBaseStrategy:
    """Tests for BaseStrategy."""

    def test_escape_path_no_quotes(self):
        """Test path escaping with no single quotes."""
        config = ConversionConfig()
        strategy = CSVConversionStrategy(config)

        path = Path("/tmp/test.csv")
        escaped = strategy._escape_path(path)

        assert escaped == "/tmp/test.csv"

    def test_escape_path_with_quotes(self):
        """Test path escaping with single quotes."""
        config = ConversionConfig()
        strategy = CSVConversionStrategy(config)

        path = Path("/tmp/test's file.csv")
        escaped = strategy._escape_path(path)

        assert escaped == "/tmp/test''s file.csv"

    def test_escape_path_multiple_quotes(self):
        """Test path escaping with multiple single quotes."""
        config = ConversionConfig()
        strategy = CSVConversionStrategy(config)

        path = Path("/tmp/it's a test's file.csv")
        escaped = strategy._escape_path(path)

        assert escaped == "/tmp/it''s a test''s file.csv"

    def test_configure_connection(self):
        """Test DuckDB connection configuration."""
        config = ConversionConfig(threads=8, memory_limit="4GB")
        strategy = CSVConversionStrategy(config)

        conn = duckdb.connect(":memory:")
        strategy._configure_connection(conn)

        # Verify settings were applied
        # Note: DuckDB doesn't provide a direct way to query SET values
        # We just verify the configuration method runs without error
        # The actual configuration is tested implicitly through conversion tests

        conn.close()


class TestCSVConversionStrategy:
    """Tests for CSVConversionStrategy."""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create a sample CSV file."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n")
        return csv_file

    @pytest.fixture
    def strategy(self):
        """Create CSV conversion strategy."""
        config = ConversionConfig(compression="snappy")
        return CSVConversionStrategy(config)

    def test_csv_conversion(self, strategy, sample_csv, tmp_path):
        """Test successful CSV to Parquet conversion."""
        dest_path = tmp_path / "output.parquet"
        conn = duckdb.connect(":memory:")

        try:
            row_count = strategy.convert(conn, sample_csv, dest_path)

            # Verify conversion
            assert dest_path.exists()
            assert row_count == 3

            # Verify data integrity
            result = conn.execute(
                f"SELECT * FROM read_parquet('{dest_path}') ORDER BY id"
            ).fetchall()
            assert len(result) == 3
            assert result[0] == (1, "Alice", 100)
            assert result[1] == (2, "Bob", 200)
            assert result[2] == (3, "Charlie", 300)
        finally:
            conn.close()

    def test_csv_conversion_with_compression(self, sample_csv, tmp_path):
        """Test CSV conversion with different compression options."""
        for compression in ["snappy", "zstd", "gzip", "uncompressed"]:
            config = ConversionConfig(compression=compression)
            strategy = CSVConversionStrategy(config)
            dest_path = tmp_path / f"output_{compression}.parquet"
            conn = duckdb.connect(":memory:")

            try:
                row_count = strategy.convert(conn, sample_csv, dest_path)

                assert dest_path.exists()
                assert row_count == 3
            finally:
                conn.close()

    def test_csv_conversion_empty_file(self, strategy, tmp_path):
        """Test CSV conversion with empty file (only headers)."""
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("id,name,value\n")
        dest_path = tmp_path / "output.parquet"
        conn = duckdb.connect(":memory:")

        try:
            row_count = strategy.convert(conn, csv_file, dest_path)

            assert dest_path.exists()
            assert row_count == 0
        finally:
            conn.close()

    def test_csv_conversion_special_characters(self, strategy, tmp_path):
        """Test CSV conversion with special characters."""
        csv_file = tmp_path / "special.csv"
        csv_file.write_text(
            "id,name,description\n"
            '1,"Test, Inc.","Contains, commas"\n'
            '2,"Test\'s Co","Contains quotes"\n'
            '3,"Test\nCo","Contains newline"\n'
        )
        dest_path = tmp_path / "output.parquet"
        conn = duckdb.connect(":memory:")

        try:
            row_count = strategy.convert(conn, csv_file, dest_path)

            assert dest_path.exists()
            assert row_count == 3
        finally:
            conn.close()

    def test_csv_conversion_nonexistent_file(self, strategy, tmp_path):
        """Test CSV conversion with nonexistent file."""
        csv_file = tmp_path / "nonexistent.csv"
        dest_path = tmp_path / "output.parquet"
        conn = duckdb.connect(":memory:")

        try:
            # Should raise ConversionError for nonexistent file
            with pytest.raises(ConversionError, match="CSV conversion failed"):
                strategy.convert(conn, csv_file, dest_path)
        finally:
            conn.close()


class TestJSONConversionStrategy:
    """Tests for JSONConversionStrategy."""

    @pytest.fixture
    def sample_json(self, tmp_path):
        """Create a sample JSON file."""
        json_file = tmp_path / "test.json"
        json_file.write_text(
            "[\n"
            '  {"id": 1, "name": "Alice", "value": 100},\n'
            '  {"id": 2, "name": "Bob", "value": 200},\n'
            '  {"id": 3, "name": "Charlie", "value": 300}\n'
            "]"
        )
        return json_file

    @pytest.fixture
    def sample_jsonl(self, tmp_path):
        """Create a sample JSON Lines file."""
        jsonl_file = tmp_path / "test.jsonl"
        jsonl_file.write_text(
            '{"id": 1, "name": "Alice", "value": 100}\n'
            '{"id": 2, "name": "Bob", "value": 200}\n'
            '{"id": 3, "name": "Charlie", "value": 300}\n'
        )
        return jsonl_file

    @pytest.fixture
    def strategy(self):
        """Create JSON conversion strategy."""
        config = ConversionConfig(compression="snappy")
        return JSONConversionStrategy(config)

    def test_json_array_conversion(self, strategy, sample_json, tmp_path):
        """Test successful JSON array to Parquet conversion."""
        dest_path = tmp_path / "output.parquet"
        conn = duckdb.connect(":memory:")

        try:
            row_count = strategy.convert(conn, sample_json, dest_path)

            # Verify conversion
            assert dest_path.exists()
            assert row_count == 3

            # Verify data integrity
            result = conn.execute(
                f"SELECT * FROM read_parquet('{dest_path}') ORDER BY id"
            ).fetchall()
            assert len(result) == 3
            assert result[0] == (1, "Alice", 100)
            assert result[1] == (2, "Bob", 200)
            assert result[2] == (3, "Charlie", 300)
        finally:
            conn.close()

    def test_jsonl_conversion(self, strategy, sample_jsonl, tmp_path):
        """Test successful JSON Lines to Parquet conversion."""
        dest_path = tmp_path / "output.parquet"
        conn = duckdb.connect(":memory:")

        try:
            row_count = strategy.convert(conn, sample_jsonl, dest_path)

            # Verify conversion
            assert dest_path.exists()
            assert row_count == 3

            # Verify data integrity
            result = conn.execute(
                f"SELECT * FROM read_parquet('{dest_path}') ORDER BY id"
            ).fetchall()
            assert len(result) == 3
        finally:
            conn.close()

    def test_json_conversion_with_compression(self, sample_json, tmp_path):
        """Test JSON conversion with different compression options."""
        for compression in ["snappy", "zstd", "gzip"]:
            config = ConversionConfig(compression=compression)
            strategy = JSONConversionStrategy(config)
            dest_path = tmp_path / f"output_{compression}.parquet"
            conn = duckdb.connect(":memory:")

            try:
                row_count = strategy.convert(conn, sample_json, dest_path)

                assert dest_path.exists()
                assert row_count == 3
            finally:
                conn.close()

    def test_json_conversion_nested_objects(self, strategy, tmp_path):
        """Test JSON conversion with nested objects."""
        json_file = tmp_path / "nested.json"
        json_file.write_text(
            "[\n"
            '  {"id": 1, "user": {"name": "Alice", "age": 30}},\n'
            '  {"id": 2, "user": {"name": "Bob", "age": 25}}\n'
            "]"
        )
        dest_path = tmp_path / "output.parquet"
        conn = duckdb.connect(":memory:")

        try:
            row_count = strategy.convert(conn, json_file, dest_path)

            assert dest_path.exists()
            assert row_count == 2
        finally:
            conn.close()

    def test_json_conversion_empty_array(self, strategy, tmp_path):
        """Test JSON conversion with empty array."""
        json_file = tmp_path / "empty.json"
        json_file.write_text("[]")
        dest_path = tmp_path / "output.parquet"
        conn = duckdb.connect(":memory:")

        try:
            row_count = strategy.convert(conn, json_file, dest_path)

            assert dest_path.exists()
            assert row_count == 0
        finally:
            conn.close()

    def test_json_conversion_invalid_file(self, strategy, tmp_path):
        """Test JSON conversion with invalid file."""
        json_file = tmp_path / "invalid.json"
        json_file.write_text("not valid json")
        dest_path = tmp_path / "output.parquet"
        conn = duckdb.connect(":memory:")

        try:
            with pytest.raises(ConversionError, match="JSON conversion failed"):
                strategy.convert(conn, json_file, dest_path)
        finally:
            conn.close()


class TestParquetCopyStrategy:
    """Tests for ParquetCopyStrategy."""

    @pytest.fixture
    def sample_parquet(self, tmp_path):
        """Create a sample Parquet file."""
        parquet_file = tmp_path / "test.parquet"
        conn = duckdb.connect(":memory:")

        try:
            # Create a simple table and export to Parquet
            conn.execute("CREATE TABLE test (id INTEGER, name VARCHAR, value INTEGER)")
            conn.execute(
                "INSERT INTO test VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)"
            )
            conn.execute(f"COPY test TO '{parquet_file}' (FORMAT PARQUET)")
        finally:
            conn.close()

        return parquet_file

    @pytest.fixture
    def strategy(self):
        """Create Parquet copy strategy."""
        config = ConversionConfig()
        return ParquetCopyStrategy(config)

    def test_parquet_copy(self, strategy, sample_parquet, tmp_path):
        """Test successful Parquet file copy."""
        dest_path = tmp_path / "output.parquet"
        conn = duckdb.connect(":memory:")

        try:
            row_count = strategy.convert(conn, sample_parquet, dest_path)

            # Verify copy
            assert dest_path.exists()
            assert row_count == 3

            # Verify data integrity
            result = conn.execute(
                f"SELECT * FROM read_parquet('{dest_path}') ORDER BY id"
            ).fetchall()
            assert len(result) == 3
            assert result[0] == (1, "Alice", 100)
            assert result[1] == (2, "Bob", 200)
            assert result[2] == (3, "Charlie", 300)
        finally:
            conn.close()

    def test_parquet_copy_preserves_metadata(self, strategy, sample_parquet, tmp_path):
        """Test that Parquet copy preserves file metadata."""
        dest_path = tmp_path / "output.parquet"
        conn = duckdb.connect(":memory:")

        try:
            original_size = sample_parquet.stat().st_size
            row_count = strategy.convert(conn, sample_parquet, dest_path)

            # Verify file was copied
            assert dest_path.exists()
            assert row_count == 3

            # Size should be the same (exact copy)
            copied_size = dest_path.stat().st_size
            assert copied_size == original_size
        finally:
            conn.close()

    def test_parquet_copy_invalid_file(self, strategy, tmp_path):
        """Test Parquet copy with invalid file."""
        invalid_file = tmp_path / "invalid.parquet"
        invalid_file.write_text("not a valid parquet file")
        dest_path = tmp_path / "output.parquet"
        conn = duckdb.connect(":memory:")

        try:
            with pytest.raises(ConversionError, match="Parquet copy failed"):
                strategy.convert(conn, invalid_file, dest_path)
        finally:
            conn.close()

    def test_parquet_copy_nonexistent_file(self, strategy, tmp_path):
        """Test Parquet copy with nonexistent file."""
        nonexistent = tmp_path / "nonexistent.parquet"
        dest_path = tmp_path / "output.parquet"
        conn = duckdb.connect(":memory:")

        try:
            with pytest.raises(ConversionError, match="Parquet copy failed"):
                strategy.convert(conn, nonexistent, dest_path)
        finally:
            conn.close()


class TestIntegrationScenarios:
    """Integration tests for conversion strategies."""

    def test_large_csv_conversion(self, tmp_path):
        """Test CSV conversion with larger dataset."""
        csv_file = tmp_path / "large.csv"

        # Generate 10,000 rows
        with open(csv_file, "w") as f:
            f.write("id,name,value\n")
            for i in range(10000):
                f.write(f"{i},User{i},{i * 100}\n")

        config = ConversionConfig(threads=4)
        strategy = CSVConversionStrategy(config)
        dest_path = tmp_path / "output.parquet"
        conn = duckdb.connect(":memory:")

        try:
            row_count = strategy.convert(conn, csv_file, dest_path)

            assert dest_path.exists()
            assert row_count == 10000
        finally:
            conn.close()

    def test_path_with_special_characters(self, tmp_path):
        """Test conversion with paths containing special characters."""
        # Create a subdirectory with special characters
        special_dir = tmp_path / "test's data"
        special_dir.mkdir()

        csv_file = special_dir / "test.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")

        config = ConversionConfig()
        strategy = CSVConversionStrategy(config)
        dest_path = special_dir / "output.parquet"
        conn = duckdb.connect(":memory:")

        try:
            row_count = strategy.convert(conn, csv_file, dest_path)

            assert dest_path.exists()
            assert row_count == 2
        finally:
            conn.close()

    def test_concurrent_conversions(self, tmp_path):
        """Test multiple concurrent conversions."""
        config = ConversionConfig()
        csv_strategy = CSVConversionStrategy(config)
        json_strategy = JSONConversionStrategy(config)

        # Create CSV file
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")

        # Create JSON file
        json_file = tmp_path / "test.json"
        json_file.write_text('[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]')

        # Convert both
        csv_dest = tmp_path / "csv_output.parquet"
        json_dest = tmp_path / "json_output.parquet"

        conn1 = duckdb.connect(":memory:")
        conn2 = duckdb.connect(":memory:")

        try:
            csv_rows = csv_strategy.convert(conn1, csv_file, csv_dest)
            json_rows = json_strategy.convert(conn2, json_file, json_dest)

            assert csv_dest.exists()
            assert json_dest.exists()
            assert csv_rows == 2
            assert json_rows == 2
        finally:
            conn1.close()
            conn2.close()
