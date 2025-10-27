"""Tests for Phase 8.1: Arrow IPC Protocol Handler."""

import asyncio
import pytest
from pathlib import Path
import tempfile

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from duckpond.streaming.arrow_ipc import ArrowIPCHandler
from duckpond.streaming.exceptions import (
    ProtocolError,
    SchemaValidationError,
)


class TestArrowIPCHandler:
    """Tests for ArrowIPCHandler."""

    @pytest.fixture
    def sample_schema(self):
        """Create a sample Arrow schema."""
        return pa.schema(
            [
                ("id", pa.int64()),
                ("name", pa.string()),
                ("value", pa.float64()),
            ]
        )

    @pytest.fixture
    def sample_batches(self, sample_schema):
        """Create sample record batches."""
        batches = []
        for i in range(3):
            batch = pa.record_batch(
                [
                    pa.array([i * 10 + j for j in range(10)], type=pa.int64()),
                    pa.array([f"name_{i}_{j}" for j in range(10)], type=pa.string()),
                    pa.array(
                        [float(i * 10 + j) * 1.5 for j in range(10)], type=pa.float64()
                    ),
                ],
                schema=sample_schema,
            )
            batches.append(batch)
        return batches

    @pytest.fixture
    def ipc_stream_file(self, tmp_path, sample_schema, sample_batches):
        """Create a sample IPC stream file."""
        stream_path = tmp_path / "test.arrows"
        with pa.OSFile(str(stream_path), "wb") as sink:
            writer = ipc.new_stream(sink, sample_schema)
            for batch in sample_batches:
                writer.write_batch(batch)
            writer.close()
        return stream_path

    @pytest.mark.asyncio
    async def test_read_stream(self, ipc_stream_file, sample_schema):
        """Test reading Arrow IPC stream."""
        handler = ArrowIPCHandler()
        batches = []

        async for batch in handler.read_stream(ipc_stream_file):
            batches.append(batch)

        # Verify we got all batches
        assert len(batches) == 3

        # Verify schema
        assert handler.received_schema == sample_schema

        # Verify data
        total_rows = sum(batch.num_rows for batch in batches)
        assert total_rows == 30  # 3 batches * 10 rows each

    @pytest.mark.asyncio
    async def test_read_stream_with_schema_validation(
        self, ipc_stream_file, sample_schema
    ):
        """Test reading with schema validation (matching)."""
        handler = ArrowIPCHandler(expected_schema=sample_schema)
        batches = []

        async for batch in handler.read_stream(ipc_stream_file):
            batches.append(batch)

        assert len(batches) == 3

    @pytest.mark.asyncio
    async def test_read_stream_schema_mismatch(self, ipc_stream_file):
        """Test schema validation fails on mismatch."""
        wrong_schema = pa.schema(
            [
                ("id", pa.int64()),
                ("different", pa.string()),
            ]
        )

        handler = ArrowIPCHandler(expected_schema=wrong_schema)

        with pytest.raises(SchemaValidationError, match="Schema mismatch"):
            async for _ in handler.read_stream(ipc_stream_file):
                pass

    @pytest.mark.asyncio
    async def test_write_stream(self, tmp_path, sample_schema, sample_batches):
        """Test writing Arrow IPC stream."""
        dest_path = tmp_path / "output.arrows"

        async def batch_generator():
            for batch in sample_batches:
                yield batch

        handler = ArrowIPCHandler()
        total_rows = await handler.write_stream(batch_generator(), dest_path)

        # Verify row count
        assert total_rows == 30

        # Verify file was created
        assert dest_path.exists()

        # Verify we can read it back
        with pa.memory_map(str(dest_path), "r") as source:
            reader = ipc.open_stream(source)
            assert reader.schema == sample_schema
            batches_read = list(reader)
            assert len(batches_read) == 3

    @pytest.mark.asyncio
    async def test_write_stream_schema_inconsistency(
        self, tmp_path, sample_schema, sample_batches
    ):
        """Test write fails with inconsistent schemas."""
        dest_path = tmp_path / "output.arrows"

        # Create a batch with different schema
        bad_batch = pa.record_batch(
            [
                pa.array([1, 2, 3], type=pa.int32()),  # Different type
            ],
            schema=pa.schema([("id", pa.int32())]),
        )

        async def batch_generator():
            yield sample_batches[0]
            yield bad_batch  # Different schema

        handler = ArrowIPCHandler()

        with pytest.raises(ProtocolError, match="Inconsistent schema"):
            await handler.write_stream(batch_generator(), dest_path)

    @pytest.mark.asyncio
    async def test_write_stream_empty_batches(self, tmp_path):
        """Test write fails with no batches."""
        dest_path = tmp_path / "output.arrows"

        async def empty_generator():
            if False:
                yield

        handler = ArrowIPCHandler()

        with pytest.raises(ValueError, match="No batches to write"):
            await handler.write_stream(empty_generator(), dest_path)

    @pytest.mark.asyncio
    async def test_convert_to_parquet(self, ipc_stream_file, tmp_path):
        """Test converting IPC to Parquet."""
        parquet_path = tmp_path / "output.parquet"

        handler = ArrowIPCHandler()
        total_rows = await handler.convert_to_parquet(ipc_stream_file, parquet_path)

        # Verify row count
        assert total_rows == 30

        # Verify Parquet file was created
        assert parquet_path.exists()

        # Verify we can read the Parquet file
        table = pq.read_table(str(parquet_path))
        assert len(table) == 30
        assert table.num_columns == 3

    @pytest.mark.asyncio
    async def test_convert_preserves_data(
        self, ipc_stream_file, tmp_path, sample_schema
    ):
        """Test that conversion preserves data correctly."""
        parquet_path = tmp_path / "output.parquet"

        handler = ArrowIPCHandler()
        await handler.convert_to_parquet(ipc_stream_file, parquet_path)

        # Read back and verify data
        table = pq.read_table(str(parquet_path))

        # Check first row
        first_row = table.slice(0, 1).to_pydict()
        assert first_row["id"][0] == 0
        assert first_row["name"][0] == "name_0_0"
        assert abs(first_row["value"][0] - 0.0) < 0.01

        # Check last row
        last_row = table.slice(29, 1).to_pydict()
        assert last_row["id"][0] == 29
        assert last_row["name"][0] == "name_2_9"
        assert abs(last_row["value"][0] - 43.5) < 0.01

    @pytest.mark.asyncio
    async def test_round_trip_ipc_to_parquet_to_ipc(
        self, ipc_stream_file, tmp_path, sample_schema
    ):
        """Test round-trip conversion IPC -> Parquet -> IPC."""
        parquet_path = tmp_path / "intermediate.parquet"
        final_ipc_path = tmp_path / "final.arrows"

        handler = ArrowIPCHandler()

        # IPC -> Parquet
        rows1 = await handler.convert_to_parquet(ipc_stream_file, parquet_path)

        # Read Parquet and write to IPC
        table = pq.read_table(str(parquet_path))

        async def table_to_batches():
            for batch in table.to_batches():
                yield batch

        rows2 = await handler.write_stream(table_to_batches(), final_ipc_path)

        # Verify row counts match
        assert rows1 == rows2 == 30

        # Verify final IPC can be read
        batches = []
        async for batch in handler.read_stream(final_ipc_path):
            batches.append(batch)

        assert len(batches) > 0
        assert sum(b.num_rows for b in batches) == 30

    def test_validate_schema_valid(self, sample_schema):
        """Test schema validation with valid schema."""
        assert ArrowIPCHandler.validate_schema(sample_schema) is True

    def test_validate_schema_empty(self):
        """Test validation fails on empty schema."""
        empty_schema = pa.schema([])

        with pytest.raises(SchemaValidationError, match="no fields"):
            ArrowIPCHandler.validate_schema(empty_schema)

    def test_validate_schema_duplicate_names(self):
        """Test validation fails on duplicate field names."""
        bad_schema = pa.schema(
            [
                ("id", pa.int64()),
                ("name", pa.string()),
                ("id", pa.float64()),  # Duplicate!
            ]
        )

        with pytest.raises(SchemaValidationError, match="duplicate field names"):
            ArrowIPCHandler.validate_schema(bad_schema)

    def test_validate_schema_null_type(self):
        """Test validation fails on NULL type fields."""
        bad_schema = pa.schema(
            [
                ("id", pa.int64()),
                ("null_field", pa.null()),  # NULL type not supported
            ]
        )

        with pytest.raises(SchemaValidationError, match="unsupported NULL type"):
            ArrowIPCHandler.validate_schema(bad_schema)

    @pytest.mark.asyncio
    async def test_large_batches(self, tmp_path):
        """Test handling large record batches."""
        # Create larger batches
        schema = pa.schema([("value", pa.int64())])
        large_batches = []

        for i in range(5):
            batch = pa.record_batch(
                [
                    pa.array(range(i * 10000, (i + 1) * 10000), type=pa.int64()),
                ],
                schema=schema,
            )
            large_batches.append(batch)

        # Write to IPC
        ipc_path = tmp_path / "large.arrows"

        async def batch_gen():
            for batch in large_batches:
                yield batch

        handler = ArrowIPCHandler()
        total_rows = await handler.write_stream(batch_gen(), ipc_path)

        assert total_rows == 50000

        # Convert to Parquet
        parquet_path = tmp_path / "large.parquet"
        rows = await handler.convert_to_parquet(ipc_path, parquet_path)

        assert rows == 50000

    @pytest.mark.asyncio
    async def test_concurrent_reads(self, ipc_stream_file):
        """Test multiple concurrent reads from same file."""
        handler = ArrowIPCHandler()

        async def read_and_count():
            count = 0
            async for batch in handler.read_stream(ipc_stream_file):
                count += batch.num_rows
            return count

        # Run 3 concurrent reads
        results = await asyncio.gather(
            read_and_count(),
            read_and_count(),
            read_and_count(),
        )

        # All should read 30 rows
        assert results == [30, 30, 30]

    @pytest.mark.asyncio
    async def test_nested_schema_support(self, tmp_path):
        """Test support for nested/complex schemas."""
        # Create schema with nested struct
        schema = pa.schema(
            [
                ("id", pa.int64()),
                (
                    "metadata",
                    pa.struct(
                        [
                            ("created", pa.timestamp("us")),
                            ("tags", pa.list_(pa.string())),
                        ]
                    ),
                ),
            ]
        )

        # Create batch with nested data
        import datetime

        batch = pa.record_batch(
            [
                pa.array([1, 2, 3], type=pa.int64()),
                pa.StructArray.from_arrays(
                    [
                        pa.array(
                            [
                                datetime.datetime(2024, 1, 1),
                                datetime.datetime(2024, 1, 2),
                                datetime.datetime(2024, 1, 3),
                            ],
                            type=pa.timestamp("us"),
                        ),
                        pa.array(
                            [
                                ["tag1", "tag2"],
                                ["tag3"],
                                ["tag4", "tag5", "tag6"],
                            ],
                            type=pa.list_(pa.string()),
                        ),
                    ],
                    fields=schema.field("metadata").type,
                ),
            ],
            schema=schema,
        )

        # Write to IPC
        ipc_path = tmp_path / "nested.arrows"

        async def batch_gen():
            yield batch

        handler = ArrowIPCHandler()
        rows = await handler.write_stream(batch_gen(), ipc_path)
        assert rows == 3

        # Convert to Parquet
        parquet_path = tmp_path / "nested.parquet"
        rows = await handler.convert_to_parquet(ipc_path, parquet_path)
        assert rows == 3

        # Verify nested data preserved
        table = pq.read_table(str(parquet_path))
        assert len(table) == 3
        assert table.schema == schema

    @pytest.mark.asyncio
    async def test_error_handling_corrupted_file(self, tmp_path):
        """Test error handling for corrupted IPC file."""
        # Create a corrupted file
        bad_file = tmp_path / "corrupted.arrows"
        bad_file.write_bytes(b"not a valid arrow ipc stream")

        handler = ArrowIPCHandler()

        with pytest.raises(ProtocolError, match="Failed to read IPC stream"):
            async for _ in handler.read_stream(bad_file):
                pass

    @pytest.mark.asyncio
    async def test_error_handling_missing_file(self, tmp_path):
        """Test error handling for missing file."""
        missing_file = tmp_path / "nonexistent.arrows"

        handler = ArrowIPCHandler()

        with pytest.raises(ProtocolError):
            async for _ in handler.read_stream(missing_file):
                pass

    @pytest.mark.asyncio
    async def test_schema_evolution(self, tmp_path, sample_schema):
        """Test handling schema evolution across batches."""
        # This should fail - schemas must be consistent
        batch1 = pa.record_batch(
            [
                pa.array([1, 2, 3], type=pa.int64()),
                pa.array(["a", "b", "c"], type=pa.string()),
                pa.array([1.0, 2.0, 3.0], type=pa.float64()),
            ],
            schema=sample_schema,
        )

        # Different schema (extra field)
        schema2 = pa.schema(
            [
                ("id", pa.int64()),
                ("name", pa.string()),
                ("value", pa.float64()),
                ("extra", pa.int32()),  # New field
            ]
        )

        batch2 = pa.record_batch(
            [
                pa.array([4, 5, 6], type=pa.int64()),
                pa.array(["d", "e", "f"], type=pa.string()),
                pa.array([4.0, 5.0, 6.0], type=pa.float64()),
                pa.array([10, 20, 30], type=pa.int32()),
            ],
            schema=schema2,
        )

        async def mixed_batches():
            yield batch1
            yield batch2

        handler = ArrowIPCHandler()
        dest = tmp_path / "output.arrows"

        with pytest.raises(ProtocolError, match="Inconsistent schema"):
            await handler.write_stream(mixed_batches(), dest)
