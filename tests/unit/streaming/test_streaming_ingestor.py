"""Tests for streaming ingestion pipeline."""

import asyncio
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pytest

from duckpond.catalog.manager import create_catalog_manager
from duckpond.streaming import (
    ArrowIPCHandler,
    BufferManager,
    StreamingError,
    StreamingIngestor,
)


@pytest.fixture
def catalog_manager():
    """Create a test catalog manager (optional, set to None for simplicity)."""
    return None


@pytest.fixture
def buffer_manager():
    """Create a test buffer manager."""
    return BufferManager(
        max_buffer_size_bytes=10 * 1024 * 1024,  # 10 MB
        max_queue_depth=50,
    )


@pytest.fixture
def test_schema():
    """Create a test Arrow schema."""
    return pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("value", pa.float64()),
        ]
    )


@pytest.fixture
async def sample_ipc_stream(tmp_path, test_schema):
    """Create a sample IPC stream file."""
    ipc_path = tmp_path / "test_stream.arrow"

    # Create sample batches
    batches = []
    for i in range(5):
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([i * 10 + j for j in range(100)], type=pa.int64()),
                pa.array([f"name_{i}_{j}" for j in range(100)], type=pa.string()),
                pa.array(
                    [float(i * 10 + j) * 1.5 for j in range(100)], type=pa.float64()
                ),
            ],
            schema=test_schema,
        )
        batches.append(batch)

    # Write IPC stream
    handler = ArrowIPCHandler()

    async def batch_generator():
        for batch in batches:
            yield batch

    await handler.write_stream(batch_generator(), ipc_path)

    return ipc_path, batches


class TestStreamingIngestor:
    """Test streaming ingestor."""

    @pytest.mark.asyncio
    async def test_basic_ingestion(
        self,
        catalog_manager,
        buffer_manager,
        sample_ipc_stream,
        tmp_path,
        test_schema,
    ):
        """Test basic end-to-end ingestion."""
        ipc_path, batches = sample_ipc_stream
        tenant_id = str(uuid4())
        stream_name = "test_dataset"
        storage_root = tmp_path / "storage"

        ingestor = StreamingIngestor(catalog_manager, buffer_manager)

        metrics = await ingestor.ingest_stream(
            tenant_id=tenant_id,
            stream_name=stream_name,
            ipc_stream_path=ipc_path,
            storage_root=storage_root,
            expected_schema=test_schema,
            batch_flush_count=2,  # Flush every 2 batches
        )

        # Verify metrics
        assert metrics["total_batches"] == 5
        assert metrics["total_rows"] == 500  # 5 batches * 100 rows
        assert metrics["total_bytes"] > 0
        assert metrics["files_written"] == 3  # 5 batches / 2 = 3 files (2+2+1)
        assert metrics["duration_seconds"] >= 0

        # Verify Parquet files were created
        parquet_files = list(storage_root.glob("*.parquet"))
        assert len(parquet_files) == 3

    @pytest.mark.asyncio
    async def test_single_batch_flush(
        self,
        catalog_manager,
        buffer_manager,
        sample_ipc_stream,
        tmp_path,
        test_schema,
    ):
        """Test flushing every single batch."""
        ipc_path, batches = sample_ipc_stream
        tenant_id = str(uuid4())
        stream_name = "single_batch"
        storage_root = tmp_path / "storage"

        ingestor = StreamingIngestor(catalog_manager, buffer_manager)

        metrics = await ingestor.ingest_stream(
            tenant_id=tenant_id,
            stream_name=stream_name,
            ipc_stream_path=ipc_path,
            storage_root=storage_root,
            batch_flush_count=1,  # Flush every batch
        )

        assert metrics["total_batches"] == 5
        assert metrics["files_written"] == 5  # One file per batch

    @pytest.mark.asyncio
    async def test_large_batch_flush(
        self,
        catalog_manager,
        buffer_manager,
        sample_ipc_stream,
        tmp_path,
        test_schema,
    ):
        """Test flushing all batches together."""
        ipc_path, batches = sample_ipc_stream
        tenant_id = str(uuid4())
        stream_name = "large_batch"
        storage_root = tmp_path / "storage"

        ingestor = StreamingIngestor(catalog_manager, buffer_manager)

        metrics = await ingestor.ingest_stream(
            tenant_id=tenant_id,
            stream_name=stream_name,
            ipc_stream_path=ipc_path,
            storage_root=storage_root,
            batch_flush_count=100,  # Larger than total batches
        )

        assert metrics["total_batches"] == 5
        assert metrics["files_written"] == 1  # All batches in one file

    @pytest.mark.asyncio
    async def test_file_not_found(
        self,
        catalog_manager,
        buffer_manager,
        tmp_path,
    ):
        """Test handling of missing IPC file."""
        tenant_id = str(uuid4())
        stream_name = "missing_file"
        storage_root = tmp_path / "storage"
        nonexistent_path = tmp_path / "nonexistent.arrow"

        ingestor = StreamingIngestor(catalog_manager, buffer_manager)

        with pytest.raises(StreamingError, match="IPC stream file not found"):
            await ingestor.ingest_stream(
                tenant_id=tenant_id,
                stream_name=stream_name,
                ipc_stream_path=nonexistent_path,
                storage_root=storage_root,
            )

    @pytest.mark.asyncio
    async def test_schema_validation(
        self,
        catalog_manager,
        buffer_manager,
        sample_ipc_stream,
        tmp_path,
    ):
        """Test schema validation during ingestion."""
        ipc_path, batches = sample_ipc_stream
        tenant_id = str(uuid4())
        stream_name = "schema_test"
        storage_root = tmp_path / "storage"

        # Wrong schema
        wrong_schema = pa.schema(
            [
                ("id", pa.int32()),  # Wrong type
                ("name", pa.string()),
            ]
        )

        ingestor = StreamingIngestor(catalog_manager, buffer_manager)

        with pytest.raises(StreamingError):
            await ingestor.ingest_stream(
                tenant_id=tenant_id,
                stream_name=stream_name,
                ipc_stream_path=ipc_path,
                storage_root=storage_root,
                expected_schema=wrong_schema,
            )

    @pytest.mark.asyncio
    async def test_empty_stream(
        self,
        catalog_manager,
        buffer_manager,
        tmp_path,
        test_schema,
    ):
        """Test handling of empty IPC stream."""
        ipc_path = tmp_path / "empty_stream.arrow"
        tenant_id = str(uuid4())
        stream_name = "empty_dataset"
        storage_root = tmp_path / "storage"

        # Create IPC stream with one empty batch (ArrowIPCHandler doesn't support truly empty streams)
        handler = ArrowIPCHandler()

        async def minimal_generator():
            # Create a batch with zero rows
            batch = pa.RecordBatch.from_arrays(
                [
                    pa.array([], type=pa.int64()),
                    pa.array([], type=pa.string()),
                    pa.array([], type=pa.float64()),
                ],
                schema=test_schema,
            )
            yield batch

        await handler.write_stream(minimal_generator(), ipc_path)

        ingestor = StreamingIngestor(catalog_manager, buffer_manager)

        metrics = await ingestor.ingest_stream(
            tenant_id=tenant_id,
            stream_name=stream_name,
            ipc_stream_path=ipc_path,
            storage_root=storage_root,
        )

        # Stream with empty batch should result in 1 batch with 0 rows
        assert metrics["total_batches"] == 1
        assert metrics["total_rows"] == 0
        assert metrics["files_written"] == 1

    @pytest.mark.asyncio
    async def test_concurrent_ingestion(
        self,
        catalog_manager,
        tmp_path,
        test_schema,
    ):
        """Test multiple concurrent ingestion operations."""
        tenant_id = str(uuid4())

        # Create multiple IPC streams
        streams = []
        for i in range(3):
            ipc_path = tmp_path / f"stream_{i}.arrow"
            handler = ArrowIPCHandler()

            async def batch_gen(idx=i):
                for j in range(3):
                    batch = pa.RecordBatch.from_arrays(
                        [
                            pa.array(
                                [idx * 100 + j * 10 + k for k in range(50)],
                                type=pa.int64(),
                            ),
                            pa.array(
                                [f"s{idx}_b{j}_{k}" for k in range(50)],
                                type=pa.string(),
                            ),
                            pa.array(
                                [float(idx * 100 + j * 10 + k) for k in range(50)],
                                type=pa.float64(),
                            ),
                        ],
                        schema=test_schema,
                    )
                    yield batch

            await handler.write_stream(batch_gen(), ipc_path)
            streams.append(ipc_path)

        # Ingest concurrently
        async def ingest_one(idx, ipc_path):
            buffer_mgr = BufferManager(
                max_buffer_size_bytes=10 * 1024 * 1024, max_queue_depth=50
            )
            ingestor = StreamingIngestor(
                None, buffer_mgr
            )  # No catalog for concurrent test
            storage_root = tmp_path / f"storage_{idx}"

            return await ingestor.ingest_stream(
                tenant_id=tenant_id,
                stream_name=f"dataset_{idx}",
                ipc_stream_path=ipc_path,
                storage_root=storage_root,
                batch_flush_count=2,
            )

        # Run all ingestions concurrently
        results = await asyncio.gather(
            *[ingest_one(i, stream) for i, stream in enumerate(streams)]
        )

        # Verify all succeeded
        assert len(results) == 3
        for result in results:
            assert result["total_batches"] == 3
            assert result["total_rows"] == 150  # 3 batches * 50 rows

    @pytest.mark.asyncio
    async def test_backpressure_handling(
        self,
        catalog_manager,
        tmp_path,
        test_schema,
    ):
        """Test backpressure with small buffer."""
        ipc_path = tmp_path / "large_stream.arrow"
        tenant_id = str(uuid4())
        stream_name = "backpressure_test"
        storage_root = tmp_path / "storage"

        # Create a larger stream
        handler = ArrowIPCHandler()

        async def batch_generator():
            for i in range(20):
                batch = pa.RecordBatch.from_arrays(
                    [
                        pa.array([i * 100 + j for j in range(1000)], type=pa.int64()),
                        pa.array(
                            [f"row_{i}_{j}" for j in range(1000)], type=pa.string()
                        ),
                        pa.array(
                            [float(i * 100 + j) for j in range(1000)], type=pa.float64()
                        ),
                    ],
                    schema=test_schema,
                )
                yield batch

        await handler.write_stream(batch_generator(), ipc_path)

        # Use small buffer to trigger backpressure
        small_buffer = BufferManager(
            max_buffer_size_bytes=100 * 1024,  # 100 KB
            max_queue_depth=5,
        )

        ingestor = StreamingIngestor(catalog_manager, small_buffer)

        metrics = await ingestor.ingest_stream(
            tenant_id=tenant_id,
            stream_name=stream_name,
            ipc_stream_path=ipc_path,
            storage_root=storage_root,
            batch_flush_count=5,
        )

        assert metrics["total_batches"] == 20
        assert metrics["total_rows"] == 20000
        # May have buffer overflows due to small buffer
        assert metrics["max_queue_depth"] > 0

    @pytest.mark.asyncio
    async def test_parquet_output_validity(
        self,
        catalog_manager,
        buffer_manager,
        sample_ipc_stream,
        tmp_path,
        test_schema,
    ):
        """Test that output Parquet files are valid and readable."""
        ipc_path, batches = sample_ipc_stream
        tenant_id = str(uuid4())
        stream_name = "parquet_test"
        storage_root = tmp_path / "storage"

        ingestor = StreamingIngestor(catalog_manager, buffer_manager)

        await ingestor.ingest_stream(
            tenant_id=tenant_id,
            stream_name=stream_name,
            ipc_stream_path=ipc_path,
            storage_root=storage_root,
            batch_flush_count=10,
        )

        # Read back the Parquet file
        parquet_files = list(storage_root.glob("*.parquet"))
        assert len(parquet_files) == 1

        # Verify we can read it
        import pyarrow.parquet as pq

        table = pq.read_table(parquet_files[0])

        assert table.num_rows == 500
        assert table.schema.names == ["id", "name", "value"]
        assert table.schema.field("id").type == pa.int64()

    @pytest.mark.asyncio
    async def test_metrics_accuracy(
        self,
        catalog_manager,
        buffer_manager,
        sample_ipc_stream,
        tmp_path,
    ):
        """Test that metrics are accurately reported."""
        ipc_path, batches = sample_ipc_stream
        tenant_id = str(uuid4())
        stream_name = "metrics_test"
        storage_root = tmp_path / "storage"

        ingestor = StreamingIngestor(catalog_manager, buffer_manager)

        metrics = await ingestor.ingest_stream(
            tenant_id=tenant_id,
            stream_name=stream_name,
            ipc_stream_path=ipc_path,
            storage_root=storage_root,
            batch_flush_count=3,
        )

        # Verify all metric fields are present
        assert "total_batches" in metrics
        assert "total_rows" in metrics
        assert "total_bytes" in metrics
        assert "buffer_overflows" in metrics
        assert "max_queue_depth" in metrics
        assert "files_written" in metrics
        assert "duration_seconds" in metrics

        # Verify values are sensible
        assert metrics["total_batches"] > 0
        assert metrics["total_rows"] > 0
        assert metrics["total_bytes"] > 0
        assert metrics["buffer_overflows"] >= 0
        assert metrics["max_queue_depth"] >= 0
        assert metrics["files_written"] > 0
        assert metrics["duration_seconds"] >= 0

    @pytest.mark.asyncio
    async def test_storage_root_creation(
        self,
        catalog_manager,
        buffer_manager,
        sample_ipc_stream,
        tmp_path,
    ):
        """Test that storage root directory is created if missing."""
        ipc_path, batches = sample_ipc_stream
        tenant_id = str(uuid4())
        stream_name = "auto_create_test"
        storage_root = tmp_path / "nonexistent" / "nested" / "storage"

        # Ensure it doesn't exist
        assert not storage_root.exists()

        ingestor = StreamingIngestor(catalog_manager, buffer_manager)

        await ingestor.ingest_stream(
            tenant_id=tenant_id,
            stream_name=stream_name,
            ipc_stream_path=ipc_path,
            storage_root=storage_root,
        )

        # Directory should now exist
        assert storage_root.exists()
        assert storage_root.is_dir()

    @pytest.mark.asyncio
    async def test_producer_consumer_coordination(
        self,
        catalog_manager,
        buffer_manager,
        tmp_path,
        test_schema,
    ):
        """Test proper coordination between producer and consumer."""
        ipc_path = tmp_path / "coord_stream.arrow"
        tenant_id = str(uuid4())
        stream_name = "coordination_test"
        storage_root = tmp_path / "storage"

        # Create stream with specific batch count
        handler = ArrowIPCHandler()
        batch_count = 7

        async def batch_generator():
            for i in range(batch_count):
                batch = pa.RecordBatch.from_arrays(
                    [
                        pa.array([i * 10 + j for j in range(100)], type=pa.int64()),
                        pa.array(
                            [f"batch_{i}_{j}" for j in range(100)], type=pa.string()
                        ),
                        pa.array(
                            [float(i * 10 + j) for j in range(100)], type=pa.float64()
                        ),
                    ],
                    schema=test_schema,
                )
                yield batch

        await handler.write_stream(batch_generator(), ipc_path)

        ingestor = StreamingIngestor(catalog_manager, buffer_manager)

        metrics = await ingestor.ingest_stream(
            tenant_id=tenant_id,
            stream_name=stream_name,
            ipc_stream_path=ipc_path,
            storage_root=storage_root,
            batch_flush_count=3,
        )

        # Verify all batches were processed
        assert metrics["total_batches"] == batch_count
        assert metrics["total_rows"] == batch_count * 100

        # With batch_flush_count=3 and 7 batches: 3 + 3 + 1 = 3 files
        assert metrics["files_written"] == 3
