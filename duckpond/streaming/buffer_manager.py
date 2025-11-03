"""Buffer manager for streaming ingestion with backpressure support."""

import asyncio
from collections import deque
from dataclasses import dataclass
from typing import Optional

import pyarrow as pa

from duckpond.streaming.exceptions import BufferOverflowError


@dataclass
class BufferMetrics:
    """Buffer performance metrics."""

    total_batches: int = 0
    total_rows: int = 0
    total_bytes: int = 0
    buffer_overflows: int = 0
    max_queue_depth: int = 0


class BufferManager:
    """Arc-inspired buffer manager for streaming ingestion.

    Implements bounded queue with backpressure signaling.
    """

    def __init__(
        self,
        max_buffer_size_bytes: int = 128 * 1024**2,
        max_queue_depth: int = 100,
    ):
        """Initialize buffer manager.

        Args:
            max_buffer_size_bytes: Maximum buffer size in bytes
            max_queue_depth: Maximum number of batches in queue
        """
        self.max_buffer_size_bytes = max_buffer_size_bytes
        self.max_queue_depth = max_queue_depth

        self.queue: deque[pa.RecordBatch] = deque()
        self.current_size_bytes = 0
        self.metrics = BufferMetrics()

        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Condition(self._lock)
        self._not_full = asyncio.Condition(self._lock)
        self._closed = False

    async def put(self, batch: pa.RecordBatch, timeout: Optional[float] = None) -> None:
        """Add batch to buffer with backpressure.

        Args:
            batch: Record batch to buffer
            timeout: Optional timeout in seconds

        Raises:
            BufferOverflowError: If buffer is full and timeout expires
            ValueError: If buffer is closed
        """
        batch_size = batch.nbytes

        async with self._not_full:
            deadline = None if timeout is None else asyncio.get_event_loop().time() + timeout

            while self._is_full() and not self._closed:
                if deadline:
                    remaining = deadline - asyncio.get_event_loop().time()
                    if remaining <= 0:
                        self.metrics.buffer_overflows += 1
                        raise BufferOverflowError("Buffer full, timeout expired")

                    try:
                        await asyncio.wait_for(self._not_full.wait(), timeout=remaining)
                    except asyncio.TimeoutError:
                        self.metrics.buffer_overflows += 1
                        raise BufferOverflowError("Buffer full, timeout expired")
                else:
                    await self._not_full.wait()

            if self._closed:
                raise ValueError("Buffer is closed")

            self.queue.append(batch)
            self.current_size_bytes += batch_size

            self.metrics.total_batches += 1
            self.metrics.total_rows += batch.num_rows
            self.metrics.total_bytes += batch_size
            self.metrics.max_queue_depth = max(
                self.metrics.max_queue_depth,
                len(self.queue),
            )

            self._not_empty.notify()

    async def get(self, timeout: Optional[float] = None) -> Optional[pa.RecordBatch]:
        """Get batch from buffer.

        Args:
            timeout: Optional timeout in seconds

        Returns:
            Optional[pa.RecordBatch]: Batch or None if closed

        Raises:
            asyncio.TimeoutError: If timeout expires
        """
        async with self._not_empty:
            deadline = None if timeout is None else asyncio.get_event_loop().time() + timeout

            while len(self.queue) == 0 and not self._closed:
                if deadline:
                    remaining = deadline - asyncio.get_event_loop().time()
                    if remaining <= 0:
                        raise asyncio.TimeoutError("Buffer empty, timeout expired")
                    await asyncio.wait_for(self._not_empty.wait(), timeout=remaining)
                else:
                    await self._not_empty.wait()

            if len(self.queue) == 0:
                return None

            batch = self.queue.popleft()
            self.current_size_bytes -= batch.nbytes

            self._not_full.notify()

            return batch

    async def close(self):
        """Close buffer and wake all waiters."""
        async with self._lock:
            self._closed = True
            self._not_empty.notify_all()
            self._not_full.notify_all()

    def _is_full(self) -> bool:
        """Check if buffer is at capacity."""
        return (
            len(self.queue) >= self.max_queue_depth
            or self.current_size_bytes >= self.max_buffer_size_bytes
        )

    @property
    def is_closed(self) -> bool:
        """Check if buffer is closed."""
        return self._closed

    @property
    def size_bytes(self) -> int:
        """Current buffer size in bytes."""
        return self.current_size_bytes

    @property
    def queue_depth(self) -> int:
        """Current number of batches in queue."""
        return len(self.queue)
