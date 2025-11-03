"""Prometheus metrics to Arrow RecordBatch converter.

This module converts Prometheus time series data into Apache Arrow RecordBatches
for efficient streaming ingestion into DuckDB/Parquet.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import pyarrow as pa

from duckpond.streaming.prometheus.protocol import PrometheusRemoteWrite

logger = logging.getLogger(__name__)


class PrometheusToArrowConverter:
    """Convert Prometheus metrics to Arrow RecordBatches.

    This converter transforms Prometheus time series data into Arrow format,
    handling dynamic label columns, timestamps, and various metric types.

    Features:
    - Schema inference from time series labels
    - Dynamic label column handling
    - Batch size control for memory management
    - Support for exemplars and histograms
    - Efficient timestamp conversion

    Example:
        converter = PrometheusToArrowConverter()

        batches = await converter.convert_write_request_to_batches(
            compressed_data=request_body,
            batch_size=10000
        )

        for batch in batches:
            parquet_writer.write_batch(batch)
    """

    def __init__(self):
        """Initialize converter."""
        self.label_columns: Set[str] = set()
        self._protocol = PrometheusRemoteWrite()

    def infer_schema(
        self,
        time_series: List[Dict[str, Any]],
        include_metadata: bool = True,
    ) -> pa.Schema:
        """Infer Arrow schema from time series data.

        Analyzes the time series data to determine which columns are needed
        and their types. Labels become string columns, and special fields
        like timestamps and values have fixed types.

        Args:
            time_series: List of time series dicts
            include_metadata: Whether to include metadata columns

        Returns:
            pa.Schema: Inferred Arrow schema

        Raises:
            ValueError: If time_series is empty

        Example:
            >>> converter = PrometheusToArrowConverter()
            >>> schema = converter.infer_schema(time_series_data)
            >>> print(schema)
            timestamp: timestamp[ms]
            value: double
            __name__: string
            job: string
            instance: string
        """
        if not time_series:
            raise ValueError("Cannot infer schema from empty time series")

        self.label_columns = set()

        reserved_columns = {
            "timestamp",
            "value",
            "is_exemplar",
            "is_histogram",
            "histogram_count",
            "histogram_sum",
            "histogram_schema",
            "histogram_zero_threshold",
            "histogram_zero_count",
            "metric_type",
            "help",
            "unit",
        }

        for ts in time_series:
            for key in ts.keys():
                if key not in reserved_columns:
                    self.label_columns.add(key)

        logger.info(
            f"Inferred schema with {len(self.label_columns)} label columns from "
            f"{len(time_series)} time series"
        )

        fields = [
            pa.field("timestamp", pa.timestamp("ms", tz="UTC")),
            pa.field("value", pa.float64()),
        ]

        for label in sorted(self.label_columns):
            fields.append(pa.field(label, pa.string()))

        if any(ts.get("is_exemplar") for ts in time_series):
            fields.append(pa.field("is_exemplar", pa.bool_()))

        if any(ts.get("is_histogram") for ts in time_series):
            fields.extend(
                [
                    pa.field("is_histogram", pa.bool_()),
                    pa.field("histogram_count", pa.float64()),
                    pa.field("histogram_sum", pa.float64()),
                    pa.field("histogram_schema", pa.int32()),
                    pa.field("histogram_zero_threshold", pa.float64()),
                    pa.field("histogram_zero_count", pa.float64()),
                ]
            )

        if include_metadata:
            if any(ts.get("metric_type") for ts in time_series):
                fields.append(pa.field("metric_type", pa.string()))
            if any(ts.get("help") for ts in time_series):
                fields.append(pa.field("help", pa.string()))
            if any(ts.get("unit") for ts in time_series):
                fields.append(pa.field("unit", pa.string()))

        return pa.schema(fields)

    def convert_to_record_batch(
        self,
        time_series: List[Dict[str, Any]],
        schema: pa.Schema,
    ) -> pa.RecordBatch:
        """Convert time series to Arrow RecordBatch.

        Transforms a list of time series dictionaries into an Arrow RecordBatch
        using the provided schema. Handles missing values gracefully by using
        None for nullable columns.

        Args:
            time_series: List of time series dicts
            schema: Target Arrow schema

        Returns:
            pa.RecordBatch: Converted batch

        Raises:
            ValueError: If time_series is empty

        Example:
            >>> converter = PrometheusToArrowConverter()
            >>> schema = converter.infer_schema(time_series)
            >>> batch = converter.convert_to_record_batch(time_series, schema)
            >>> print(f"Batch has {len(batch)} rows and {len(batch.schema)} columns")
        """
        if not time_series:
            raise ValueError("Cannot convert empty time series")

        logger.debug(
            f"Converting {len(time_series)} time series to RecordBatch with {len(schema)} columns"
        )

        columns = {}

        timestamps = []
        for ts in time_series:
            timestamp_ms = ts["timestamp"]
            dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
            timestamps.append(dt)

        columns["timestamp"] = pa.array(timestamps, type=pa.timestamp("ms", tz="UTC"))

        values = [ts["value"] for ts in time_series]
        columns["value"] = pa.array(values, type=pa.float64())

        for label in sorted(self.label_columns):
            label_values = [ts.get(label) for ts in time_series]
            columns[label] = pa.array(label_values, type=pa.string())

        if "is_exemplar" in schema.names:
            is_exemplar = [ts.get("is_exemplar", False) for ts in time_series]
            columns["is_exemplar"] = pa.array(is_exemplar, type=pa.bool_())

        if "is_histogram" in schema.names:
            columns["is_histogram"] = pa.array(
                [ts.get("is_histogram", False) for ts in time_series], type=pa.bool_()
            )
            columns["histogram_count"] = pa.array(
                [ts.get("histogram_count") for ts in time_series], type=pa.float64()
            )
            columns["histogram_sum"] = pa.array(
                [ts.get("histogram_sum") for ts in time_series], type=pa.float64()
            )
            columns["histogram_schema"] = pa.array(
                [ts.get("histogram_schema") for ts in time_series], type=pa.int32()
            )
            columns["histogram_zero_threshold"] = pa.array(
                [ts.get("histogram_zero_threshold") for ts in time_series],
                type=pa.float64(),
            )
            columns["histogram_zero_count"] = pa.array(
                [ts.get("histogram_zero_count") for ts in time_series],
                type=pa.float64(),
            )

        if "metric_type" in schema.names:
            columns["metric_type"] = pa.array(
                [ts.get("metric_type") for ts in time_series], type=pa.string()
            )
        if "help" in schema.names:
            columns["help"] = pa.array([ts.get("help") for ts in time_series], type=pa.string())
        if "unit" in schema.names:
            columns["unit"] = pa.array([ts.get("unit") for ts in time_series], type=pa.string())

        arrays = [columns[field.name] for field in schema]
        batch = pa.record_batch(arrays, schema=schema)

        logger.debug(
            f"Created RecordBatch: {len(batch)} rows, "
            f"{len(batch.schema)} columns, "
            f"{batch.nbytes} bytes"
        )

        return batch

    async def convert_write_request_to_batches(
        self,
        compressed_data: bytes,
        batch_size: int = 10000,
        include_metadata: bool = True,
    ) -> List[pa.RecordBatch]:
        """Convert Prometheus write request to Arrow batches.

        This is the main entry point for converting a Prometheus remote write
        request into Arrow batches ready for ingestion.

        The conversion process:
        1. Decompress and decode the write request
        2. Extract time series data points
        3. Infer Arrow schema from the data
        4. Split into batches of configurable size
        5. Convert each batch to Arrow RecordBatch

        Args:
            compressed_data: Snappy-compressed Protobuf data
            batch_size: Rows per batch (default: 10000)
            include_metadata: Whether to include metric metadata

        Returns:
            List[pa.RecordBatch]: Converted batches ready for ingestion

        Raises:
            ValueError: If write request is invalid or empty

        Example:
            >>> converter = PrometheusToArrowConverter()
            >>> batches = await converter.convert_write_request_to_batches(
            ...     compressed_data=request.body(),
            ...     batch_size=5000
            ... )
            >>> print(f"Generated {len(batches)} batches")
            >>> for batch in batches:
            ...     await ingest_batch(batch)
        """
        logger.info(
            f"Converting Prometheus write request ({len(compressed_data)} bytes compressed)"
        )

        write_request = self._protocol.decode_write_request(compressed_data)

        is_valid, error_msg = self._protocol.validate_write_request(write_request)
        if not is_valid:
            raise ValueError(f"Invalid write request: {error_msg}")

        stats = self._protocol.get_statistics(write_request)
        logger.info(
            f"Write request contains: "
            f"{stats['total_time_series']} time series, "
            f"{stats['total_samples']} samples, "
            f"{stats['unique_metrics']} unique metrics"
        )

        time_series = self._protocol.extract_time_series(
            write_request, include_metadata=include_metadata
        )

        if not time_series:
            logger.warning("No time series extracted from write request")
            return []

        logger.info(f"Extracted {len(time_series)} data points")

        schema = self.infer_schema(time_series, include_metadata=include_metadata)
        logger.info(f"Inferred schema with {len(schema)} columns")

        batches = []
        for i in range(0, len(time_series), batch_size):
            chunk = time_series[i : i + batch_size]
            batch = self.convert_to_record_batch(chunk, schema)
            batches.append(batch)

            logger.debug(
                f"Created batch {len(batches)}/{(len(time_series) + batch_size - 1) // batch_size}: "
                f"{len(batch)} rows"
            )

        logger.info(
            f"Converted to {len(batches)} Arrow batches "
            f"(total {sum(len(b) for b in batches)} rows, "
            f"{sum(b.nbytes for b in batches)} bytes)"
        )

        return batches

    def convert_write_request_to_batches_sync(
        self,
        compressed_data: bytes,
        batch_size: int = 10000,
        include_metadata: bool = True,
    ) -> List[pa.RecordBatch]:
        """Synchronous version of convert_write_request_to_batches.

        Same functionality as the async version, but can be called from
        synchronous code.

        Args:
            compressed_data: Snappy-compressed Protobuf data
            batch_size: Rows per batch (default: 10000)
            include_metadata: Whether to include metric metadata

        Returns:
            List[pa.RecordBatch]: Converted batches

        Example:
            >>> converter = PrometheusToArrowConverter()
            >>> batches = converter.convert_write_request_to_batches_sync(
            ...     compressed_data=request_body
            ... )
        """
        logger.info(
            f"Converting Prometheus write request ({len(compressed_data)} bytes compressed)"
        )

        write_request = self._protocol.decode_write_request(compressed_data)

        is_valid, error_msg = self._protocol.validate_write_request(write_request)
        if not is_valid:
            raise ValueError(f"Invalid write request: {error_msg}")

        stats = self._protocol.get_statistics(write_request)
        logger.info(
            f"Write request contains: "
            f"{stats['total_time_series']} time series, "
            f"{stats['total_samples']} samples, "
            f"{stats['unique_metrics']} unique metrics"
        )

        time_series = self._protocol.extract_time_series(
            write_request, include_metadata=include_metadata
        )

        if not time_series:
            logger.warning("No time series extracted from write request")
            return []

        logger.info(f"Extracted {len(time_series)} data points")

        schema = self.infer_schema(time_series, include_metadata=include_metadata)
        logger.info(f"Inferred schema with {len(schema)} columns")

        batches = []
        for i in range(0, len(time_series), batch_size):
            chunk = time_series[i : i + batch_size]
            batch = self.convert_to_record_batch(chunk, schema)
            batches.append(batch)

            logger.debug(
                f"Created batch {len(batches)}/{(len(time_series) + batch_size - 1) // batch_size}: "
                f"{len(batch)} rows"
            )

        logger.info(
            f"Converted to {len(batches)} Arrow batches "
            f"(total {sum(len(b) for b in batches)} rows, "
            f"{sum(b.nbytes for b in batches)} bytes)"
        )

        return batches

    def get_schema_for_metric(
        self,
        metric_name: str,
        sample_data: Optional[List[Dict[str, Any]]] = None,
    ) -> pa.Schema:
        """Get or infer schema for a specific metric.

        Useful when you want to maintain consistent schemas across batches
        for the same metric.

        Args:
            metric_name: Name of the metric
            sample_data: Optional sample data to infer schema from

        Returns:
            pa.Schema: Schema for the metric

        Raises:
            ValueError: If no sample data provided and schema cannot be determined
        """
        if sample_data:
            metric_data = [ts for ts in sample_data if ts.get("__name__") == metric_name]
            if metric_data:
                return self.infer_schema(metric_data)

        raise ValueError(f"Cannot determine schema for metric '{metric_name}': no sample data")
