"""Prometheus remote write protocol handler.

This module provides support for decoding Prometheus remote write requests,
including Snappy decompression and Protobuf parsing.
"""

import logging
from typing import Any, Dict, List, Optional

import snappy

from duckpond.streaming.prometheus import remote_pb2, types_pb2

logger = logging.getLogger(__name__)


class PrometheusRemoteWrite:
    """Handler for Prometheus remote write protocol.

    This class provides methods to decode Prometheus remote write requests
    that are sent via HTTP POST with Snappy compression and Protobuf encoding.

    Protocol details:
    - Content-Type: application/x-protobuf
    - Content-Encoding: snappy
    - Body: Snappy-compressed Protobuf WriteRequest message

    Example:
        handler = PrometheusRemoteWrite()

        write_request = handler.decode_write_request(compressed_data)

        time_series = handler.extract_time_series(write_request)

        for ts in time_series:
            print(f"Metric: {ts['__name__']}, Value: {ts['value']}, Time: {ts['timestamp']}")
    """

    @staticmethod
    def decode_write_request(
        compressed_data: bytes,
    ) -> remote_pb2.WriteRequest:
        """Decode Prometheus remote write request.

        Takes Snappy-compressed Protobuf data and returns a decoded WriteRequest
        message containing time series data, labels, samples, and metadata.

        Args:
            compressed_data: Snappy-compressed Protobuf data

        Returns:
            WriteRequest: Decoded write request with time series

        Raises:
            ValueError: If decompression or decoding fails

        Example:
            >>> handler = PrometheusRemoteWrite()
            >>> write_request = handler.decode_write_request(request_body)
            >>> print(f"Received {len(write_request.timeseries)} time series")
        """
        try:
            logger.debug(f"Decompressing {len(compressed_data)} bytes with Snappy")
            decompressed = snappy.decompress(compressed_data)
            logger.debug(f"Decompressed to {len(decompressed)} bytes")

            write_request = remote_pb2.WriteRequest()
            write_request.ParseFromString(decompressed)

            logger.info(
                f"Decoded WriteRequest with {len(write_request.timeseries)} time series, "
                f"{len(write_request.metadata)} metadata entries"
            )

            return write_request

        except snappy.UncompressError as e:
            logger.error(f"Failed to decompress Snappy data: {e}")
            raise ValueError(f"Failed to decompress Prometheus write request: {e}") from e
        except Exception as e:
            logger.error(f"Failed to decode Prometheus write request: {e}")
            raise ValueError(f"Failed to decode Prometheus write request: {e}") from e

    @staticmethod
    def extract_time_series(
        write_request: remote_pb2.WriteRequest,
        include_metadata: bool = True,
    ) -> List[Dict[str, Any]]:
        """Extract time series from write request.

        Converts Protobuf time series into a list of dictionaries suitable for
        conversion to Arrow/Parquet. Each dictionary represents a single data point
        with labels as columns.

        Args:
            write_request: Decoded write request
            include_metadata: Whether to include metric metadata in results

        Returns:
            List[dict]: Time series data as list of dicts with the following structure:
                {
                    'timestamp': int,
                    'value': float,
                    'label1': str,
                    'label2': str,
                    ...
                    'is_exemplar': bool,
                    'metric_type': str,
                }

        Example:
            >>> handler = PrometheusRemoteWrite()
            >>> time_series = handler.extract_time_series(write_request)
            >>> print(time_series[0])
            {
                'timestamp': 1698765432000,
                'value': 42.5,
                '__name__': 'http_requests_total',
                'method': 'GET',
                'status': '200'
            }
        """
        time_series = []

        metadata_map = {}
        if include_metadata:
            for metadata in write_request.metadata:
                metadata_map[metadata.metric_family_name] = {
                    "metric_type": types_pb2.MetricMetadata.MetricType.Name(metadata.type),
                    "help": metadata.help,
                    "unit": metadata.unit,
                }

        for ts in write_request.timeseries:
            labels = {}
            metric_name = None

            for label in ts.labels:
                labels[label.name] = label.value
                if label.name == "__name__":
                    metric_name = label.value

            metadata = {}
            if include_metadata and metric_name and metric_name in metadata_map:
                metadata = metadata_map[metric_name]

            for sample in ts.samples:
                data_point = {
                    "timestamp": sample.timestamp,
                    "value": sample.value,
                    **labels,
                }

                if metadata:
                    data_point.update(metadata)

                time_series.append(data_point)

            for exemplar in ts.exemplars:
                exemplar_labels = {
                    f"exemplar_{label.name}": label.value for label in exemplar.labels
                }

                data_point = {
                    "timestamp": exemplar.timestamp,
                    "value": exemplar.value,
                    "is_exemplar": True,
                    **labels,
                    **exemplar_labels,
                }

                if metadata:
                    data_point.update(metadata)

                time_series.append(data_point)

            for histogram in ts.histograms:
                hist_data = {
                    "timestamp": histogram.timestamp,
                    "histogram_count": (
                        histogram.count_int
                        if histogram.HasField("count_int")
                        else histogram.count_float
                    ),
                    "histogram_sum": histogram.sum,
                    "histogram_schema": histogram.schema,
                    "histogram_zero_threshold": histogram.zero_threshold,
                    "histogram_zero_count": (
                        histogram.zero_count_int
                        if histogram.HasField("zero_count_int")
                        else histogram.zero_count_float
                    ),
                    "is_histogram": True,
                    **labels,
                }

                if metadata:
                    hist_data.update(metadata)

                time_series.append(hist_data)

        logger.info(f"Extracted {len(time_series)} data points from time series")
        return time_series

    @staticmethod
    def get_metadata(
        write_request: remote_pb2.WriteRequest,
    ) -> List[Dict[str, Any]]:
        """Extract metadata from write request.

        Returns metric metadata information separate from the time series data.

        Args:
            write_request: Decoded write request

        Returns:
            List[dict]: Metadata entries with metric type, help text, and units

        Example:
            >>> handler = PrometheusRemoteWrite()
            >>> metadata = handler.get_metadata(write_request)
            >>> print(metadata[0])
            {
                'metric_family_name': 'http_requests_total',
                'metric_type': 'COUNTER',
                'help': 'Total number of HTTP requests',
                'unit': 'requests'
            }
        """
        metadata_list = []

        for metadata in write_request.metadata:
            metadata_list.append(
                {
                    "metric_family_name": metadata.metric_family_name,
                    "metric_type": types_pb2.MetricMetadata.MetricType.Name(metadata.type),
                    "help": metadata.help,
                    "unit": metadata.unit,
                }
            )

        return metadata_list

    @staticmethod
    def validate_write_request(
        write_request: remote_pb2.WriteRequest,
    ) -> tuple[bool, Optional[str]]:
        """Validate a WriteRequest for correctness.

        Checks that the write request contains valid time series data with
        proper labels and samples.

        Args:
            write_request: Decoded write request to validate

        Returns:
            tuple: (is_valid, error_message)
                - is_valid: True if request is valid
                - error_message: None if valid, error description if invalid

        Example:
            >>> handler = PrometheusRemoteWrite()
            >>> is_valid, error = handler.validate_write_request(write_request)
            >>> if not is_valid:
            ...     print(f"Invalid request: {error}")
        """
        if not write_request.timeseries:
            return False, "WriteRequest contains no time series"

        for idx, ts in enumerate(write_request.timeseries):
            if not ts.labels:
                return False, f"Time series {idx} has no labels"

            has_name = any(label.name == "__name__" for label in ts.labels)
            if not has_name:
                return False, f"Time series {idx} missing __name__ label"

            if not ts.samples and not ts.exemplars and not ts.histograms:
                return (
                    False,
                    f"Time series {idx} has no samples, exemplars, or histograms",
                )

            for sample_idx, sample in enumerate(ts.samples):
                if sample.timestamp <= 0:
                    return (
                        False,
                        f"Time series {idx}, sample {sample_idx} has invalid timestamp: {sample.timestamp}",
                    )

        return True, None

    @staticmethod
    def get_statistics(
        write_request: remote_pb2.WriteRequest,
    ) -> Dict[str, Any]:
        """Get statistics about the write request.

        Returns summary information about the time series data.

        Args:
            write_request: Decoded write request

        Returns:
            dict: Statistics including counts, time ranges, and label cardinality

        Example:
            >>> handler = PrometheusRemoteWrite()
            >>> stats = handler.get_statistics(write_request)
            >>> print(f"Total samples: {stats['total_samples']}")
        """
        stats = {
            "total_time_series": len(write_request.timeseries),
            "total_samples": 0,
            "total_exemplars": 0,
            "total_histograms": 0,
            "total_metadata": len(write_request.metadata),
            "unique_metrics": set(),
            "unique_labels": set(),
            "min_timestamp": None,
            "max_timestamp": None,
        }

        for ts in write_request.timeseries:
            stats["total_samples"] += len(ts.samples)
            stats["total_exemplars"] += len(ts.exemplars)
            stats["total_histograms"] += len(ts.histograms)

            for label in ts.labels:
                stats["unique_labels"].add(label.name)
                if label.name == "__name__":
                    stats["unique_metrics"].add(label.value)

            for sample in ts.samples:
                if stats["min_timestamp"] is None or sample.timestamp < stats["min_timestamp"]:
                    stats["min_timestamp"] = sample.timestamp
                if stats["max_timestamp"] is None or sample.timestamp > stats["max_timestamp"]:
                    stats["max_timestamp"] = sample.timestamp

        stats["unique_metrics"] = len(stats["unique_metrics"])
        stats["unique_labels"] = len(stats["unique_labels"])

        return stats
