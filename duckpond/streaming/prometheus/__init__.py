"""Prometheus remote write protocol support for DuckPond.

This module provides support for ingesting metrics via the Prometheus
remote write protocol, including Protobuf decoding, Snappy decompression,
and conversion to Arrow/Parquet formats.
"""

from duckpond.streaming.prometheus.protocol import PrometheusRemoteWrite
from duckpond.streaming.prometheus.parser import PrometheusParser
from duckpond.streaming.prometheus.converter import PrometheusToArrowConverter
from duckpond.streaming.prometheus.ingestor import PrometheusStreamingIngestor

__all__ = [
    "PrometheusRemoteWrite",
    "PrometheusParser",
    "PrometheusToArrowConverter",
    "PrometheusStreamingIngestor",
]
