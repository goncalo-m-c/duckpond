"""DuckLake catalog management."""

from duckpond.catalog.manager import DuckLakeCatalogManager
from duckpond.catalog.schemas import (
    CatalogInfo,
    ColumnSchema,
    ColumnType,
    CreateDatasetRequest,
    DatasetListResponse,
    DatasetMetadata,
    DatasetType,
    PartitionInfo,
    PartitionSpec,
    PartitionType,
    SchemaEvolutionRequest,
    TableFormat,
    TableSchema,
    TableStatistics,
    UpdateDatasetRequest,
)

__all__ = [
    "DuckLakeCatalogManager",
    "CatalogInfo",
    "ColumnSchema",
    "ColumnType",
    "CreateDatasetRequest",
    "DatasetListResponse",
    "DatasetMetadata",
    "DatasetType",
    "PartitionInfo",
    "PartitionSpec",
    "PartitionType",
    "SchemaEvolutionRequest",
    "TableFormat",
    "TableSchema",
    "TableStatistics",
    "UpdateDatasetRequest",
]
