"""Pydantic schemas for DuckLake catalog operations."""

import warnings
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field, field_validator, model_validator

warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message="Field name.*shadows an attribute in parent.*BaseModel",
)

if TYPE_CHECKING:
    pass


class DatasetType(str, Enum):
    """Dataset type enumeration."""

    TABLE = "table"
    VIEW = "view"


class TableFormat(str, Enum):
    """Table storage format."""

    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"


class PartitionType(str, Enum):
    """Partition type for tables."""

    NONE = "none"
    HIVE = "hive"
    RANGE = "range"
    HASH = "hash"


class ColumnType(str, Enum):
    """DuckDB column types."""

    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    DOUBLE = "DOUBLE"
    VARCHAR = "VARCHAR"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    BLOB = "BLOB"
    DECIMAL = "DECIMAL"
    ARRAY = "ARRAY"
    STRUCT = "STRUCT"
    MAP = "MAP"


class ColumnSchema(BaseModel):
    """Column schema definition."""

    name: str = Field(..., description="Column name")
    type: str = Field(..., description="DuckDB column type (e.g., INTEGER, VARCHAR)")
    nullable: bool = Field(default=True, description="Whether column can be NULL")
    default: str | None = Field(None, description="Default value expression")
    comment: str | None = Field(None, description="Column comment/description")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate column name."""
        if not v:
            raise ValueError("Column name cannot be empty")
        if not v.replace("_", "").isalnum():
            raise ValueError(
                f"Column name '{v}' must contain only alphanumeric characters and underscores"
            )
        return v.lower()

    model_config = {"frozen": True}


class PartitionSpec(BaseModel):
    """Partition specification for tables."""

    type: PartitionType = Field(..., description="Partition type")
    columns: list[str] = Field(default_factory=list, description="Partition columns")
    buckets: int | None = Field(
        None, ge=1, description="Number of buckets for hash partitioning"
    )

    @field_validator("columns", mode="after")
    @classmethod
    def validate_partition_columns(cls, v: list[str]) -> list[str]:
        """Normalize partition columns to lowercase."""
        return [col.lower() for col in v]

    def model_post_init(self, __context: Any) -> None:
        """Validate partition configuration after initialization."""
        if self.type != PartitionType.NONE and not self.columns:
            raise ValueError(f"Partition columns required for {self.type} partitioning")

        if self.type == PartitionType.NONE and self.columns:
            raise ValueError("Partition columns not allowed for NONE partitioning")

        if self.type == PartitionType.HASH and self.buckets is None:
            raise ValueError("Number of buckets required for hash partitioning")


class TableSchema(BaseModel):
    """Table schema definition."""

    columns: list[ColumnSchema] = Field(..., min_length=1, description="Table columns")
    partition: PartitionSpec | None = Field(None, description="Partition specification")
    primary_key: list[str] | None = Field(None, description="Primary key columns")
    indexes: list[str] | None = Field(None, description="Indexed columns")

    @model_validator(mode="after")
    def validate_schema(self) -> "TableSchema":
        """Validate primary key and partition columns exist in schema."""
        if self.partition is None:
            self.partition = PartitionSpec(
                type=PartitionType.NONE, columns=[], buckets=None
            )

        column_names = {col.name for col in self.columns}

        if self.primary_key:
            normalized_pk = []
            for pk_col in self.primary_key:
                pk_col_lower = pk_col.lower()
                if pk_col_lower not in column_names:
                    raise ValueError(
                        f"Primary key column '{pk_col_lower}' not found in schema"
                    )
                normalized_pk.append(pk_col_lower)
            self.primary_key = normalized_pk

        for part_col in self.partition.columns:
            if part_col not in column_names:
                raise ValueError(f"Partition column '{part_col}' not found in schema")

        return self


class DatasetMetadata(BaseModel):
    """Dataset metadata."""

    name: str = Field(..., description="Dataset name")
    type: DatasetType = Field(..., description="Dataset type")
    format: TableFormat | None = Field(None, description="Storage format (tables only)")
    schema: TableSchema | None = Field(
        None, description="Table schema (tables/views only)"
    )
    location: str | None = Field(None, description="Storage location (external tables)")
    description: str | None = Field(None, description="Dataset description")
    properties: dict[str, Any] = Field(
        default_factory=dict, description="Custom properties"
    )
    created_at: datetime | None = Field(None, description="Creation timestamp")
    updated_at: datetime | None = Field(None, description="Last update timestamp")
    row_count: int | None = Field(None, ge=0, description="Approximate row count")
    size_bytes: int | None = Field(None, ge=0, description="Storage size in bytes")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate dataset name."""
        if not v:
            raise ValueError("Dataset name cannot be empty")
        if not v.replace("_", "").isalnum():
            raise ValueError(
                f"Dataset name '{v}' must contain only alphanumeric characters and underscores"
            )
        return v.lower()

    @field_validator("schema")
    @classmethod
    def validate_schema_required(
        cls, v: TableSchema | None, info: Any
    ) -> TableSchema | None:
        """Ensure schema is provided for tables."""
        dataset_type = info.data.get("type")
        if dataset_type == DatasetType.TABLE and v is None:
            raise ValueError("Schema is required for tables")
        return v

    model_config = {
        "protected_namespaces": (),
        "json_schema_extra": {
            "example": {
                "name": "sales",
                "type": "table",
                "format": "parquet",
                "schema": {
                    "columns": [
                        {"name": "order_id", "type": "BIGINT", "nullable": False},
                        {"name": "customer_id", "type": "BIGINT", "nullable": False},
                        {"name": "amount", "type": "DECIMAL", "nullable": False},
                        {"name": "order_date", "type": "DATE", "nullable": False},
                    ],
                    "partition": {"type": "hive", "columns": ["order_date"]},
                    "primary_key": ["order_id"],
                },
                "description": "Sales transactions",
            }
        },
    }


class CreateDatasetRequest(BaseModel):
    """Request to create a new dataset."""

    name: str = Field(..., description="Dataset name")
    type: DatasetType = Field(..., description="Dataset type")
    format: TableFormat = Field(
        default=TableFormat.PARQUET, description="Storage format"
    )
    schema: TableSchema = Field(..., description="Table schema")
    location: str | None = Field(None, description="Storage location (external tables)")
    description: str | None = Field(None, description="Dataset description")
    properties: dict[str, Any] = Field(
        default_factory=dict, description="Custom properties"
    )
    if_not_exists: bool = Field(default=False, description="Skip if dataset exists")

    model_config = {"protected_namespaces": ()}


class UpdateDatasetRequest(BaseModel):
    """Request to update dataset metadata."""

    description: str | None = Field(None, description="Updated description")
    properties: dict[str, Any] | None = Field(None, description="Updated properties")


class DatasetListResponse(BaseModel):
    """Response for listing datasets."""

    datasets: list[DatasetMetadata] = Field(..., description="List of datasets")
    total: int = Field(..., ge=0, description="Total number of datasets")


class SchemaEvolutionRequest(BaseModel):
    """Request to evolve table schema."""

    add_columns: list[ColumnSchema] = Field(
        default_factory=list, description="Columns to add"
    )
    drop_columns: list[str] = Field(default_factory=list, description="Columns to drop")
    rename_columns: dict[str, str] = Field(
        default_factory=dict, description="Columns to rename (old_name -> new_name)"
    )
    alter_columns: list[ColumnSchema] = Field(
        default_factory=list, description="Columns to alter (type/nullability changes)"
    )


class PartitionInfo(BaseModel):
    """Partition metadata."""

    partition_key: str = Field(..., description="Partition key value")
    location: str | None = Field(None, description="Partition location")
    row_count: int | None = Field(None, ge=0, description="Rows in partition")
    size_bytes: int | None = Field(None, ge=0, description="Partition size")
    created_at: datetime | None = Field(None, description="Creation timestamp")


class TableStatistics(BaseModel):
    """Table statistics."""

    row_count: int = Field(..., ge=0, description="Total rows")
    size_bytes: int = Field(..., ge=0, description="Total size in bytes")
    num_files: int | None = Field(None, ge=0, description="Number of data files")
    num_partitions: int | None = Field(None, ge=0, description="Number of partitions")
    avg_row_size_bytes: float | None = Field(None, ge=0, description="Average row size")
    last_updated: datetime | None = Field(None, description="Last statistics update")


class CatalogInfo(BaseModel):
    """Catalog information."""

    catalog_name: str = Field(..., description="Catalog name")
    account_id: str = Field(..., description="Account ID")
    total_datasets: int = Field(..., ge=0, description="Total datasets")
    total_tables: int = Field(..., ge=0, description="Total tables")
    total_views: int = Field(..., ge=0, description="Total views")
    total_size_bytes: int = Field(..., ge=0, description="Total storage size")
    created_at: datetime | None = Field(None, description="Catalog creation timestamp")
