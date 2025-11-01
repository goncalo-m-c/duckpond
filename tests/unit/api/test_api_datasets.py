"""Tests for datasets API router."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock

from duckpond.api.app import create_app
from duckpond.api.dependencies import get_current_tenant
from duckpond.catalog.schemas import (
    ColumnSchema,
    CreateDatasetRequest,
    DatasetMetadata,
    DatasetType,
    TableFormat,
    TableSchema,
    PartitionSpec,
    PartitionType,
    DatasetListResponse,
    UpdateDatasetRequest,
)
from duckpond.exceptions import DatasetNotFoundError


@pytest.fixture
def client():
    """Create test client without authentication override."""
    app = create_app()
    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.clear()


@pytest.fixture
def authenticated_client():
    """Create test client with mocked authentication."""
    app = create_app()

    # Override authentication to bypass database
    async def mock_get_current_tenant():
        return "test-tenant-123"

    app.dependency_overrides[get_current_tenant] = mock_get_current_tenant

    yield TestClient(app, raise_server_exceptions=False)

    # Clean up overrides
    app.dependency_overrides.clear()


@pytest.fixture
def auth_headers():
    """Create authentication headers."""
    return {"X-API-Key": "tenant_test123_secret456"}


@pytest.fixture
def sample_dataset_metadata():
    """Create sample dataset metadata."""
    return DatasetMetadata(
        name="test_dataset",
        type=DatasetType.TABLE,
        format=TableFormat.PARQUET,
        schema=TableSchema(
            columns=[
                ColumnSchema(name="id", type="BIGINT", nullable=False),
                ColumnSchema(name="name", type="VARCHAR", nullable=True),
            ],
            partition=PartitionSpec(type=PartitionType.NONE, columns=[]),
        ),
        description="Test dataset",
        row_count=100,
        size_bytes=1024,
    )


class TestListDatasets:
    """Test dataset listing endpoint."""

    def test_list_datasets_success(self, authenticated_client, auth_headers):
        """Test successful dataset listing."""
        with patch(
            "duckpond.api.routers.datasets.create_catalog_manager"
        ) as mock_catalog:
            mock_manager = AsyncMock()
            mock_manager.list_datasets.return_value = DatasetListResponse(
                datasets=[
                    DatasetMetadata(
                        name="dataset1",
                        type=DatasetType.TABLE,
                        format=TableFormat.PARQUET,
                    ),
                    DatasetMetadata(
                        name="dataset2",
                        type=DatasetType.VIEW,
                    ),
                ],
                total=2,
            )
            mock_catalog.return_value = mock_manager

            response = authenticated_client.get("/api/v1/datasets", headers=auth_headers)

            assert response.status_code == 200
            data = response.json()
            assert data["total"] == 2
            assert len(data["datasets"]) == 2
            assert data["datasets"][0]["name"] == "dataset1"

    def test_list_datasets_with_type_filter(self, authenticated_client, auth_headers):
        """Test dataset listing with type filter."""
        with patch(
            "duckpond.api.routers.datasets.create_catalog_manager"
        ) as mock_catalog:
            mock_manager = AsyncMock()
            mock_manager.list_datasets.return_value = DatasetListResponse(
                datasets=[
                    DatasetMetadata(
                        name="table1",
                        type=DatasetType.TABLE,
                        format=TableFormat.PARQUET,
                    ),
                ],
                total=1,
            )
            mock_catalog.return_value = mock_manager

            response = authenticated_client.get(
                "/api/v1/datasets?dataset_type=table", headers=auth_headers
            )

            assert response.status_code == 200
            data = response.json()
            assert data["total"] == 1
            mock_manager.list_datasets.assert_called_once()

    def test_list_datasets_with_pattern(self, authenticated_client, auth_headers):
        """Test dataset listing with name pattern."""
        with patch(
            "duckpond.api.routers.datasets.create_catalog_manager"
        ) as mock_catalog:
            mock_manager = AsyncMock()
            mock_manager.list_datasets.return_value = DatasetListResponse(
                datasets=[
                    DatasetMetadata(
                        name="sales_2024",
                        type=DatasetType.TABLE,
                        format=TableFormat.PARQUET,
                    ),
                ],
                total=1,
            )
            mock_catalog.return_value = mock_manager

            response = authenticated_client.get(
                "/api/v1/datasets?pattern=sales%", headers=auth_headers
            )

            assert response.status_code == 200
            data = response.json()
            assert data["total"] == 1

    def test_list_datasets_empty(self, authenticated_client, auth_headers):
        """Test listing with no datasets."""
        with patch(
            "duckpond.api.routers.datasets.create_catalog_manager"
        ) as mock_catalog:
            mock_manager = AsyncMock()
            mock_manager.list_datasets.return_value = DatasetListResponse(
                datasets=[], total=0
            )
            mock_catalog.return_value = mock_manager

            response = authenticated_client.get("/api/v1/datasets", headers=auth_headers)

            assert response.status_code == 200
            data = response.json()
            assert data["total"] == 0
            assert data["datasets"] == []


class TestGetDataset:
    """Test get dataset endpoint."""

    def test_get_dataset_success(self, authenticated_client, auth_headers, sample_dataset_metadata):
        """Test successful dataset retrieval."""
        with patch(
            "duckpond.api.routers.datasets.create_catalog_manager"
        ) as mock_catalog:
            mock_manager = AsyncMock()
            mock_manager.get_dataset_metadata.return_value = sample_dataset_metadata
            mock_catalog.return_value = mock_manager

            response = authenticated_client.get("/api/v1/datasets/test_dataset", headers=auth_headers)

            assert response.status_code == 200
            data = response.json()
            assert data["name"] == "test_dataset"
            assert data["type"] == "table"

    def test_get_dataset_not_found(self, authenticated_client, auth_headers):
        """Test dataset not found."""
        with patch(
            "duckpond.api.routers.datasets.create_catalog_manager"
        ) as mock_catalog:
            mock_manager = AsyncMock()
            mock_manager.get_dataset_metadata.side_effect = DatasetNotFoundError(
                "Dataset not found"
            )
            mock_catalog.return_value = mock_manager

            response = authenticated_client.get("/api/v1/datasets/nonexistent", headers=auth_headers)

            assert response.status_code == 404
            assert "not found" in response.json()["detail"].lower()

    def test_get_dataset_no_auth(self, client):
        """Test get dataset without authentication."""
        response = client.get("/api/v1/datasets/test_dataset")

        assert response.status_code == 401
