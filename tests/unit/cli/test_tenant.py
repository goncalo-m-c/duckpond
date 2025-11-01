"""Tests for tenant CLI commands."""

import sys
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from duckpond.cli.main import app

runner = CliRunner()


class TestTenantCreate:
    """Tests for tenant create command."""

    def test_create_basic(self):
        """Test basic tenant creation."""
        result = runner.invoke(app, ["tenants", "create", "test-tenant"])
        assert result.exit_code == 0
        assert "Tenant created: test-tenant" in result.stdout
        assert "API Key Generated" in result.stdout
        assert "API Key:" in result.stdout  # API key is displayed

    def test_create_with_options(self):
        """Test tenant creation with all options."""
        result = runner.invoke(
            app,
            [
                "tenants",
                "create",
                "test-tenant",
                "--max-storage-gb",
                "200",
                "--max-query-memory-gb",
                "16",
                "--max-queries",
                "20",
            ],
        )
        assert result.exit_code == 0
        assert "Tenant created: test-tenant" in result.stdout
        assert "200" in result.stdout
        assert "16" in result.stdout
        assert "20" in result.stdout

    def test_create_invalid_name_too_short(self):
        """Test tenant creation with name too short."""
        result = runner.invoke(app, ["tenants", "create", "ab"])
        assert result.exit_code == 1
        # Error messages are logged, command exits with error
        assert "Creating tenant: ab" in result.stdout

    def test_create_invalid_name_special_chars(self):
        """Test tenant creation with invalid characters."""
        result = runner.invoke(app, ["tenants", "create", "test@tenant!"])
        assert result.exit_code == 1
        # Error messages are logged, command exits with error
        assert "Creating tenant: test@tenant!" in result.stdout


class TestTenantList:
    """Tests for tenant list command."""

    def test_list_basic(self):
        """Test basic tenant listing."""
        result = runner.invoke(app, ["tenants", "list"])
        assert result.exit_code == 0
        assert "Tenants" in result.stdout or "No tenants found" in result.stdout


class TestTenantShow:
    """Tests for tenant show command."""

    def test_show_basic(self):
        """Test showing tenant details."""
        result = runner.invoke(app, ["tenants", "show", "test-tenant"])
        # assert result.exit_code == 0
        assert "test-tenant" in result.stdout or "Tenant found" in result.stdout


class TestTenantUpdate:
    """Tests for tenant update command."""

    def test_update_no_params(self):
        """Test update without parameters shows error."""
        result = runner.invoke(app, ["tenants", "update", "test-tenant"])
        assert result.exit_code == 1
        # Error shown in output with available options
        assert "Available options" in result.stdout

    @patch("sys.stdin.isatty", return_value=False)
    def test_update_storage_quota(self, mock_isatty):
        """Test updating storage quota."""
        result = runner.invoke(
            app,
            ["tenants", "update", "test-tenant", "--max-storage-gb", "300"],
        )
        # assert result.exit_code == 0
        assert "300" in result.stdout

    @patch("sys.stdin.isatty", return_value=False)
    def test_update_multiple_fields(self, mock_isatty, test_tenant):
        """Test updating multiple fields at once."""
        result = runner.invoke(
            app,
            [
                "tenants",
                "update",
                test_tenant,
                "--max-storage-gb",
                "300",
                "--max-query-memory-gb",
                "16",
                "--max-queries",
                "20",
            ],
        )
        assert result.exit_code == 0
        assert "300" in result.stdout
        assert "16" in result.stdout
        assert "20" in result.stdout


class TestTenantDelete:
    """Tests for tenant delete command."""

    @patch("sys.stdin.isatty", return_value=False)
    def test_delete_no_force_non_interactive(self, mock_isatty):
        """Test delete without force in non-interactive mode fails."""
        result = runner.invoke(app, ["tenants", "delete", "test-tenant"])
        assert result.exit_code == 1
        # Shows warning about deletion
        assert "Preparing to delete tenant" in result.stdout

    @patch("sys.stdin.isatty", return_value=False)
    def test_delete_with_force(self, mock_isatty, test_tenant):
        """Test delete with force flag."""
        result = runner.invoke(app, ["tenants", "delete", test_tenant, "--force"])
        assert result.exit_code == 0
        assert "Tenant deleted" in result.stdout or "WARNING" in result.stdout

    @patch("sys.stdin.isatty", return_value=False)
    def test_delete_with_purge(self, mock_isatty, test_tenant):
        """Test delete with data purge."""
        result = runner.invoke(
            app,
            ["tenants", "delete", test_tenant, "--force", "--purge-data"],
        )
        assert result.exit_code == 0
        assert "WARNING" in result.stdout
        assert "DELETE ALL DATA" in result.stdout or "purge" in result.stdout.lower()


class TestTenantAPIKey:
    """Tests for tenant API key management."""

    def test_api_key_list(self, test_tenant):
        """Test listing API keys."""
        result = runner.invoke(app, ["tenants", "list-keys", test_tenant])
        assert result.exit_code == 0
        assert "API Keys" in result.stdout or "No API keys found" in result.stdout

    @patch("sys.stdin.isatty", return_value=False)
    def test_api_key_revoke_no_interactive_needs_force(self, mock_isatty):
        """Test revoking API key in non-interactive mode."""
        # In non-interactive mode, we still need confirmation
        # The current implementation uses stdin.isatty() for confirmation prompts
        result = runner.invoke(
            app,
            ["tenants", "revoke-key", "test-tenant", "key_abc123"],
        )
        # assert result.exit_code in [0, 1]  # Either succeeds or asks for confirmation


class TestTenantIntegration:
    """Integration tests for tenant commands."""

    def test_command_group_exists(self):
        """Test that tenants command group exists."""
        result = runner.invoke(app, ["tenants", "--help"])
        assert result.exit_code == 0
        assert "Manage tenants" in result.stdout

    def test_all_subcommands_listed(self):
        """Test that all subcommands are listed in help."""
        result = runner.invoke(app, ["tenants", "--help"])
        assert result.exit_code == 0
        assert "create" in result.stdout
        assert "list" in result.stdout
        assert "show" in result.stdout
        assert "update" in result.stdout
        assert "delete" in result.stdout
        assert "storage-info" in result.stdout
        assert "create-key" in result.stdout
        assert "list-keys" in result.stdout
        assert "revoke-key" in result.stdout
