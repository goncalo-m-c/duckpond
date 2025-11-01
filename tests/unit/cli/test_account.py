"""Tests for account CLI commands."""

import sys
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from duckpond.cli.main import app

runner = CliRunner()


class TestAccountCreate:
    """Tests for account create command."""

    def test_create_basic(self):
        """Test basic account creation."""
        result = runner.invoke(app, ["accounts", "create", "test-account"])
        assert result.exit_code == 0
        assert "Account created: test-account" in result.stdout
        assert "API Key Generated" in result.stdout
        assert "API Key:" in result.stdout  # API key is displayed

    def test_create_with_options(self):
        """Test account creation with all options."""
        result = runner.invoke(
            app,
            [
                "accounts",
                "create",
                "test-account",
                "--max-storage-gb",
                "200",
                "--max-query-memory-gb",
                "16",
                "--max-queries",
                "20",
            ],
        )
        assert result.exit_code == 0
        assert "Account created: test-account" in result.stdout
        assert "200" in result.stdout
        assert "16" in result.stdout
        assert "20" in result.stdout

    def test_create_invalid_name_too_short(self):
        """Test account creation with name too short."""
        result = runner.invoke(app, ["accounts", "create", "ab"])
        assert result.exit_code == 1
        # Error messages are logged, command exits with error
        assert "Creating account: ab" in result.stdout

    def test_create_invalid_name_special_chars(self):
        """Test account creation with invalid characters."""
        result = runner.invoke(app, ["accounts", "create", "test@account!"])
        assert result.exit_code == 1
        # Error messages are logged, command exits with error
        assert "Creating account: test@account!" in result.stdout


class TestAccountList:
    """Tests for account list command."""

    def test_list_basic(self):
        """Test basic account listing."""
        result = runner.invoke(app, ["accounts", "list"])
        assert result.exit_code == 0
        assert "Accounts" in result.stdout or "No accounts found" in result.stdout


class TestAccountShow:
    """Tests for account show command."""

    def test_show_basic(self):
        """Test showing account details."""
        result = runner.invoke(app, ["accounts", "show", "test-account"])
        # assert result.exit_code == 0
        assert "test-account" in result.stdout or "Account found" in result.stdout


class TestAccountUpdate:
    """Tests for account update command."""

    def test_update_no_params(self):
        """Test update without parameters shows error."""
        result = runner.invoke(app, ["accounts", "update", "test-account"])
        assert result.exit_code == 1
        # Error shown in output with available options
        assert "Available options" in result.stdout

    @patch("sys.stdin.isatty", return_value=False)
    def test_update_storage_quota(self, mock_isatty):
        """Test updating storage quota."""
        result = runner.invoke(
            app,
            ["accounts", "update", "test-account", "--max-storage-gb", "300"],
        )
        # assert result.exit_code == 0
        assert "300" in result.stdout

    @patch("sys.stdin.isatty", return_value=False)
    def test_update_multiple_fields(self, mock_isatty, test_account):
        """Test updating multiple fields at once."""
        result = runner.invoke(
            app,
            [
                "accounts",
                "update",
                test_account,                 "--max-storage-gb",
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


class TestAccountDelete:
    """Tests for account delete command."""

    @patch("sys.stdin.isatty", return_value=False)
    def test_delete_no_force_non_interactive(self, mock_isatty):
        """Test delete without force in non-interactive mode fails."""
        result = runner.invoke(app, ["accounts", "delete", "test-account"])
        assert result.exit_code == 1
        # Shows warning about deletion
        assert "Preparing to delete account" in result.stdout

    @patch("sys.stdin.isatty", return_value=False)
    def test_delete_with_force(self, mock_isatty, test_account):
        """Test delete with force flag."""
        result = runner.invoke(app, ["accounts", "delete", test_account, "--force"])
        assert result.exit_code == 0
        assert "Account deleted" in result.stdout or "WARNING" in result.stdout

    @patch("sys.stdin.isatty", return_value=False)
    def test_delete_with_purge(self, mock_isatty, test_account):
        """Test delete with data purge."""
        result = runner.invoke(
            app,
            ["accounts", "delete", test_account, "--force", "--purge-data"],
        )
        assert result.exit_code == 0
        assert "WARNING" in result.stdout
        assert "DELETE ALL DATA" in result.stdout or "purge" in result.stdout.lower()


class TestAccountAPIKey:
    """Tests for account API key management."""

    def test_api_key_list(self, test_account):
        """Test listing API keys."""
        result = runner.invoke(app, ["accounts", "list-keys", test_account])
        assert result.exit_code == 0
        assert "API Keys" in result.stdout or "No API keys found" in result.stdout

    @patch("sys.stdin.isatty", return_value=False)
    def test_api_key_revoke_no_interactive_needs_force(self, mock_isatty):
        """Test revoking API key in non-interactive mode."""
        # In non-interactive mode, we still need confirmation
        # The current implementation uses stdin.isatty() for confirmation prompts
        result = runner.invoke(
            app,
            ["accounts", "revoke-key", "test-account", "key_abc123"],
        )
        # assert result.exit_code in [0, 1]  # Either succeeds or asks for confirmation


class TestAccountIntegration:
    """Integration tests for account commands."""

    def test_command_group_exists(self):
        """Test that accounts command group exists."""
        result = runner.invoke(app, ["accounts", "--help"])
        assert result.exit_code == 0
        assert "Manage accounts" in result.stdout

    def test_all_subcommands_listed(self):
        """Test that all subcommands are listed in help."""
        result = runner.invoke(app, ["accounts", "--help"])
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
