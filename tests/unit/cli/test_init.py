"""Tests for init commands."""

import tempfile
from pathlib import Path

from typer.testing import CliRunner

from duckpond.cli.main import app

runner = CliRunner()


class TestInitCommand:
    """Tests for init init command."""

    def test_init_help(self):
        """Test init help command."""
        result = runner.invoke(app, ["init", "--help"])
        assert result.exit_code == 0
        assert "Initialize DuckPond application" in result.stdout

    def test_init_init_help(self):
        """Test init init help command."""
        result = runner.invoke(app, ["init", "init", "--help"])
        assert result.exit_code == 0
        assert "Initialize DuckPond application" in result.stdout
        assert "--force" in result.stdout

    def test_init_init_with_temp_dir(self):
        """Test init init command creates directory structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Run init with force flag
            result = runner.invoke(app, ["init", "init", "--force"])

            # Should succeed
            assert result.exit_code == 0
            assert "Initialization Complete" in result.stdout
            assert "Storage initialized" in result.stdout

            # Check that default directories were created
            from duckpond.config import get_settings

            settings = get_settings()
            storage_path = settings.local_storage_path

            # These should exist after init
            assert storage_path.exists()
            assert (storage_path / "tenants").exists()
            assert (storage_path / "temp").exists()

    def test_init_init_without_force_existing(self):
        """Test init init with existing database."""
        # Use default settings which will have existing database
        result = runner.invoke(app, ["init", "init"])

        # In non-interactive mode, should show warning or proceed
        # Depends on whether database already exists from previous runs
        if "already exists" in result.stdout or "already exists" in result.stderr:
            # Database exists, expects force flag
            assert result.exit_code in [0, 1]
        else:
            # Database doesn't exist, can proceed
            assert result.exit_code == 0


class TestInitIntegration:
    """Integration tests for init commands."""

    def test_command_group_exists(self):
        """Test that init command group exists."""
        result = runner.invoke(app, ["init", "--help"])
        assert result.exit_code == 0
        assert "Initialize DuckPond application" in result.stdout

    def test_init_subcommand_listed(self):
        """Test that init subcommand is listed in help."""
        result = runner.invoke(app, ["init", "--help"])
        assert result.exit_code == 0
        assert "init" in result.stdout.lower()

    def test_invalid_subcommand(self):
        """Test that invalid subcommand shows error."""
        result = runner.invoke(app, ["init", "invalid-command"])
        assert result.exit_code == 2
        error_output = result.stdout + result.stderr
        assert (
            "No such command" in error_output
            or "invalid" in error_output.lower()
            or "Unknown" in error_output
        )


class TestInitParameterValidation:
    """Tests for parameter validation."""

    def test_init_accepts_force_option(self):
        """Test that init command accepts force option."""
        result = runner.invoke(app, ["init", "init", "--help"])
        assert result.exit_code == 0
        assert "--force" in result.stdout or "-f" in result.stdout

    def test_init_with_force_flag(self):
        """Test init with force flag works."""
        result = runner.invoke(app, ["init", "init", "--force"])
        assert result.exit_code == 0
        assert "Initialization Complete" in result.stdout
