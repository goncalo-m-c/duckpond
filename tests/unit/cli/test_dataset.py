"""Tests for dataset CLI commands."""

from unittest.mock import patch

from typer.testing import CliRunner

from duckpond.cli.main import app

runner = CliRunner()


class TestDatasetList:
    """Tests for dataset list command."""

    def test_list_missing_account(self):
        """Test list without required account parameter."""
        result = runner.invoke(app, ["dataset", "list"])
        assert result.exit_code == 2
        # Error messages go to stderr in Typer
        error_output = result.stdout + result.stderr
        assert "Missing option" in error_output or "required" in error_output.lower()


class TestDatasetGet:
    """Tests for dataset get command."""

    def test_get_missing_account(self):
        """Test get without required account parameter."""
        result = runner.invoke(app, ["dataset", "get", "test-dataset"])
        assert result.exit_code == 2
        error_output = result.stdout + result.stderr
        assert "Missing option" in error_output or "required" in error_output.lower()

    def test_get_missing_dataset_name(self):
        """Test get without dataset name."""
        result = runner.invoke(app, ["dataset", "get", "--account", "test-account"])
        assert result.exit_code == 2
        error_output = result.stdout + result.stderr
        assert "Missing argument" in error_output or "required" in error_output.lower()


class TestDatasetDelete:
    """Tests for dataset delete command."""

    @patch("sys.stdin.isatty", return_value=False)
    def test_delete_requires_force_non_interactive(self, mock_isatty):
        """Test delete requires --force in non-interactive mode."""
        result = runner.invoke(
            app,
            ["dataset", "delete", "test-dataset", "--account", "test-account"],
        )
        assert result.exit_code == 1
        error_output = result.stdout + result.stderr
        assert (
            "Cannot confirm deletion" in error_output or "force" in error_output.lower()
        )

    def test_delete_missing_account(self):
        """Test delete without required account parameter."""
        result = runner.invoke(
            app,
            ["dataset", "delete", "test-dataset", "--force"],
        )
        assert result.exit_code == 2
        error_output = result.stdout + result.stderr
        assert "Missing option" in error_output or "required" in error_output.lower()


class TestDatasetUpload:
    """Tests for dataset upload command."""

    def test_upload_missing_account(self, tmp_path):
        """Test upload without required account parameter."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("data")

        result = runner.invoke(
            app,
            ["dataset", "upload", "test-dataset", str(test_file)],
        )
        assert result.exit_code == 2
        error_output = result.stdout + result.stderr
        assert "Missing option" in error_output or "required" in error_output.lower()

    def test_upload_nonexistent_file(self):
        """Test upload with non-existent file."""
        result = runner.invoke(
            app,
            [
                "dataset",
                "upload",
                "test-dataset",
                "/nonexistent/file.csv",
                "--account",
                "test-account",
            ],
        )
        assert result.exit_code == 2
        error_output = result.stdout + result.stderr
        assert "does not exist" in error_output or "Invalid value" in error_output

    def test_upload_missing_dataset_name(self, tmp_path):
        """Test upload without dataset name."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("data")

        result = runner.invoke(
            app,
            ["dataset", "upload", str(test_file), "--account", "test-account"],
        )
        assert result.exit_code == 2
        error_output = result.stdout + result.stderr
        assert (
            "Missing argument" in error_output
            or "required" in error_output.lower()
            or "dataset" in error_output.lower()
        )


class TestDatasetRegister:
    """Tests for dataset register command."""

    def test_register_missing_account(self):
        """Test register without required account parameter."""
        result = runner.invoke(
            app,
            ["dataset", "register", "test-dataset"],
        )
        assert result.exit_code == 2
        error_output = result.stdout + result.stderr
        assert "Missing option" in error_output or "required" in error_output.lower()

    def test_register_missing_dataset_name(self):
        """Test register without dataset name."""
        result = runner.invoke(
            app,
            ["dataset", "register", "--account", "test-account"],
        )
        assert result.exit_code == 2
        error_output = result.stdout + result.stderr
        assert "Missing argument" in error_output or "required" in error_output.lower()


class TestDatasetSnapshots:
    """Tests for dataset snapshots command."""

    def test_snapshots_missing_account(self):
        """Test snapshots without required account parameter."""
        result = runner.invoke(
            app,
            ["dataset", "snapshots", "test-dataset"],
        )
        assert result.exit_code == 2
        error_output = result.stdout + result.stderr
        assert "Missing option" in error_output or "required" in error_output.lower()

    def test_snapshots_missing_dataset_name(self):
        """Test snapshots without dataset name."""
        result = runner.invoke(
            app,
            ["dataset", "snapshots", "--account", "test-account"],
        )
        assert result.exit_code == 2
        error_output = result.stdout + result.stderr
        assert "Missing argument" in error_output or "required" in error_output.lower()


class TestGlobalOptions:
    """Tests for global CLI options with dataset commands."""

    def test_help_flag(self):
        """Test --help flag for dataset commands."""
        result = runner.invoke(app, ["dataset", "--help"])
        assert result.exit_code == 0
        assert "Manage datasets" in result.stdout

    def test_list_help(self):
        """Test help for list command."""
        result = runner.invoke(app, ["dataset", "list", "--help"])
        assert result.exit_code == 0
        assert "List all datasets" in result.stdout or "List datasets" in result.stdout

    def test_get_help(self):
        """Test help for get command."""
        result = runner.invoke(app, ["dataset", "get", "--help"])
        assert result.exit_code == 0
        assert "Get detailed information" in result.stdout

    def test_delete_help(self):
        """Test help for delete command."""
        result = runner.invoke(app, ["dataset", "delete", "--help"])
        assert result.exit_code == 0
        assert "Delete a dataset" in result.stdout

    def test_upload_help(self):
        """Test help for upload command."""
        result = runner.invoke(app, ["dataset", "upload", "--help"])
        assert result.exit_code == 0
        assert "Upload a file" in result.stdout

    def test_register_help(self):
        """Test help for register command."""
        result = runner.invoke(app, ["dataset", "register", "--help"])
        assert result.exit_code == 0
        assert "Register a dataset from storage" in result.stdout

    def test_snapshots_help(self):
        """Test help for snapshots command."""
        result = runner.invoke(app, ["dataset", "snapshots", "--help"])
        assert result.exit_code == 0
        assert "List all snapshots" in result.stdout


class TestDatasetIntegration:
    """Integration tests for dataset commands."""

    def test_command_group_exists(self):
        """Test that dataset command group exists."""
        result = runner.invoke(app, ["dataset", "--help"])
        assert result.exit_code == 0
        assert "Manage datasets" in result.stdout

    def test_all_subcommands_listed(self):
        """Test that all subcommands are listed in help."""
        result = runner.invoke(app, ["dataset", "--help"])
        assert result.exit_code == 0
        assert "list" in result.stdout
        assert "get" in result.stdout
        assert "delete" in result.stdout
        assert "upload" in result.stdout
        assert "register" in result.stdout
        assert "snapshots" in result.stdout

    def test_invalid_subcommand(self):
        """Test that invalid subcommand shows error."""
        result = runner.invoke(app, ["dataset", "invalid-command"])
        assert result.exit_code == 2
        error_output = result.stdout + result.stderr
        assert (
            "No such command" in error_output
            or "invalid" in error_output.lower()
            or "Unknown" in error_output
        )


class TestDatasetParameterValidation:
    """Tests for parameter validation across dataset commands."""

    def test_list_accepts_account(self):
        """Test that list command accepts account parameter."""
        result = runner.invoke(app, ["dataset", "list", "--help"])
        assert result.exit_code == 0
        assert "--account" in result.stdout or "-t" in result.stdout

    def test_get_accepts_account_and_name(self):
        """Test that get command accepts account and dataset name."""
        result = runner.invoke(app, ["dataset", "get", "--help"])
        assert result.exit_code == 0
        assert "--account" in result.stdout or "-t" in result.stdout
        assert "DATASET_NAME" in result.stdout or "dataset" in result.stdout.lower()

    def test_upload_accepts_required_params(self):
        """Test that upload command accepts required parameters."""
        result = runner.invoke(app, ["dataset", "upload", "--help"])
        assert result.exit_code == 0
        assert "--account" in result.stdout or "-t" in result.stdout
        assert "--catalog" in result.stdout or "-c" in result.stdout
        assert "FILE_PATH" in result.stdout or "file" in result.stdout.lower()

    def test_register_accepts_catalog_option(self):
        """Test that register command accepts catalog option."""
        result = runner.invoke(app, ["dataset", "register", "--help"])
        assert result.exit_code == 0
        assert "--catalog" in result.stdout or "-c" in result.stdout

    def test_delete_accepts_force_option(self):
        """Test that delete command accepts force option."""
        result = runner.invoke(app, ["dataset", "delete", "--help"])
        assert result.exit_code == 0
        assert "--force" in result.stdout or "-f" in result.stdout
