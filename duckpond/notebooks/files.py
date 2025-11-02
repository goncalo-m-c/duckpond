"""Notebook file management utilities."""

from pathlib import Path

import structlog

from duckpond.notebooks.exceptions import NotebookNotFoundException
from duckpond.notebooks.security import validate_filename, validate_notebook_path

logger = structlog.get_logger(__name__)


DEFAULT_NOTEBOOK_TEMPLATE = '''import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    import duckdb
    import os
    return mo, duckdb, os


@app.cell
def __(mo):
    mo.md(
        r"""
        # DuckPond Notebook

        This notebook has direct access to your tenant's DuckDB database.
        """
    )
    return


if __name__ == "__main__":
    app.run()
'''


async def create_notebook(
    filename: str,
    account_notebook_dir: Path,
    content: str | None = None,
) -> Path:
    """
    Create a new notebook file.

    Args:
        filename: Notebook filename (must end with .py)
        account_notebook_dir: Tenant's notebook directory
        content: Optional notebook content (uses default template if None)

    Returns:
        Absolute path to created notebook

    Raises:
        PathSecurityException: If filename is invalid
        FileExistsError: If file already exists
    """
    validate_filename(filename)

    notebook_path = account_notebook_dir / filename

    if notebook_path.exists():
        raise FileExistsError(f"Notebook already exists: {filename}")

    notebook_content = content or DEFAULT_NOTEBOOK_TEMPLATE

    notebook_path.write_text(notebook_content, encoding="utf-8")

    logger.info(
        "notebook_created",
        filename=filename,
        path=str(notebook_path),
    )

    return notebook_path


async def list_notebooks(account_notebook_dir: Path) -> list[dict]:
    """
    List all notebooks in tenant's directory.

    Args:
        account_notebook_dir: Tenant's notebook directory

    Returns:
        List of notebook metadata dictionaries
    """
    if not account_notebook_dir.exists():
        return []

    notebooks = []
    for notebook_file in account_notebook_dir.glob("*.py"):
        stat = notebook_file.stat()
        notebooks.append(
            {
                "filename": notebook_file.name,
                "path": str(notebook_file.relative_to(account_notebook_dir)),
                "size_bytes": stat.st_size,
                "modified_at": stat.st_mtime,
            }
        )

    notebooks.sort(key=lambda x: x["modified_at"], reverse=True)

    logger.debug(
        "notebooks_listed",
        count=len(notebooks),
        directory=str(account_notebook_dir),
    )

    return notebooks


async def read_notebook(
    notebook_path: str | Path,
    account_notebook_dir: Path,
) -> str:
    """
    Read notebook file content.

    Args:
        notebook_path: Path to notebook (relative or absolute)
        account_notebook_dir: Tenant's notebook directory

    Returns:
        Notebook content as string

    Raises:
        PathSecurityException: If path validation fails
        NotebookNotFoundException: If file doesn't exist
    """
    validated_path = validate_notebook_path(notebook_path, account_notebook_dir)

    if not validated_path.exists():
        raise NotebookNotFoundException(str(validated_path))

    content = validated_path.read_text(encoding="utf-8")

    logger.debug(
        "notebook_read",
        path=str(validated_path),
        size=len(content),
    )

    return content


async def update_notebook(
    notebook_path: str | Path,
    account_notebook_dir: Path,
    content: str,
) -> Path:
    """
    Update notebook file content.

    Args:
        notebook_path: Path to notebook (relative or absolute)
        account_notebook_dir: Tenant's notebook directory
        content: New notebook content

    Returns:
        Absolute path to updated notebook

    Raises:
        PathSecurityException: If path validation fails
        NotebookNotFoundException: If file doesn't exist
    """
    validated_path = validate_notebook_path(notebook_path, account_notebook_dir)

    if not validated_path.exists():
        raise NotebookNotFoundException(str(validated_path))

    validated_path.write_text(content, encoding="utf-8")

    logger.info(
        "notebook_updated",
        path=str(validated_path),
        size=len(content),
    )

    return validated_path


async def delete_notebook(
    notebook_path: str | Path,
    account_notebook_dir: Path,
) -> None:
    """
    Delete notebook file.

    Args:
        notebook_path: Path to notebook (relative or absolute)
        account_notebook_dir: Tenant's notebook directory

    Raises:
        PathSecurityException: If path validation fails
        NotebookNotFoundException: If file doesn't exist
    """
    validated_path = validate_notebook_path(notebook_path, account_notebook_dir)

    if not validated_path.exists():
        raise NotebookNotFoundException(str(validated_path))

    validated_path.unlink()

    logger.info(
        "notebook_deleted",
        path=str(validated_path),
    )
