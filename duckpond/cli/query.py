"""Query execution commands for DuckPond."""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from duckpond.cli.output import (
    print_error,
    print_info,
    print_json,
    print_success,
    print_table,
    print_warning,
)
from duckpond.db.session import get_engine, create_session_factory, get_session
from duckpond.logging_config import get_logger
from duckpond.query.ducklake import AccountDuckLakeManager
from duckpond.query.executor import QueryExecutor
from duckpond.accounts.manager import AccountManager

app = typer.Typer(help="Execute SQL queries")
console = Console()
logger = get_logger(__name__)


@app.command()
def execute(
    account_id: str = typer.Option(..., "--account", "-t", help="Account ID"),
    sql: Optional[str] = typer.Option(None, "--sql", "-s", help="SQL query string"),
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-f",
        help="SQL file path",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    attach_catalog: Optional[str] = typer.Option(
        None,
        "--catalog",
        "-c",
        help="Attach additional catalog (provide catalog name, e.g., 'default')",
    ),
    output_format: str = typer.Option(
        "table", "--output", "-o", help="Output format: table, json, csv, arrow"
    ),
    limit: Optional[int] = typer.Option(None, "--limit", "-l", help="Row limit"),
    timeout: int = typer.Option(30, "--timeout", help="Query timeout in seconds"),
    export_file: Optional[Path] = typer.Option(
        None, "--export", "-e", help="Export results to file"
    ),
    timestamp: Optional[str] = typer.Option(
        None, "--as-of", help="Time travel query (AS OF timestamp)"
    ),
    pretty: bool = typer.Option(
        True, "--pretty/--no-pretty", help="Pretty-print JSON output"
    ),
) -> None:
    """
    Execute SQL query against account's DuckLake catalog.

    Queries are executed with validation and security checks. Results can be
    output in multiple formats and optionally exported to a file.

    Examples:
        duckpond query execute --account abc123 --sql "SELECT * FROM catalog.sales LIMIT 10"

        duckpond query execute --account abc123 --file query.sql

        duckpond query execute --account abc123 --attach "default" \\
            --sql "SELECT * FROM default.my_table"

        duckpond query execute --account abc123 --sql "SELECT * FROM catalog.sales" --export results.json

        duckpond query execute --account abc123 --sql "SELECT * FROM catalog.sales" --as-of "2024-01-01"

        duckpond query execute --account abc123 --sql "SELECT * FROM catalog.sales" --output csv
    """
    try:
        if not sql and not file:
            print_error("Provide SQL via --sql or --file")
            raise typer.Exit(1)

        if sql and file:
            print_error("Provide either --sql or --file, not both")
            raise typer.Exit(1)

        if file:
            print_info(f"Reading SQL from: {file}")
            query_sql = file.read_text().strip()
        else:
            query_sql = sql.strip()

        if not query_sql:
            print_error("SQL query cannot be empty")
            raise typer.Exit(1)

        valid_formats = ["table", "json", "csv", "arrow"]
        if output_format not in valid_formats:
            print_error(f"Invalid output format: {output_format}")
            print_info(f"Valid formats: {', '.join(valid_formats)}")
            raise typer.Exit(1)

        if timestamp:
            try:
                ts = datetime.fromisoformat(timestamp)
                query_sql = f"{query_sql} FOR SYSTEM_TIME AS OF '{ts.isoformat()}'"
                print_info(f"Time travel query: {timestamp}")
            except ValueError:
                print_error(f"Invalid timestamp format: {timestamp}")
                print_info("Use ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS")
                raise typer.Exit(1)

        print_info(f"Executing query for account: {account_id}")
        if limit:
            print_info(f"Row limit: {limit}")
        if attach_catalog:
            print_info(f"Attaching catalog: {attach_catalog}")

        async def _execute():
            engine = get_engine()
            session_factory = create_session_factory(engine)
            async with get_session(session_factory) as session:
                manager = AccountManager(session)
                account = await manager.get_account(account_id)

                if not account:
                    raise ValueError(f"Account not found: {account_id}")

                ducklake = AccountDuckLakeManager(account)
                executor = QueryExecutor(ducklake)

                exec_format = output_format if output_format != "table" else "json"

                result = await executor.execute_query(
                    sql=query_sql,
                    output_format=exec_format,
                    limit=limit,
                    timeout_seconds=timeout,
                    attach_catalog=attach_catalog,
                )

                return result

        result = asyncio.run(_execute())

        if export_file:
            _export_results(result, export_file, output_format, pretty)
            print_success(f"Results exported to: {export_file}")
        else:
            _display_results(result, output_format, pretty)

        print_info(
            f"\nRows: {result.row_count:,} | Time: {result.execution_time_seconds:.3f}s"
        )

    except typer.Exit:
        raise
    except Exception as e:
        print_error(f"Query failed: {str(e)}")
        logger.exception("Query execution error")
        raise typer.Exit(1)


@app.command()
def explain(
    account_id: str = typer.Option(..., "--account", "-t", help="Account ID"),
    sql: Optional[str] = typer.Option(None, "--sql", "-s", help="SQL query string"),
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-f",
        help="SQL file path",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    attach_catalog: Optional[str] = typer.Option(
        None,
        "--catalog",
        "-c",
        help="Attach additional catalog (provide catalog name, e.g., 'default')",
    ),
) -> None:
    """
    Show query execution plan.

    Displays the query plan that DuckDB would use to execute the query,
    without actually running it.

    Examples:
        duckpond query explain --account abc123 --sql "SELECT * FROM catalog.sales"
        duckpond query explain --account abc123 --file query.sql
        duckpond query explain --account abc123 --attach default --sql "SELECT * FROM default.sales"
    """
    try:
        if not sql and not file:
            print_error("Provide SQL via --sql or --file")
            raise typer.Exit(1)

        if file:
            query_sql = file.read_text().strip()
        else:
            query_sql = sql.strip()

        print_info(f"Explaining query for account: {account_id}")
        if attach_catalog:
            print_info(f"Attaching catalog: {attach_catalog}")

        async def _explain():
            engine = get_engine()
            session_factory = create_session_factory(engine)
            async with get_session(session_factory) as session:
                manager = AccountManager(session)
                account = await manager.get_account(account_id)

                if not account:
                    raise ValueError(f"Account not found: {account_id}")

                ducklake = AccountDuckLakeManager(account)
                executor = QueryExecutor(ducklake)

                plan = await executor.explain_query(
                    query_sql, attach_catalog=attach_catalog
                )
                return plan

        plan = asyncio.run(_explain())

        console.print("\n[bold]Query Execution Plan:[/bold]\n")
        console.print(plan)

    except typer.Exit:
        raise
    except Exception as e:
        print_error(f"EXPLAIN failed: {str(e)}")
        logger.exception("EXPLAIN error")
        raise typer.Exit(1)


def _display_results(result, output_format: str, pretty: bool = True) -> None:
    """Display query results in specified format."""
    if output_format == "table":
        if isinstance(result.data, list) and result.data:
            print_table(result.data, title="Query Results")
        elif isinstance(result.data, list):
            print_warning("No results returned")
        else:
            print_error("Cannot display non-JSON data in table format")

    elif output_format == "json":
        if isinstance(result.data, list):
            if pretty:
                print_json(result.data)
            else:
                print(json.dumps(result.data))
        else:
            print_error("Cannot display non-JSON data in JSON format")

    elif output_format == "csv":
        if isinstance(result.data, str):
            console.print(result.data)
        else:
            print_error("CSV data not available")

    elif output_format == "arrow":
        print_info("Arrow format results written (use --export to save to file)")
        print_warning("Arrow tables cannot be displayed directly in terminal")


def _export_results(
    result, export_file: Path, output_format: str, pretty: bool = True
) -> None:
    """Export query results to file."""
    try:
        if output_format == "json" or output_format == "table":
            if isinstance(result.data, list):
                with open(export_file, "w") as f:
                    if pretty:
                        json.dump(result.data, f, indent=2)
                    else:
                        json.dump(result.data, f)
            else:
                raise ValueError("Cannot export non-JSON data to JSON file")

        elif output_format == "csv":
            if isinstance(result.data, str):
                export_file.write_text(result.data)
            else:
                raise ValueError("CSV data not available")

        elif output_format == "arrow":
            import pyarrow as pa
            import pyarrow.feather as feather

            if isinstance(result.data, pa.Table):
                feather.write_feather(result.data, export_file)
            else:
                raise ValueError("Arrow data not available")

    except Exception as e:
        raise Exception(f"Failed to export results: {e}")
