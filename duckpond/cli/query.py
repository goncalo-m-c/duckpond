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
from duckpond.query.docker_executor import DockerQueryExecutor
from duckpond.accounts.manager import AccountManager

app = typer.Typer(help="Execute SQL queries")
console = Console()
logger = get_logger(__name__)


@app.command()
def execute(
    account_id: str = typer.Option(..., "--account", "-a", help="Account ID"),
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
    pretty: bool = typer.Option(True, "--pretty/--no-pretty", help="Pretty-print JSON output"),
    use_docker: bool = typer.Option(
        False, "--docker", help="Execute query in isolated Docker container"
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

        duckpond query execute --account abc123 --sql "SELECT * FROM catalog.sales" --output csv

        duckpond query execute --account abc123 --sql "SELECT * FROM catalog.sales" --docker

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
        if use_docker:
            print_info("Using Docker isolation")
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

                exec_format = output_format if output_format != "table" else "json"

                if use_docker:
                    # Use Docker-based executor
                    executor = DockerQueryExecutor.from_account(account)
                    result = await executor.execute_query(
                        sql=query_sql,
                        output_format=exec_format,
                        limit=limit,
                        timeout_seconds=timeout,
                        attach_catalog=attach_catalog,
                    )
                else:
                    # Use standard executor
                    ducklake = AccountDuckLakeManager(account)
                    await ducklake.initialize()
                    try:
                        executor = QueryExecutor(ducklake)
                        result = await executor.execute_query(
                            sql=query_sql,
                            output_format=exec_format,
                            limit=limit,
                            timeout_seconds=timeout,
                            attach_catalog=attach_catalog,
                        )
                    finally:
                        await ducklake.close()

                return result

        result = asyncio.run(_execute())

        if export_file:
            _export_results(result, export_file, output_format, pretty)
            print_success(f"Results exported to: {export_file}")
        else:
            _display_results(result, output_format, pretty)

        print_info(f"\nRows: {result.row_count:,} | Time: {result.execution_time_seconds:.3f}s")

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
    use_docker: bool = typer.Option(
        False, "--docker", help="Execute EXPLAIN in isolated Docker container"
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
        duckpond query explain --account abc123 --sql "SELECT * FROM catalog.sales" --docker
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
        if use_docker:
            print_info("Using Docker isolation")
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

                if use_docker:
                    # Use Docker-based executor
                    executor = DockerQueryExecutor.from_account(account)
                    plan = await executor.explain_query(query_sql, attach_catalog=attach_catalog)
                else:
                    # Use standard executor
                    ducklake = AccountDuckLakeManager(account)
                    await ducklake.initialize()
                    try:
                        executor = QueryExecutor(ducklake)
                        plan = await executor.explain_query(
                            query_sql, attach_catalog=attach_catalog
                        )
                    finally:
                        await ducklake.close()

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


@app.command()
def shell(
    account_id: str = typer.Option(..., "--account", "-a", help="Account ID"),
    attach_catalog: Optional[str] = typer.Option(
        None,
        "--catalog",
        "-c",
        help="Attach additional catalog (provide catalog name, e.g., 'default')",
    ),
    use_docker: bool = typer.Option(
        True, "--docker/--no-docker", help="Use Docker isolation (recommended)"
    ),
) -> None:
    """
    Open interactive DuckDB shell with catalog attached.

    Launches an interactive DuckDB shell session with the account's catalog
    already attached and ready to use. Optionally attach additional catalogs.

    The shell runs in Docker by default for isolation and consistency.

    Examples:
        duckpond query shell --account test8

        duckpond query shell --account test8 --catalog default

        duckpond query shell --account test8 --no-docker
    """
    try:
        print_info(f"Opening DuckDB shell for account: {account_id}")
        if use_docker:
            print_info("Using Docker isolation")
        if attach_catalog:
            print_info(f"Attaching catalog: {attach_catalog}")

        async def _open_shell():
            engine = get_engine()
            session_factory = create_session_factory(engine)
            async with get_session(session_factory) as session:
                manager = AccountManager(session)
                account = await manager.get_account(account_id)

                if not account:
                    raise ValueError(f"Account not found: {account_id}")

                if use_docker:
                    # Use Docker-based shell
                    from duckpond.docker.runners import QueryRunner
                    from pathlib import Path
                    from duckpond.config import get_settings

                    settings = get_settings()
                    account_data_dir = (
                        Path(settings.local_storage_path).expanduser()
                        / "accounts"
                        / account.account_id
                    )

                    # Determine catalog path
                    catalog_url = account.ducklake_catalog_url
                    if catalog_url.startswith("sqlite:"):
                        catalog_path = Path(catalog_url[7:])
                    else:
                        catalog_path = Path(catalog_url)

                    runner = QueryRunner(
                        account_data_dir=account_data_dir,
                        account_id=account.account_id,
                        catalog_path=catalog_path,
                    )

                    # Start container
                    print_info("Starting Docker container...")
                    await runner.start()

                    try:
                        # Build initialization commands
                        init_commands = []
                        init_commands.append("INSTALL ducklake;")
                        init_commands.append("LOAD ducklake;")

                        # Attach main catalog - properly escape single quotes in path
                        ducklake_url = f"sqlite:{catalog_path}"
                        # Escape single quotes for SQL
                        escaped_url = ducklake_url.replace("'", "''")
                        init_commands.append(f"ATTACH '{escaped_url}' AS catalog (TYPE ducklake);")

                        # Attach additional catalog if specified
                        if attach_catalog:
                            attach_catalog_path = (
                                account_data_dir / f"{attach_catalog}_catalog.sqlite"
                            )
                            if not attach_catalog_path.exists():
                                # List available catalogs to help user
                                available = list(account_data_dir.glob("*_catalog.sqlite"))
                                available_names = [
                                    p.stem.replace("_catalog", "") for p in available
                                ]
                                raise ValueError(
                                    f"Catalog '{attach_catalog}' not found at: {attach_catalog_path}\n"
                                    f"Available catalogs: {', '.join(available_names)}"
                                )
                            attach_ducklake_url = f"sqlite:{attach_catalog_path}"
                            # Escape single quotes for SQL
                            escaped_attach_url = attach_ducklake_url.replace("'", "''")
                            init_commands.append(
                                f"ATTACH '{escaped_attach_url}' AS \"{attach_catalog}\" (TYPE ducklake);"
                            )

                        # Create init SQL file
                        init_sql = "\n".join(init_commands)

                        print_success("\nDuckDB shell ready!")
                        print_info("Available catalogs:")
                        print_info("  - catalog (main)")
                        if attach_catalog:
                            print_info(f'  - "{attach_catalog}"')
                        print_info("\nType .help for DuckDB commands, Ctrl+D to exit\n")

                        # Open interactive shell with init commands
                        import subprocess

                        container_id = runner.get_container_id()

                        # Write init SQL to a file in the container
                        # Use proper shell quoting to handle special characters
                        import shlex

                        write_init_cmd = [
                            "docker",
                            "exec",
                            container_id,
                            "sh",
                            "-c",
                            f"cat > /tmp/init.sql << 'EOF'\n{init_sql}\nEOF",
                        ]
                        subprocess.run(write_init_cmd, check=True)

                        # Use docker exec with interactive terminal and -init to execute init
                        shell_cmd = [
                            "docker",
                            "exec",
                            "-it",
                            container_id,
                            "duckdb",
                            "-init",
                            "/tmp/init.sql",
                        ]

                        subprocess.run(shell_cmd)

                    finally:
                        print_info("\nClosing shell and stopping container...")
                        await runner.stop()
                        print_success("Shell closed successfully")

                else:
                    # Use direct local shell (no Docker)
                    print_warning("Direct shell not implemented yet. Use --docker flag.")
                    raise typer.Exit(1)

        asyncio.run(_open_shell())

    except typer.Exit:
        raise
    except Exception as e:
        print_error(f"Shell failed: {str(e)}")
        logger.exception("Shell error")
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


def _export_results(result, export_file: Path, output_format: str, pretty: bool = True) -> None:
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
