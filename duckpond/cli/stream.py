"""Streaming ingestion commands for DuckPond.

This module provides CLI commands for ingesting data using Arrow IPC streaming,
which offers zero-copy performance for high-throughput data ingestion.
"""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
)
from rich.table import Table

from duckpond.cli.output import (
    print_dict,
    print_error,
    print_info,
    print_success,
    print_warning,
)
from duckpond.config import get_settings
from duckpond.logging_config import get_logger
from duckpond.streaming import StreamingIngestor, BufferManager

app = typer.Typer(help="Streaming data ingestion using Arrow IPC format")
console = Console()
logger = get_logger(__name__)


def _format_bytes(bytes_value: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"


def _format_duration(seconds: float) -> str:
    """Format duration in seconds as human-readable string."""
    if seconds < 1:
        return f"{seconds * 1000:.2f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.1f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


@app.command()
def ingest(
    ipc_file: Path = typer.Argument(
        ...,
        help="Path to Arrow IPC stream file to ingest",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    stream: str = typer.Option(..., "--stream", "-d", help="Target stream name"),
    tenant_id: str = typer.Option(..., "--tenant", "-t", help="Tenant ID (UUID)"),
    storage_path: Optional[Path] = typer.Option(
        None,
        "--storage",
        "-s",
        help="Storage path for Parquet files (default: from config)",
    ),
    batch_flush_count: int = typer.Option(
        10,
        "--batch-flush",
        "-b",
        help="Number of batches to buffer before flushing to Parquet",
        min=1,
        max=1000,
    ),
    max_buffer_mb: int = typer.Option(
        100,
        "--buffer-size",
        help="Maximum buffer size in megabytes",
        min=10,
        max=10240,
    ),
    max_queue_depth: int = typer.Option(
        100,
        "--queue-depth",
        help="Maximum queue depth for buffering",
        min=10,
        max=10000,
    ),
    show_progress: bool = typer.Option(
        True,
        "--progress/--no-progress",
        help="Show progress during ingestion",
    ),
) -> None:
    """
    Ingest Arrow IPC stream file into a streams.

    Reads an Arrow IPC stream file and ingests it into the specified stream
    using zero-copy streaming with batching and backpressure management.
    The data is written as Parquet files and registered in the catalog.

    Arrow IPC format provides efficient, zero-copy serialization for high-
    throughput data ingestion. This is ideal for:
    - Bulk data imports from Arrow-compatible systems
    - High-frequency data ingestion pipelines
    - ETL processes with large datasets

    Examples:
        duckpond stream ingest data.arrow --tenant abc123 --stream events

        duckpond stream ingest data.arrow -t abc123 -d logs \\
            --storage /data/warehouse/logs \\
            --batch-flush 20 \\
            --buffer-size 256

        duckpond stream ingest data.arrow -t abc123 -d metrics --no-progress
    """
    try:
        settings = get_settings()

        if storage_path is None:
            storage_path = (
                Path(settings.local_storage_path)
                / "tenants"
                / tenant_id
                / "streams"
                / stream
            )

        storage_path = storage_path.resolve()

        if not ipc_file.exists():
            print_error(f"IPC file not found: {ipc_file}")
            raise typer.Exit(1)

        print_info("🚀 Starting Arrow IPC ingestion")
        print_info(f"   Source: {ipc_file}")
        print_info(f"   Stream: {stream}")
        print_info(f"   Tenant: {tenant_id}")
        print_info(f"   Storage: {storage_path}")
        print_info(f"   Buffer: {max_buffer_mb}MB, Queue depth: {max_queue_depth}")
        print_info("")

        buffer_manager = BufferManager(
            max_buffer_size_bytes=max_buffer_mb * 1024 * 1024,
            max_queue_depth=max_queue_depth,
        )

        catalog = None
        if settings.catalog_enabled:
            try:
                from duckpond.catalog.manager import create_catalog_manager

                catalog = asyncio.run(create_catalog_manager(tenant_id))
            except Exception as e:
                print_warning(f"Failed to initialize catalog: {e}")
                print_info("Continuing without catalog registration")

        ingestor = StreamingIngestor(catalog, buffer_manager)

        start_time = datetime.now()

        if show_progress:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("Ingesting...", total=None)

                metrics = asyncio.run(
                    ingestor.ingest_stream(
                        tenant_id=tenant_id,
                        stream_name=stream,
                        ipc_stream_path=ipc_file,
                        storage_root=storage_path,
                        batch_flush_count=batch_flush_count,
                    )
                )

                progress.update(task, completed=True)
        else:
            metrics = asyncio.run(
                ingestor.ingest_stream(
                    tenant_id=tenant_id,
                    stream_name=stream,
                    ipc_stream_path=ipc_file,
                    storage_root=storage_path,
                    batch_flush_count=batch_flush_count,
                )
            )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print_success("✅ Ingestion completed successfully!")
        print_info("")

        rows_per_sec = metrics["total_rows"] / duration if duration > 0 else 0
        bytes_per_sec = metrics["total_bytes"] / duration if duration > 0 else 0

        results = {
            "Duration": _format_duration(duration),
            "Total Rows": f"{metrics['total_rows']:,}",
            "Total Batches": f"{metrics['total_batches']:,}",
            "Data Size": _format_bytes(metrics["total_bytes"]),
            "Files Written": str(metrics["files_written"]),
            "Throughput": f"{rows_per_sec:,.0f} rows/sec ({_format_bytes(int(bytes_per_sec))}/sec)",
            "Max Queue Depth": str(metrics["max_queue_depth"]),
        }

        if metrics["buffer_overflows"] > 0:
            results["Buffer Overflows"] = f"⚠️  {metrics['buffer_overflows']}"

        print_dict(results, title="Ingestion Summary")

        if catalog:
            print_info("📚 Metadata registered in catalog")

        logger.info(
            f"Ingested {metrics['total_rows']} rows into {stream} "
            f"for tenant {tenant_id} in {duration:.2f}s"
        )

    except typer.Exit:
        raise
    except Exception as e:
        print_error(f"Ingestion failed: {e}")
        logger.error(f"Stream ingestion failed: {e}", exc_info=True)
        raise typer.Exit(1)


@app.command()
def validate(
    ipc_file: Path = typer.Argument(
        ...,
        help="Path to Arrow IPC stream file to validate",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    show_schema: bool = typer.Option(
        True,
        "--schema/--no-schema",
        help="Display schema information",
    ),
    show_samples: int = typer.Option(
        0,
        "--samples",
        "-n",
        help="Number of sample rows to display (0 = none)",
        min=0,
        max=100,
    ),
) -> None:
    """
    Validate and inspect an Arrow IPC stream file.

    Reads and validates an Arrow IPC stream file without ingesting it.
    Shows schema, row counts, and optionally sample data.

    This is useful for:
    - Verifying file format before ingestion
    - Inspecting schema and data structure
    - Debugging data issues

    Examples:
        duckpond stream validate data.arrow

        duckpond stream validate data.arrow --samples 10

        duckpond stream validate data.arrow --no-schema
    """
    try:
        import pyarrow as pa

        from duckpond.streaming.arrow_ipc import ArrowIPCHandler

        print_info(f"🔍 Validating Arrow IPC file: {ipc_file}")
        print_info("")

        handler = ArrowIPCHandler()
        total_rows = 0
        total_batches = 0
        schema = None
        sample_table = None

        async def validate_stream():
            nonlocal total_rows, total_batches, schema, sample_table

            batches_for_sample = []

            async for batch in handler.read_stream(ipc_file):
                if schema is None:
                    schema = batch.schema

                total_rows += batch.num_rows
                total_batches += 1

                if show_samples > 0 and len(batches_for_sample) < 10:
                    batches_for_sample.append(batch)

            if batches_for_sample and show_samples > 0:
                sample_table = pa.Table.from_batches(batches_for_sample)

        asyncio.run(validate_stream())

        print_success("✅ File is valid!")
        print_info("")

        file_size = ipc_file.stat().st_size

        stats = {
            "File Size": _format_bytes(file_size),
            "Total Batches": f"{total_batches:,}",
            "Total Rows": f"{total_rows:,}",
            "Avg Rows/Batch": f"{total_rows / total_batches:.1f}"
            if total_batches > 0
            else "0",
        }

        print_dict(stats, title="File Statistics")

        if show_schema and schema:
            print_info("")
            console.print("[bold]Schema:[/bold]")
            console.print("")

            schema_table = Table(show_header=True, header_style="bold cyan")
            schema_table.add_column("Column", style="cyan")
            schema_table.add_column("Type", style="yellow")
            schema_table.add_column("Nullable", style="magenta")

            for field in schema:
                nullable = "Yes" if field.nullable else "No"
                schema_table.add_row(field.name, str(field.type), nullable)

            console.print(schema_table)

        if show_samples > 0 and sample_table:
            print_info("")
            console.print(f"[bold]Sample Data (first {show_samples} rows):[/bold]")
            console.print("")

            sample_table = sample_table.slice(
                0, min(show_samples, sample_table.num_rows)
            )

            try:
                df = sample_table.to_pandas()
                console.print(df.to_string(index=False))
            except Exception as e:
                print_warning(f"Could not display sample data: {e}")

        logger.info(
            f"Validated IPC file: {ipc_file} ({total_rows} rows, {total_batches} batches)"
        )

    except typer.Exit:
        raise
    except Exception as e:
        print_error(f"Validation failed: {e}")
        logger.error(f"IPC validation failed: {e}", exc_info=True)
        raise typer.Exit(1)


@app.command()
def info() -> None:
    """
    Show information about streaming ingestion capabilities.

    Displays information about Arrow IPC streaming, buffer settings,
    and best practices for high-performance data ingestion.
    """
    console.print("")
    console.print("[bold cyan]DuckPond Streaming Ingestion[/bold cyan]")
    console.print("")
    console.print("DuckPond uses Apache Arrow IPC (Inter-Process Communication) format")
    console.print("for high-performance streaming data ingestion.")
    console.print("")

    console.print("[bold]Key Features:[/bold]")
    console.print("  • Zero-copy data transfer for maximum throughput")
    console.print("  • Automatic batching and backpressure management")
    console.print("  • Direct conversion to Parquet for storage")
    console.print("  • Catalog integration for metadata tracking")
    console.print("")

    console.print("[bold]Typical Workflow:[/bold]")
    console.print("  1. Generate Arrow IPC stream from your data source")
    console.print(
        "  2. Validate the stream (optional): [cyan]duckpond stream validate[/cyan]"
    )
    console.print("  3. Ingest into dataset: [cyan]duckpond stream ingest[/cyan]")
    console.print("  4. Query the data: [cyan]duckpond query exec[/cyan]")
    console.print("")

    console.print("[bold]Performance Tips:[/bold]")
    console.print("  • Increase buffer size (--buffer-size) for large datasets")
    console.print("  • Adjust batch flush count (--batch-flush) based on row size")
    console.print("  • Use SSD storage for best write performance")
    console.print("  • Monitor buffer overflows - increase buffer if > 0")
    console.print("")

    console.print("[bold]Generating Arrow IPC Files:[/bold]")
    console.print("")
    console.print("From Python:")
    console.print("  [dim]import pyarrow as pa[/dim]")
    console.print(
        "  [dim]schema = pa.schema([('id', pa.int64()), ('name', pa.string())])[/dim]"
    )
    console.print(
        "  [dim]with pa.ipc.new_stream('data.arrow', schema) as writer:[/dim]"
    )
    console.print("  [dim]    writer.write_batch(batch)[/dim]")
    console.print("")

    console.print("From pandas DataFrame:")
    console.print("  [dim]import pyarrow as pa[/dim]")
    console.print("  [dim]table = pa.Table.from_pandas(df)[/dim]")
    console.print(
        "  [dim]with pa.ipc.new_stream('data.arrow', table.schema) as writer:[/dim]"
    )
    console.print("  [dim]    writer.write_table(table)[/dim]")
    console.print("")

    settings = get_settings()
    console.print("[bold]Current Configuration:[/bold]")
    console.print(f"  Data Directory: {settings.local_storage_path}")
    console.print(f"  Catalog Enabled: {'Yes' if settings.catalog_enabled else 'No'}")
    console.print("")
