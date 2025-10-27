"""Tenant management commands for DuckPond."""

import asyncio
import sys
from datetime import datetime, timedelta
from typing import Optional

import typer
from rich.console import Console

from duckpond.cli.output import (
    confirm,
    print_dict,
    print_error,
    print_info,
    print_json,
    print_panel,
    print_success,
    print_table,
    print_warning,
)
from duckpond.db.session import get_engine, create_session_factory, get_session
from duckpond.logging_config import get_logger
from duckpond.tenants.manager import (
    APIKeyNotFoundError,
    TenantAlreadyExistsError,
    TenantManager,
    TenantNotFoundError,
)

app = typer.Typer(help="Manage tenants")
console = Console()
logger = get_logger(__name__)


@app.command()
def create(
    ctx: typer.Context,
    name: str = typer.Argument(..., help="Tenant name (must be unique)"),
    storage_backend: str = typer.Option(
        "local",
        "--storage-backend",
        "-b",
        help="Storage backend (local, s3)",
    ),
    s3_bucket: Optional[str] = typer.Option(
        None,
        "--s3-bucket",
        help="S3 bucket name (required when storage-backend is s3)",
    ),
    s3_region: str = typer.Option(
        "us-east-1",
        "--s3-region",
        help="S3 region (default: us-east-1)",
    ),
    s3_endpoint: Optional[str] = typer.Option(
        None,
        "--s3-endpoint",
        help="Custom S3 endpoint URL (for S3-compatible services like MinIO)",
    ),
    s3_access_key_id: Optional[str] = typer.Option(
        None,
        "--s3-access-key-id",
        help="AWS access key ID (uses environment AWS_ACCESS_KEY_ID if not provided)",
    ),
    s3_secret_access_key: Optional[str] = typer.Option(
        None,
        "--s3-secret-access-key",
        help="AWS secret access key (uses environment AWS_SECRET_ACCESS_KEY if not provided)",
    ),
    local_base_path: Optional[str] = typer.Option(
        None,
        "--local-path",
        help="Base path for local storage (default: ./data)",
    ),
    max_storage_gb: int = typer.Option(
        100,
        "--max-storage-gb",
        "-s",
        help="Maximum storage quota in GB",
    ),
    max_query_memory_gb: int = typer.Option(
        4,
        "--max-query-memory-gb",
        "-m",
        help="Maximum query memory in GB",
    ),
    max_concurrent_queries: int = typer.Option(
        10,
        "--max-queries",
        "-q",
        help="Maximum concurrent queries",
    ),
) -> None:
    """
    Create a new tenant.

    Creates a tenant with specified quotas and generates an API key.
    The API key will only be displayed once on creation.

    Examples:
        duckpond tenants create mycompany

        duckpond tenants create mycompany --storage-backend s3 --s3-bucket my-bucket

        duckpond tenants create mycompany --max-storage-gb 200 --max-query-memory-gb 8

        duckpond tenants create mycompany --storage-backend s3 \\
            --s3-bucket my-bucket --s3-endpoint http://localhost:9000
    """

    async def _create():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = TenantManager(session)
            result = await manager.create_tenant(
                name=name,
                storage_backend=storage_backend,
                storage_config=storage_config,
                max_storage_gb=max_storage_gb,
                max_query_memory_gb=max_query_memory_gb,
                max_concurrent_queries=max_concurrent_queries,
            )
            return result

    try:
        print_info(f"Creating tenant: {name}")

        if not name or len(name) < 3:
            print_error("Tenant name must be at least 3 characters")
            raise typer.Exit(1)

        if not name.replace("-", "").replace("_", "").isalnum():
            print_error(
                "Tenant name must be alphanumeric (hyphens and underscores allowed)"
            )
            raise typer.Exit(1)

        storage_config = {}

        if storage_backend == "s3":
            if not s3_bucket:
                print_error("S3 bucket name is required when using S3 storage backend")
                console.print("  Use --s3-bucket <bucket-name>")
                raise typer.Exit(1)

            storage_config = {
                "bucket": s3_bucket,
                "region": s3_region,
            }

            if s3_endpoint:
                storage_config["endpoint_url"] = s3_endpoint
            if s3_access_key_id:
                storage_config["aws_access_key_id"] = s3_access_key_id
            if s3_secret_access_key:
                storage_config["aws_secret_access_key"] = s3_secret_access_key

        elif storage_backend == "local":
            if local_base_path:
                storage_config = {"base_path": local_base_path}

        else:
            print_error(f"Unsupported storage backend: {storage_backend}")
            console.print("  Supported backends: local, s3")
            raise typer.Exit(1)

        tenant, api_key = asyncio.run(_create())

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "tenant_id": tenant.tenant_id,
                    "name": tenant.name,
                    "api_key": api_key,
                    "storage_backend": tenant.storage_backend,
                    "ducklake_catalog_url": tenant.ducklake_catalog_url,
                    "max_storage_gb": tenant.max_storage_gb,
                    "max_query_memory_gb": tenant.max_query_memory_gb,
                    "max_concurrent_queries": tenant.max_concurrent_queries,
                    "created_at": tenant.created_at.isoformat(),
                }
            )
        else:
            console.print()
            print_success(f"Tenant created: {name}")
            console.print()

            display_data = {
                "Tenant ID": tenant.tenant_id,
                "Name": tenant.name,
                "Storage Backend": tenant.storage_backend,
                "Catalog URL": tenant.ducklake_catalog_url,
                "Max Storage": f"{tenant.max_storage_gb} GB",
                "Query Memory": f"{tenant.max_query_memory_gb} GB",
                "Max Queries": str(tenant.max_concurrent_queries),
                "Created": tenant.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
            print_dict(display_data, title="Tenant Details")

            console.print()
            print_panel(
                f"[bold yellow]API Key:[/bold yellow] {api_key}\n\n"
                "[bold red]⚠ IMPORTANT:[/bold red] Save this API key now!\n"
                "It will not be shown again for security reasons.\n\n"
                "Use this key in the Authorization header:\n"
                f"  Authorization: Bearer {api_key}",
                title="API Key Generated",
                border_style="yellow",
            )
            console.print()
            print_info(f"View tenant details: duckpond tenants show {tenant.tenant_id}")

    except TenantAlreadyExistsError:
        print_error(f"Tenant already exists: {name}")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception("Failed to create tenant")
        print_error(f"Failed to create tenant: {str(e)}")
        raise typer.Exit(1)


@app.command()
def list(
    ctx: typer.Context,
    limit: int = typer.Option(
        100,
        "--limit",
        "-l",
        help="Maximum number of tenants to display",
    ),
    offset: int = typer.Option(
        0,
        "--offset",
        help="Number of tenants to skip",
    ),
) -> None:
    """
    List all tenants.

    Displays all tenants with their basic information.

    Examples:
        duckpond tenants list
        duckpond tenants list --limit 50
        duckpond tenants list --output json
    """

    async def _list():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = TenantManager(session)
            return await manager.list_tenants(offset=offset, limit=limit)

    try:
        print_info("Retrieving tenant list...")

        tenants, total = asyncio.run(_list())

        if not tenants:
            print_warning("No tenants found")
            console.print()
            print_info("Create a tenant with: duckpond tenants create <name>")
            return

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "tenants": [
                        {
                            "tenant_id": t.tenant_id,
                            "name": t.name,
                            "storage_backend": t.storage_backend,
                            "max_storage_gb": t.max_storage_gb,
                            "max_query_memory_gb": t.max_query_memory_gb,
                            "max_concurrent_queries": t.max_concurrent_queries,
                            "created_at": t.created_at.isoformat(),
                        }
                        for t in tenants
                    ],
                    "total": total,
                    "offset": offset,
                    "limit": limit,
                }
            )
        else:
            columns = [
                "Tenant ID",
                "Name",
                "Backend",
                "Status",
                "Storage (GB)",
                "Created",
            ]

            display_data = []
            for tenant in tenants:
                row = {
                    "Tenant ID": tenant.tenant_id[:16] + "...",
                    "Name": tenant.name,
                    "Backend": tenant.storage_backend,
                    "Storage (GB)": f"{tenant.max_storage_gb}",
                    "Created": tenant.created_at.strftime("%Y-%m-%d"),
                }
                display_data.append(row)

            console.print()
            print_table(display_data, title="Tenants", columns=columns)
            console.print()
            print_info(f"Showing {len(tenants)} of {total} tenants")

    except Exception as e:
        logger.exception("Failed to list tenants")
        print_error(f"Failed to list tenants: {str(e)}")
        raise typer.Exit(1)


@app.command()
def show(
    ctx: typer.Context,
    tenant_id: str = typer.Argument(..., help="Tenant ID"),
) -> None:
    """
    Show detailed tenant information.

    Displays comprehensive tenant details including quotas and catalog URL.

    Examples:
        duckpond tenants show <tenant-id>
        duckpond tenants show <tenant-id> --output json
    """

    async def _show():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = TenantManager(session)
            return await manager.get_tenant_by_id(tenant_id)

    try:
        print_info(f"Retrieving tenant: {tenant_id}")

        tenant = asyncio.run(_show())

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "tenant_id": tenant.tenant_id,
                    "name": tenant.name,
                    "storage_backend": tenant.storage_backend,
                    "storage_config": tenant.storage_config,
                    "ducklake_catalog_url": tenant.ducklake_catalog_url,
                    "max_storage_gb": tenant.max_storage_gb,
                    "max_query_memory_gb": tenant.max_query_memory_gb,
                    "max_concurrent_queries": tenant.max_concurrent_queries,
                    "created_at": tenant.created_at.isoformat(),
                    "updated_at": tenant.updated_at.isoformat()
                    if tenant.updated_at
                    else None,
                }
            )
        else:
            console.print()
            print_success(f"Tenant found: {tenant.name}")
            console.print()

            basic_info = {
                "Tenant ID": tenant.tenant_id,
                "Name": tenant.name,
                "Storage Backend": tenant.storage_backend,
                "Catalog URL": tenant.ducklake_catalog_url,
                "Created": tenant.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
            if tenant.updated_at:
                basic_info["Last Updated"] = tenant.updated_at.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )

            print_dict(basic_info, title="Basic Information")
            console.print()

            quota_info = {
                "Max Storage": f"{tenant.max_storage_gb} GB",
                "Query Memory": f"{tenant.max_query_memory_gb} GB",
                "Max Concurrent Queries": str(tenant.max_concurrent_queries),
            }
            print_dict(quota_info, title="Quotas & Limits")

    except TenantNotFoundError:
        print_error(f"Tenant not found: {tenant_id}")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception("Failed to show tenant")
        print_error(f"Failed to show tenant: {str(e)}")
        raise typer.Exit(1)


@app.command()
def update(
    ctx: typer.Context,
    tenant_id: str = typer.Argument(..., help="Tenant ID"),
    max_storage_gb: Optional[int] = typer.Option(
        None,
        "--max-storage-gb",
        "-s",
        help="New storage quota in GB",
    ),
    max_query_memory_gb: Optional[int] = typer.Option(
        None,
        "--max-query-memory-gb",
        "-m",
        help="New query memory limit in GB",
    ),
    max_concurrent_queries: Optional[int] = typer.Option(
        None,
        "--max-queries",
        "-q",
        help="New maximum concurrent queries",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Skip confirmation prompt",
    ),
) -> None:
    """
    Update tenant quotas.

    Updates tenant quota configuration.
    At least one update parameter must be provided.

    Examples:
        duckpond tenants update <tenant-id> --max-storage-gb 200
        duckpond tenants update <tenant-id> --max-query-memory-gb 16 --max-queries 20
    """

    async def _update():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = TenantManager(session)
            return await manager.update_tenant_quotas(
                tenant_id=tenant_id,
                max_storage_gb=max_storage_gb,
                max_query_memory_gb=max_query_memory_gb,
                max_concurrent_queries=max_concurrent_queries,
            )

    try:
        if not any(
            [
                max_storage_gb is not None,
                max_query_memory_gb is not None,
                max_concurrent_queries is not None,
            ]
        ):
            print_error("At least one update parameter must be provided")
            console.print("\nAvailable options:")
            console.print("  --max-storage-gb, --max-query-memory-gb, --max-queries")
            raise typer.Exit(1)

        print_info(f"Updating tenant: {tenant_id}")

        updates = {}
        if max_storage_gb is not None:
            updates["Max Storage"] = f"{max_storage_gb} GB"
        if max_query_memory_gb is not None:
            updates["Query Memory"] = f"{max_query_memory_gb} GB"
        if max_concurrent_queries is not None:
            updates["Max Queries"] = str(max_concurrent_queries)

        console.print()
        print_dict(updates, title="Pending Updates")
        console.print()

        if not force and sys.stdin.isatty():
            if not confirm("Apply these updates?", default=True):
                print_info("Update cancelled")
                raise typer.Exit(0)

        tenant = asyncio.run(_update())

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "tenant_id": tenant.tenant_id,
                    "name": tenant.name,
                    "max_storage_gb": tenant.max_storage_gb,
                    "max_query_memory_gb": tenant.max_query_memory_gb,
                    "max_concurrent_queries": tenant.max_concurrent_queries,
                    "updated_at": tenant.updated_at.isoformat()
                    if tenant.updated_at
                    else None,
                }
            )
        else:
            print_success(f"Tenant updated: {tenant.name}")

    except TenantNotFoundError:
        print_error(f"Tenant not found: {tenant_id}")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception("Failed to update tenant")
        print_error(f"Failed to update tenant: {str(e)}")
        raise typer.Exit(1)


@app.command(name="storage-info")
def storage_info(
    ctx: typer.Context,
    tenant_id: str = typer.Argument(..., help="Tenant ID"),
) -> None:
    """
    Show storage usage information for a tenant.

    Displays storage backend configuration and calculates actual storage usage.

    Examples:
        duckpond tenants storage-info <tenant-id>
        duckpond tenants storage-info <tenant-id> --output json
    """

    async def _get_storage_info():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = TenantManager(session)

            tenant = await manager.get_tenant_by_id(tenant_id)

            usage_bytes = await manager.calculate_storage_usage(tenant_id)

            return tenant, usage_bytes

    try:
        print_info(f"Retrieving storage information for: {tenant_id}")

        tenant, usage_bytes = asyncio.run(_get_storage_info())

        from duckpond.storage.utils import format_storage_size

        usage_str = format_storage_size(usage_bytes)
        usage_gb = usage_bytes / (1024**3)
        usage_pct = (
            (usage_gb / tenant.max_storage_gb * 100) if tenant.max_storage_gb > 0 else 0
        )

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "tenant_id": tenant.tenant_id,
                    "name": tenant.name,
                    "storage_backend": tenant.storage_backend,
                    "storage_config": tenant.storage_config,
                    "usage_bytes": usage_bytes,
                    "usage_formatted": usage_str,
                    "usage_gb": round(usage_gb, 2),
                    "quota_gb": tenant.max_storage_gb,
                    "usage_percentage": round(usage_pct, 1),
                }
            )
        else:
            console.print()
            print_success(f"Storage info for tenant: {tenant.name}")
            console.print()

            storage_info = {
                "Backend": tenant.storage_backend,
            }

            if tenant.storage_backend == "s3":
                config = tenant.storage_config or {}
                storage_info["S3 Bucket"] = config.get("bucket", "N/A")
                storage_info["S3 Region"] = config.get("region", "N/A")
                if config.get("endpoint_url"):
                    storage_info["S3 Endpoint"] = config["endpoint_url"]
            elif tenant.storage_backend == "local":
                config = tenant.storage_config or {}
                storage_info["Base Path"] = config.get("base_path", "./data")

            print_dict(storage_info, title="Storage Configuration")
            console.print()

            usage_info = {
                "Current Usage": usage_str,
                "Storage Quota": f"{tenant.max_storage_gb} GB",
                "Usage Percentage": f"{usage_pct:.1f}%",
                "Available": format_storage_size(
                    (tenant.max_storage_gb * 1024**3) - usage_bytes
                ),
            }

            if usage_pct >= 90:
                status_color = "red"
                status_text = "CRITICAL"
            elif usage_pct >= 75:
                status_color = "yellow"
                status_text = "WARNING"
            else:
                status_color = "green"
                status_text = "OK"

            usage_info["Status"] = f"[{status_color}]{status_text}[/{status_color}]"

            print_dict(usage_info, title="Storage Usage")

    except TenantNotFoundError:
        print_error(f"Tenant not found: {tenant_id}")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception("Failed to get storage info")
        print_error(f"Failed to get storage info: {str(e)}")
        raise typer.Exit(1)


@app.command()
def delete(
    ctx: typer.Context,
    tenant_id: str = typer.Argument(..., help="Tenant ID"),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Skip confirmation prompt",
    ),
    purge_data: bool = typer.Option(
        False,
        "--purge-data",
        help="Delete all tenant data (cannot be undone)",
    ),
) -> None:
    """
    Delete a tenant.

    Permanently removes a tenant and optionally all associated data.
    This operation requires confirmation unless --force is used.

    WARNING: This is a destructive operation that cannot be undone!

    Examples:
        duckpond tenants delete <tenant-id>
        duckpond tenants delete <tenant-id> --purge-data
        duckpond tenants delete <tenant-id> --force --purge-data
    """

    async def _get_tenant():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = TenantManager(session)
            return await manager.get_tenant_by_id(tenant_id)

    async def _delete():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = TenantManager(session)
            await manager.delete_tenant(tenant_id, purge_data=purge_data)

    try:
        print_warning(f"Preparing to delete tenant: {tenant_id}")

        tenant = asyncio.run(_get_tenant())

        console.print()
        print_panel(
            f"[bold red]⚠ WARNING:[/bold red] You are about to delete tenant:\n\n"
            f"  ID: {tenant.tenant_id}\n"
            f"  Name: {tenant.name}\n\n"
            + (
                "[bold red]This will also DELETE ALL DATA[/bold red] associated with this tenant!\n"
                if purge_data
                else "Metadata will be removed but data files will be preserved.\n"
            )
            + "\n[bold]This action cannot be undone![/bold]",
            title="Confirm Deletion",
            border_style="red",
        )
        console.print()

        if not force:
            if sys.stdin.isatty():
                confirmed = confirm(
                    f"Type the tenant name '{tenant.name}' to confirm deletion",
                    default=False,
                )
                if not confirmed:
                    print_info("Deletion cancelled")
                    raise typer.Exit(0)

                if purge_data:
                    console.print()
                    if not confirm(
                        "Are you ABSOLUTELY SURE you want to delete all data?",
                        default=False,
                    ):
                        print_info("Deletion cancelled")
                        raise typer.Exit(0)
            else:
                print_error(
                    "Deletion requires confirmation. Use --force in non-interactive mode"
                )
                raise typer.Exit(1)

        asyncio.run(_delete())

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "deleted": True,
                    "tenant_id": tenant_id,
                    "data_purged": purge_data,
                }
            )
        else:
            console.print()
            print_success(f"Tenant deleted: {tenant.name}")

            if purge_data:
                print_info("All tenant data has been purged")
            else:
                print_info("Tenant metadata removed, data files preserved")

    except TenantNotFoundError:
        print_error(f"Tenant not found: {tenant_id}")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception("Failed to delete tenant")
        print_error(f"Failed to delete tenant: {str(e)}")
        raise typer.Exit(1)


@app.command(name="create-key")
def create_key(
    ctx: typer.Context,
    tenant_id: str = typer.Argument(..., help="Tenant ID"),
    description: Optional[str] = typer.Option(
        None,
        "--description",
        "-d",
        help="Description for the API key",
    ),
    expires_days: Optional[int] = typer.Option(
        None,
        "--expires-in-days",
        "-e",
        help="Number of days until key expires",
    ),
) -> None:
    """
    Create a new API key for a tenant.

    Generates a new API key for tenant authentication.
    The API key will only be displayed once for security.

    Examples:
        duckpond tenants create-key <tenant-id>
        duckpond tenants create-key <tenant-id> --description "Production key"
        duckpond tenants create-key <tenant-id> --expires-in-days 90
    """

    async def _create_key():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = TenantManager(session)
            expires_at = None
            if expires_days is not None:
                expires_at = datetime.utcnow() + timedelta(days=expires_days)
            return await manager.create_api_key(
                tenant_id=tenant_id,
                description=description,
                expires_at=expires_at,
            )

    try:
        print_info(f"Generating API key for tenant: {tenant_id}")

        api_key_obj, plain_key = asyncio.run(_create_key())

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "key_id": api_key_obj.key_id,
                    "api_key": plain_key,
                    "tenant_id": tenant_id,
                    "description": description,
                    "created_at": api_key_obj.created_at.isoformat(),
                    "expires_at": api_key_obj.expires_at.isoformat()
                    if api_key_obj.expires_at
                    else None,
                }
            )
        else:
            console.print()
            print_success("API key generated successfully")
            console.print()

            print_panel(
                f"[bold]Key ID:[/bold] {api_key_obj.key_id}\n"
                + (f"[bold]Description:[/bold] {description}\n" if description else "")
                + (
                    f"[bold]Expires:[/bold] {api_key_obj.expires_at.strftime('%Y-%m-%d')}\n"
                    if api_key_obj.expires_at
                    else ""
                )
                + f"\n[bold yellow]API Key:[/bold yellow]\n{plain_key}\n\n"
                "[bold red]⚠ IMPORTANT:[/bold red] Save this API key now!\n"
                "It will not be shown again for security reasons.\n\n"
                "Use this key in the Authorization header:\n"
                f"  Authorization: Bearer {plain_key}",
                title="API Key Generated",
                border_style="yellow",
            )

    except TenantNotFoundError:
        print_error(f"Tenant not found: {tenant_id}")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception("Failed to create API key")
        print_error(f"Failed to create API key: {str(e)}")
        raise typer.Exit(1)


@app.command(name="list-keys")
def list_keys(
    ctx: typer.Context,
    tenant_id: str = typer.Argument(..., help="Tenant ID"),
) -> None:
    """
    List all API keys for a tenant.

    Displays all API keys (showing key prefix only for security).

    Examples:
        duckpond tenants list-keys <tenant-id>
        duckpond tenants list-keys <tenant-id> --output json
    """

    async def _list_keys():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = TenantManager(session)
            return await manager.list_api_keys(tenant_id)

    try:
        print_info(f"Listing API keys for tenant: {tenant_id}")

        keys = asyncio.run(_list_keys())

        if not keys:
            print_warning("No API keys found for this tenant")
            console.print()
            print_info(f"Generate one with: duckpond tenants create-key {tenant_id}")
            return

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "tenant_id": tenant_id,
                    "keys": [
                        {
                            "key_id": key.key_id,
                            "description": key.description,
                            "created_at": key.created_at.isoformat(),
                            "expires_at": key.expires_at.isoformat()
                            if key.expires_at
                            else None,
                            "last_used": key.last_used.isoformat()
                            if key.last_used
                            else None,
                        }
                        for key in keys
                    ],
                    "total": len(keys),
                }
            )
        else:
            display_data = [
                {
                    "Key ID": key.key_id[:16] + "...",
                    "Description": key.description or "N/A",
                    "Created": key.created_at.strftime("%Y-%m-%d"),
                    "Expires": key.expires_at.strftime("%Y-%m-%d")
                    if key.expires_at
                    else "Never",
                    "Last Used": key.last_used.strftime("%Y-%m-%d")
                    if key.last_used
                    else "Never",
                }
                for key in keys
            ]

            console.print()
            print_table(display_data, title="API Keys for Tenant")
            console.print()
            print_info(f"Total keys: {len(keys)}")

    except TenantNotFoundError:
        print_error(f"Tenant not found: {tenant_id}")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception("Failed to list API keys")
        print_error(f"Failed to list API keys: {str(e)}")
        raise typer.Exit(1)


@app.command(name="revoke-key")
def revoke_key(
    ctx: typer.Context,
    key_id: str = typer.Argument(..., help="API key ID to revoke"),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Skip confirmation prompt",
    ),
) -> None:
    """
    Revoke an API key.

    Permanently revokes an API key. This action cannot be undone.

    Examples:
        duckpond tenants revoke-key <key-id>
        duckpond tenants revoke-key <key-id> --force
    """

    async def _revoke():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = TenantManager(session)
            await manager.revoke_api_key(key_id)

    try:
        print_warning(f"Revoking API key: {key_id}")

        if not force and sys.stdin.isatty():
            if not confirm(
                f"Revoke API key {key_id}? This cannot be undone.", default=False
            ):
                print_info("Revocation cancelled")
                raise typer.Exit(0)

        asyncio.run(_revoke())

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "revoked": True,
                    "key_id": key_id,
                }
            )
        else:
            console.print()
            print_success(f"API key revoked: {key_id}")
            print_info("Any requests using this key will now be rejected")

    except APIKeyNotFoundError:
        print_error(f"API key not found: {key_id}")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception("Failed to revoke API key")
        print_error(f"Failed to revoke API key: {str(e)}")
        raise typer.Exit(1)
