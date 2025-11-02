"""Account management commands for DuckPond."""

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
from duckpond.accounts.manager import (
    APIKeyNotFoundError,
    AccountAlreadyExistsError,
    AccountManager,
    AccountNotFoundError,
)

app = typer.Typer(help="Manage accounts")
console = Console()
logger = get_logger(__name__)


@app.command()
def create(
    ctx: typer.Context,
    name: str = typer.Argument(..., help="Account name (must be unique)"),
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
    Create a new account.

    Creates a account with specified quotas and generates an API key.
    The API key will only be displayed once on creation.

    Examples:
        duckpond accounts create mycompany

        duckpond accounts create mycompany --storage-backend s3 --s3-bucket my-bucket

        duckpond accounts create mycompany --max-storage-gb 200 --max-query-memory-gb 8

        duckpond accounts create mycompany --storage-backend s3 \\
            --s3-bucket my-bucket --s3-endpoint http://localhost:9000
    """

    async def _create():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = AccountManager(session)
            result = await manager.create_account(
                name=name,
                storage_backend=storage_backend,
                storage_config=storage_config,
                max_storage_gb=max_storage_gb,
                max_query_memory_gb=max_query_memory_gb,
                max_concurrent_queries=max_concurrent_queries,
            )
            return result

    try:
        print_info(f"Creating account: {name}")

        if not name or len(name) < 3:
            print_error("Account name must be at least 3 characters")
            raise typer.Exit(1)

        if not name.replace("-", "").replace("_", "").isalnum():
            print_error(
                "Account name must be alphanumeric (hyphens and underscores allowed)"
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

        account, api_key = asyncio.run(_create())

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "account_id": account.account_id,
                    "name": account.name,
                    "api_key": api_key,
                    "storage_backend": account.storage_backend,
                    "ducklake_catalog_url": account.ducklake_catalog_url,
                    "max_storage_gb": account.max_storage_gb,
                    "max_query_memory_gb": account.max_query_memory_gb,
                    "max_concurrent_queries": account.max_concurrent_queries,
                    "created_at": account.created_at.isoformat(),
                }
            )
        else:
            console.print()
            print_success(f"Account created: {name}")
            console.print()

            display_data = {
                "Account ID": account.account_id,
                "Name": account.name,
                "Storage Backend": account.storage_backend,
                "Catalog URL": account.ducklake_catalog_url,
                "Max Storage": f"{account.max_storage_gb} GB",
                "Query Memory": f"{account.max_query_memory_gb} GB",
                "Max Queries": str(account.max_concurrent_queries),
                "Created": account.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
            print_dict(display_data, title="Account Details")

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
            print_info(f"View account details: duckpond accounts show {account.account_id}")

    except AccountAlreadyExistsError:
        print_error(f"Account already exists: {name}")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception("Failed to create account")
        print_error(f"Failed to create account: {str(e)}")
        raise typer.Exit(1)


@app.command()
def list(
    ctx: typer.Context,
    limit: int = typer.Option(
        100,
        "--limit",
        "-l",
        help="Maximum number of accounts to display",
    ),
    offset: int = typer.Option(
        0,
        "--offset",
        help="Number of accounts to skip",
    ),
) -> None:
    """
    List all accounts.

    Displays all accounts with their basic information.

    Examples:
        duckpond accounts list
        duckpond accounts list --limit 50
        duckpond accounts list --output json
    """

    async def _list():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = AccountManager(session)
            return await manager.list_accounts(offset=offset, limit=limit)

    try:
        print_info("Retrieving account list...")

        accounts, total = asyncio.run(_list())

        if not accounts:
            print_warning("No accounts found")
            console.print()
            print_info("Create a account with: duckpond accounts create <name>")
            return

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "accounts": [
                        {
                            "account_id": t.account_id,
                            "name": t.name,
                            "storage_backend": t.storage_backend,
                            "max_storage_gb": t.max_storage_gb,
                            "max_query_memory_gb": t.max_query_memory_gb,
                            "max_concurrent_queries": t.max_concurrent_queries,
                            "created_at": t.created_at.isoformat(),
                        }
                        for t in accounts                     ],
                    "total": total,
                    "offset": offset,
                    "limit": limit,
                }
            )
        else:
            columns = [
                "Account ID",
                "Name",
                "Backend",
                "Status",
                "Storage (GB)",
                "Created",
            ]

            display_data = []
            for account in accounts:
                row = {
                    "Account ID": account.account_id[:16] + "...",
                    "Name": account.name,
                    "Backend": account.storage_backend,
                    "Storage (GB)": f"{account.max_storage_gb}",
                    "Created": account.created_at.strftime("%Y-%m-%d"),
                }
                display_data.append(row)

            console.print()
            print_table(display_data, title="Accounts", columns=columns)
            console.print()
            print_info(f"Showing {len(accounts)} of {total} accounts")

    except Exception as e:
        logger.exception("Failed to list accounts")
        print_error(f"Failed to list accounts: {str(e)}")
        raise typer.Exit(1)


@app.command()
def show(
    ctx: typer.Context,
    account_id: str = typer.Argument(..., help="Account ID"),
) -> None:
    """
    Show detailed account information.

    Displays comprehensive account details including quotas and catalog URL.

    Examples:
        duckpond accounts show <account-id>
        duckpond accounts show <account-id> --output json
    """

    async def _show():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = AccountManager(session)
            return await manager.get_account_by_id(account_id)

    try:
        print_info(f"Retrieving account: {account_id}")

        account = asyncio.run(_show())

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "account_id": account.account_id,
                    "name": account.name,
                    "storage_backend": account.storage_backend,
                    "storage_config": account.storage_config,
                    "ducklake_catalog_url": account.ducklake_catalog_url,
                    "max_storage_gb": account.max_storage_gb,
                    "max_query_memory_gb": account.max_query_memory_gb,
                    "max_concurrent_queries": account.max_concurrent_queries,
                    "created_at": account.created_at.isoformat(),
                    "updated_at": account.updated_at.isoformat()
                    if account.updated_at
                    else None,
                }
            )
        else:
            console.print()
            print_success(f"Account found: {account.name}")
            console.print()

            basic_info = {
                "Account ID": account.account_id,
                "Name": account.name,
                "Storage Backend": account.storage_backend,
                "Catalog URL": account.ducklake_catalog_url,
                "Created": account.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
            if account.updated_at:
                basic_info["Last Updated"] = account.updated_at.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )

            print_dict(basic_info, title="Basic Information")
            console.print()

            quota_info = {
                "Max Storage": f"{account.max_storage_gb} GB",
                "Query Memory": f"{account.max_query_memory_gb} GB",
                "Max Concurrent Queries": str(account.max_concurrent_queries),
            }
            print_dict(quota_info, title="Quotas & Limits")

    except AccountNotFoundError:
        print_error(f"Account not found: {account_id}")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception("Failed to show account")
        print_error(f"Failed to show account: {str(e)}")
        raise typer.Exit(1)


@app.command()
def update(
    ctx: typer.Context,
    account_id: str = typer.Argument(..., help="Account ID"),
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
    Update account quotas.

    Updates account quota configuration.
    At least one update parameter must be provided.

    Examples:
        duckpond accounts update <account-id> --max-storage-gb 200
        duckpond accounts update <account-id> --max-query-memory-gb 16 --max-queries 20
    """

    async def _update():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = AccountManager(session)
            return await manager.update_account_quotas(
                account_id=account_id,
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

        print_info(f"Updating account: {account_id}")

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

        account = asyncio.run(_update())

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "account_id": account.account_id,
                    "name": account.name,
                    "max_storage_gb": account.max_storage_gb,
                    "max_query_memory_gb": account.max_query_memory_gb,
                    "max_concurrent_queries": account.max_concurrent_queries,
                    "updated_at": account.updated_at.isoformat()
                    if account.updated_at
                    else None,
                }
            )
        else:
            print_success(f"Account updated: {account.name}")

    except AccountNotFoundError:
        print_error(f"Account not found: {account_id}")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception("Failed to update account")
        print_error(f"Failed to update account: {str(e)}")
        raise typer.Exit(1)


@app.command(name="storage-info")
def storage_info(
    ctx: typer.Context,
    account_id: str = typer.Argument(..., help="Account ID"),
) -> None:
    """
    Show storage usage information for a account.

    Displays storage backend configuration and calculates actual storage usage.

    Examples:
        duckpond accounts storage-info <account-id>
        duckpond accounts storage-info <account-id> --output json
    """

    async def _get_storage_info():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = AccountManager(session)

            account = await manager.get_account_by_id(account_id)

            usage_bytes = await manager.calculate_storage_usage(account_id)

            return account, usage_bytes

    try:
        print_info(f"Retrieving storage information for: {account_id}")

        account, usage_bytes = asyncio.run(_get_storage_info())

        from duckpond.storage.utils import format_storage_size

        usage_str = format_storage_size(usage_bytes)
        usage_gb = usage_bytes / (1024**3)
        usage_pct = (
            (usage_gb / account.max_storage_gb * 100) if account.max_storage_gb > 0 else 0
        )

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "account_id": account.account_id,
                    "name": account.name,
                    "storage_backend": account.storage_backend,
                    "storage_config": account.storage_config,
                    "usage_bytes": usage_bytes,
                    "usage_formatted": usage_str,
                    "usage_gb": round(usage_gb, 2),
                    "quota_gb": account.max_storage_gb,
                    "usage_percentage": round(usage_pct, 1),
                }
            )
        else:
            console.print()
            print_success(f"Storage info for account: {account.name}")
            console.print()

            storage_info = {
                "Backend": account.storage_backend,
            }

            if account.storage_backend == "s3":
                config = account.storage_config or {}
                storage_info["S3 Bucket"] = config.get("bucket", "N/A")
                storage_info["S3 Region"] = config.get("region", "N/A")
                if config.get("endpoint_url"):
                    storage_info["S3 Endpoint"] = config["endpoint_url"]
            elif account.storage_backend == "local":
                config = account.storage_config or {}
                storage_info["Base Path"] = config.get("base_path", "./data")

            print_dict(storage_info, title="Storage Configuration")
            console.print()

            usage_info = {
                "Current Usage": usage_str,
                "Storage Quota": f"{account.max_storage_gb} GB",
                "Usage Percentage": f"{usage_pct:.1f}%",
                "Available": format_storage_size(
                    (account.max_storage_gb * 1024**3) - usage_bytes
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

    except AccountNotFoundError:
        print_error(f"Account not found: {account_id}")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception("Failed to get storage info")
        print_error(f"Failed to get storage info: {str(e)}")
        raise typer.Exit(1)


@app.command()
def delete(
    ctx: typer.Context,
    account_id: str = typer.Argument(..., help="Account ID"),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Skip confirmation prompt",
    ),
    purge_data: bool = typer.Option(
        False,
        "--purge-data",
        help="Delete all account data (cannot be undone)",
    ),
) -> None:
    """
    Delete a account.

    Permanently removes a account and optionally all associated data.
    This operation requires confirmation unless --force is used.

    WARNING: This is a destructive operation that cannot be undone!

    Examples:
        duckpond accounts delete <account-id>
        duckpond accounts delete <account-id> --purge-data
        duckpond accounts delete <account-id> --force --purge-data
    """

    async def _get_account():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = AccountManager(session)
            return await manager.get_account_by_id(account_id)

    async def _delete():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = AccountManager(session)
            await manager.delete_account(account_id, purge_data=purge_data)

    try:
        print_warning(f"Preparing to delete account: {account_id}")

        account = asyncio.run(_get_account())

        console.print()
        print_panel(
            f"[bold red]⚠ WARNING:[/bold red] You are about to delete account:\n\n"
            f"  ID: {account.account_id}\n"
            f"  Name: {account.name}\n\n"
            + (
                "[bold red]This will also DELETE ALL DATA[/bold red] associated with this account!\n"
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
                    f"Type the account name '{account.name}' to confirm deletion",
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
                    "account_id": account_id,
                    "data_purged": purge_data,
                }
            )
        else:
            console.print()
            print_success(f"Account deleted: {account.name}")

            if purge_data:
                print_info("All account data has been purged")
            else:
                print_info("Account metadata removed, data files preserved")

    except AccountNotFoundError:
        print_error(f"Account not found: {account_id}")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception("Failed to delete account")
        print_error(f"Failed to delete account: {str(e)}")
        raise typer.Exit(1)


@app.command(name="create-key")
def create_key(
    ctx: typer.Context,
    account_id: str = typer.Argument(..., help="Account ID"),
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
    Create a new API key for a account.

    Generates a new API key for account authentication.
    The API key will only be displayed once for security.

    Examples:
        duckpond accounts create-key <account-id>
        duckpond accounts create-key <account-id> --description "Production key"
        duckpond accounts create-key <account-id> --expires-in-days 90
    """

    async def _create_key():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = AccountManager(session)
            expires_at = None
            if expires_days is not None:
                expires_at = datetime.utcnow() + timedelta(days=expires_days)
            return await manager.create_api_key(
                account_id=account_id,
                description=description,
                expires_at=expires_at,
            )

    try:
        print_info(f"Generating API key for account: {account_id}")

        api_key_obj, plain_key = asyncio.run(_create_key())

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "key_id": api_key_obj.key_id,
                    "api_key": plain_key,
                    "account_id": account_id,
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

    except AccountNotFoundError:
        print_error(f"Account not found: {account_id}")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception("Failed to create API key")
        print_error(f"Failed to create API key: {str(e)}")
        raise typer.Exit(1)


@app.command(name="list-keys")
def list_keys(
    ctx: typer.Context,
    account_id: str = typer.Argument(..., help="Account ID"),
) -> None:
    """
    List all API keys for a account.

    Displays all API keys (showing key prefix only for security).

    Examples:
        duckpond accounts list-keys <account-id>
        duckpond accounts list-keys <account-id> --output json
    """

    async def _list_keys():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = AccountManager(session)
            return await manager.list_api_keys(account_id)

    try:
        print_info(f"Listing API keys for account: {account_id}")

        keys = asyncio.run(_list_keys())

        if not keys:
            print_warning("No API keys found for this account")
            console.print()
            print_info(f"Generate one with: duckpond accounts create-key {account_id}")
            return

        output_format = ctx.obj.output_format if ctx.obj else "table"

        if output_format == "json":
            print_json(
                {
                    "account_id": account_id,
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
            print_table(display_data, title="API Keys for Account")
            console.print()
            print_info(f"Total keys: {len(keys)}")

    except AccountNotFoundError:
        print_error(f"Account not found: {account_id}")
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
        duckpond accounts revoke-key <key-id>
        duckpond accounts revoke-key <key-id> --force
    """

    async def _revoke():
        engine = get_engine()
        session_factory = create_session_factory(engine)
        async with get_session(session_factory) as session:
            manager = AccountManager(session)
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
