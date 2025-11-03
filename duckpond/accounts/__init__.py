"""Account management functionality for DuckPond."""

from duckpond.accounts.manager import (
    AccountAlreadyExistsError,
    AccountManager,
    AccountManagerError,
    AccountNotFoundError,
)
from duckpond.accounts.models import Account, AccountStatus, APIKey
from duckpond.accounts.schemas import (
    AccountCreate,
    AccountCreateResponse,
    AccountListResponse,
    AccountResponse,
    AccountUpdate,
)

__all__ = [
    # Core
    "Account",
    "APIKey",
    "AccountStatus",
    "AccountManager",
    "AccountManagerError",
    "AccountAlreadyExistsError",
    "AccountNotFoundError",
    # Schemas
    "AccountCreate",
    "AccountUpdate",
    "AccountResponse",
    "AccountCreateResponse",
    "AccountListResponse",
    # Backward compatibility (deprecated)
    "AccountCreate",
    "AccountUpdate",
    "AccountResponse",
    "AccountCreateResponse",
    "AccountListResponse",
]
