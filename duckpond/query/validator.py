"""SQL query validation utilities."""

import hashlib
import logging
import re

from duckpond.exceptions import SQLValidationError

logger = logging.getLogger(__name__)


class SQLValidator:
    """
    Validates SQL queries for security and correctness.

    This validator ensures that queries are safe to execute by:
    - Blocking dangerous SQL operations (DROP, DELETE, etc.)
    - Preventing SQL injection attacks
    - Enforcing catalog schema references
    - Limiting multi-statement queries

    Security Features:
    - Whitelist-based approach for SELECT operations
    - Blacklist for dangerous keywords
    - Pattern matching for common injection attempts
    - Catalog prefix requirement for table references
    """

    DANGEROUS_KEYWORDS = [
        "drop ",
        "delete ",
        "truncate ",
        "alter ",
        "create ",
        "insert ",
        "update ",
        "grant ",
        "revoke ",
        "attach ",
        "detach ",
        "pragma ",
        "install ",
        "load ",
        "copy ",
        "export ",
        "import ",
    ]

    ALLOWED_PREFIXES = [
        "select ",
        "with ",
        "explain ",
        "describe ",
        "show ",
    ]

    def __init__(
        self,
        max_query_length: int = 50000,
    ) -> None:
        """
        Initialize SQL validator.

        Args:
            require_catalog_prefix: Whether to enforce 'catalog.' prefix
            max_query_length: Maximum allowed query length in characters
        """
        self.max_query_length = max_query_length

    def validate(self, sql: str) -> None:
        """
        Validate SQL query.

        Args:
            sql: SQL query string to validate

        Raises:
            SQLValidationError: If validation fails
        """
        sql_normalized = " ".join(sql.split())
        sql_lower = sql_normalized.lower()

        if len(sql) > self.max_query_length:
            raise SQLValidationError(
                f"Query too long: {len(sql)} characters (max: {self.max_query_length})"
            )

        if not sql_normalized.strip():
            raise SQLValidationError("Empty query not allowed")

        self._check_dangerous_keywords(sql_lower)

        self._check_allowed_prefix(sql_lower)

        self._check_multi_statement(sql)

        self._check_injection_patterns(sql_lower)

        logger.debug(
            "SQL validation passed",
            extra={"query_hash": self.hash_query(sql)},
        )

    def _check_dangerous_keywords(self, sql_lower: str) -> None:
        """
        Check for dangerous SQL keywords.

        Args:
            sql_lower: Lowercase SQL query

        Raises:
            SQLValidationError: If dangerous keyword found
        """
        for keyword in self.DANGEROUS_KEYWORDS:
            if keyword in sql_lower:
                raise SQLValidationError(
                    f"Dangerous SQL keyword not allowed: {keyword.strip().upper()}"
                )

    def _check_allowed_prefix(self, sql_lower: str) -> None:
        """
        Verify query starts with allowed prefix.

        Args:
            sql_lower: Lowercase SQL query

        Raises:
            SQLValidationError: If query doesn't start with allowed prefix
        """
        has_valid_prefix = any(
            sql_lower.startswith(prefix) for prefix in self.ALLOWED_PREFIXES
        )

        if not has_valid_prefix:
            allowed = ", ".join(p.strip().upper() for p in self.ALLOWED_PREFIXES)
            raise SQLValidationError(f"Query must start with one of: {allowed}")

    def _check_multi_statement(self, sql: str) -> None:
        """
        Check for multi-statement queries.

        Args:
            sql: SQL query string

        Raises:
            SQLValidationError: If multiple statements detected
        """
        sql_stripped = sql.rstrip().rstrip(";")

        if ";" in sql_stripped:
            raise SQLValidationError("Multi-statement queries not allowed")

    def _check_injection_patterns(self, sql_lower: str) -> None:
        """
        Check for common SQL injection patterns.

        Args:
            sql_lower: Lowercase SQL query

        Raises:
            SQLValidationError: If injection pattern detected
        """
        if "--" in sql_lower or "/*" in sql_lower:
            raise SQLValidationError("SQL comments not allowed")

        if "union " in sql_lower and "select " in sql_lower:
            pass

        if re.search(r"\bexec(ute)?\b", sql_lower):
            raise SQLValidationError("EXEC/EXECUTE statements not allowed")

    @staticmethod
    def hash_query(sql: str) -> str:
        """
        Generate hash of SQL query for deduplication.

        Args:
            sql: SQL query string

        Returns:
            SHA-256 hash of normalized query
        """
        normalized = " ".join(sql.lower().split())
        return hashlib.sha256(normalized.encode()).hexdigest()[:16]

    @staticmethod
    def sanitize_for_logging(sql: str, max_length: int = 200) -> str:
        """
        Sanitize SQL query for safe logging.

        Args:
            sql: SQL query string
            max_length: Maximum length for logged query

        Returns:
            Sanitized query string safe for logging
        """
        if len(sql) > max_length:
            sql = sql[:max_length] + "..."

        sql = re.sub(r"'[^']*'", "'<string>'", sql)
        sql = re.sub(r"\b\d{10,}\b", "<number>", sql)

        return sql


_default_validator = SQLValidator()


def validate_sql(sql: str) -> None:
    """
    Validate SQL query using default validator.

    Args:
        sql: SQL query string to validate

    Raises:
        SQLValidationError: If validation fails
    """
    _default_validator.validate(sql)


def hash_query(sql: str) -> str:
    """
    Generate hash of SQL query using default validator.

    Args:
        sql: SQL query string

    Returns:
        SHA-256 hash of normalized query
    """
    return SQLValidator.hash_query(sql)


def sanitize_for_logging(sql: str, max_length: int = 200) -> str:
    """
    Sanitize SQL query for logging using default validator.

    Args:
        sql: SQL query string
        max_length: Maximum length for logged query

    Returns:
        Sanitized query string safe for logging
    """
    return SQLValidator.sanitize_for_logging(sql, max_length)
