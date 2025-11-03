"""Prometheus parser for HTTP request handling.

This module provides utilities for parsing Prometheus remote write HTTP requests,
including validation of headers, content extraction, and error handling.
"""

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class PrometheusParser:
    """Parser for Prometheus remote write HTTP requests.

    This class provides methods to parse and validate HTTP requests sent by
    Prometheus servers using the remote write protocol.

    Expected request format:
    - Method: POST
    - Content-Type: application/x-protobuf
    - Content-Encoding: snappy
    - X-Prometheus-Remote-Write-Version: 0.1.0 (optional)
    - Body: Snappy-compressed Protobuf WriteRequest

    Example:
        parser = PrometheusParser()

        is_valid, error = parser.validate_headers(headers)
        if not is_valid:
            raise ValueError(error)

        result = parser.parse_request(headers, body)
        print(f"Received {result['sample_count']} samples")
    """

    EXPECTED_CONTENT_TYPE = "application/x-protobuf"
    EXPECTED_CONTENT_ENCODING = "snappy"
    SUPPORTED_VERSIONS = ["0.1.0", "1.0.0"]

    @staticmethod
    def validate_headers(headers: Dict[str, str]) -> tuple[bool, Optional[str]]:
        """Validate HTTP headers for Prometheus remote write request.

        Checks that required headers are present and have correct values.

        Args:
            headers: HTTP request headers (case-insensitive dict recommended)

        Returns:
            tuple: (is_valid, error_message)
                - is_valid: True if headers are valid
                - error_message: None if valid, error description if invalid

        Example:
            >>> parser = PrometheusParser()
            >>> headers = {
            ...     'Content-Type': 'application/x-protobuf',
            ...     'Content-Encoding': 'snappy'
            ... }
            >>> is_valid, error = parser.validate_headers(headers)
            >>> assert is_valid
        """
        normalized_headers = {k.lower(): v for k, v in headers.items()}

        content_type = normalized_headers.get("content-type", "")
        if PrometheusParser.EXPECTED_CONTENT_TYPE not in content_type:
            return (
                False,
                f"Invalid Content-Type: expected '{PrometheusParser.EXPECTED_CONTENT_TYPE}', "
                f"got '{content_type}'",
            )

        content_encoding = normalized_headers.get("content-encoding", "")
        if PrometheusParser.EXPECTED_CONTENT_ENCODING not in content_encoding.lower():
            return (
                False,
                f"Invalid Content-Encoding: expected '{PrometheusParser.EXPECTED_CONTENT_ENCODING}', "
                f"got '{content_encoding}'",
            )

        version = normalized_headers.get("x-prometheus-remote-write-version", "")
        if version and version not in PrometheusParser.SUPPORTED_VERSIONS:
            logger.warning(
                f"Unsupported Prometheus remote write version: {version}. "
                f"Supported versions: {PrometheusParser.SUPPORTED_VERSIONS}"
            )

        return True, None

    @staticmethod
    def parse_request(headers: Dict[str, str], body: bytes) -> Dict[str, Any]:
        """Parse Prometheus remote write HTTP request.

        Validates headers and extracts basic information about the request
        without decoding the full payload.

        Args:
            headers: HTTP request headers
            body: Request body (compressed Protobuf data)

        Returns:
            dict: Request information including:
                - body_size: Size of compressed body in bytes
                - content_type: Content type from headers
                - content_encoding: Content encoding from headers
                - version: Prometheus remote write version (if present)
                - is_valid: Whether request passes validation

        Raises:
            ValueError: If headers are invalid

        Example:
            >>> parser = PrometheusParser()
            >>> info = parser.parse_request(headers, body)
            >>> print(f"Received {info['body_size']} bytes")
        """
        is_valid, error = PrometheusParser.validate_headers(headers)
        if not is_valid:
            raise ValueError(f"Invalid Prometheus remote write request: {error}")

        normalized_headers = {k.lower(): v for k, v in headers.items()}

        info = {
            "body_size": len(body),
            "content_type": normalized_headers.get("content-type", ""),
            "content_encoding": normalized_headers.get("content-encoding", ""),
            "version": normalized_headers.get("x-prometheus-remote-write-version", "unknown"),
            "is_valid": True,
        }

        logger.debug(
            f"Parsed Prometheus request: {info['body_size']} bytes, version {info['version']}"
        )

        return info

    @staticmethod
    def validate_request_size(
        body_size: int, max_size: int = 100 * 1024 * 1024
    ) -> tuple[bool, Optional[str]]:
        """Validate that request size is within acceptable limits.

        Args:
            body_size: Size of request body in bytes
            max_size: Maximum allowed size in bytes (default: 100 MB)

        Returns:
            tuple: (is_valid, error_message)

        Example:
            >>> parser = PrometheusParser()
            >>> is_valid, error = parser.validate_request_size(1024 * 1024)
            >>> assert is_valid
        """
        if body_size <= 0:
            return False, "Request body is empty"

        if body_size > max_size:
            return (
                False,
                f"Request body too large: {body_size} bytes "
                f"(max: {max_size} bytes, {max_size / (1024 * 1024):.1f} MB)",
            )

        return True, None

    @staticmethod
    def extract_account_id(headers: Dict[str, str]) -> Optional[str]:
        """Extract account ID from request headers.

        Looks for account identification in common header fields:
        - X-Scope-OrgID (Cortex/Mimir standard)
        - X-Account-ID
        - X-Organization-ID

        Args:
            headers: HTTP request headers

        Returns:
            Optional[str]: Account ID if found, None otherwise

        Example:
            >>> parser = PrometheusParser()
            >>> headers = {'X-Scope-OrgID': 'account-123'}
            >>> account_id = parser.extract_account_id(headers)
            >>> assert account_id == 'account-123'
        """
        normalized_headers = {k.lower(): v for k, v in headers.items()}

        account_headers = [
            "x-scope-orgid",
            "x-account-id",
            "x-organization-id",
            "x-org-id",
        ]

        for header in account_headers:
            account_id = normalized_headers.get(header)
            if account_id:
                logger.debug(f"Extracted account ID: {account_id} from header {header}")
                return account_id

        return None

    @staticmethod
    def get_user_agent(headers: Dict[str, str]) -> Optional[str]:
        """Extract User-Agent from request headers.

        Args:
            headers: HTTP request headers

        Returns:
            Optional[str]: User-Agent string if present

        Example:
            >>> parser = PrometheusParser()
            >>> headers = {'User-Agent': 'Prometheus/2.45.0'}
            >>> ua = parser.get_user_agent(headers)
            >>> assert 'Prometheus' in ua
        """
        normalized_headers = {k.lower(): v for k, v in headers.items()}
        return normalized_headers.get("user-agent")

    @staticmethod
    def create_response_headers(
        status: str = "success", compressed_size: Optional[int] = None
    ) -> Dict[str, str]:
        """Create response headers for Prometheus remote write response.

        Prometheus expects a 2xx status code and doesn't require specific headers.
        This method creates standard headers for the response.

        Args:
            status: Status message ('success' or 'error')
            compressed_size: Size of compressed data that was processed (optional)

        Returns:
            dict: Response headers

        Example:
            >>> parser = PrometheusParser()
            >>> headers = parser.create_response_headers('success', 1024)
            >>> assert headers['Content-Type'] == 'application/json'
        """
        headers = {
            "Content-Type": "application/json",
            "X-Prometheus-Remote-Write-Status": status,
        }

        if compressed_size is not None:
            headers["X-Prometheus-Remote-Write-Bytes-Received"] = str(compressed_size)

        return headers

    @staticmethod
    def parse_remote_write_version(version_string: str) -> tuple[int, int, int]:
        """Parse Prometheus remote write version string.

        Args:
            version_string: Version string like '0.1.0' or '1.0.0'

        Returns:
            tuple: (major, minor, patch) version numbers

        Raises:
            ValueError: If version string is invalid

        Example:
            >>> parser = PrometheusParser()
            >>> major, minor, patch = parser.parse_remote_write_version('0.1.0')
            >>> assert (major, minor, patch) == (0, 1, 0)
        """
        try:
            parts = version_string.split(".")
            if len(parts) != 3:
                raise ValueError(f"Invalid version format: {version_string}")

            major = int(parts[0])
            minor = int(parts[1])
            patch = int(parts[2])

            return major, minor, patch
        except (ValueError, IndexError) as e:
            raise ValueError(f"Failed to parse version string '{version_string}': {e}") from e
