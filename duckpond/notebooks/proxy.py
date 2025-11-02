"""WebSocket and HTTP proxy for marimo integration."""

import asyncio
from typing import Any

import httpx
import structlog
from fastapi import WebSocket, WebSocketDisconnect
from starlette.datastructures import Headers
from starlette.responses import Response, StreamingResponse
from websockets import client as ws_client
from websockets.exceptions import ConnectionClosed

from duckpond.notebooks.session import NotebookSession

logger = structlog.get_logger(__name__)


async def proxy_websocket(websocket: WebSocket, session: NotebookSession) -> None:
    """
    Proxy WebSocket connection to marimo process.

    Establishes bidirectional communication between client WebSocket and
    marimo's WebSocket endpoint.

    Args:
        websocket: Client WebSocket connection
        session: Notebook session information

    Raises:
        WebSocketDisconnect: When either side disconnects
    """
    marimo_ws_url = f"ws://127.0.0.1:{session.port}/ws"

    logger.info(
        "establishing_websocket_proxy",
        session_id=session.session_id,
        marimo_url=marimo_ws_url,
    )

    await websocket.accept()

    session.update_last_accessed()

    try:
        async with ws_client.connect(marimo_ws_url) as marimo_ws:
            logger.info(
                "websocket_proxy_connected",
                session_id=session.session_id,
            )

            async def forward_to_marimo():
                """Forward messages from client to marimo."""
                try:
                    while True:
                        data = await websocket.receive()

                        session.update_last_accessed()

                        if "text" in data:
                            await marimo_ws.send(data["text"])
                            logger.debug(
                                "ws_client_to_marimo_text",
                                session_id=session.session_id,
                                size=len(data["text"]),
                            )
                        elif "bytes" in data:
                            await marimo_ws.send(data["bytes"])
                            logger.debug(
                                "ws_client_to_marimo_bytes",
                                session_id=session.session_id,
                                size=len(data["bytes"]),
                            )
                except WebSocketDisconnect:
                    logger.info(
                        "client_websocket_disconnected",
                        session_id=session.session_id,
                    )
                except ConnectionClosed:
                    logger.info(
                        "marimo_websocket_closed",
                        session_id=session.session_id,
                    )
                except Exception as e:
                    logger.error(
                        "ws_forward_to_marimo_error",
                        session_id=session.session_id,
                        error=str(e),
                        exc_info=True,
                    )

            async def forward_to_client():
                """Forward messages from marimo to client."""
                try:
                    async for message in marimo_ws:
                        session.update_last_accessed()

                        if isinstance(message, str):
                            await websocket.send_text(message)
                            logger.debug(
                                "ws_marimo_to_client_text",
                                session_id=session.session_id,
                                size=len(message),
                            )
                        elif isinstance(message, bytes):
                            await websocket.send_bytes(message)
                            logger.debug(
                                "ws_marimo_to_client_bytes",
                                session_id=session.session_id,
                                size=len(message),
                            )
                except WebSocketDisconnect:
                    logger.info(
                        "client_websocket_disconnected_during_forward",
                        session_id=session.session_id,
                    )
                except ConnectionClosed:
                    logger.info(
                        "marimo_websocket_closed_during_forward",
                        session_id=session.session_id,
                    )
                except Exception as e:
                    logger.error(
                        "ws_forward_to_client_error",
                        session_id=session.session_id,
                        error=str(e),
                        exc_info=True,
                    )

            await asyncio.gather(
                forward_to_marimo(),
                forward_to_client(),
                return_exceptions=True,
            )

    except Exception as e:
        logger.error(
            "websocket_proxy_error",
            session_id=session.session_id,
            error=str(e),
            exc_info=True,
        )
        raise
    finally:
        logger.info(
            "websocket_proxy_closed",
            session_id=session.session_id,
        )


async def proxy_http_request(
    method: str,
    path: str,
    session: NotebookSession,
    headers: Headers,
    body: bytes | None = None,
) -> Response:
    """
    Proxy HTTP request to marimo process.

    Forwards HTTP requests to marimo's web server and returns the response.

    Args:
        method: HTTP method (GET, POST, etc.)
        path: Request path
        session: Notebook session
        headers: Request headers
        body: Request body (optional)

    Returns:
        Response from marimo server
    """
    marimo_url = f"http://127.0.0.1:{session.port}/{path.lstrip('/')}"

    session.update_last_accessed()

    filtered_headers = _filter_hop_by_hop_headers(dict(headers))

    logger.debug(
        "proxying_http_request",
        session_id=session.session_id,
        method=method,
        url=marimo_url,
    )

    try:
        async with httpx.AsyncClient() as client:
            response = await client.request(
                method=method,
                url=marimo_url,
                headers=filtered_headers,
                content=body,
                timeout=30.0,
                follow_redirects=False,
            )

            response_headers = _filter_hop_by_hop_headers(dict(response.headers))

            if response.headers.get("content-type", "").startswith("text/event-stream"):
                return StreamingResponse(
                    content=response.aiter_bytes(),
                    status_code=response.status_code,
                    headers=response_headers,
                    media_type="text/event-stream",
                )

            logger.debug(
                "http_proxy_response",
                session_id=session.session_id,
                status=response.status_code,
                size=len(response.content),
            )

            return Response(
                content=response.content,
                status_code=response.status_code,
                headers=response_headers,
                media_type=response.headers.get("content-type"),
            )

    except Exception as e:
        logger.error(
            "http_proxy_error",
            session_id=session.session_id,
            error=str(e),
            exc_info=True,
        )
        raise


def _filter_hop_by_hop_headers(headers: dict[str, Any]) -> dict[str, Any]:
    """
    Filter out hop-by-hop headers that should not be forwarded.

    Args:
        headers: Original headers

    Returns:
        Filtered headers
    """
    hop_by_hop = {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        "proxy-connection",
        "host",
    }

    return {k: v for k, v in headers.items() if k.lower() not in hop_by_hop}
