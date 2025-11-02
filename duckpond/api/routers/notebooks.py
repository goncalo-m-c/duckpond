"""Notebook API endpoints."""

import asyncio
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from duckpond.api.dependencies import (
    CurrentTenant,
    DatabaseSession,
    get_settings_dependency,
    get_current_tenant,
)
from duckpond.config import Settings
from duckpond.notebooks import (
    NotebookNotFoundException,
    PathSecurityException,
    PortExhaustedException,
    SessionLimitException,
    SessionNotFoundException,
    get_tenant_notebook_directory,
    proxy_http_request,
    proxy_websocket,
)
from duckpond.notebooks.files import (
    create_notebook,
    delete_notebook,
    list_notebooks,
    read_notebook,
    update_notebook,
)

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/notebooks", tags=["notebooks"])


class CreateSessionRequest(BaseModel):
    """Request to create a notebook session."""

    notebook_path: str = Field(
        ...,
        description="Path to notebook file (relative to tenant notebook directory)",
        examples=["analysis.py", "reports/monthly.py"],
    )


class CreateSessionResponse(BaseModel):
    """Response from creating a notebook session."""

    session_id: str = Field(..., description="Unique session identifier")
    notebook_path: str = Field(..., description="Absolute path to notebook")
    port: int = Field(..., description="Port marimo is listening on")
    ws_url: str = Field(
        ...,
        description="WebSocket URL for connecting to notebook (add ?X-API-KEY=<your-key> for browser access)",
    )
    ui_url: str = Field(
        ...,
        description="URL for notebook UI (add ?X-API-KEY=<your-key> for browser access)",
    )
    status: str = Field(..., description="Session status")


class SessionInfoResponse(BaseModel):
    """Notebook session information."""

    session_id: str
    tenant_id: str
    notebook_path: str
    port: int
    status: str
    created_at: str
    last_accessed: str
    pid: int | None
    is_alive: bool


class NotebookFileRequest(BaseModel):
    """Request to create or update a notebook file."""

    filename: str = Field(
        ...,
        description="Notebook filename (must end with .py)",
        examples=["analysis.py"],
    )
    content: str | None = Field(
        None,
        description="Notebook content (uses default template if omitted)",
    )


class NotebookFileResponse(BaseModel):
    """Notebook file metadata."""

    filename: str
    path: str
    size_bytes: int
    modified_at: float


class StatusResponse(BaseModel):
    """Notebook service status."""

    enabled: bool
    active_sessions: int
    max_sessions: int
    available_ports: int
    sessions_by_status: dict[str, int]


@router.get("/status", response_model=StatusResponse)
async def get_status(
    request: Request,
) -> StatusResponse:
    """
    Get notebook service status.

    Returns current status including active sessions and resource availability.
    """
    if not hasattr(request.app.state, "notebook_manager"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Notebook service not available",
        )

    manager = request.app.state.notebook_manager
    status_dict = manager.get_status()

    return StatusResponse(**status_dict)


@router.post(
    "/sessions",
    response_model=CreateSessionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_session(
    request: Request,
    create_request: CreateSessionRequest,
    tenant_id: CurrentTenant,
    settings: Annotated[Settings, Depends(get_settings_dependency)],
) -> CreateSessionResponse:
    """
    Create a new notebook session.

    Spawns a marimo process for the specified notebook and returns connection details.
    """
    if not settings.notebook_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Notebook feature is disabled",
        )

    if not hasattr(request.app.state, "notebook_manager"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Notebook manager not initialized",
        )

    manager = request.app.state.notebook_manager

    try:
        session = await manager.create_session(
            tenant_id=tenant_id,
            notebook_path=create_request.notebook_path,
        )

        base_url = str(request.base_url).rstrip("/")
        ws_scheme = "wss" if request.url.scheme == "https" else "ws"

        return CreateSessionResponse(
            session_id=session.session_id,
            notebook_path=str(session.notebook_path),
            port=session.port,
            ws_url=f"{ws_scheme}://{request.url.netloc}/notebooks/sessions/{session.session_id}/ws",
            ui_url=f"{base_url}/notebooks/sessions/{session.session_id}/ui",
            status=session.status.value,
        )

    except SessionLimitException as e:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too many concurrent sessions: {e.current}/{e.maximum}",
            headers={"Retry-After": "60"},
        )
    except PortExhaustedException:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="No available ports for notebook session",
            headers={"Retry-After": "60"},
        )
    except NotebookNotFoundException as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Notebook not found: {e.notebook_path}",
        )
    except PathSecurityException as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Invalid path: {e.reason}",
        )
    except Exception as e:
        logger.error(
            "session_creation_error",
            tenant_id=tenant_id,
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create session: {str(e)}",
        )


@router.get("/sessions", response_model=list[SessionInfoResponse])
async def list_sessions(
    request: Request,
    tenant_id: CurrentTenant,
) -> list[SessionInfoResponse]:
    """List all active notebook sessions for the current tenant."""
    if not hasattr(request.app.state, "notebook_manager"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Notebook manager not initialized",
        )

    manager = request.app.state.notebook_manager
    sessions = await manager.list_sessions(tenant_id=tenant_id)

    return [SessionInfoResponse(**session.to_dict()) for session in sessions]


@router.get("/sessions/{session_id}", response_model=SessionInfoResponse)
async def get_session(
    request: Request,
    session_id: str,
    tenant_id: CurrentTenant,
) -> SessionInfoResponse:
    """Get information about a specific notebook session."""
    if not hasattr(request.app.state, "notebook_manager"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Notebook manager not initialized",
        )

    manager = request.app.state.notebook_manager

    try:
        session = await manager.get_session(session_id)

        if session.tenant_id != tenant_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied to this session",
            )

        return SessionInfoResponse(**session.to_dict())

    except SessionNotFoundException:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Session not found: {session_id}",
        )


@router.delete("/sessions/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
async def terminate_session(
    request: Request,
    session_id: str,
    tenant_id: CurrentTenant,
) -> None:
    """Terminate a notebook session and clean up resources."""
    if not hasattr(request.app.state, "notebook_manager"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Notebook manager not initialized",
        )

    manager = request.app.state.notebook_manager

    try:
        session = await manager.get_session(session_id)

        if session.tenant_id != tenant_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied to this session",
            )

        await manager.terminate_session(session_id)

    except SessionNotFoundException:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Session not found: {session_id}",
        )


@router.websocket("/sessions/{session_id}/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    session_id: str,
    tenant_id: CurrentTenant,
) -> None:
    """
    WebSocket endpoint for notebook communication.

    Proxies WebSocket traffic between client and marimo process.

    Authentication supports query parameter (X-API-KEY) for WebSocket compatibility.
    """
    if not hasattr(websocket.app.state, "notebook_manager"):
        await websocket.close(code=1011, reason="Notebook manager not available")
        return

    manager = websocket.app.state.notebook_manager

    try:
        session = await manager.get_session(session_id)

        # Verify tenant access
        if session.tenant_id != tenant_id:
            await websocket.close(code=1008, reason="Access denied to this session")
            return

        await proxy_websocket(websocket, session)

    except SessionNotFoundException:
        await websocket.close(code=1008, reason="Session not found")
    except Exception as e:
        logger.error(
            "websocket_error",
            session_id=session_id,
            error=str(e),
            exc_info=True,
        )
        await websocket.close(code=1011, reason="Internal error")


@router.websocket("/sessions/{session_id}/ui/ws")
async def marimo_websocket_proxy(
    websocket: WebSocket,
    session_id: str,
) -> None:
    """
    WebSocket proxy for marimo's internal WebSocket.

    Marimo uses /ws for its reactive notebook communication.
    This proxies that WebSocket through our authentication layer.
    """
    if not hasattr(websocket.app.state, "notebook_manager"):
        await websocket.close(code=1011, reason="Notebook manager not available")
        return

    # Authenticate using multi-source helper with manual session management
    from duckpond.api.dependencies import get_db_session

    db_session = None
    try:
        async for session_gen in get_db_session():
            db_session = session_gen
            break

        tenant_id = await authenticate_from_multi_source(websocket, db_session)
    except HTTPException as e:
        if db_session:
            await db_session.close()
        await websocket.close(code=1008, reason=e.detail)
        return
    except Exception as e:
        if db_session:
            await db_session.close()
        logger.error(
            "marimo_ws_auth_error",
            session_id=session_id,
            error=str(e),
            exc_info=True,
        )
        await websocket.close(code=1011, reason="Authentication error")
        return

    manager = websocket.app.state.notebook_manager

    try:
        session = await manager.get_session(session_id)

        # Verify tenant access
        if session.tenant_id != tenant_id:
            await websocket.close(code=1008, reason="Access denied to this session")
            return

        # Accept the WebSocket connection
        await websocket.accept()

        # Proxy to marimo's WebSocket
        import websockets
        from websockets.exceptions import ConnectionClosed

        marimo_ws_url = f"ws://127.0.0.1:{session.port}/ws"

        # Forward query parameters (marimo needs session_id)
        query_string = str(websocket.url.query)
        if query_string:
            # Remove our API key from query string before forwarding
            import urllib.parse

            params = urllib.parse.parse_qs(query_string)
            params.pop("X-API-KEY", None)
            if params:
                marimo_ws_url += "?" + urllib.parse.urlencode(params, doseq=True)

        logger.info(
            "marimo_ws_connecting",
            session_id=session_id,
            url=marimo_ws_url,
        )

        async with websockets.connect(marimo_ws_url) as marimo_ws:

            async def forward_to_marimo():
                try:
                    async for message in websocket.iter_text():
                        await marimo_ws.send(message)
                except Exception:
                    pass

            async def forward_to_client():
                try:
                    async for message in marimo_ws:
                        if isinstance(message, str):
                            await websocket.send_text(message)
                        elif isinstance(message, bytes):
                            await websocket.send_bytes(message)
                except Exception:
                    pass

            await asyncio.gather(
                forward_to_marimo(),
                forward_to_client(),
                return_exceptions=True,
            )

    except SessionNotFoundException:
        await websocket.close(code=1008, reason="Session not found")
    except Exception as e:
        logger.error(
            "marimo_ws_error",
            session_id=session_id,
            error=str(e),
            exc_info=True,
        )
        await websocket.close(code=1011, reason="Internal error")
    finally:
        if db_session:
            await db_session.close()


@router.api_route(
    "/sessions/{session_id}/ui/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    response_model=None,
)
async def proxy_ui(
    request: Request,
    session_id: str,
    path: str,
    tenant_id: CurrentTenant,
):
    """
    Proxy HTTP requests to marimo UI.

    Forwards all HTTP traffic to marimo's web server.

    Authentication supports both header-based (X-API-Key) and query parameter
    (X-API-KEY) for browser compatibility.
    """
    if not hasattr(request.app.state, "notebook_manager"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Notebook manager not initialized",
        )

    # Track where the API key came from for cookie setting
    api_key_from_query = request.query_params.get("X-API-KEY") is not None

    manager = request.app.state.notebook_manager

    try:
        session = await manager.get_session(session_id)

        if session.tenant_id != tenant_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied to this session",
            )

        body = await request.body()

        response = await proxy_http_request(
            method=request.method,
            path=path,
            session=session,
            headers=request.headers,
            body=body if body else None,
        )

        # Set cookie if authentication was via query parameter (for subsequent requests)
        if api_key_from_query:
            api_key = request.query_params.get("X-API-KEY")
            response.set_cookie(
                key="notebook_api_key",
                value=api_key,
                httponly=True,
                samesite="lax",
                max_age=3600,  # 1 hour
                path=f"/notebooks/sessions/{session_id}/ui",
            )

        return response

    except SessionNotFoundException:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Session not found: {session_id}",
        )


@router.post(
    "/files", response_model=NotebookFileResponse, status_code=status.HTTP_201_CREATED
)
async def create_notebook_file(
    file_request: NotebookFileRequest,
    tenant_id: CurrentTenant,
    settings: Annotated[Settings, Depends(get_settings_dependency)],
) -> NotebookFileResponse:
    """Create a new notebook file."""
    tenant_notebook_dir = get_tenant_notebook_directory(
        tenant_id, settings.local_storage_path
    )

    try:
        notebook_path = await create_notebook(
            filename=file_request.filename,
            tenant_notebook_dir=tenant_notebook_dir,
            content=file_request.content,
        )

        stat = notebook_path.stat()
        return NotebookFileResponse(
            filename=notebook_path.name,
            path=str(notebook_path.relative_to(tenant_notebook_dir)),
            size_bytes=stat.st_size,
            modified_at=stat.st_mtime,
        )

    except PathSecurityException as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid filename: {e.reason}",
        )
    except FileExistsError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        )


@router.get("/files", response_model=list[NotebookFileResponse])
async def list_notebook_files(
    tenant_id: CurrentTenant,
    settings: Annotated[Settings, Depends(get_settings_dependency)],
) -> list[NotebookFileResponse]:
    """List all notebook files for the current tenant."""
    tenant_notebook_dir = get_tenant_notebook_directory(
        tenant_id, settings.local_storage_path
    )

    notebooks = await list_notebooks(tenant_notebook_dir)
    return [NotebookFileResponse(**nb) for nb in notebooks]


@router.get("/files/{filename}")
async def get_notebook_file(
    filename: str,
    tenant_id: CurrentTenant,
    settings: Annotated[Settings, Depends(get_settings_dependency)],
) -> dict:
    """Get notebook file content."""
    tenant_notebook_dir = get_tenant_notebook_directory(
        tenant_id, settings.local_storage_path
    )

    try:
        content = await read_notebook(
            notebook_path=filename,
            tenant_notebook_dir=tenant_notebook_dir,
        )

        return {"filename": filename, "content": content}

    except NotebookNotFoundException:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Notebook not found: {filename}",
        )
    except PathSecurityException as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Invalid path: {e.reason}",
        )


@router.put("/files/{filename}")
async def update_notebook_file(
    filename: str,
    file_request: NotebookFileRequest,
    tenant_id: CurrentTenant,
    settings: Annotated[Settings, Depends(get_settings_dependency)],
) -> dict:
    """Update notebook file content."""
    tenant_notebook_dir = get_tenant_notebook_directory(
        tenant_id, settings.local_storage_path
    )

    if not file_request.content:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Content is required for update",
        )

    try:
        await update_notebook(
            notebook_path=filename,
            tenant_notebook_dir=tenant_notebook_dir,
            content=file_request.content,
        )

        return {"filename": filename, "message": "Notebook updated successfully"}

    except NotebookNotFoundException:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Notebook not found: {filename}",
        )
    except PathSecurityException as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Invalid path: {e.reason}",
        )


@router.delete("/files/{filename}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_notebook_file(
    filename: str,
    tenant_id: CurrentTenant,
    settings: Annotated[Settings, Depends(get_settings_dependency)],
) -> None:
    """Delete a notebook file."""
    tenant_notebook_dir = get_tenant_notebook_directory(
        tenant_id, settings.local_storage_path
    )

    try:
        await delete_notebook(
            notebook_path=filename,
            tenant_notebook_dir=tenant_notebook_dir,
        )

    except NotebookNotFoundException:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Notebook not found: {filename}",
        )
    except PathSecurityException as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Invalid path: {e.reason}",
        )
