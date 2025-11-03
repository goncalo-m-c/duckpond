FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.8.21 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1


RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    python3-dev \
    libpq-dev \
    libffi-dev \
    libsnappy-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://install.duckdb.org | sh

ENV PATH="/root/.duckdb/cli/latest:$PATH"

COPY pyproject.toml uv.lock /app/

COPY duckpond /app/

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev


ENV PATH="/app/.venv/bin:$PATH"

# RUN groupadd -g 1001 appgroup && \
#     useradd -u 1001 -g appgroup -m -d /app -s /bin/false appuser

WORKDIR /app

# USER appuser
