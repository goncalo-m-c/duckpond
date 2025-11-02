FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Set environment variables to optimize builds
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Copy dependency files first for better layer caching
COPY pyproject.toml uv.lock ./

# Install dependencies with cache mount for faster rebuilds
# --no-dev excludes dev dependencies from production image
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Copy application code
COPY duckpond ./duckpond

# Install the project itself
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev
