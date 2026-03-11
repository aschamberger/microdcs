# @see https://raw.githubusercontent.com/astral-sh/uv-docker-example/refs/heads/main/Dockerfile
# @see https://www.joshkasuboski.com/posts/distroless-python-uv/
# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim AS builder

# Install the project into `/app`
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Omit development dependencies
ENV UV_NO_DEV=1

# Ensure installed tools can be executed out of the box
ENV UV_TOOL_BIN_DIR=/usr/local/bin

# Configure the Python directory so it is consistent
ENV UV_PYTHON_INSTALL_DIR=/python

# Only use the managed Python version
ENV UV_PYTHON_PREFERENCE=only-managed

# Install Python before the project for caching
RUN uv python install 3.14.3

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project

# Then, add the rest of the project source code and install it
# Installing separately from its dependencies allows optimal layer caching
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

# Build final image without uv
FROM gcr.io/distroless/cc-debian13:nonroot

# Copy the Python version
COPY --from=builder /python /python

# Install the project into `/app`
WORKDIR /app

# Copy the application from the builder
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/app /app/app

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Disable Python output buffering for better logging
ENV PYTHONUNBUFFERED=1

# Reset the entrypoint, don't invoke `uv`
ENTRYPOINT []

# Run the application
#CMD ["opentelemetry-instrument", "python3", "-m", "app", "2>&1"]
CMD ["python3", "-m", "app"]
