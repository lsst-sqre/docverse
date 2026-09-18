# This Dockerfile has three stages:
#
# base-image
#   Updates the base Python image with security patches and common system
#   packages. This image becomes the base of all other images.
# install-image
#   Installs dependencies and the application into a virtual environment.
#   This virtual environment is ideal for copying across build stages.
# runtime-image
#   - Copies the virtual environment into place.
#   - Runs a non-root user.
#   - Sets up the entrypoint and port.

FROM python:3.14.7-slim-trixie AS base-image

# Update system packages.
COPY scripts/install-base-packages.sh .
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    ./install-base-packages.sh && rm ./install-base-packages.sh

FROM base-image AS install-image

# Install uv.
COPY --from=ghcr.io/astral-sh/uv:0.12.8 /uv /bin/uv

# Install system packages only needed for building dependencies.
COPY scripts/install-dependency-packages.sh .
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    ./install-dependency-packages.sh

# Disable hard links during uv package installation since we're using a
# cache on a separate file system.
ENV UV_LINK_MODE=copy

# Force use of system Python so that the Python version is controlled by
# the Docker base image version, not by whatever uv decides to install.
ENV UV_PYTHON_PREFERENCE=only-system

# Install the dependencies.
WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=client/pyproject.toml,target=client/pyproject.toml \
    uv sync --frozen --no-default-groups --compile-bytecode --no-install-workspace

# Version stamps for setuptools_scm. CI derives these from a full-history
# checkout and passes them as build arguments (see the `version` job in
# .github/workflows/ci.yaml). The build context carries no .git directory, so
# a build without them yields the pyproject `fallback_version` (0.0.0) for
# both packages. An empty value is ignored by setuptools_scm.
ARG SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DOCVERSE_SERVER
ARG SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DOCVERSE

# Install the application itself. The two workspace packages are always
# rebuilt: uv's cache keys their wheels on the source tree alone, so a
# persistent cache would otherwise hand back a wheel stamped with whatever
# version the previous build was given.
ADD . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-default-groups --compile-bytecode --no-editable \
    --reinstall-package docverse-server --reinstall-package docverse

FROM base-image AS runtime-image

# Create a non-root user.
RUN useradd --create-home appuser

# Copy the virtualenv, alembic config, and scripts.
COPY --from=install-image /app/.venv /app/.venv
COPY --from=install-image /app/alembic.ini /app/alembic.ini
COPY --from=install-image /app/alembic /app/alembic
COPY --from=install-image /app/scripts/start-service.sh /app/scripts/start-service.sh

# Switch to the non-root user.
USER appuser

# Expose the port.
EXPOSE 8080

# Make sure we use the virtualenv.
ENV PATH="/app/.venv/bin:$PATH"

# Set environment variable for Alembic config; other variables are set
# via Kubernetes.
ENV DOCVERSE_ALEMBIC_CONFIG_PATH="/app/alembic.ini"

# Set a sensible default working directory.
WORKDIR /app

# Run the application.
CMD ["/app/scripts/start-service.sh"]
