FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# uv: faster installs, single binary. pin so the image is reproducible.
ARG UV_VERSION=0.5.5
RUN curl -LsSf https://astral.sh/uv/${UV_VERSION}/install.sh | sh \
    && mv /root/.local/bin/uv /usr/local/bin/uv

WORKDIR /opt/dagster/app

COPY pyproject.toml ./
# uv.lock is committed once we generate it locally; copy it in conditionally
COPY uv.loc[k] ./

RUN uv pip install --system -e .[dev]

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/data/warehouse

EXPOSE 3000
