ARG AIRFLOW_VERSION=3.2.2
FROM apache/airflow:${AIRFLOW_VERSION}

COPY --from=ghcr.io/astral-sh/uv:0.9.4 /uv /uvx /bin/

WORKDIR /opt/airflow

COPY pyproject.toml uv.lock ./

# Use uv for dependency installation, but avoid `uv sync` on the Airflow base image
# because sync performs exact environment reconciliation.
RUN uv export --frozen --format requirements.txt --output-file requirements.txt \
    && uv pip install --python /home/airflow/.local/bin/python -r requirements.txt \
    && rm requirements.txt
