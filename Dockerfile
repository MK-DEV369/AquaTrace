FROM python:3.12-slim AS builder

ENV VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DEFAULT_TIMEOUT=300 \
    PIP_RETRIES=5

RUN python -m venv "$VIRTUAL_ENV"

# Install CPU-only PyTorch separately to avoid pulling a CUDA-enabled wheel.
RUN pip install --no-cache-dir \
        --index-url https://download.pytorch.org/whl/cpu \
        --extra-index-url https://pypi.org/simple \
        "torch>=2.6.0,<3"

# Keep this dependency layer stable when application source changes.
RUN pip install --no-cache-dir --index-url https://pypi.org/simple \
        "confluent-kafka>=2.6,<3" \
        "fastapi>=0.115,<1" \
        "httpx>=0.27,<1" \
        "pandas>=2.2,<3" \
        "pydeck>=0.9,<1" \
        "pydantic>=2.9,<3" \
        "pydantic-settings>=2.5,<3" \
        "psycopg[binary]>=3.2,<4" \
        "python-dotenv>=1.0,<2" \
        "streamlit>=1.39,<2" \
        "uvicorn[standard]>=0.30,<1" \
        "websockets>=13,<16" \
        "PyJWT>=2.9,<3" \
        "geoalchemy2>=0.20,<1" \
        "shapely>=2.0,<3" \
        "geopandas>=1.0,<2" \
        "scikit-learn>=1.5,<2" \
        "numpy>=2.0,<3" \
        "sentence-transformers>=3.0,<4" \
        "spacy>=3.8,<4" \
        "transformers>=4.40,<5"

RUN python -m spacy download en_core_web_sm

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps .

FROM python:3.12-slim AS runtime

ENV VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Runtime shared library needed by scientific Python wheels.
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY services ./services
COPY data/sample ./data/sample

# Run as an unprivileged user in production.
RUN useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /app
USER appuser
