FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

COPY pyproject.toml README.md ./
COPY src ./src
COPY services ./services

# Install ship-analytics package without deps, then install all deps
# with CUDA torch from the PyTorch CUDA index (no CPU fallback)
RUN pip install --no-cache-dir --no-deps . && \
    pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cu124 \
        "confluent-kafka>=2.6,<3" \
        "fastapi>=0.115,<1" \
        "httpx>=0.27,<1" \
        "pandas>=2.2,<3" \
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
        "torch>=2.6.0,<3" \
        "sentence-transformers>=3.0,<4" \
        "spacy>=3.8,<4" && \
    python -m spacy download en_core_web_sm

COPY data/sample ./data/sample