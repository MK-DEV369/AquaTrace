#!/usr/bin/env bash
# Ship Analytics Quickstart — build, run, and verify the full stack
set -e
cd "$(dirname "$0")"

echo "=== Ship Analytics Quickstart ==="

# 1. Build API image with all Phase 2 dependencies
echo -e "\n[1/4] Building API image..."
docker compose build api

# 2. Start all services
echo -e "\n[2/4] Starting services..."
docker compose up -d

# 3. Wait for API to be ready
echo -e "\n[3/4] Waiting for API..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
        echo "  API is ready"
        break
    fi
    sleep 2
done

# 4. Run tests
echo -e "\n[4/4] Running tests..."
python3 -m pytest tests/ -v

echo -e "\n=== Done ==="
echo "API:        http://localhost:8000"
echo "Docs:       http://localhost:8000/docs"
echo "Dashboard:  http://localhost:8501"
echo "PostgreSQL: localhost:5432"
echo "Kafka:      localhost:29092"