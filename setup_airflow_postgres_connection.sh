#!/bin/bash
set -e

# Disable Git Bash path conversion on Windows
export MSYS_NO_PATHCONV=1
export PYTHONWARNINGS="ignore::DeprecationWarning,ignore::SyntaxWarning"

if [ ! -f ".env" ]; then
    echo "Error: .env file not found in $(pwd)"
    exit 1
fi

source .env

echo "Setting up PostgreSQL connection: postgres_kafka_default"

# Delete old connection if exists (suppress warnings)
docker compose -f docker-compose-airflow.yml exec -T airflow-scheduler \
    bash -c "PYTHONWARNINGS='ignore' airflow connections delete postgres_kafka_default" \
    2>&1 | grep -E "(Successfully deleted|Did not find)" || true

# Add new connection (suppress warnings)
docker compose -f docker-compose-airflow.yml exec -T airflow-scheduler \
    bash -c "PYTHONWARNINGS='ignore' airflow connections add postgres_kafka_default \
        --conn-type postgres \
        --conn-host kafka-postgres \
        --conn-port '${POSTGRES_PORT:-5432}' \
        --conn-login '$POSTGRES_USER' \
        --conn-password '$POSTGRES_PASSWORD' \
        --conn-schema '$POSTGRES_DB'" \
    2>&1 | grep "Successfully added" || echo "Connection may already exist"

echo "✅ PostgreSQL connection 'postgres_kafka_default' created successfully!"