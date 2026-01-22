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

echo "Setting up Snowflake connection: snowflake_default"

# Delete old connection if exists
docker compose -f docker-compose-airflow.yml exec -T airflow-scheduler \
    airflow connections delete snowflake_default || true

# Smart key selection: prefer container path, fall back to host path
PRIVATE_KEY_PATH="${SNOWFLAKE_PRIVATE_KEY_FILE:-$SNOWFLAKE_PRIVATE_KEY_FILE_PATH}"

if [ -z "$PRIVATE_KEY_PATH" ]; then
    echo "Error: Neither SNOWFLAKE_PRIVATE_KEY_FILE nor SNOWFLAKE_PRIVATE_KEY_FILE_PATH is set in .env"
    exit 1
fi

# If the key is inside the container (recommended way), just use private_key_file
if [[ "$PRIVATE_KEY_PATH" == /opt/airflow/.secret/* ]]; then
    echo "Using private key from inside container: $PRIVATE_KEY_PATH"
    docker compose -f docker-compose-airflow.yml exec -T airflow-scheduler \
        airflow connections add snowflake_default \
            --conn-type snowflake \
            --conn-login "$SNOWFLAKE_USER" \
            --conn-schema "$SNOWFLAKE_SCHEMA" \
            --conn-extra "{
                \"account\": \"$SNOWFLAKE_ACCOUNT\",
                \"warehouse\": \"$SNOWFLAKE_WAREHOUSE\",
                \"database\": \"$SNOWFLAKE_DATABASE\",
                \"role\": \"$SNOWFLAKE_ROLE\",
                \"private_key_file\": \"$PRIVATE_KEY_PATH\",
                \"private_key_passphrase\": \"$SNOWFLAKE_PRIVATE_KEY_FILE_PWD\"
            }"
    echo "✅ Snowflake connection 'snowflake_default' created successfully!"
    exit 0
fi

# Fallback: key is on host → read and base64-encode it
if [ -f "$PRIVATE_KEY_PATH" ]; then
    echo "Reading private key from host: $PRIVATE_KEY_PATH"
    PRIVATE_KEY_CONTENT=$(cat "$PRIVATE_KEY_PATH" | base64 -w 0)
    
    docker compose -f docker-compose-airflow.yml exec -T airflow-scheduler \
        airflow connections add snowflake_default \
            --conn-type snowflake \
            --conn-login "$SNOWFLAKE_USER" \
            --conn-schema "$SNOWFLAKE_SCHEMA" \
            --conn-extra "{
                \"account\": \"$SNOWFLAKE_ACCOUNT\",
                \"warehouse\": \"$SNOWFLAKE_WAREHOUSE\",
                \"database\": \"$SNOWFLAKE_DATABASE\",
                \"role\": \"$SNOWFLAKE_ROLE\",
                \"private_key_content\": \"$PRIVATE_KEY_CONTENT\",
                \"private_key_passphrase\": \"$SNOWFLAKE_PRIVATE_KEY_FILE_PWD\"
            }"
    echo "✅ Snowflake connection 'snowflake_default' created successfully!"
else
    echo "Error: Private key not found at $PRIVATE_KEY_PATH"
    exit 1
fi