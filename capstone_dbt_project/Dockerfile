# Dockerfile - dbt-build Cloud Run Job image.
# Just dbt-bigquery + this dbt project, run as `dbt build` on a daily Cloud Scheduler
# trigger. Auth is ADC from the Cloud Run job's service account (profiles.yml's
# `method: oauth` picks it up from the metadata server - no key file needed).
FROM python:3.13-slim

WORKDIR /app

# Pinned to the versions in the repo's uv.lock.
RUN pip install --no-cache-dir dbt-core==1.10.13 dbt-bigquery==1.12.1

# Copy only what dbt needs - NOT the committed .venv/ (see CLAUDE.md gotchas).
COPY dbt_project.yml packages.yml package-lock.yml ./
COPY .dbt/profiles.yml ./.dbt/profiles.yml
COPY macros/ ./macros/
COPY models/ ./models/
COPY tests/ ./tests/
COPY seeds/ ./seeds/
COPY snapshots/ ./snapshots/

# Resolve packages at build time so runs don't depend on hub.getdbt.com.
RUN dbt deps --profiles-dir .dbt

ENV DBT_PROFILES_DIR=/app/.dbt
# Freshness check first (Guardian + one per VN site): a stale source (ingestion
# "succeeding" but landing nothing) fails the execution, which fires the email
# alert. The build still runs, so one stale site doesn't freeze the fresh ones.
# Warn-level freshness exits 0.
CMD ["sh", "-c", "dbt source freshness --target dev; f=$?; dbt build --target dev; b=$?; [ $f -eq 0 ] && [ $b -eq 0 ]"]
