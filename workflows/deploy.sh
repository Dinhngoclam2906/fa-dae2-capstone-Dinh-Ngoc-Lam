#!/usr/bin/env sh
# Deploys the news-pipeline workflow and its two schedules. Idempotent: re-run after
# editing news_pipeline.yaml. Usage: sh workflows/deploy.sh
#
# One-time prerequisites (project owner): the Workflows API, and for dbt-runner
#   roles/workflows.invoker (Scheduler -> workflow), roles/run.viewer (poll executions),
#   roles/logging.logWriter (sys.log). It already has roles/run.invoker.
set -eu

PROJECT="${GCP_PROJECT:?set GCP_PROJECT}"
REGION=asia-southeast1
SA="dbt-runner@${PROJECT}.iam.gserviceaccount.com"
WORKFLOW=news-pipeline
DIR="$(dirname "$0")"
EXEC_URI="https://workflowexecutions.googleapis.com/v1/projects/${PROJECT}/locations/${REGION}/workflows/${WORKFLOW}/executions"

gcloud workflows deploy "$WORKFLOW" --project="$PROJECT" --location="$REGION" \
  --source="$DIR/news_pipeline.yaml" --service-account="$SA" \
  --description="guardian-ingestion -> vn-ingestion [-> dbt-build -> embed-sync]"

# name, cron (UTC), transform
upsert_schedule() {
  body="{\"argument\": \"{\\\"transform\\\": $3}\"}"
  if gcloud scheduler jobs describe "$1" --project="$PROJECT" --location="$REGION" >/dev/null 2>&1; then
    verb=update headers=--update-headers
  else
    verb=create headers=--headers
  fi
  gcloud scheduler jobs "$verb" http "$1" --project="$PROJECT" --location="$REGION" \
    --schedule="$2" --time-zone=Etc/UTC --uri="$EXEC_URI" --http-method=POST \
    --message-body="$body" "$headers=Content-Type=application/json" \
    --oauth-service-account-email="$SA"
}

# Full chain every 4h, so the agent sees new articles within ~4h (avg ~2h). Every 3h
# (~590 GB/month billed vs ~450) cuts the avg wait to ~1.6h; timing the runs to each
# source's peak hours gains only ~10 min (PROGRESS_LOG full history, 2026-10-19).
upsert_schedule news-pipeline-trigger "0 */4 * * *" true
