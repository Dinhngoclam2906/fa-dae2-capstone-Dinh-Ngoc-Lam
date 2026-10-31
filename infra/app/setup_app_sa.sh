#!/usr/bin/env sh
# One-time: the published app's own service account, read-only, so a busy demo can't touch
# the pipeline's data or starve its jobs. Idempotent (re-running only re-applies bindings).
# Usage: GCP_PROJECT=... sh infra/app/setup_app_sa.sh
#
#   BigQuery  jobUser on the project (run queries) + dataViewer on `marts` and `vectors` only
#             (obt_articles / the search view, article_bodies for the passage rescue).
#   Vertex    aiplatform.user (gemini-embedding-2 query embeddings).
#   Secrets   secretAccessor on the three secrets the app reads, each secret only.
# Secrets the app needs (create them first; values never go in this repo):
#   openai-app-key    the key of a separate OpenAI project with a hard monthly limit
#   pinecone-api-key  exists (shared with embed-sync, read-only use here)
#   app-password      the password gate (APP_PASSWORD)
set -eu

PROJECT="${GCP_PROJECT:?set GCP_PROJECT}"
NAME=news-agent-app
SA="${NAME}@${PROJECT}.iam.gserviceaccount.com"

gcloud iam service-accounts describe "$SA" --project="$PROJECT" >/dev/null 2>&1 ||
  gcloud iam service-accounts create "$NAME" --project="$PROJECT" \
    --display-name="Daily News AI Assistant (published app, read-only)"

for role in roles/bigquery.jobUser roles/aiplatform.user; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$SA" \
    --role="$role" --condition=None --quiet >/dev/null
done

for ds in marts vectors; do
  bq --project_id="$PROJECT" query --use_legacy_sql=false --quiet \
    "GRANT \`roles/bigquery.dataViewer\` ON SCHEMA \`${PROJECT}.${ds}\` TO \"serviceAccount:${SA}\""
done

for secret in openai-app-key pinecone-api-key app-password; do
  if gcloud secrets describe "$secret" --project="$PROJECT" >/dev/null 2>&1; then
    gcloud secrets add-iam-policy-binding "$secret" --project="$PROJECT" \
      --member="serviceAccount:$SA" --role=roles/secretmanager.secretAccessor --quiet >/dev/null
  else
    echo "secret $secret missing: create it, then re-run this script" >&2
  fi
done
echo "ok: $SA"
