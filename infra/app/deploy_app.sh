#!/usr/bin/env sh
# Build and deploy the published app (Cloud Run service `news-agent-app`). Idempotent.
# Usage (repo root): GCP_PROJECT=... sh infra/app/deploy_app.sh [build|deploy|all]   (default all)
# Prerequisite: infra/app/setup_app_sa.sh (service account + the three secrets).
#
# Settings, and why:
#   min 0 / max 1 instance   scale to zero when idle; ONE instance because the chat history
#                            and the daily counters (core/guards.py) live in its memory
#   session affinity         a browser's websocket stays on the same instance
#   timeout 3600 s           Streamlit holds a websocket per open tab
#   2 GiB                    Streamlit + LangGraph + FlashRank (demo mode)
#   --allow-unauthenticated  anyone can reach the URL; APP_PASSWORD gates the page
set -eu

PROJECT="${GCP_PROJECT:?set GCP_PROJECT}"
REGION=asia-southeast1
SERVICE=news-agent-app
SA="${SERVICE}@${PROJECT}.iam.gserviceaccount.com"
REPO="${REGION}-docker.pkg.dev/${PROJECT}/${SERVICE}"
TAG="$(git rev-parse --short HEAD)"
IMAGE="${REPO}/${SERVICE}"
STEP="${1:-all}"

build() {
  gcloud artifacts repositories describe "$SERVICE" --location="$REGION" --project="$PROJECT" >/dev/null 2>&1 ||
    gcloud artifacts repositories create "$SERVICE" --repository-format=docker \
      --location="$REGION" --project="$PROJECT"
  # Minimal context (the repo root has no .gcloudignore): only what Dockerfile.app COPYs.
  CTX="$(mktemp -d)"
  trap 'rm -rf "$CTX"' EXIT
  mkdir -p "$CTX/infra/docker" "$CTX/agent/core"
  cp infra/docker/Dockerfile.app "$CTX/infra/docker/"
  cp agent/__init__.py agent/app.py agent/requirements-app.txt "$CTX/agent/"
  cp agent/core/*.py "$CTX/agent/core/"
  cat > "$CTX/cloudbuild.yaml" <<EOF
steps:
- name: gcr.io/cloud-builders/docker
  args: [build, -f, infra/docker/Dockerfile.app, -t, ${IMAGE}:${TAG}, -t, ${IMAGE}:latest, .]
images: [${IMAGE}:${TAG}, ${IMAGE}:latest]
EOF
  gcloud builds submit "$CTX" --config "$CTX/cloudbuild.yaml" --project="$PROJECT"
}

deploy() {
  gcloud run deploy "$SERVICE" --project="$PROJECT" --region="$REGION" \
    --image="${IMAGE}:${TAG}" --service-account="$SA" \
    --min-instances=0 --max-instances=1 --concurrency=80 --cpu=1 --memory=2Gi \
    --timeout=3600 --session-affinity --cpu-boost --allow-unauthenticated \
    --set-env-vars="GCP_PROJECT=${PROJECT},AGENT_DAILY_QUESTION_LIMIT=100,AGENT_RATE_LIMIT_PER_HOUR=20,AGENT_MAX_BYTES_PER_DAY=10000000000" \
    --set-secrets="OPENAI_API_KEY=openai-app-key:latest,PINECONE_API_KEY=pinecone-api-key:latest,APP_PASSWORD=app-password:latest"
}

case "$STEP" in
  build) build ;;
  deploy) deploy ;;
  all) build && deploy ;;
  *) echo "usage: $0 [build|deploy|all]" >&2; exit 2 ;;
esac
