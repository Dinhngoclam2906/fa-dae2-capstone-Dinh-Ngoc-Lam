#!/usr/bin/env bash
# Deploy the daily cost email: a Cloud Run Job `cost-report` (monitoring/cost_report.py
# --save --email) run every day at 02:00 UTC (09:00 ICT) by Cloud Scheduler.
#
#   GCP_PROJECT=... bash monitoring/deploy_cost_report.sh      (from the repo root, Git Bash)
#
# Needs the secret `smtp-app-password` first (a Gmail app password, see docs/cost_report.md).
# Re-runnable: every step skips or updates what already exists.
#
# What it creates / grants (all for the new service account cost-reporter, nothing else):
#   project roles, read-only: bigquery.jobUser, bigquery.resourceViewer (INFORMATION_SCHEMA.JOBS),
#     bigquery.metadataViewer + bigquery.dataViewer (table sizes, embedding token counts),
#     run.viewer (job executions), monitoring.viewer (bucket sizes), artifactregistry.reader
#   dataset `monitoring`: bigquery.dataEditor (writes monitoring.cost_daily)
#   secrets: secretAccessor on openai-admin-key, pinecone-api-key, smtp-app-password
#   job `cost-report`: run.invoker (so the scheduler, acting as the same account, can start it)
set -euo pipefail

P="${GCP_PROJECT:?set GCP_PROJECT}"
R=asia-southeast1
SA="cost-reporter@$P.iam.gserviceaccount.com"
IMAGE="$R-docker.pkg.dev/$P/cost-report/cost-report:latest"
EMAIL="${REPORT_EMAIL:-lamdinhngoc76@gmail.com}"

gcloud secrets describe smtp-app-password --project "$P" >/dev/null \
  || { echo "Create the secret smtp-app-password first (docs/cost_report.md)"; exit 1; }

echo "== service account"
gcloud iam service-accounts describe "$SA" --project "$P" >/dev/null 2>&1 \
  || gcloud iam service-accounts create cost-reporter --project "$P" \
       --display-name "Daily cost report (read-only + monitoring.cost_daily)"

echo "== project roles (read-only)"
for role in bigquery.jobUser bigquery.resourceViewer bigquery.metadataViewer bigquery.dataViewer \
            run.viewer monitoring.viewer artifactregistry.reader; do
  gcloud projects add-iam-policy-binding "$P" --member "serviceAccount:$SA" --role "roles/$role" \
    --condition=None --quiet >/dev/null && echo "  roles/$role"
done

echo "== dataset monitoring: dataEditor"
uv run python - "$P" "$SA" <<'EOF'
import sys
from google.cloud import bigquery
project, sa = sys.argv[1], sys.argv[2]
c = bigquery.Client(project=project)
ds = c.get_dataset(f"{project}.monitoring")
entry = bigquery.AccessEntry("WRITER", "userByEmail", sa)
if not any(e.entity_id == sa and e.role == "WRITER" for e in ds.access_entries):
    ds.access_entries = [*ds.access_entries, entry]
    c.update_dataset(ds, ["access_entries"])
print("  ok")
EOF

echo "== secrets"
for s in openai-admin-key pinecone-api-key smtp-app-password; do
  gcloud secrets add-iam-policy-binding "$s" --project "$P" --member "serviceAccount:$SA" \
    --role roles/secretmanager.secretAccessor >/dev/null && echo "  $s"
done

echo "== image (minimal build context)"
gcloud artifacts repositories describe cost-report --location "$R" --project "$P" >/dev/null 2>&1 \
  || gcloud artifacts repositories create cost-report --repository-format docker --location "$R" --project "$P"
CTX="$(mktemp -d)"
trap 'rm -rf "$CTX"' EXIT
cp --parents infra/docker/Dockerfile.cost monitoring/requirements-cost.txt monitoring/cost_report.py "$CTX"/
cat > "$CTX/cloudbuild.yaml" <<EOF
steps:
- name: gcr.io/cloud-builders/docker
  args: [build, -f, infra/docker/Dockerfile.cost, -t, $IMAGE, .]
images: [$IMAGE]
EOF
CTXP="$CTX"; command -v cygpath >/dev/null && CTXP="$(cygpath -w "$CTX")"  # Windows gcloud under Git Bash
gcloud builds submit "$CTXP" --config "$CTXP/cloudbuild.yaml" --project "$P"

echo "== job cost-report"
ENV="GCP_PROJECT=$P,SMTP_USER=$EMAIL,REPORT_TO=$EMAIL"
SECRETS="OPENAI_ADMIN_KEY=openai-admin-key:latest,PINECONE_API_KEY=pinecone-api-key:latest,SMTP_PASSWORD=smtp-app-password:latest"
if gcloud run jobs describe cost-report --region "$R" --project "$P" >/dev/null 2>&1; then
  gcloud run jobs update cost-report --image "$IMAGE" --region "$R" --project "$P" \
    --set-env-vars "$ENV" --set-secrets "$SECRETS"
else
  gcloud run jobs create cost-report --image "$IMAGE" --region "$R" --project "$P" \
    --service-account "$SA" --cpu 1 --memory 512Mi --task-timeout 10m --max-retries 1 \
    --set-env-vars "$ENV" --set-secrets "$SECRETS"
fi
gcloud run jobs add-iam-policy-binding cost-report --region "$R" --project "$P" \
  --member "serviceAccount:$SA" --role roles/run.invoker >/dev/null

echo "== schedule 02:00 UTC daily"
URI="https://$R-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$P/jobs/cost-report:run"
if gcloud scheduler jobs describe cost-report-trigger --location "$R" --project "$P" >/dev/null 2>&1; then
  gcloud scheduler jobs update http cost-report-trigger --location "$R" --project "$P" \
    --schedule "0 2 * * *" --time-zone Etc/UTC --uri "$URI" --http-method POST \
    --oauth-service-account-email "$SA"
else
  gcloud scheduler jobs create http cost-report-trigger --location "$R" --project "$P" \
    --schedule "0 2 * * *" --time-zone Etc/UTC --uri "$URI" --http-method POST \
    --oauth-service-account-email "$SA"
fi

echo "Done. Test now: gcloud run jobs execute cost-report --region $R --project $P --wait"
