# Daily cost report

`monitoring/cost_report.py` answers "what did each service cost yesterday, and this
month?" from **measured usage × list prices**, because Google's billing data lags by hours
and the trial credit hides it. Built 2026-10-23.

| Service | Measured from | Price (asia-southeast1, Cloud Billing Catalog API, 2026-10-23) |
|---|---|---|
| BigQuery queries | `INFORMATION_SCHEMA.JOBS_BY_PROJECT` bytes billed, split by who ran them (pipeline / local / CI) | first 1 TiB/month free, then $8.4375/TiB |
| BigQuery storage | table metadata (logical bytes, active vs long-term) | 10 GiB free each; $0.02 / $0.01 per GiB-month |
| Gemini embeddings | `vectors.article_embeddings.input_tokens` by `embedded_at` | $0.20 / M tokens |
| Cloud Run jobs | executions API duration × the job's vCPU / memory | $0.0000216 / vCPU-s, $0.0000024 / GiB-s (no free tier assumed) |
| Cloud Storage | Monitoring metric `storage/total_bytes` per bucket | Standard $0.02, Coldline $0.005 / GiB-month |
| Artifact Registry | repository sizes | 0.5 GiB free, then $0.10 / GiB-month |
| Pinecone | vector counts | Starter plan: $0 (2 GB cap; paid plan = $50/month minimum) |
| OpenAI | **billed** cost, Costs API (admin key, Secret Manager `openai-admin-key`) | as billed |

Free monthly allowances are applied in date order (a day is charged only once the month's
allowance is used). "Month at this day's rate" = the day × days in the month; month-to-date
also includes one-offs (the 10-18 embedding backfill ≈ $37). Not counted: Workflows,
Scheduler, Secret Manager, Cloud Build, Logging (free tiers), Gemini query embeddings and
one-off eval embeddings (e.g. the $1.23 chunk pilot); the billing export will show those.

```bash
uv run --env-file .env python monitoring/cost_report.py              # yesterday (UTC)
uv run --env-file .env python monitoring/cost_report.py --day 2026-10-22 --save
```

`--save` replaces that day's partition of `monitoring.cost_daily` (idempotent; load jobs are
free), for a dashboard later. `--email` sends the HTML report via Gmail SMTP.

First result (2026-10-22): **$0.29 for the day, ≈ $8.84/month at that rate**, inside the
$5–10 budget. Biggest lines: vn-ingestion Cloud Run (~$3/month), Gemini embeddings
(~$4/month), Artifact Registry old images (5 GiB ≈ $0.45/month, removable with a cleanup
policy). BigQuery queries were free (41% of the 1 TiB used by 10-22).

## Daily email (Cloud Run Job `cost-report`, 02:00 UTC = 09:00 ICT)

One-off setup, done by the owner (it grants IAM roles):

1. **Gmail app password** (needs 2-Step Verification on the Google account):
   myaccount.google.com/apppasswords → name it `cost-report` → copy the 16-character code.
2. Store it without it touching the repo or a chat: save it in Notepad as
   `C:\Users\DELL\smtp.txt`, then in your own PowerShell window:
   ```powershell
   gcloud secrets create smtp-app-password --replication-policy=automatic --data-file="C:\Users\DELL\smtp.txt" --project project-f5433681-7b47-481f-9c7
   Remove-Item "C:\Users\DELL\smtp.txt"
   ```
3. From the repo root in Git Bash: `GCP_PROJECT=project-f5433681-7b47-481f-9c7 bash monitoring/deploy_cost_report.sh`.
   It creates the read-only service account `cost-reporter` (roles listed at the top of the
   script), the image, the job and the 02:00 UTC schedule.
4. Test: `gcloud run jobs execute cost-report --region asia-southeast1 --project project-f5433681-7b47-481f-9c7 --wait`.

The report's own cost: ~0.1 GB of BigQuery and ~1 minute of Cloud Run a day (≈ $0.02/month).

## Guards and history (2026-10-23)

- **Budget** `news-pipeline monthly ($10)`: ₫260,000/month (the billing account is in VND;
  ≈ $10 at the trial's ₫26,100/$), project-scoped, **credits excluded** so it reacts to real
  usage during the trial. Emails the billing admins at 50%, 90%, 100% of actual spend and
  100% of forecast. A budget alerts; it never stops anything.
- **Failure alert**: `cost-report` added to the "Pipeline job failed" policy.
- **Billing export**: dataset `billing_export` (asia-southeast1) created. Turning the export
  on has no API: Console → Billing → Billing export → BigQuery export → *Standard usage
  cost* → Edit settings → project `project-f5433681-7b47-481f-9c7`, dataset
  `billing_export` → Save. It's the official record (credits included); the report stays
  the fast, per-action view.
- **Artifact Registry cleanup**: `monitoring/ar_cleanup_policy.json` (keep the 3
  newest versions and `latest`, delete the rest after 7 days). Apply per repo:
  `gcloud artifacts repositories set-cleanup-policies <repo> --location asia-southeast1 --policy monitoring/ar_cleanup_policy.json --no-dry-run`
  for `guardian-ingestion`, `dbt-build`, `embed-sync`, `cost-report`.
- **History backfill**: `monitoring.cost_daily` was filled for 2026-10-01 → 10-22 by running
  the report per day. Usage-based rows are exact; storage and Pinecone rows for past days
  use the sizes on 10-23 (pennies of error).
