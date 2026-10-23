---
name: check-pipeline
description: Check the news-pipeline's health by rows, not exit codes - workflow runs, each Cloud Run job's last execution, raw rows per source, OBT rows without a vector, Pinecone vs OBT. Use at the start of a session, when the user says jobs are failing, or after any deploy/image rebuild.
---

# Check the pipeline

A green exit code proves nothing here: a "nothing to do" run exits 0 and once hid a week of
missing data. Always judge by rows.

## 1. Run the check (read-only, ~35 s, ~0.1 GB of BigQuery)

From the repo root, with the Bash tool:

```bash
uv run --env-file .env python .claude/skills/check-pipeline/check.py --runs 6
```

## 2. Read the result

Healthy looks like:
- the newest workflow runs are `SUCCEEDED`, every 4 h (00/04/08/12/16/20 UTC);
- all 4 jobs' last execution `OK`;
- every one of the 10 sources has rows in the last 24 h and `newest` within ~4 h of now
  (the small English editions, `vneconomy_en` / `tuoitre_en`, can have quiet days; a big
  Vietnamese source or `guardian` at 0 is a real problem);
- `without a current vector: 0` and Pinecone `+0 vs OBT` (a small positive gap right after
  an embed-sync is fine until the next run; a growing gap means sync is failing).

Older FAILED runs may be incidents that are already fixed: check `PROGRESS_LOG.md` before
alarming the user.

## 3. If something failed, find the error

```bash
P=$GCP_PROJECT  # or read it from .env
gcloud run jobs executions list --job <job> --region asia-southeast1 --project $P --limit 3
gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="<job>" AND severity>=ERROR' \
  --project $P --limit 30 --freshness 1d --format="value(timestamp,textPayload)"
```

Use the Bash tool for `gcloud logging` filters: PowerShell mangles the quoting.

Known causes, in `docs/lessons_learned.md` (search it before debugging from scratch):
- `bytesBilledLimitExceeded` in embed-sync: a query lost partition pruning (a self-reference
  next to the embedding column).
- dbt-build duplicate `article_id`: a re-dated article (`delete_moved_articles` post-hook).
- A single vn-ingestion failure on a sitemap 5xx: external, usually recovers next run.

## Rules

- Never run `sync_vector_index.py sync` while an embed is running.
- Fixes to production (deletes, IAM, index switches) need the user's go-ahead.
- Record every incident in `docs/lessons_learned.md` and log it with the `log-update` skill.
