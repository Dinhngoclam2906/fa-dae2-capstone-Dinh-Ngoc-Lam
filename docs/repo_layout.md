# Repo layout: old → new paths

The repo was regrouped by pipeline component on 2026-10-24 (Phase A `c195d5b`, Phase B:
plan in `docs/plan_phase_b.md`). Historical reports in `docs/` (phase reports, eval
reports, `lessons_learned.md`, decisions) keep the paths that were true when they were
written; use this table to find a file today. Files were moved with `git mv`, so
`git log --follow <new path>` shows their full history.

| Before | Now |
|---|---|
| `scripts/batch_data/` | `ingestion/` |
| `scripts/batch_data/batch_data_collector.py`, `batch_data_ingestion.py` | `ingestion/legacy/` |
| `scripts/real_time_data/` | `ingestion/streaming/` |
| `scripts/monitoring/` | `monitoring/` |
| `capstone_dbt_project/` | `transform/` (the dbt project and profile are still named `capstone_dbt_project`) |
| `ai_agents/` | `agent/` |
| `ai_agents/embed_articles.py`, `sync_vector_index.py`, `run_embed_job.sh`, `requirements-embed.txt` | `agent/jobs/` |
| `ai_agents/evaluation.py`, `baseline_systems.py`, `label_ground_truth*.py`, `visualize_results.py`, `capstone_tool_calling_agent.py` | `agent/legacy/eval_2026_01/` |
| `workflows/` | `orchestration/workflows/` |
| `dags/` | `orchestration/airflow/dags/` |
| `Dockerfile.ingestion`, `Dockerfile.embed`, `Dockerfile.cost` | `infra/docker/` |
| `docker-compose*.yml`, `Dockerfile.airflow`, `setup_airflow_*.sh` (briefly `deploy/local/`) | `infra/local/` |
| `sql/init.sql` | `infra/local/sql/init.sql` |
| `images/` | `docs/images/` |

Removed on 2026-10-24: `ai_agents/services/`, `core/uploads.py`, the upload scripts and
tests (PDF uploads dropped), `data_ingestion_to_pinecone.py`, `count_pinecone_articles.py`,
`pinecone_ingestion_log_manual.py`, `diagnostic.py`, `update_tracking_table_manual.py`.

**Unchanged on purpose:** `data/` (the crawlers' state paths are relative to the working
directory), container paths (`/app` in the Cloud Run images, `/opt/airflow/...` in local
Airflow), Cloud Run job names, the Workflows DAG name, GCS / BigQuery / Pinecone names.
