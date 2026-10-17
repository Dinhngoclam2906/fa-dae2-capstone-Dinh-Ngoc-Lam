# capstone_crawling_ingestion.py
"""
Guardian Ingestion DAG

Incremental ("real-time enough") Guardian Content API crawl -> BigQuery -> trigger dbt.

Replaces the old Kafka producer/consumer real-time path (kept in
scripts/real_time_data/ but not scheduled here - see PROGRESS_LOG.md's Kafka
parking-lot entry for the full keep-vs-remove discussion, still open). Each run of
guardian_api_backfill.py resumes from its own saved cursor up to today, so scheduling
this frequently gives near-real-time freshness without a message broker - news
articles don't need sub-minute delivery, and there's only ever been one consumer of
the data anyway.

The one-time historical HF-archive load (hf_archive_to_parquet.py +
load_raw_to_bigquery.py --source hf) already ran manually during Phase 1 and does NOT
belong in a recurring schedule - the archive is static, so a repeat run would find
nothing new. If a deeper historical backfill is ever needed, run it manually.
"""
import os
from datetime import timedelta

import pendulum  # type: ignore
from airflow.sdk import dag, task  # type: ignore
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore


def get_ingestion_env_vars():
    """Env vars for the Guardian API crawler + BigQuery loader.

    NOTE (same gap as capstone_dbt_orchestration.py's get_dbt_env_vars): this
    container has no GCP service-account key (org policy blocks creation), so
    `load_raw_to_bigquery.py`'s BigQuery/GCS clients need *something* mounted for
    Application Default Credentials to resolve - Workload Identity Federation, or a
    mounted ADC token. Locally (outside Docker) ADC already works via
    `gcloud auth application-default login` - see PROGRESS_LOG.md Phase 0. This DAG
    is correct to define now; it needs that auth solved before a real run in Airflow
    will actually reach BigQuery.
    """
    return {
        "API_KEY": os.environ.get("API_KEY", ""),
        "API_URL": os.environ.get("API_URL", "https://content.guardianapis.com"),
        "GCP_PROJECT": os.environ["GCP_PROJECT"],  # required - fail fast, no fallback
        "BQ_LOCATION": os.environ.get("BQ_LOCATION", "asia-southeast1"),
        "BQ_DATASET_RAW": os.environ.get("BQ_DATASET_RAW", "raw"),
        "GCS_LANDING_BUCKET": os.environ.get("GCS_LANDING_BUCKET", ""),
        "GCS_LANDING_PREFIX": os.environ.get("GCS_LANDING_PREFIX", "guardian"),
        "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        "PATH": "/home/airflow/.local/bin:" + os.environ.get("PATH", ""),
    }


@dag(
    dag_id="guardian_ingestion",
    # Every 4 hours (6 runs/day), not @hourly: the crawl itself is cheap, but each run
    # also MERGEs into raw.raw_data, and BigQuery bills by bytes scanned even for a
    # MERGE that only affects a handful of rows - this project runs under a hard
    # $5-10/mo cap (see PROGRESS_LOG.md Phase 4), so 24 merges/day isn't worth it for
    # a news feed. Tune freely if that tradeoff doesn't hold.
    schedule="0 */4 * * *",
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    catchup=False,
    tags=["guardian", "ingestion", "bigquery", "capstone"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    max_active_runs=1,
    description="Incremental Guardian Content API crawl -> BigQuery -> trigger dbt (no Kafka)",
)
def guardian_ingestion():

    @task.bash(env=get_ingestion_env_vars(), append_env=True)
    def crawl_guardian_incremental():
        """Resumable crawl from the last saved cursor (data/batch/guardian_api_state.json)
        up to today. Safe to run frequently - it's a no-op once caught up.

        --max-calls is capped well below the script's own 450 default: that default
        assumes a single manual catch-up run against the 500-calls/day dev-key quota
        (see PROGRESS_LOG.md's Phase 1 incident), but this DAG runs 6x/day -
        60 calls/run x 6 = 360, safely under quota with margin for retries."""
        return """
        cd /opt/airflow/project
        python scripts/batch_data/guardian_api_backfill.py --max-calls 60
        """

    @task.bash(env=get_ingestion_env_vars(), append_env=True)
    def load_to_bigquery():
        """Upload any newly-crawled Parquet to GCS and MERGE into raw.raw_data.

        load_raw_to_bigquery.py now stages+merges only files pending from this run
        (tracked in data/batch/.pending_merge.<source>.json, cleared on a successful
        merge) instead of the source's entire accumulated Parquet history - a normal
        incremental run bills tens of MB, not multiple GB (confirmed: a 96-article
        test run billed 37 MB, vs. 2.58 GB for the same scale before this fix). 6 GB
        is just a generous ceiling for an unusual multi-day catch-up batch, not the
        expected cost - it should basically never actually bind now."""
        return """
        cd /opt/airflow/project
        python scripts/batch_data/load_raw_to_bigquery.py --source api --max-bytes-billed 6000000000
        """

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_orchestration",
        trigger_dag_id="capstone_dbt_orchestration",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    crawl_guardian_incremental() >> load_to_bigquery() >> trigger_dbt  # type: ignore


# Instantiate DAG
guardian_ingestion()
