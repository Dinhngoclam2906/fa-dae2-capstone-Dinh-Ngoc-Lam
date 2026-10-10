# capstone_dbt_orchestration.py
"""
Capstone dbt Orchestration DAG - Final Working Version for Your Environment
"""
import os
from datetime import timedelta

import pendulum # type: ignore
from airflow.decorators import dag, task # type: ignore


def get_dbt_env_vars():
    """Provide all required env vars for dbt to connect via profiles.yml.

    NOTE (BigQuery port, Phase 3): container-level auth is NOT wired up yet - this
    repo has no GCP service-account key (org policy `iam.disableServiceAccountKeyCreation`
    blocks creation; see docs/phase0_baseline.md §7), so dbt's `method: oauth` here
    still needs *something* mounted into the container for ADC to resolve
    (GOOGLE_APPLICATION_CREDENTIALS -> a mounted token, or Workload Identity Federation).
    That's the same auth gap already called out as deferred-to-Phase-5 for CI; running
    this DAG for real needs it solved first. Locally (outside Docker), ADC already
    works via `gcloud auth application-default login` - see PROGRESS_LOG.md Phase 0.
    """
    return {
        "DBT_PROJECT_DIR": "/opt/airflow/capstone_dbt_project",
        "DBT_PROFILES_DIR": "/opt/airflow/.dbt",
        "GCP_PROJECT": os.environ.get("GCP_PROJECT", "project-f5433681-7b47-481f-9c7"),
        "BQ_LOCATION": os.environ.get("BQ_LOCATION", "asia-southeast1"),
        "BQ_DATASET_DEFAULT": os.environ.get("BQ_DATASET_DEFAULT", "scratch"),
        # Unresolved - see docstring above.
        "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        # Ensure dbt is in PATH
        "PATH": "/home/airflow/.local/bin:" + os.environ.get("PATH", "")
    }


@dag(
    dag_id="capstone_dbt_orchestration",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["capstone", "dbt", "bigquery"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=1,
    description="Production-ready dbt orchestration using 1 task = 1 pipeline",
)
def capstone_dbt_orchestration():
    
    @task.bash(env=get_dbt_env_vars(), append_env=True)
    def dbt_debug():
        return """
        echo "=== Environment Check ==="
        echo "Project dir: $DBT_PROJECT_DIR"
        echo "Profiles dir: $DBT_PROFILES_DIR"
        echo "GCP project: $GCP_PROJECT"
        cd $DBT_PROJECT_DIR && dbt debug
        """

    @task.bash(env=get_dbt_env_vars(), append_env=True)
    def dbt_clean():
        return """
        echo "=== Cleaning dbt artifacts ==="
        cd $DBT_PROJECT_DIR
        dbt clean
        echo "✓ Cache cleared successfully"
        """

    @task.bash(env=get_dbt_env_vars(), append_env=True)
    def dbt_build():
        return """
        echo "=== Running dbt build (run + test) ==="
        cd $DBT_PROJECT_DIR
        dbt build --fail-fast
        echo "=== Build completed successfully ==="
        """

    # Task flow
    debug = dbt_debug()
    clean = dbt_clean()
    build = dbt_build()
    
    debug >> clean >> build # type: ignore


# Instantiate DAG
capstone_dbt_orchestration()