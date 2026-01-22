# capstone_dbt_orchestration.py
"""
Capstone dbt Orchestration DAG - Final Working Version for Your Environment
"""
import os
from datetime import timedelta

import pendulum # type: ignore
from airflow.decorators import dag, task # type: ignore


def get_dbt_env_vars():
    """Provide all required env vars for dbt to connect via profiles.yml"""
    return {
        "DBT_PROJECT_DIR": "/opt/airflow/capstone_dbt_project",
        "DBT_PROFILES_DIR": "/opt/airflow/.dbt",
        "SNOWFLAKE_ACCOUNT": os.environ.get("SNOWFLAKE_ACCOUNT", "GEZJPYC-FOUNDRY_AI_ACADEMY"),
        "SNOWFLAKE_USER": os.environ.get("SNOWFLAKE_USER", "T26"),
        "SNOWFLAKE_ROLE": os.environ.get("SNOWFLAKE_ROLE", "RL_T26"),
        "SNOWFLAKE_WAREHOUSE": os.environ.get("SNOWFLAKE_WAREHOUSE", "WH_T26"),
        "SNOWFLAKE_DATABASE": os.environ.get("SNOWFLAKE_DATABASE", "DB_T26"),
        "SNOWFLAKE_SCHEMA": os.environ.get("SNOWFLAKE_SCHEMA", "SC_T26"),  
        "SNOWFLAKE_PRIVATE_KEY_PATH": "/opt/airflow/.secret/rsa_key.p8",
        "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": "",  
        # Ensure dbt is in PATH
        "PATH": "/home/airflow/.local/bin:" + os.environ.get("PATH", "")
    }


@dag(
    dag_id="capstone_dbt_orchestration",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["capstone", "dbt", "snowflake"],
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
        echo "Private key path: $SNOWFLAKE_PRIVATE_KEY_PATH"
        ls -la $SNOWFLAKE_PRIVATE_KEY_PATH || echo "Key not found!"
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