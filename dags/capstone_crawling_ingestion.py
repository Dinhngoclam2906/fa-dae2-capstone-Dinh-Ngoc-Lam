# capstone_crawling_ingestion.py
"""
Historical Ingestion DAG
Handles:
- Historical load (HuggingFace dataset) → direct MERGE to Snowflake (manual/one-time, SAFE)
"""
from datetime import timedelta
import pendulum # type: ignore
from airflow.sdk import dag, task  # type: ignore
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore


@dag(
    dag_id="guardian_ingestion",
    schedule="@daily",  # Adjust to @hourly if you want more frequent crawls
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    catchup=False,
    tags=["guardian", "kafka", "ingestion", "capstone"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    max_active_runs=1,
    description="Daily crawl of new Guardian articles → Kafka → PG → Snowflake → trigger dbt",
)
def guardian_ingestion():
    
    # @task.bash
    # def crawl_real_time_articles():
    #     """
    #     Run the real-time Guardian API crawler (your real_time_data_collector.py)
    #     """
    #     script_path = "/opt/airflow/project/scripts/data_collection/real_time_data_collector.py"
    #     return f"""
    #     cd /opt/airflow/project
    #     python {script_path}
    #     # Optional: pass --from-date {{ yesterday_ds }} if you want stricter incremental
    #     """

    # @task.bash
    # def produce_to_kafka():
    #     """
    #     Send only new articles (based on crawl_timestamp > last in PG) to Kafka
    #     """
    #     script_path = "/opt/airflow/project/scripts/ingestion/load_parquet_to_postgre_then_snowflake.py"
    #     return f"""
    #     cd /opt/airflow/project
    #     python {script_path} --mode producer
    #     """

    # @task.bash
    # def consume_from_kafka():
    #     """
    #     Process all messages in Kafka topic → UPSERT to PostgreSQL → sync to Snowflake
    #     """
    #     script_path = "/opt/airflow/project/scripts/ingestion/load_parquet_to_postgre_then_snowflake.py"
    #     return f"""
    #     cd /opt/airflow/project
    #     python {script_path} --mode consumer
    #     """

    # NEW: Safe, incremental historical load (manual trigger only)
    @task.bash
    # (task_id="load_historical_batch")
    def load_historical_data():
        """
        Generate historical batch (if needed) and MERGE safely into Snowflake raw_data.
        Preserves all existing data. Run manually when needed.
        """
        batch_script = "/opt/airflow/project/scripts/batch_data/batch_data_collector.py"
        load_script = "/opt/airflow/project/scripts/batch_data/batch_data_ingestion.py"
        return f"""
        cd /opt/airflow/project
        # Delete old parquet to force new random sample
        rm -f /opt/airflow/project/data/batch/guardian_historical_articles.parquet
        echo "=== Generating/preprocessing historical batch data (500 articles) ==="
        python {batch_script}
        echo "=== MERGING historical batch into Snowflake raw_data (safe upsert) ==="
        python {load_script}
        """

    # Daily real-time flow
    # crawl = crawl_real_time_articles()
    # produce = produce_to_kafka()
    # consume = consume_from_kafka()

    # crawl >> produce >> consume # type: ignore

    # Historical load task (manual trigger only, not part of daily workflow)
    # load_historical_data()

    # Trigger dbt after real-time load
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_orchestration",
        trigger_dag_id="capstone_dbt_orchestration",
        # logical_date="{{ ds }}",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    load_historical_data() >> trigger_dbt # type: ignore

# Instantiate DAG
guardian_ingestion()