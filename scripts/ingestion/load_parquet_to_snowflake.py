import snowflake.connector
from dotenv import load_dotenv
import os
import time
import polars as pl
from pathlib import Path
import logging

# Configure logging for better consistency
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_snowflake_connection():
    """Get Snowflake connection (shared helper)."""
    # load_dotenv() moved to main() to load only once
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        authenticator="SNOWFLAKE_JWT",
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        timezone="Asia/Ho_Chi_Minh"
    )

# CHANGED: Removed get_next_id_from_sf() - no longer needed without surrogate id

def setup_schema_and_stage(cursor, database, schema):
    """Shared setup for schema, stage, and context (avoids duplication)."""
    # Confirm session context (safe unpack with defaults)
    cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()")
    result = cursor.fetchone()
    db = result[0] if result else os.getenv("SNOWFLAKE_DATABASE", "UNKNOWN")
    sch = result[1] if result else os.getenv("SNOWFLAKE_SCHEMA", "UNKNOWN")
    role = result[2] if result else "UNKNOWN"
    logger.info(f"Current context: Database={db}, Schema={sch}, Role={role}")

    # NEW: Drop schema with CASCADE to clear all tables/views/other objects
    drop_schema_cmd = f"DROP SCHEMA IF EXISTS {database}.{schema} CASCADE"
    cursor.execute(drop_schema_cmd)
    logger.info(f"✅ Dropped schema {database}.{schema} with CASCADE (cleared all objects: tables, views, etc.)")

    # Create schema if not exists
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
    logger.info(f"✅ Schema {database}.{schema} created or already exists")

    # Set schema explicitly
    cursor.execute(f"USE SCHEMA {database}.{schema}")

    # Create stage if not exists
    cursor.execute(f"CREATE STAGE IF NOT EXISTS {database}.{schema}.CSV_STAGE")
    logger.info(f"✅ Stage {database}.{schema}.CSV_STAGE created or already exists")

def ingest_parquet_to_snowflake(parquet_file_path):
    """Full ingestion: Clear table, upload Parquet to stage, then load to table (single connection)."""
    conn = None
    try:
        # Single connection init
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        database = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")
        stage_name = f"@{database}.{schema}.CSV_STAGE"
        table_name = f"{database}.{schema}.raw_data"
        
        # Setup schema and stage (now includes CASCADE drop)
        setup_schema_and_stage(cursor, database, schema)

        # Clear ALL files
        cursor.execute(f"REMOVE {stage_name}")
        logger.info("✅ Cleared ALL files from stage")

        # Dynamic parallelism based on file size
        total_size_mb = os.path.getsize(parquet_file_path) / (1024 * 1024)
        parallel_threads = 1
        logger.info(f"🚀 Uploading {total_size_mb:.1f}MB Parquet directly (PARALLEL={parallel_threads})")

        start_time = time.time()
        put_command = f"PUT file://{parquet_file_path} {stage_name}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE PARALLEL={parallel_threads}"
        cursor.execute(put_command)

        upload_time = time.time() - start_time
        upload_speed = total_size_mb / upload_time if upload_time > 0 else 0
        logger.info(f"✅ Single-file parallel upload completed in {upload_time:.1f}s ({upload_speed:.1f} MB/s)")

        # Verify stage
        cursor.execute(f"LIST {stage_name}")
        stage_contents = cursor.fetchall()
        logger.info("Stage contents: %s", [(row[0], row[1], row[2]) for row in stage_contents])

        # CHANGED: Create table without 'id'; article_id as PK (no drop needed now, since schema dropped)
        create_table_command = f"""
        CREATE TABLE {table_name} (
            article_id VARCHAR(2000) PRIMARY KEY,
            crawl_timestamp TIMESTAMP,
            web_publication_date TIMESTAMP,
            web_title STRING,
            body_text STRING,
            web_url VARCHAR(500),
            section_name VARCHAR(255),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """
        cursor.execute(create_table_command)
        logger.info(f"✅ Table {table_name} created (aligned to PostgreSQL schema)")

        # CHANGED: Load data without 'id' (omit from SELECT/INSERT)
        load_start_time = time.time()
        copy_command = f"""
        COPY INTO {table_name} (
            article_id, crawl_timestamp, web_publication_date, web_title,
            body_text, web_url, section_name, loaded_at
        )
        FROM (
            SELECT
                $1:article_id::STRING AS article_id,
                TRY_TO_TIMESTAMP_NTZ($1:crawl_timestamp::STRING) AS crawl_timestamp,
                TRY_TO_TIMESTAMP_NTZ($1:web_publication_date::STRING) AS web_publication_date,
                $1:web_title::STRING AS web_title,
                $1:body_text::STRING AS body_text,
                $1:web_url::STRING AS web_url,
                $1:section_name::STRING AS section_name,
                CURRENT_TIMESTAMP() AS loaded_at
            FROM {stage_name}/
        )
        FILE_FORMAT = (TYPE = 'PARQUET')
        ON_ERROR = 'CONTINUE'
        PURGE = TRUE
        """

        logger.debug("Executing COPY command:\n" + copy_command)  # Debug only for security
        cursor.execute(copy_command)
        load_time = time.time() - load_start_time
        logger.info(f"🚀 Data load completed in {load_time:.1f}s")

        # Get loading results (fallback to COUNT if no result)
        result = cursor.fetchall()
        if result:
            for row in result:
                logger.info(f"✅ Data loaded: {row[0]} rows to {table_name}")
        else:
            # Fallback verification (safe fetchone)
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count_result = cursor.fetchone()
            row_count = count_result[0] if count_result else 0
            logger.info(f"✅ Data loaded: {row_count} rows to {table_name}")

        # Verify loaded data (enhanced null checks)
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_result = cursor.fetchone()
        row_count = count_result[0] if count_result else 0
        logger.info(f"✅ Table contains {row_count} rows")

        cursor.execute(f"""
        SELECT 
            COUNT(*) AS total_rows,
            COUNT(CASE WHEN crawl_timestamp IS NULL THEN 1 END) AS null_crawl_timestamps,
            COUNT(CASE WHEN web_publication_date IS NULL THEN 1 END) AS null_pub_dates
        FROM {table_name}
        """)
        verification_result = cursor.fetchone()
        if verification_result:
            logger.info(f"Data quality check: {verification_result[0]} total, {verification_result[1]} null crawl_ts, {verification_result[2]} null pub dates")
        else:
            logger.warning("No verification results—table may be empty")

        # Count distinct article_ids (safe)
        cursor.execute(f"SELECT COUNT(DISTINCT article_id) AS distinct_articles FROM {table_name}")
        distinct_result = cursor.fetchone()
        distinct_count = distinct_result[0] if distinct_result else 0
        logger.info(f"✅ Distinct article_ids: {distinct_count}")
        
        return True

    except snowflake.connector.errors.ProgrammingError as e:
        logger.error(f"❌ Snowflake Programming Error: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False
    finally:
        if conn:
            conn.close()
            logger.info("✅ Snowflake connection closed")

def main(parquet_file_path=None):
    """Main execution logic."""
    # Load env once
    load_dotenv()

    # Full script timer start
    overall_start = time.perf_counter()
    logger.info("⏱️ Full script timer started")

    if parquet_file_path is None:
        parquet_file_path = r"data\batch\guardian_historical_articles.parquet"
    
    # CHANGED: No start_id needed
    success = ingest_parquet_to_snowflake(parquet_file_path)
    if not success:
        logger.error("❌ Ingestion failed—check logs above.")
        return False
    
    # Full script timer end
    overall_end = time.perf_counter()
    overall_time = overall_end - overall_start
    logger.info(f"⏱️ Full script completed in {overall_time:.1f}s")

    return True

if __name__ == "__main__":
    main()