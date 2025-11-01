import os
import sys
from pathlib import Path
import psycopg
from psycopg import sql  # Added for safe SQL composition
import snowflake.connector
from dotenv import load_dotenv
import pandas as pd
import logging
from datetime import datetime, timedelta  # Added timedelta
from tenacity import retry, stop_after_attempt, wait_exponential
import subprocess
import argparse
import warnings
import time

# Suppress pandas warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Global connections (lazy-init)
_pg_conn = None
_sf_conn = None

# --- Connection Helpers ---

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_pg_connection():
    """Get PostgreSQL connection using env vars."""
    global _pg_conn
    params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),  # Fixed: Cast to int
        "dbname": os.getenv("POSTGRES_DB", "staging_db"),
        "user": os.getenv("POSTGRES_USER", "staging_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "staging_password"),
    }
    return psycopg.connect(**params)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_sf_connection():
    """Get Snowflake connection using env vars."""
    global _sf_conn
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

# --- Docker & ID Functions ---

def run_docker_command(command_parts, cwd=None):
    """Run a docker-compose command and log output."""
    try:
        result = subprocess.run(command_parts, cwd=cwd, capture_output=True, text=True, check=True)
        logger.info(f"✅ Docker command succeeded: {' '.join(command_parts)}")
        if result.stdout:
            logger.debug(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Docker command failed: {' '.join(command_parts)}\nStdout: {e.stdout}\nStderr: {e.stderr}")
        return False

def reset_docker(docker_compose_path='docker-compose.yml'):
    """Full reset: down with volume removal."""
    logger.info("🔄 Resetting Docker Compose (stopping and removing volumes)...")
    if not run_docker_command(['docker-compose', 'down', '-v'], cwd=Path(docker_compose_path).parent):
        return False
    logger.info("✅ Docker reset complete.")
    return True

def start_docker(docker_compose_path='docker-compose.yml'):
    """Start Docker Compose services in detached mode and poll readiness."""
    logger.info("🚀 Starting Docker Compose services...")
    if not run_docker_command(['docker-compose', 'up', '-d'], cwd=Path(docker_compose_path).parent):
        return False
    # Poll PG readiness instead of fixed sleep
    max_wait = 30
    start_time = time.time()
    while time.time() - start_time < max_wait:
        try:
            with get_pg_connection() as conn:
                conn.execute(sql.SQL("SELECT 1"))  # Fixed: Wrapped in sql.SQL
            logger.info("✅ Docker services started and PG ready.")
            return True
        except:
            time.sleep(2)
    logger.error("❌ PG not ready after wait.")
    return False

# --- PostgreSQL Setup & Load Functions ---

def create_raw_table():
    """Create staging.raw_data without surrogate 'id'; use article_id as PK."""
    drop_sql = sql.SQL("DROP TABLE IF EXISTS staging.raw_data;")
    create_sql = sql.SQL("""
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE TABLE IF NOT EXISTS staging.raw_data (
            crawl_timestamp TIMESTAMP,
            article_id VARCHAR(255) PRIMARY KEY,
            web_publication_date TIMESTAMP,
            web_title TEXT,
            body_text TEXT,
            web_url VARCHAR(500),
            section_name VARCHAR(255),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(drop_sql)
                cur.execute(create_sql)
                
                # Verify table structure
                cur.execute(sql.SQL("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'staging' AND table_name = 'raw_data' ORDER BY ordinal_position"))
                cols = cur.fetchall()
                logger.info(f"✅ Table structure: {[col[0] for col in cols]}")
                conn.commit()
                logger.info("✅ staging.raw_data table created/verified with PK on article_id")
    except Exception as e:
        logger.error(f"❌ Failed to create table: {e}")
        return False
    return True

def load_parquet_to_db(parquet_path: Path):
    """Load Parquet into staging.raw_data (UPSERT on article_id PK)."""
    try:
        df = pd.read_parquet(parquet_path)
        logger.info(f"📊 Loaded {len(df)} rows from {parquet_path}")
        logger.info(f"DEBUG: Columns found in Parquet file: {df.columns.tolist()}")

        # Rename source 'id' to 'article_id' if needed
        if 'id' in df.columns and 'article_id' not in df.columns:
            df = df.rename(columns={'id': 'article_id'})
            logger.info("✅ Renamed source 'id' to 'article_id'.")

        # Apply other renames
        rename_map = {
            'crawlTimestamp': 'crawl_timestamp',
            'webPublicationDate': 'web_publication_date',
            'webTitle': 'web_title',
            'bodyText': 'body_text',
            'webUrl': 'web_url',
            'sectionName': 'section_name',
        }
        cols_to_rename = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(columns=cols_to_rename)
        
        # Ensure article_id is present
        if 'article_id' not in df.columns:
            logger.error("❌ Critical column 'article_id' missing.")
            return False

        # Data types
        df['crawl_timestamp'] = pd.to_datetime(df['crawl_timestamp'], errors='coerce')
        df['web_publication_date'] = pd.to_datetime(df['web_publication_date'], errors='coerce')
        
        total_upserted = 0
        
        with get_pg_connection() as conn:
            with conn.cursor() as cur:

                # Optional truncate for full refresh—comment out for incremental
                cur.execute(sql.SQL("TRUNCATE TABLE staging.raw_data;"))
                conn.commit()
                logger.info("✅ Truncated for refresh (now 0 rows)")

                cur.execute(sql.SQL("SELECT COUNT(*) FROM staging.raw_data"))
                result = cur.fetchone()
                old_count = result[0] if result else 0
                logger.info(f"📊 Current rows before upsert: {old_count}")
                
                # Log unique article_ids
                article_ids_list = df['article_id'].dropna().unique().tolist()
                existing_articles_query = sql.SQL("""
                    SELECT COUNT(*) FROM staging.raw_data 
                    WHERE article_id = ANY(%s)
                """)
                cur.execute(existing_articles_query, (article_ids_list,))
                result = cur.fetchone()
                existing_matches = result[0] if result else 0
                new_count = len(article_ids_list) - existing_matches
                logger.info(f"📊 Unique article_ids to sync: {len(article_ids_list)} | Expected new inserts: {new_count} | Matches to update: {existing_matches}")
                
                batch_size = 100
                upsert_sql = sql.SQL("""
                    INSERT INTO staging.raw_data (
                        crawl_timestamp, article_id, web_publication_date, web_title,
                        body_text, web_url, section_name
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (article_id) DO UPDATE SET
                        crawl_timestamp = EXCLUDED.crawl_timestamp,
                        web_publication_date = EXCLUDED.web_publication_date,
                        web_title = EXCLUDED.web_title,
                        body_text = EXCLUDED.body_text,
                        web_url = EXCLUDED.web_url,
                        section_name = EXCLUDED.section_name
                """)
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i+batch_size]
                    
                    values_list = [
                        (
                            row['crawl_timestamp'], row['article_id'],
                            row['web_publication_date'],
                            row['web_title'], row['body_text'], row['web_url'], row['section_name']
                        )
                        for _, row in batch.iterrows()
                    ]
                    
                    cur.executemany(upsert_sql, values_list)
                    
                    total_upserted += len(batch)
                    logger.info(f"  UPSERT batch {i//batch_size + 1}: {len(batch)} rows (total processed: {total_upserted})")
                
                # Post-upsert: Log count only
                cur.execute(sql.SQL("SELECT COUNT(*) FROM staging.raw_data"))
                result = cur.fetchone()
                final_count = result[0] if result else 0
                logger.info(f"✅ Processed {total_upserted} rows; total rows in PG: {final_count}")
                conn.commit()
                
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to load Parquet: {e}")
        return False

# --- Snowflake & Verification Functions (Updated: No 'id' column) ---

def verify_pg_load(interval_minutes=15):
    """Verify the load by querying row count and recent timestamps in PostgreSQL."""
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("SELECT COUNT(*) FROM staging.raw_data"))
                result = cur.fetchone()
                total_count = result[0] if result else 0
                
                # Use timedelta for INTERVAL
                recent_interval = timedelta(minutes=interval_minutes)
                recent_query = sql.SQL("""
                    SELECT COUNT(*), MIN(loaded_at), MAX(loaded_at) 
                    FROM staging.raw_data 
                    WHERE loaded_at >= NOW() - %s
                """)
                cur.execute(recent_query, (recent_interval,))
                result = cur.fetchone()
                if result:
                    recent_count, min_loaded, max_loaded = result
                else:
                    recent_count, min_loaded, max_loaded = 0, None, None
                logger.info(f"📋 Total rows: {total_count} | Recent load: {recent_count} rows between {min_loaded} and {max_loaded}")
                
                crawl_query = sql.SQL("""
                    SELECT MIN(crawl_timestamp), MAX(crawl_timestamp) 
                    FROM staging.raw_data 
                    WHERE loaded_at >= NOW() - %s
                """)
                cur.execute(crawl_query, (recent_interval,))
                result = cur.fetchone()
                if result:
                    crawl_min, crawl_max = result
                else:
                    crawl_min, crawl_max = None, None
                logger.info(f"📋 Recent crawl_timestamp range: {crawl_min} to {crawl_max}")
                
                null_query = sql.SQL("""
                    SELECT 
                        COUNT(CASE WHEN crawl_timestamp IS NULL THEN 1 END) AS null_crawl_ts,
                        COUNT(CASE WHEN web_publication_date IS NULL THEN 1 END) AS null_pub_dates
                    FROM staging.raw_data
                """)
                cur.execute(null_query)
                result = cur.fetchone()
                if result:
                    nulls = result
                else:
                    nulls = (0, 0)
                logger.info(f"Data quality check: {nulls[0]} null crawl_ts, {nulls[1]} null pub dates")
                
    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")

def ingest_realtime_parquet_to_snowflake(parquet_file_path, sf_conn):
    """Upload Parquet to stage, then MERGE INTO main table using article_id (reuses provided SF conn)."""
    cursor = None
    try:
        cursor = sf_conn.cursor()
        database = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")
        stage_name = f"@{database}.{schema}.CSV_STAGE"
        table_name = f"{database}.{schema}.raw_data"
        temp_view_name = f"{database}.{schema}.temp_parquet_view"
        file_name = os.path.basename(parquet_file_path)
        file_format_name = f"{database}.{schema}.temp_parquet_ff"

        cursor.execute(f"USE SCHEMA {database}.{schema}")
        cursor.execute(f"CREATE OR REPLACE FILE FORMAT {file_format_name} TYPE = 'PARQUET'")
        logger.info(f"✅ Created temp file format {file_format_name}")
        cursor.execute(f"REMOVE {stage_name}")
        logger.info("✅ Cleared stage for realtime")

        total_size_mb = os.path.getsize(parquet_file_path) / (1024 * 1024)
        parallel_threads = 1
        logger.info(f"🚀 Uploading {total_size_mb:.1f}MB realtime Parquet directly (PARALLEL={parallel_threads})")

        start_time = time.time()
        put_command = f"PUT file://{parquet_file_path} {stage_name}/ AUTO_COMPRESS=TRUE OVERWRITE=TRUE PARALLEL={parallel_threads}"
        cursor.execute(put_command)
        upload_time = time.time() - start_time
        logger.info(f"✅ Realtime upload completed in {upload_time:.1f}s")

        cursor.execute(f"SHOW TABLES LIKE 'raw_data' IN SCHEMA {database}.{schema}")
        result = cursor.fetchone()
        if not result:
            raise ValueError(f"Table {table_name} does not exist—run batch load first.")

        # --- Use a Temporary View over the Stage for MERGE ---
        cursor.execute(f"DROP VIEW IF EXISTS {temp_view_name}")
        create_view_sql = f"""
        CREATE TEMPORARY VIEW {temp_view_name} AS 
        SELECT
            $1:article_id::STRING AS ARTICLE_ID,
            TRY_TO_TIMESTAMP_NTZ($1:crawl_timestamp::STRING) AS CRAWL_TIMESTAMP,
            TRY_TO_TIMESTAMP_NTZ($1:web_publication_date::STRING) AS WEB_PUBLICATION_DATE,
            $1:web_title::STRING AS WEB_TITLE,
            $1:body_text::STRING AS BODY_TEXT,
            $1:web_url::STRING AS WEB_URL,
            $1:section_name::STRING AS SECTION_NAME
        FROM {stage_name}/{file_name} (FILE_FORMAT => '{file_format_name.split('.')[-1]}')
        """
        cursor.execute(create_view_sql)
        logger.info(f"✅ Created temporary view {temp_view_name} over the staged Parquet file.")

        # --- MERGE INTO TARGET TABLE ---
        merge_start = time.time()
        merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING {temp_view_name} AS source
        ON target.ARTICLE_ID = source.ARTICLE_ID
        WHEN MATCHED THEN 
            UPDATE SET 
                target.CRAWL_TIMESTAMP = source.CRAWL_TIMESTAMP,
                target.WEB_PUBLICATION_DATE = source.WEB_PUBLICATION_DATE,
                target.WEB_TITLE = source.WEB_TITLE,
                target.BODY_TEXT = source.BODY_TEXT,
                target.WEB_URL = source.WEB_URL,
                target.SECTION_NAME = source.SECTION_NAME,
                target.LOADED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (
                ARTICLE_ID, CRAWL_TIMESTAMP, WEB_PUBLICATION_DATE, WEB_TITLE, 
                BODY_TEXT, WEB_URL, SECTION_NAME, LOADED_AT
            )
            VALUES (
                source.ARTICLE_ID, source.CRAWL_TIMESTAMP, source.WEB_PUBLICATION_DATE, 
                source.WEB_TITLE, source.BODY_TEXT, source.WEB_URL, source.SECTION_NAME, 
                CURRENT_TIMESTAMP()
            )
        """
        cursor.execute(merge_sql)
        merge_time = time.time() - merge_start
        logger.info(f"✅ MERGE INTO main table completed in {merge_time:.1f}s")

        # Fetch the results of the MERGE operation for logging
        try:
            cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
            result = cursor.fetchall()
            if result:
                logger.info(f"✅ MERGE executed successfully. Check Snowflake query history for details.")
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch MERGE result: {e}")
        
        # Final Total Row Count (Safety check)
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = cursor.fetchone()
        total_row_count = result[0] if result else 0
        logger.info(f"✅ Load complete. Total unique rows in {table_name}: {total_row_count}")

        sf_conn.commit()
        
        # Clean up the temporary view, staged file, and file format
        cursor.execute(f"DROP VIEW {temp_view_name}")
        cursor.execute(f"REMOVE {stage_name}")
        cursor.execute(f"DROP FILE FORMAT IF EXISTS {file_format_name}")
        logger.info("✅ Cleaned up temp view, stage, and file format.")

        return True

    except Exception as e:
        logger.error(f"❌ Realtime SF load failed: {e}")
        return False
    finally:
        if parquet_file_path and os.path.exists(parquet_file_path):
             os.remove(parquet_file_path) # Clean up temp file

def load_postgres_to_snowflake(sf_conn=None):
    """Extract data from PostgreSQL (no id), export to Parquet, load to Snowflake (reuses provided SF conn)."""
    parquet_path = Path('temp_realtime.parquet')
    try:
        # Extract ALL data from staging.raw_data (which now contains all incremental history)
        with get_pg_connection() as pg_conn:
            query = sql.SQL("""
                SELECT crawl_timestamp, article_id, web_publication_date, 
                        web_title, body_text, web_url, section_name 
                FROM staging.raw_data
            """)
            # Fix: Render clean SQL string using as_string
            df = pd.read_sql_query(query.as_string(pg_conn), pg_conn)
        logger.info(f"✅ Extracted {len(df)} records from PostgreSQL")

        if len(df) == 0:
            logger.warning("⚠️ No records to load.")
            return True

        df['crawl_timestamp'] = pd.to_datetime(df['crawl_timestamp'], errors='coerce')
        df['web_publication_date'] = pd.to_datetime(df['web_publication_date'], errors='coerce')

        # Export to Parquet
        df.to_parquet(parquet_path, index=False)
        logger.info(f"📦 Exported to {parquet_path}")

        # Bulk load via Parquet (now uses MERGE/UPSERT mode)
        return ingest_realtime_parquet_to_snowflake(parquet_path, sf_conn)

    except Exception as e:
        logger.error(f"❌ Loading failed: {e}")
        if parquet_path.exists():
            os.remove(parquet_path)
        return False
    
def verify_sf_load(sf_conn, interval_minutes=15):
    """Verify the load in Snowflake by querying recent row count and quality (reuses provided SF conn)."""
    cursor = None
    try:
        cursor = sf_conn.cursor()
        database = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")
        table = f"{database}.{schema}.raw_data"
        
        recent_interval = f"INTERVAL '{interval_minutes} minutes'"
        
        cursor.execute(f"""
             SELECT COUNT(*) 
             FROM {table} 
             WHERE LOADED_AT >= CURRENT_TIMESTAMP - {recent_interval}
        """)
        result = cursor.fetchone()
        recent_count = result[0] if result else 0
        
        cursor.execute(f"""
             SELECT MIN(LOADED_AT), MAX(LOADED_AT) 
             FROM {table} 
             WHERE LOADED_AT >= CURRENT_TIMESTAMP - {recent_interval}
        """)
        result = cursor.fetchone()
        if result:
            load_range = result
        else:
            load_range = (None, None)
        logger.info(f"📋 Recent Snowflake load: {recent_count} rows between {load_range[0]} and {load_range[1]}")
        
        cursor.execute(f"""
             SELECT MIN(CRAWL_TIMESTAMP), MAX(CRAWL_TIMESTAMP) 
             FROM {table} 
             WHERE LOADED_AT >= CURRENT_TIMESTAMP - {recent_interval}
        """)
        result = cursor.fetchone()
        if result:
            crawl_range = result
        else:
            crawl_range = (None, None)
        logger.info(f"📋 Recent CRAWL_TIMESTAMP range: {crawl_range[0]} to {crawl_range[1]}")

        cursor.execute(f"""
             SELECT 
                 COUNT(CASE WHEN CRAWL_TIMESTAMP IS NULL THEN 1 END) AS null_crawl_ts,
                 COUNT(CASE WHEN WEB_PUBLICATION_DATE IS NULL THEN 1 END) AS null_pub_dates
             FROM {table}
        """)
        result = cursor.fetchone()
        if result:
            nulls = result
        else:
            nulls = (0, 0)
        logger.info(f"Data quality check: {nulls[0]} null crawl_ts, {nulls[1]} null pub dates")

        # Check for duplicates based on the unique key ARTICLE_ID
        cursor.execute(f"""
             SELECT ARTICLE_ID, COUNT(*) 
             FROM {table} 
             GROUP BY ARTICLE_ID 
             HAVING COUNT(*) > 1
        """)
        dups = cursor.fetchall()
        if dups:
             logger.error(f"❌ CRITICAL ERROR: Duplicate ARTICLE_IDs found (first 5): {dups[:5]}")
        else:
             logger.info("✅ No duplicate ARTICLE_IDs in Snowflake")
        
    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")

# --- Main Execution ---

def main():
    """Main function to orchestrate the full pipeline: Docker -> Parquet -> PostgreSQL -> Snowflake."""
    parser = argparse.ArgumentParser(description="End-to-end pipeline for loading data to PG and Snowflake.")
    parser.add_argument('--reset', action='store_true', help="Reset Docker Compose (down -v) before starting.")
    args = parser.parse_args()

    print("🚀 End-to-End Pipeline: Docker -> Parquet -> PostgreSQL -> Snowflake")
    print("=" * 80)

    parquet_path = os.getenv("REALTIME_PARQUET_PATH", r"data\real_time\guardian_real_time_articles.parquet")
    parquet_path = Path(parquet_path)
    if not parquet_path.exists():
        logger.error(f"❌ Parquet not found: {parquet_path}")
        sys.exit(1)

    # Step 0: Docker Start (Reset is optional)
    if args.reset:
        print("0️⃣ Resetting Docker...")
        if not reset_docker():
            print("❌ Docker reset failed. Exiting.")
            sys.exit(1)
    print("0️⃣ Starting Docker...")
    if not start_docker():
        print("❌ Docker start failed. Exiting.")
        sys.exit(1)

    # Step 1: Create table in PostgreSQL (no id param needed)
    print("1️⃣ Creating/Verifying PostgreSQL table...")
    if not create_raw_table():
        print("❌ Table creation failed. Exiting.")
        sys.exit(1)

    # Step 2: Load Parquet to PostgreSQL (APPEND/UPSERT ONLY)
    print("2️⃣ Loading Parquet to PostgreSQL (APPEND/UPSERT)...")
    if not load_parquet_to_db(parquet_path):
        print("❌ Parquet load to PostgreSQL failed. Exiting.")
        sys.exit(1)

    # Step 3: Verify PostgreSQL load
    print("3️⃣ Verifying PostgreSQL load...")
    verify_pg_load()

    # Step 4: Load from PostgreSQL to Snowflake (Transfers all PG data)
    print("4️⃣ Loading data from PostgreSQL to Snowflake (MERGE)...")
    
    # Init single SF connection post-PG
    global _sf_conn
    _sf_conn = get_sf_connection()
    
    if not load_postgres_to_snowflake(sf_conn=_sf_conn):
        print("❌ Load to Snowflake failed. Exiting.")
        sys.exit(1)

    # Step 5: Verify Snowflake load
    print("5️⃣ Verifying Snowflake load (Checking for Duplicates)...")
    verify_sf_load(_sf_conn)

    print("✅ Full pipeline completed successfully!")
    
    # Close SF connection
    if _sf_conn:
        _sf_conn.close()
        logger.info("✅ Snowflake connection closed")
    
    return True

if __name__ == "__main__":
    main()