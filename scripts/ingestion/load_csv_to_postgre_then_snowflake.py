import os
import sys
from pathlib import Path
import psycopg
import snowflake.connector
from dotenv import load_dotenv
import pandas as pd
import logging
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
import subprocess
import argparse
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_pg_connection():
    """Get PostgreSQL connection using env vars."""
    params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB", "staging_db"),
        "user": os.getenv("POSTGRES_USER", "staging_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "staging_password"),
    }
    return psycopg.connect(**params)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_sf_connection():
    """Get Snowflake connection using env vars."""
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        authenticator="SNOWFLAKE_JWT",
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
        private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        timezone="Asia/Ho_Chi_Minh"
    )

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
    """Start Docker Compose services in detached mode."""
    logger.info("🚀 Starting Docker Compose services...")
    if not run_docker_command(['docker-compose', 'up', '-d'], cwd=Path(docker_compose_path).parent):
        return False
    # Wait a bit for PostgreSQL to be ready (adjust sleep if needed)
    import time
    time.sleep(5)
    logger.info("✅ Docker services started.")
    return True

def create_raw_table():
    """Create staging.raw_data table if it doesn't exist, and add unique constraint on article_id."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS staging.raw_data (
        id SERIAL PRIMARY KEY,
        crawl_timestamp TIMESTAMP,
        article_id VARCHAR(255),
        web_publication_date TIMESTAMP,
        web_title TEXT,
        body_text TEXT,
        web_url VARCHAR(500),
        section_name VARCHAR(255),
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    drop_constraint_sql = """
    ALTER TABLE staging.raw_data 
    DROP CONSTRAINT IF EXISTS unique_article_id;
    """
    add_constraint_sql = """
    ALTER TABLE staging.raw_data 
    ADD CONSTRAINT unique_article_id UNIQUE (article_id);
    """
    set_sequence_sql = """
    SELECT setval(pg_get_serial_sequence('staging.raw_data', 'id'), 87575);
    """
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                cur.execute(drop_constraint_sql)  # Idempotent drop
                cur.execute(add_constraint_sql)   # Always add fresh
                cur.execute(set_sequence_sql)     # Set sequence so next ID is 88576
                conn.commit()
                logger.info("✅ staging.raw_data table created/verified with unique constraint on article_id")
                logger.info("✅ ID sequence set to start new inserts from 88576 onwards")
    except Exception as e:
        logger.error(f"❌ Failed to create table/constraint: {e}")
        return False
    return True

def load_csv_to_db(csv_path: Path):
    """Load CSV data into staging.raw_data table with trim + UPSERT to overwrite historical data."""
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
        logger.info(f"📊 Loaded {len(df)} rows from {csv_path}")
        
        # Convert timestamp columns
        df['crawlTimestamp'] = pd.to_datetime(df['crawlTimestamp'], errors='coerce')
        df['webPublicationDate'] = pd.to_datetime(df['webPublicationDate'], errors='coerce')
        
        # Rename for SQL (avoid camelCase issues)
        df = df.rename(columns={
            'crawlTimestamp': 'crawl_timestamp',
            'id': 'article_id',
            'webPublicationDate': 'web_publication_date',
            'webTitle': 'web_title',
            'bodyText': 'body_text',
            'webUrl': 'web_url',
            'sectionName': 'section_name'
        })
        
        # NEW: Clear table before load to force fresh inserts with new ID sequence
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE staging.raw_data RESTART IDENTITY")  # Clears data + resets sequence to 1, but we'll re-set after
                conn.commit()
                logger.info("🗑️ Truncated staging.raw_data (fresh start for real-time IDs)")
        
        # Re-prepare article_ids (no trim needed now, since empty)
        article_ids_list = df['article_id'].dropna().unique().tolist()
        num_ids = len(article_ids_list)
        logger.info(f"📊 Unique article_ids to sync: {num_ids}")
        
        total_upserted = 0
        with get_pg_connection() as conn:
            with conn.cursor() as cur:

                cur.execute("SELECT setval(pg_get_serial_sequence('staging.raw_data', 'id'), 87575)")
                conn.commit()
                logger.info("✅ ID sequence re-set to start inserts from 87576")
                
                # Get current count before upsert (should be 0)
                cur.execute("SELECT COUNT(*) FROM staging.raw_data")
                old_count = cur.fetchone()[0]
                logger.info(f"📊 Current rows before upsert: {old_count}")
                
                # UPSERT batch-wise (now all inserts, since empty)
                batch_size = 100
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i+batch_size]
                    # Use executemany with parameterized UPSERT query
                    cur.executemany("""
                        INSERT INTO staging.raw_data (
                            crawl_timestamp, article_id, web_publication_date, web_title,
                            body_text, web_url, section_name, loaded_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (article_id) DO UPDATE SET
                            crawl_timestamp = EXCLUDED.crawl_timestamp,
                            web_publication_date = EXCLUDED.web_publication_date,
                            web_title = EXCLUDED.web_title,
                            body_text = EXCLUDED.body_text,
                            web_url = EXCLUDED.web_url,
                            section_name = EXCLUDED.section_name,
                            loaded_at = CURRENT_TIMESTAMP
                    """, [
                        (
                            row['crawl_timestamp'], row['article_id'], row['web_publication_date'],
                            row['web_title'], row['body_text'], row['web_url'], row['section_name']
                        )
                        for _, row in batch.iterrows()
                    ])
                    total_upserted += len(batch)
                    logger.info(f"   UPSERT batch {i//batch_size + 1}: {len(batch)} rows (total: {total_upserted})")
                
                # Final count
                cur.execute("SELECT COUNT(*) FROM staging.raw_data")
                final_count = cur.fetchone()[0]
                logger.info(f"✅ Loaded {final_count} rows total (all fresh inserts)")
                # NEW: Log max ID to confirm
                cur.execute("SELECT MAX(id) FROM staging.raw_data")
                max_id = cur.fetchone()[0]
                logger.info(f"✅ Max ID after load: {max_id} (should be ~{num_ids + 87575})")
                conn.commit()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to load CSV: {e}")
        return False

def verify_pg_load():
    """Verify the load by querying row count and recent timestamps in PostgreSQL."""
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                # Total count (unchanged, for overall health)
                cur.execute("SELECT COUNT(*) FROM staging.raw_data")
                total_count = cur.fetchone()[0]
                
                # Filter recent loads for timestamp range
                recent_interval = "INTERVAL '15 minutes'"  # Your update
                cur.execute(f"""
                    SELECT COUNT(*), MIN(loaded_at), MAX(loaded_at) 
                    FROM staging.raw_data 
                    WHERE loaded_at >= NOW() - {recent_interval}
                """)
                recent_count, min_loaded, max_loaded = cur.fetchone()
                logger.info(f"📋 Total rows: {total_count} | Recent load: {recent_count} rows between {min_loaded} and {max_loaded}")
                
                # Also check recent crawl_timestamp for your re-crawl confirmation
                cur.execute(f"""
                    SELECT MIN(crawl_timestamp), MAX(crawl_timestamp) 
                    FROM staging.raw_data 
                    WHERE loaded_at >= NOW() - {recent_interval}
                """)
                crawl_min, crawl_max = cur.fetchone()
                logger.info(f"📋 Recent crawl_timestamp range: {crawl_min} to {crawl_max}")
                
    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")

def load_postgres_to_snowflake():
    """Extract data from PostgreSQL and MERGE to Snowflake (upsert/overwrite only if newer crawl_timestamp)."""
    ctx = None
    try:
        # Extract data from PostgreSQL
        with get_pg_connection() as pg_conn:
            query = """
                SELECT id, crawl_timestamp, article_id, web_publication_date, 
                       web_title, body_text, web_url, section_name 
                FROM staging.raw_data
            """
            df = pd.read_sql_query(query, pg_conn)
        logger.info(f"✅ Extracted {len(df)} records from PostgreSQL")

        if len(df) == 0:
            logger.warning("⚠️ No records to load.")
            return True

        # Ensure datetime columns are properly formatted
        df['crawl_timestamp'] = pd.to_datetime(df['crawl_timestamp'], errors='coerce')
        df['web_publication_date'] = pd.to_datetime(df['web_publication_date'], errors='coerce')

        # Connect to Snowflake
        ctx = get_sf_connection()
        cursor = ctx.cursor()

        # Create temporary table for merge (use STRING to avoid binding issues)
        temp_table_sql = """
        CREATE OR REPLACE TEMPORARY TABLE temp_raw_data (
            ID INTEGER,
            CRAWL_TIMESTAMP STRING,
            ARTICLE_ID STRING,
            WEB_PUBLICATION_DATE STRING,
            WEB_TITLE STRING,
            BODY_TEXT STRING,
            WEB_URL STRING,
            SECTION_NAME STRING
        )
        """
        cursor.execute(temp_table_sql)

        # Insert data into temp table (batch-wise, with ISO strings for timestamps)
        batch_size = 100
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            batch_values = [
                (
                    row['id'],
                    row['crawl_timestamp'].isoformat() if pd.notna(row['crawl_timestamp']) else None,
                    row['article_id'],
                    row['web_publication_date'].isoformat() if pd.notna(row['web_publication_date']) else None,
                    row['web_title'],
                    row['body_text'],
                    row['web_url'],
                    row['section_name']
                )
                for _, row in batch.iterrows()
            ]
            insert_temp_sql = """
            INSERT INTO temp_raw_data (
                ID, CRAWL_TIMESTAMP, ARTICLE_ID, WEB_PUBLICATION_DATE, 
                WEB_TITLE, BODY_TEXT, WEB_URL, SECTION_NAME
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_temp_sql, batch_values)
            ctx.commit()
            logger.info(f"   Inserted batch {i//batch_size + 1} to temp: {len(batch)} records")

        merge_sql = """
        MERGE INTO SC_T26.RAW_DATA AS target
        USING temp_raw_data AS source
        ON target.ARTICLE_ID = source.ARTICLE_ID
        WHEN MATCHED THEN
            UPDATE SET
                target.CRAWL_TIMESTAMP = TO_TIMESTAMP_NTZ(source.CRAWL_TIMESTAMP),
                target.WEB_PUBLICATION_DATE = TO_TIMESTAMP_NTZ(source.WEB_PUBLICATION_DATE),
                target.WEB_TITLE = source.WEB_TITLE,
                target.BODY_TEXT = source.BODY_TEXT,
                target.WEB_URL = source.WEB_URL,
                target.SECTION_NAME = source.SECTION_NAME,
                target.LOADED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (
                ID, CRAWL_TIMESTAMP, ARTICLE_ID, WEB_PUBLICATION_DATE, 
                WEB_TITLE, BODY_TEXT, WEB_URL, SECTION_NAME, LOADED_AT
            )
            VALUES (
                source.ID, TO_TIMESTAMP_NTZ(source.CRAWL_TIMESTAMP), source.ARTICLE_ID, 
                TO_TIMESTAMP_NTZ(source.WEB_PUBLICATION_DATE),
                source.WEB_TITLE, source.BODY_TEXT, source.WEB_URL, source.SECTION_NAME,
                CURRENT_TIMESTAMP()
            )
        """
        cursor.execute(merge_sql)
        ctx.commit()

        # Clean up temp table
        cursor.execute("DROP TABLE IF EXISTS temp_raw_data")
        ctx.commit()

        logger.info(f"✅ Merged {len(df)} records to Snowflake (overwritten only where crawl_timestamp is newer)")
        return True

    except Exception as e:
        logger.error(f"❌ Loading failed: {e}")
        return False
    finally:
        if ctx:
            ctx.close()
 
def verify_sf_load():
    """Verify the load in Snowflake by querying recent row count."""
    ctx = None
    try:
        ctx = get_sf_connection()
        cursor = ctx.cursor()
        
        # Configurable recent interval (match PG)
        recent_interval = "INTERVAL '15 minutes'"  # Your update
        
        # Recent count and loaded_at range
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM SC_T26.RAW_DATA 
            WHERE LOADED_AT >= CURRENT_TIMESTAMP - {recent_interval}
        """)
        recent_count = cursor.fetchone()[0]
        
        cursor.execute(f"""
            SELECT MIN(LOADED_AT), MAX(LOADED_AT) 
            FROM SC_T26.RAW_DATA 
            WHERE LOADED_AT >= CURRENT_TIMESTAMP - {recent_interval}
        """)
        load_range = cursor.fetchone()
        logger.info(f"📋 Recent Snowflake load: {recent_count} rows between {load_range[0]} and {load_range[1]}")
        
        # Recent CRAWL_TIMESTAMP range (for re-crawl confirmation)
        cursor.execute(f"""
            SELECT MIN(CRAWL_TIMESTAMP), MAX(CRAWL_TIMESTAMP) 
            FROM SC_T26.RAW_DATA 
            WHERE LOADED_AT >= CURRENT_TIMESTAMP - {recent_interval}
        """)
        crawl_range = cursor.fetchone()
        logger.info(f"📋 Recent CRAWL_TIMESTAMP range: {crawl_range[0]} to {crawl_range[1]}")

        cursor.execute("""
            SELECT ID, COUNT(*) 
            FROM SC_T26.RAW_DATA 
            GROUP BY ID 
            HAVING COUNT(*) > 1
        """)
        dups = cursor.fetchall()
        if dups:
            logger.warning(f"⚠️ Duplicate IDs found: {dups}")
        else:
            logger.info("✅ No duplicate IDs in Snowflake")
        
        # Also log recent/min/max IDs
        cursor.execute("SELECT MIN(ID), MAX(ID) FROM SC_T26.RAW_DATA")
        id_range = cursor.fetchone()
        logger.info(f"📋 ID range: {id_range[0]} to {id_range[1]}")
        
    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")
    finally:
        if ctx:
            ctx.close()

def main():
    """Main function to orchestrate the full pipeline: Docker -> CSV -> PostgreSQL -> Snowflake."""
    parser = argparse.ArgumentParser(description="End-to-end pipeline for loading data to PG and Snowflake.")
    parser.add_argument('--reset', action='store_true', help="Reset Docker Compose (down -v) before starting.")
    args = parser.parse_args()

    print("🚀 End-to-End Pipeline: Docker -> CSV -> PostgreSQL -> Snowflake")
    print("=" * 80)

    csv_path = r'data\external\guardian_all_articles.csv'
    csv_path = Path(csv_path)

    # Step 0: Optional Docker reset and start
    if args.reset:
        print("0️⃣ Resetting Docker...")
        if not reset_docker():
            print("❌ Docker reset failed. Exiting.")
            sys.exit(1)
    print("0️⃣ Starting Docker...")
    if not start_docker():
        print("❌ Docker start failed. Exiting.")
        sys.exit(1)

    # Step 1: Create table in PostgreSQL
    print("1️⃣ Creating/Verifying PostgreSQL table...")
    if not create_raw_table():
        print("❌ Table creation failed. Exiting.")
        sys.exit(1)

    # Step 2: Load CSV to PostgreSQL
    print("2️⃣ Loading CSV to PostgreSQL...")
    if not load_csv_to_db(csv_path):
        print("❌ CSV load to PostgreSQL failed. Exiting.")
        sys.exit(1)

    # Step 3: Verify PostgreSQL load
    print("3️⃣ Verifying PostgreSQL load...")
    verify_pg_load()

    # Step 4: Load from PostgreSQL to Snowflake
    print("4️⃣ Loading data from PostgreSQL to Snowflake...")
    if not load_postgres_to_snowflake():
        print("❌ Load to Snowflake failed. Exiting.")
        sys.exit(1)

    # Step 5: Verify Snowflake load
    print("5️⃣ Verifying Snowflake load...")
    verify_sf_load()

    print("✅ Full pipeline completed successfully!")

if __name__ == "__main__":
    main()