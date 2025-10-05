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
    )

def load_postgres_to_snowflake():
    """Extract data from PostgreSQL and load to Snowflake."""
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

        # Ensure datetime columns are properly formatted (leave as Timestamp for isoformat)
        df['crawl_timestamp'] = pd.to_datetime(df['crawl_timestamp'], errors='coerce')
        df['web_publication_date'] = pd.to_datetime(df['web_publication_date'], errors='coerce')

        # Connect to Snowflake
        ctx = get_sf_connection()
        cursor = ctx.cursor()

        # Prepare batch insert with %s placeholders and ISO string timestamps
        batch_size = 100
        total_loaded = 0
        insert_sql = """
            INSERT INTO SC_T26.RAW_DATA (
                ID, CRAWL_TIMESTAMP, ARTICLE_ID, WEB_PUBLICATION_DATE, 
                WEB_TITLE, BODY_TEXT, WEB_URL, SECTION_NAME, 
                LOADED_AT
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            batch_values = [
                (
                    row['id'],
                    row['crawl_timestamp'].isoformat() if pd.notna(row['crawl_timestamp']) else None,  # ISO string for timestamp
                    row['article_id'],
                    row['web_publication_date'].isoformat() if pd.notna(row['web_publication_date']) else None,  # ISO string
                    row['web_title'],
                    row['body_text'],
                    row['web_url'],
                    row['section_name'],
                    datetime.now().isoformat()  # ISO string for loaded_at
                )
                for _, row in batch.iterrows()
            ]
            cursor.executemany(insert_sql, batch_values)
            ctx.commit()
            total_loaded += len(batch)
            logger.info(f"   Loaded batch {i//batch_size + 1}: {len(batch)} records (total: {total_loaded})")

        logger.info(f"✅ Loaded {total_loaded} records to Snowflake")
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
        cursor.execute("""
            SELECT COUNT(*) 
            FROM SC_T26.RAW_DATA 
            WHERE LOADED_AT >= CURRENT_TIMESTAMP - INTERVAL '1 HOUR'
        """)
        count = cursor.fetchone()[0]
        cursor.execute("""
            SELECT MIN(LOADED_AT), MAX(LOADED_AT) 
            FROM SC_T26.RAW_DATA 
            WHERE LOADED_AT >= CURRENT_TIMESTAMP - INTERVAL '1 HOUR'
        """)
        load_range = cursor.fetchone()
        logger.info(f"📋 Verification: {count} rows loaded between {load_range[0]} and {load_range[1]}")
    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")
    finally:
        if ctx:
            ctx.close()

def main():
    """Main function to orchestrate the load from PostgreSQL to Snowflake."""
    print("🚀 Loading PostgreSQL data to Snowflake")
    print("=" * 50)

    # Steps
    print("1️⃣ Loading data...")
    if not load_postgres_to_snowflake():
        print("❌ Load failed. Exiting.")
        sys.exit(1)

    print("2️⃣ Verifying load...")
    verify_sf_load()

    print("✅ Load completed successfully!")

if __name__ == "__main__":
    main()