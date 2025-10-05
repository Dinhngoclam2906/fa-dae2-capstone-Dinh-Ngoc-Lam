import os
import sys
from pathlib import Path
import psycopg
from dotenv import load_dotenv
import pandas as pd
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_connection():
    """Get PostgreSQL connection using env vars (adapted from Lab #2)."""
    params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB", "staging_db"),
        "user": os.getenv("POSTGRES_USER", "staging_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "staging_password"),
    }
    return psycopg.connect(**params)

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
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                cur.execute(drop_constraint_sql)  # Idempotent drop
                cur.execute(add_constraint_sql)   # Always add fresh
                conn.commit()
                logger.info("✅ staging.raw_data table created/verified with unique constraint on article_id")
    except Exception as e:
        logger.error(f"❌ Failed to create table/constraint: {e}")
        return False
    return True

def load_csv_to_db(csv_path: Path):
    """Load CSV data into staging.raw_data table."""
    # if not csv_path.exists():
    #     logger.error(f"❌ CSV file not found: {csv_path}")
    #     return False

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
        
        # Insert batch-wise for efficiency
        batch_size = 100
        total_inserted = 0
        with get_connection() as conn:
            with conn.cursor() as cur:
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i+batch_size]
                    # Use executemany with parameterized query
                    cur.executemany("""
                        INSERT INTO staging.raw_data (
                            crawl_timestamp, article_id, web_publication_date, web_title,
                            body_text, web_url, section_name
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (article_id) DO NOTHING  -- Skip duplicates
                    """, [
                        (
                            row['crawl_timestamp'], row['article_id'], row['web_publication_date'],
                            row['web_title'], row['body_text'], row['web_url'], row['section_name']
                        )
                        for _, row in batch.iterrows()
                    ])
                    conn.commit()
                    total_inserted += len(batch)
                    logger.info(f"   Inserted batch {i//batch_size + 1}: {len(batch)} rows (total: {total_inserted})")
        
        logger.info(f"✅ Loaded {total_inserted} rows into staging.raw_data")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to load CSV: {e}")
        return False

def verify_load():
    """Verify the load by querying row count."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM staging.raw_data")
                count = cur.fetchone()[0]
                cur.execute("SELECT MIN(loaded_at), MAX(loaded_at) FROM staging.raw_data")
                load_range = cur.fetchone()
                logger.info(f"📋 Verification: {count} rows loaded between {load_range[0]} and {load_range[1]}")
    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")

def main():
    """Main function to orchestrate the load."""
    print("🚀 Loading Guardian API CSV data to PostgreSQL")
    print("=" * 50)

    csv_path = r'data\external\guardian_all_articles.csv'
    csv_path = Path(csv_path)  # Convert to Path for consistency

    # Steps
    if not create_raw_table():
        print("❌ Table creation failed. Exiting.")
        sys.exit(1)

    print("1️⃣ Loading CSV to DB...")
    if not load_csv_to_db(csv_path):
        print("❌ CSV load failed. Exiting.")
        sys.exit(1)

    print("2️⃣ Verifying load...")
    verify_load()

    print("✅ Load completed successfully!")

if __name__ == "__main__":
    main()