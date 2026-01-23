"""
Simple one-time script to mark ALL articles in Snowflake as ingested.

This assumes you've already uploaded everything to Pinecone.
Much faster than querying Pinecone or row-by-row updates.

Usage:
    python bulk_mark_ingested.py
"""

import os
import logging
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_sf_connection():
    """Get Snowflake connection."""
    container_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE")
    local_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH")
    private_key_path = container_key_path or local_key_path

    if not private_key_path or not os.path.exists(private_key_path):
        raise FileNotFoundError(f"Snowflake private key not found: {private_key_path}")

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        authenticator="SNOWFLAKE_JWT",
        private_key_file=private_key_path,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        timezone="Asia/Ho_Chi_Minh"
    )
    
    logger.info("✅ Snowflake connected")
    return conn


def bulk_mark_all_as_ingested(namespace: str = "guardian_articles"):
    """
    Mark ALL articles in FCT_ARTICLES as ingested in one shot.
    Uses a single INSERT statement - super fast!
    """
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    conn = get_sf_connection()
    
    try:
        with conn.cursor() as cur:
            # First, check how many articles we have
            cur.execute(f"""
                SELECT COUNT(DISTINCT ARTICLE_ID)
                FROM {database}.{schema}.FCT_ARTICLES
                WHERE BODY_TEXT IS NOT NULL
                  AND LENGTH(BODY_TEXT) > 1000
                  AND IS_CURRENT = TRUE
            """)
            total_articles = cur.fetchone()[0]
            
            logger.info(f"📊 Found {total_articles:,} articles in Snowflake")
            
            # Check how many already tracked
            cur.execute(f"""
                SELECT COUNT(*)
                FROM {database}.{schema}.PINECONE_INGESTION_LOG
            """)
            already_tracked = cur.fetchone()[0]
            
            logger.info(f"📝 Currently tracked: {already_tracked:,} articles")
            logger.info(f"🔄 Will insert/update: {total_articles:,} articles")
            
            print()
            response = input(f"Proceed with bulk update? (yes/no): ").strip().lower()
            
            if response not in ['yes', 'y']:
                print("❌ Cancelled")
                return
            
            logger.info("⏳ Running bulk MERGE (this will take ~10-30 seconds)...")
            
            # Single MERGE statement for ALL articles
            merge_sql = f"""
            MERGE INTO {database}.{schema}.PINECONE_INGESTION_LOG AS target
            USING (
                SELECT 
                    ARTICLE_ID,
                    1 AS chunk_count,  -- Placeholder, not critical
                    '{namespace}' AS namespace
                FROM {database}.{schema}.FCT_ARTICLES
                WHERE BODY_TEXT IS NOT NULL
                  AND LENGTH(BODY_TEXT) > 1000
                  AND IS_CURRENT = TRUE
            ) AS source
            ON target.ARTICLE_ID = source.ARTICLE_ID
            WHEN MATCHED THEN
                UPDATE SET 
                    INGESTED_AT = CURRENT_TIMESTAMP(),
                    CHUNK_COUNT = source.chunk_count,
                    PINECONE_NAMESPACE = source.namespace,
                    INGESTION_STATUS = 'SUCCESS'
            WHEN NOT MATCHED THEN
                INSERT (ARTICLE_ID, INGESTED_AT, CHUNK_COUNT, PINECONE_NAMESPACE, INGESTION_STATUS)
                VALUES (source.ARTICLE_ID, CURRENT_TIMESTAMP(), source.chunk_count, source.namespace, 'SUCCESS')
            """
            
            cur.execute(merge_sql)
            rows_affected = cur.rowcount
            conn.commit()
            
            logger.info(f"✅ Committed {rows_affected:,} rows")
            logger.info(f"🎉 Done! All articles marked as ingested")
            
    except Exception as e:
        logger.error(f"❌ Failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    print("\n" + "="*60)
    print("Bulk Mark All Articles as Ingested")
    print("="*60 + "\n")
    
    try:
        bulk_mark_all_as_ingested(namespace="guardian_articles")
        print("\n✅ Success! Your tracking table is now up to date.")
        print("   Future --incremental runs will only process new articles.")
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()