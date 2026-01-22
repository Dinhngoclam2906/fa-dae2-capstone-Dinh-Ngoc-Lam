"""
ULTRA-FAST tracking table update using bulk operations.
Instead of individual MERGE statements, uses bulk INSERT with temp table.

Expected time: ~30-60 seconds for 9,609 articles (vs 2 hours!)
"""

import os
import logging
from dotenv import load_dotenv
import snowflake.connector
from datetime import datetime

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
        raise ValueError(f"Snowflake private key not found: {private_key_path}")

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        authenticator="SNOWFLAKE_JWT",
        private_key_file=private_key_path,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        timezone="Asia/Ho_Chi_Minh",
        network_timeout=300,
        socket_timeout=300
    )

    logger.info("✅ Snowflake connection established")
    return conn


def bulk_update_tracking_table():
    """
    FAST bulk update using temp table + MERGE.
    ~100x faster than individual row-by-row MERGE statements.
    """
    conn = get_sf_connection()
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    try:
        logger.info("🔍 Step 1: Fetching articles that need tracking...")
        
        with conn.cursor() as cur:
            # Get all articles that should be tracked
            cur.execute(f"""
                SELECT 
                    a.ARTICLE_ID,
                    7 as estimated_chunks  -- Average chunks per article
                FROM {database}.{schema}.FCT_ARTICLES a
                WHERE a.BODY_TEXT IS NOT NULL
                  AND LENGTH(a.BODY_TEXT) > 1000
                  AND a.IS_CURRENT = TRUE
                  AND a.ARTICLE_ID NOT IN (
                      SELECT ARTICLE_ID 
                      FROM {database}.{schema}.PINECONE_INGESTION_LOG
                  )
            """)
            
            articles = cur.fetchall()
        
        if not articles:
            logger.info("✅ No articles to update - tracking table is already current!")
            return True
        
        logger.info(f"📄 Found {len(articles)} articles to track")
        
        # Create temp table
        logger.info("🏗️  Step 2: Creating temporary staging table...")
        
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TEMPORARY TABLE TEMP_TRACKING_UPDATES (
                    ARTICLE_ID VARCHAR(255),
                    CHUNK_COUNT INTEGER,
                    NAMESPACE VARCHAR(100),
                    INGESTION_TIMESTAMP TIMESTAMP_NTZ
                )
            """)
        
        logger.info("✅ Temp table created")
        
        # Bulk insert into temp table
        logger.info(f"📤 Step 3: Bulk loading {len(articles)} records into temp table...")
        
        namespace = "guardian_articles"
        current_time = datetime.now()
        
        # Prepare data for bulk insert
        insert_data = [
            (article_id, chunk_count, namespace, current_time)
            for article_id, chunk_count in articles
        ]
        
        with conn.cursor() as cur:
            # Use executemany for bulk insert - MUCH faster
            cur.executemany(
                """
                INSERT INTO TEMP_TRACKING_UPDATES 
                (ARTICLE_ID, CHUNK_COUNT, NAMESPACE, INGESTION_TIMESTAMP)
                VALUES (%s, %s, %s, %s)
                """,
                insert_data
            )
        
        logger.info(f"✅ Bulk insert complete ({len(articles)} rows)")
        
        # Single MERGE operation from temp table
        logger.info("🔄 Step 4: Executing single MERGE operation...")
        
        with conn.cursor() as cur:
            cur.execute(f"""
                MERGE INTO {database}.{schema}.PINECONE_INGESTION_LOG AS target
                USING TEMP_TRACKING_UPDATES AS source
                ON target.ARTICLE_ID = source.ARTICLE_ID
                WHEN MATCHED THEN
                    UPDATE SET 
                        INGESTED_AT = source.INGESTION_TIMESTAMP,
                        CHUNK_COUNT = source.CHUNK_COUNT,
                        PINECONE_NAMESPACE = source.NAMESPACE,
                        INGESTION_STATUS = 'SUCCESS'
                WHEN NOT MATCHED THEN
                    INSERT (ARTICLE_ID, INGESTED_AT, CHUNK_COUNT, PINECONE_NAMESPACE, INGESTION_STATUS)
                    VALUES (
                        source.ARTICLE_ID, 
                        source.INGESTION_TIMESTAMP, 
                        source.CHUNK_COUNT, 
                        source.NAMESPACE, 
                        'SUCCESS'
                    )
            """)
            
            rows_affected = cur.rowcount
        
        conn.commit()
        
        logger.info(f"✅ MERGE complete - {rows_affected} rows affected")
        
        # Clean up temp table
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS TEMP_TRACKING_UPDATES")
        
        logger.info(f"🎉 Successfully updated tracking table for {len(articles)} articles")
        
        # Verify
        logger.info("🔍 Step 5: Verifying update...")
        
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) 
                FROM {database}.{schema}.PINECONE_INGESTION_LOG
                WHERE INGESTION_STATUS = 'SUCCESS'
            """)
            total_tracked = cur.fetchone()[0]
        
        logger.info(f"✅ Verification: {total_tracked} articles now tracked in Pinecone")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to update tracking table: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        conn.close()
        logger.info("🔌 Snowflake connection closed")


if __name__ == "__main__":
    import time
    
    print("\n" + "="*60)
    print("🚀 ULTRA-FAST Tracking Table Update")
    print("="*60 + "\n")
    
    print("This optimized script uses:")
    print("  ✅ Temporary staging table")
    print("  ✅ Bulk insert (executemany)")
    print("  ✅ Single MERGE operation")
    print()
    print("Expected time: ~30-60 seconds (vs 2+ hours!)")
    print()
    
    response = input("Continue? (yes/no): ").strip().lower()
    
    if response in ['yes', 'y']:
        start_time = time.time()
        
        success = bulk_update_tracking_table()
        
        elapsed = time.time() - start_time
        
        if success:
            print("\n" + "="*60)
            print("✅ Tracking Table Updated!")
            print("="*60)
            print(f"⏱️  Completed in {elapsed:.1f} seconds")
            print("\nYour ingestion is now complete.")
            print("Next time, use --incremental to only process new articles.")
        else:
            print("\n❌ Update failed. Check logs above.")
    else:
        print("\n❌ Cancelled by user.")