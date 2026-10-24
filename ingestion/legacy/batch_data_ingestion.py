# batch_data_ingestion.py
"""
Load historical Parquet data directly to Snowflake (Airflow-orchestrated batch).

Usage:
    python batch_data_ingestion.py
    
Environment Variables Required:
    - SNOWFLAKE_* : Snowflake connection details
"""

import os
import logging
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


# ==================== SNOWFLAKE CONNECTION ====================

def get_snowflake_connection():
       """Create Snowflake connection"""
       try:
           # Try file path first
           private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH")
           
           if private_key_path and os.path.exists(private_key_path):
               logger.info("🔌 Connecting to Snowflake (using key file)...")
               sf_conn = snowflake.connector.connect(
                   account=os.getenv("SNOWFLAKE_ACCOUNT"),
                   user=os.getenv("SNOWFLAKE_USER"),
                   authenticator="SNOWFLAKE_JWT",
                   private_key_file=private_key_path,
                   warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                   database=os.getenv("SNOWFLAKE_DATABASE"),
                   schema=os.getenv("SNOWFLAKE_SCHEMA"),
                   role=os.getenv("SNOWFLAKE_ROLE"),
                   timezone='Asia/Ho_Chi_Minh'
               )
           else:
               # Fallback: use raw key content from env var
               logger.info("🔌 Connecting to Snowflake (using key content)...")
               from cryptography.hazmat.primitives import serialization
               from cryptography.hazmat.backends import default_backend
               
               key_content = os.getenv("SNOWFLAKE_PRIVATE_KEY_CONTENT")
               if not key_content:
                   raise ValueError("Neither SNOWFLAKE_PRIVATE_KEY_FILE_PATH nor SNOWFLAKE_PRIVATE_KEY_CONTENT set")
               
               private_key = serialization.load_pem_private_key(
                   key_content.encode(),
                   password=None,
                   backend=default_backend()
               )
               
               sf_conn = snowflake.connector.connect(
                   account=os.getenv("SNOWFLAKE_ACCOUNT"),
                   user=os.getenv("SNOWFLAKE_USER"),
                   authenticator="SNOWFLAKE_JWT",
                   private_key=private_key,
                   warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                   database=os.getenv("SNOWFLAKE_DATABASE"),
                   schema=os.getenv("SNOWFLAKE_SCHEMA"),
                   role=os.getenv("SNOWFLAKE_ROLE"),
                   timezone='Asia/Ho_Chi_Minh'
               )
           
           logger.info("✅ Connected to Snowflake")
           return sf_conn
           
       except Exception as e:
           logger.error(f"❌ Failed to connect to Snowflake: {e}")
           raise

# ==================== BATCH LOAD TO SNOWFLAKE ====================

def load_parquet_to_snowflake(parquet_path: Path, sf_conn) -> bool:
    """
    Load historical Parquet data directly into Snowflake RAW_DATA table.
    Uses the SAME MERGE logic as kafka_consumer.py's sync_postgres_to_snowflake().
    
    Args:
        parquet_path: Path to the parquet file
        sf_conn: Active Snowflake connection
        
    Returns:
        True if successful, False otherwise
    """
    temp_parquet = Path("/tmp/guardian_batch_load.parquet")

    HUGGINGFACE_CUTOFF_DATE = '2024-05-11'
    
    try:
        # Read and validate Parquet file
        logger.info(f"📖 Reading Parquet file: {parquet_path}")
        df = pd.read_parquet(parquet_path)
        logger.info(f"📊 Loaded {len(df):,} historical records")
        
        if len(df) == 0:
            logger.warning("⚠️ Parquet file is empty")
            return False
        
        # Validate required columns
        required_cols = [
            'article_id', 'crawl_timestamp', 'web_publication_date',
            'web_title', 'body_text', 'web_url', 'section_name'
        ]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"❌ Missing required columns: {missing_cols}")
            return False

        df['web_publication_date'] = pd.to_datetime(df['web_publication_date'])
        df['data_source'] = df['web_publication_date'].apply(
            lambda x: 'batch' if x.date() <= pd.Timestamp(HUGGINGFACE_CUTOFF_DATE).date() else 'realtime'
        )

        batch_count = (df['data_source'] == 'batch').sum()
        realtime_count = (df['data_source'] == 'realtime').sum()
        logger.info(f"🏷️ Data source tagging (cutoff: {HUGGINGFACE_CUTOFF_DATE}):")
        logger.info(f"   📚 Batch (≤ {HUGGINGFACE_CUTOFF_DATE}): {batch_count:,} articles")
        logger.info(f"   🔴 Real-time (> {HUGGINGFACE_CUTOFF_DATE}): {realtime_count:,} articles")
        
        # Show sample data
        logger.info(f"📋 Column names: {list(df.columns)}")
        logger.info(f"📏 Date range: {df['web_publication_date'].min()} to {df['web_publication_date'].max()}")
        
        # Save to temporary file for upload
        df.to_parquet(temp_parquet, index=False)
        logger.info(f"💾 Saved to temp file: {temp_parquet}")
        
        # Upload to Snowflake and MERGE
        with sf_conn.cursor() as cursor:
            # Set session timezone
            cursor.execute("ALTER SESSION SET TIMEZONE = 'Asia/Ho_Chi_Minh'")
            
            db = os.getenv("SNOWFLAKE_DATABASE")
            schema = os.getenv("SNOWFLAKE_SCHEMA")
            stage = f"@{db}.{schema}.CSV_STAGE"
            table = f"{db}.{schema}.RAW_DATA"
            
            logger.info(f"🎯 Target table: {table}")
            
            # Use schema
            cursor.execute(f"USE SCHEMA {db}.{schema}")
            
            # Create file format if not exists (same as consumer)
            cursor.execute("""
                CREATE FILE FORMAT IF NOT EXISTS MY_PARQUET_FORMAT 
                TYPE = PARQUET
            """)
            logger.info("✅ File format verified")
            
            # Clear stage
            cursor.execute(f"REMOVE {stage}")
            logger.info(f"🧹 Cleared stage: {stage}")
            
            # Upload Parquet to stage
            put_cmd = f"PUT file://{temp_parquet.absolute()} {stage}/ OVERWRITE=TRUE PARALLEL=4"
            logger.info(f"📤 Uploading {len(df):,} rows to Snowflake stage...")
            cursor.execute(put_cmd)
            logger.info("✅ Upload complete")
            
            # MERGE into target table (EXACT SAME LOGIC as consumer)
            logger.info("🔄 Starting MERGE operation...")
            merge_sql = f"""
                MERGE INTO {table} AS target
                USING (
                    SELECT
                        $1:article_id::STRING AS article_id,
                        TRY_TO_TIMESTAMP_NTZ($1:crawl_timestamp::STRING) AS crawl_timestamp,
                        TRY_TO_TIMESTAMP_NTZ($1:web_publication_date::STRING) AS web_publication_date,
                        $1:web_title::STRING AS web_title,
                        $1:body_text::STRING AS body_text,
                        $1:web_url::STRING AS web_url,
                        $1:section_name::STRING AS section_name,
                        $1:data_source::STRING AS data_source,
                        CURRENT_TIMESTAMP() AS loaded_at
                    FROM {stage}/{temp_parquet.name}
                    (FILE_FORMAT => 'MY_PARQUET_FORMAT')
                ) AS source
                ON target.article_id = source.article_id
                WHEN MATCHED THEN
                    UPDATE SET
                        target.data_source = source.data_source,
                        target.loaded_at = source.loaded_at
                WHEN NOT MATCHED THEN
                    INSERT (
                        article_id, crawl_timestamp, web_publication_date,
                        web_title, body_text, web_url, section_name,
                        data_source, loaded_at
                    )
                    VALUES (
                        source.article_id, source.crawl_timestamp, source.web_publication_date,
                        source.web_title, source.body_text, source.web_url,
                        source.section_name, source.data_source, source.loaded_at
                    )
            """
            
            cursor.execute(merge_sql)
            logger.info(f"✅ MERGE completed - {len(df):,} rows processed")
            
            # Get final statistics
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total_count = cursor.fetchone()[0]
            logger.info(f"📊 Total rows in Snowflake RAW_DATA: {total_count:,}")
            
            cursor.execute(f"""
                SELECT data_source, COUNT(*) 
                FROM {table} 
                GROUP BY data_source
                ORDER BY data_source
            """)
            breakdown = cursor.fetchall()
            logger.info("📊 Data source breakdown:")
            for source, count in breakdown:
                emoji = "📚" if source == "batch" else "🔴" if source == "realtime" else "❓"
                logger.info(f"   {emoji} {source}: {count:,} rows")
                
            # Clean up stage
            cursor.execute(f"REMOVE {stage}")
            logger.info("🧹 Cleaned up stage")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to load to Snowflake: {e}", exc_info=True)
        return False
    
    finally:
        # Clean up temp file
        if temp_parquet.exists():
            temp_parquet.unlink()
            logger.debug("🗑️ Removed temp file")


# ==================== MAIN ====================

def main():
    """
    Main function for Airflow-orchestrated batch load.
    
    Flow:
    1. Read historical Parquet file
    2. Upload directly to Snowflake (no Kafka, no PostgreSQL)
    3. MERGE using same logic as real-time consumer
    """
    logger.info("🚀 Historical Batch Data Loader Starting...")
    logger.info("=" * 70)
    logger.info("📋 Architecture: Parquet → Snowflake (Direct)")
    logger.info("🔧 Orchestrated by: Airflow")
    logger.info("=" * 70)
    
    # Path to historical Parquet (from batch_data_collector.py)
    parquet_path = Path("/opt/airflow/project/data/batch/guardian_historical_articles.parquet")
    
    # Validate file exists
    if not parquet_path.exists():
        logger.error(f"❌ Historical Parquet not found: {parquet_path}")
        logger.info("💡 Run batch_data_collector.py first to generate it.")
        return False
    
    logger.info(f"📂 Found historical data: {parquet_path.name}")
    logger.info(f"📏 File size: {parquet_path.stat().st_size / 1024 / 1024:.2f} MB")
    
    # Connect to Snowflake
    try:
        sf_conn = get_snowflake_connection()
    except Exception as e:
        logger.error(f"❌ Cannot proceed without Snowflake connection")
        return False
    
    # Load data
    try:
        logger.info("\n" + "=" * 70)
        logger.info("📤 Starting Parquet → Snowflake load")
        logger.info("=" * 70)
        
        success = load_parquet_to_snowflake(parquet_path, sf_conn)
        
        if success:
            logger.info("\n" + "=" * 70)
            logger.info("✅ Historical batch data successfully loaded to Snowflake")
            logger.info("=" * 70)
            return True
        else:
            logger.error("\n" + "=" * 70)
            logger.error("❌ Historical load failed")
            logger.error("=" * 70)
            return False
            
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}", exc_info=True)
        return False
    
    finally:
        # Always close connection
        try:
            sf_conn.close()
            logger.info("🔌 Closed Snowflake connection")
        except:
            pass


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)