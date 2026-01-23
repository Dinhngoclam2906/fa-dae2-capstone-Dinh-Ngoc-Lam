# CI Demo
"""
Guardian Kafka Consumer → PostgreSQL → Snowflake
Consumes articles from Kafka, upserts to PostgreSQL, and periodically syncs to Snowflake.

Usage:
    python guardian_kafka_consumer.py
    python guardian_kafka_consumer.py --once  # Process available messages and exit
    python guardian_kafka_consumer.py --once --max-messages 100  # Process max 100 messages
    python guardian_kafka_consumer.py --no-state  # Skip incremental sync (re-sync all data)
    
Environment Variables Required:
    - KAFKA_BOOTSTRAP_SERVERS: Kafka broker address
    - KAFKA_TOPIC: Topic name for articles
    - POSTGRES_* : PostgreSQL connection details
    - SNOWFLAKE_* : Snowflake connection details
"""

import json
import os
import time
from datetime import datetime, timezone 
from pathlib import Path

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
import pandas as pd
import snowflake.connector

# Configure logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ==================== CONFIGURATION ====================

load_dotenv()

# Environment detection
def is_docker_environment():
    """Check if running inside Docker."""
    return os.path.exists('/.dockerenv') or os.path.exists('/run/.containerenv')

def get_kafka_bootstrap_servers():
    """Get Kafka servers based on environment."""
    configured = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    if is_docker_environment():
        return configured
    return "localhost:29092"

def get_postgres_config():
    """Get PostgreSQL config based on environment."""
    is_docker = is_docker_environment()
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres-staging") if is_docker else "localhost",
        "port": int(os.getenv("POSTGRES_PORT", "5432")) if is_docker else 5434,
        "dbname": os.getenv("POSTGRES_DB", "staging_db"),
        "user": os.getenv("POSTGRES_USER", "staging_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "staging_password"),
    }

# Kafka
KAFKA_BOOTSTRAP_SERVERS = get_kafka_bootstrap_servers()
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_articles")
KAFKA_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "guardian-snowflake-sync")

# Batching configuration
BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", 50))  # Increased from 10 to 50
BATCH_TIMEOUT_MINUTES = int(os.getenv("KAFKA_BATCH_TIMEOUT_MINUTES", 60))

# Track last synced timestamp to avoid re-syncing same data
_last_sync_timestamp = None

_sf_connection = None
_sf_connection_time = None

def get_or_create_sf_connection():
    """
    Reuse Snowflake connection instead of creating new one each sync.
    Recreate connection every 30 minutes to avoid timeouts.
    """
    global _sf_connection, _sf_connection_time
    
    # Check if we need new connection (first time or 30 min old)
    now = datetime.now()
    needs_new = (
        _sf_connection is None or 
        _sf_connection_time is None or
        (now - _sf_connection_time).total_seconds() > 1800  # 30 minutes
    )
    
    if needs_new:
        if _sf_connection:
            try:
                _sf_connection.close()
            except:
                pass
        
        private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH") or os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE")
        if not private_key_path or not os.path.exists(private_key_path):
            logger.warning(f"⚠️ Snowflake key not found: {private_key_path}")
            return None
        
        _sf_connection = snowflake.connector.connect(
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
        _sf_connection_time = now
        logger.info("🔌 Created new Snowflake connection")
    
    return _sf_connection

# ==================== DATABASE SETUP ====================

def ensure_staging_table():
    """Create staging.raw_data table if it doesn't exist."""
    create_sql = """
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE TABLE IF NOT EXISTS staging.raw_data (
            crawl_timestamp TIMESTAMP,
            article_id VARCHAR(255) PRIMARY KEY,
            web_publication_date TIMESTAMP,
            web_title TEXT,
            body_text TEXT,
            web_url VARCHAR(500),
            section_name VARCHAR(255),
            data_source VARCHAR(20) DEFAULT 'realtime',
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_crawl_timestamp 
            ON staging.raw_data(crawl_timestamp);
        CREATE INDEX IF NOT EXISTS idx_web_publication_date 
            ON staging.raw_data(web_publication_date);
        CREATE INDEX IF NOT EXISTS idx_data_source 
            ON staging.raw_data(data_source);
    """
    
    try:
        pg_config = get_postgres_config()
        with psycopg2.connect(**pg_config) as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                conn.commit()
        logger.info("✅ staging.raw_data table verified")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to create table: {e}")
        return False


def upsert_article_to_postgres(article_data: dict) -> tuple[bool, str]:
    """
    Upsert a single article to PostgreSQL.
    
    Args:
        article_data: Article dictionary from Kafka message
    
    Returns:
        True if successful, False otherwise
    """

    try:
        # Parse timestamps
        crawl_timestamp = datetime.fromisoformat(
            article_data['crawl_timestamp'].replace('Z', '+00:00')
        )
        web_publication_date = datetime.fromisoformat(
            article_data['web_publication_date'].replace('Z', '+00:00')
        )
        
        data_source = 'realtime'

        check_sql = "SELECT 1 FROM staging.raw_data WHERE article_id = %s"

        upsert_sql = """
            INSERT INTO staging.raw_data (
                crawl_timestamp, article_id, web_publication_date,
                web_title, body_text, web_url, section_name, data_source
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (article_id) DO UPDATE SET
                crawl_timestamp = EXCLUDED.crawl_timestamp,
                web_publication_date = EXCLUDED.web_publication_date,
                web_title = EXCLUDED.web_title,
                body_text = EXCLUDED.body_text,
                web_url = EXCLUDED.web_url,
                section_name = EXCLUDED.section_name,
                data_source = EXCLUDED.data_source,
                loaded_at = CURRENT_TIMESTAMP
        """
        
        pg_config = get_postgres_config()
        with psycopg2.connect(**pg_config) as conn:
            with conn.cursor() as cur:

                cur.execute(check_sql, (article_data['article_id'],))
                exists = cur.fetchone() is not None

                cur.execute(upsert_sql, (
                    crawl_timestamp,
                    article_data['article_id'],
                    web_publication_date,
                    article_data['web_title'],
                    article_data['body_text'],
                    article_data['web_url'],
                    article_data['section_name'],
                    data_source
                ))
                conn.commit()

        action = 'updated' if exists else 'inserted'
        return True, action
        
    except Exception as e:
        logger.error(f"❌ Failed to upsert article: {e}")
        return False, 'error'


# ==================== SNOWFLAKE SYNC ====================

def sync_postgres_to_snowflake(no_state_mode: bool = False) -> bool:
    """
    Sync NEW data from PostgreSQL staging.raw_data to Snowflake (incremental MERGE).
    Only syncs rows that have been inserted/updated since the last sync.
    
    Args:
        no_state_mode: If True, sync all data (ignore incremental tracking)
    """
    global _last_sync_timestamp
    temp_parquet = Path("/tmp/guardian_sync.parquet")
    
    try:
        logger.info("🔄 Starting PostgreSQL → Snowflake incremental sync...")
        
        # Build query to get only new/updated rows
        if no_state_mode or _last_sync_timestamp is None:
            # First sync or no-state mode - get all rows
            query = "SELECT * FROM staging.raw_data"
            if no_state_mode:
                logger.info("📊 NO-STATE MODE: Syncing all rows")
            else:
                logger.info("📊 First sync - fetching all rows")
        else:
            # Incremental sync - only rows updated since last sync
            query = f"""
                SELECT * FROM staging.raw_data 
                WHERE loaded_at > '{_last_sync_timestamp}'
            """
            logger.info(f"📊 Incremental sync - fetching rows since {_last_sync_timestamp}")
        
        # Export PostgreSQL data to temp Parquet
        pg_config = get_postgres_config()
        with psycopg2.connect(**pg_config) as conn:
            df = pd.read_sql(query, conn)
            
            # Get current timestamp FROM PostgreSQL (not Python) for next checkpoint
            # This ensures we use the same timezone as the loaded_at column
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_TIMESTAMP")
            current_sync_time = cursor.fetchone()[0]
            cursor.close()
        
        if len(df) == 0:
            logger.info("📭 No new data to sync")
            # CRITICAL: Update timestamp even when no data, otherwise we'll keep checking the same range!
            if not no_state_mode:
                _last_sync_timestamp = current_sync_time
            return True

        if 'data_source' in df.columns:
            source_counts = df['data_source'].value_counts()
            logger.info("📊 Syncing data breakdown:")
            for source, count in source_counts.items():
                emoji = "📚" if source == "batch" else "🔴" if source == "realtime" else "❓"
                logger.info(f"   {emoji} {source}: {count:,} articles")
        
        df.to_parquet(temp_parquet, index=False)
        logger.info(f"📦 Exported {len(df)} new rows to temp Parquet")
        
        # Connect to Snowflake
        sf_conn = get_or_create_sf_connection()
        if not sf_conn:
            logger.warning("💡 Skipping Snowflake sync (PostgreSQL data is safe)")
            return True
        
        with sf_conn.cursor() as cursor:
            cursor.execute("ALTER SESSION SET TIMEZONE = 'Asia/Ho_Chi_Minh'")
            db = os.getenv("SNOWFLAKE_DATABASE")
            schema = os.getenv("SNOWFLAKE_SCHEMA")
            
            # Create file format if not exists
            cursor.execute("""
                CREATE FILE FORMAT IF NOT EXISTS MY_PARQUET_FORMAT 
                TYPE = PARQUET
            """)
            
            stage = f"@{db}.{schema}.CSV_STAGE"
            table = f"{db}.{schema}.RAW_DATA"
            
            cursor.execute(f"USE SCHEMA {db}.{schema}")
            cursor.execute(f"REMOVE {stage}")
            
            # Upload Parquet
            put_cmd = f"PUT file://{temp_parquet.absolute()} {stage}/ OVERWRITE=TRUE PARALLEL=4"
            cursor.execute(put_cmd)
            logger.info(f"📤 Uploaded {len(df)} rows to Snowflake stage")
            
            # MERGE
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
            logger.info(f"✅ MERGE completed - {len(df)} rows processed")
            
            # Update last sync timestamp AFTER successful sync (unless in no-state mode)
            if not no_state_mode:
                _last_sync_timestamp = current_sync_time
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Snowflake sync failed: {e}", exc_info=True)
        return False
    
    finally:
        if temp_parquet.exists():
            temp_parquet.unlink()


# ==================== MAIN CONSUMER LOOP ====================

def main():
    """Main consumer loop with batching and periodic Snowflake sync."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Guardian Kafka Consumer")
    parser.add_argument(
        '--once',
        action='store_true',
        help='Process available messages once and exit (for testing)'
    )
    parser.add_argument(
        '--no-state',
        action='store_true',
        help='Skip incremental sync tracking - sync all data to Snowflake (DEMO MODE ONLY)'
    )
    parser.add_argument(
        '--max-messages',
        type=int,
        default=None,
        help='Maximum messages to process before exiting (requires --once)'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=10,
        help='Timeout in seconds when polling for messages in --once mode (default: 10)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.max_messages and not args.once:
        logger.error("❌ --max-messages requires --once flag")
        return
    
    logger.info("🚀 Guardian Kafka Consumer Starting...")
    logger.info(f"📡 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"📰 Topic: {KAFKA_TOPIC}")
    logger.info(f"👥 Group ID: {KAFKA_GROUP_ID}")
    logger.info(f"📦 Batch size: {BATCH_SIZE} messages (sync to Snowflake after each batch)")
    
    if args.once:
        logger.info(f"🔄 Mode: One-time run (will exit after processing available messages)")
        logger.info(f"⏱️  Timeout: {args.timeout}s of no messages")
        if args.max_messages:
            logger.info(f"📊 Max messages: {args.max_messages}")
    else:
        logger.info(f"🔄 Mode: Continuous consumption")
        logger.info(f"⚡ Incremental sync: Only new rows will be uploaded to Snowflake")
    
    if args.no_state:
        logger.info("⚠️  DEMO MODE: Incremental sync disabled - will sync ALL data to Snowflake!")
        global _last_sync_timestamp
        _last_sync_timestamp = None  # Reset state to force full sync
    
    # Ensure staging table exists
    if not ensure_staging_table():
        logger.error("❌ Failed to initialize staging table!")
        return
    
    # Configure Kafka consumer
    consumer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
        "max.poll.interval.ms": 300000,  # 5 minutes
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([KAFKA_TOPIC])
    logger.info(f"✅ Subscribed to topic: {KAFKA_TOPIC}")
    
    messages_processed = 0
    rows_written = 0
    batch_count = 0
    last_sync_time = datetime.now()
    consecutive_empty_polls = 0
    max_empty_polls = args.timeout if args.once else 999999  # Only enforce timeout in --once mode
    
    try:
        logger.info("🔄 Starting consumption loop...")
        
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                consecutive_empty_polls += 1
                
                # In --once mode, exit after timeout seconds of no messages
                if args.once and consecutive_empty_polls >= max_empty_polls:
                    logger.info(f"⏱️  No messages for {args.timeout}s - exiting")
                    break
                
                continue
            
            # Reset empty poll counter when we receive a message
            consecutive_empty_polls = 0
            
            # Check for errors
            error = msg.error()
            if error is not None:
                from confluent_kafka import KafkaError as KE
                if error.code() == KE._PARTITION_EOF: # type: ignore
                    logger.debug("Reached end of partition")
                    # In --once mode, treat EOF as signal to exit
                    if args.once:
                        logger.info("📭 Reached end of partition - exiting")
                        break
                else:
                    logger.error(f"Kafka error: {error}")
                continue
            
            try:
                # Decode message
                msg_value = msg.value()
                if msg_value is None:
                    logger.warning("⚠️ Received message with None value, skipping")
                    continue
                    
                article_data = json.loads(msg_value.decode('utf-8'))
                messages_processed += 1
                
                # Validate required fields
                required = [
                    'article_id', 'crawl_timestamp', 'web_publication_date',
                    'web_title', 'body_text', 'web_url', 'section_name'
                ]
                if not all(k in article_data for k in required):
                    logger.warning("⚠️ Skipping invalid message (missing fields)")
                    continue
                
                # Upsert to PostgreSQL
                success, action = upsert_article_to_postgres(article_data)
                if success:
                    rows_written += 1
                    
                    # Show different emoji/message for INSERT vs UPDATE
                    emoji = "🆕" if action == 'inserted' else "🔄"
                    action_text = "New" if action == 'inserted' else "Updated"
                    
                    logger.info(
                        f"{emoji} [{rows_written}] {action_text}: "
                        f"{article_data['article_id'][:50]}..."
                    )
                
                # Check if we should sync to Snowflake
                should_sync = (
                    rows_written > 0 and 
                    rows_written % BATCH_SIZE == 0
                )
                
                if should_sync:
                    batch_count += 1
                    logger.info(f"\n{'='*60}")
                    logger.info(f"📊 Batch #{batch_count} complete ({BATCH_SIZE} messages)")
                    logger.info(f"🔄 Triggering Snowflake sync...")
                    logger.info(f"{'='*60}\n")
                    
                    if sync_postgres_to_snowflake(no_state_mode=args.no_state):
                        logger.info("✅ Snowflake sync successful")
                        last_sync_time = datetime.now()
                    else:
                        logger.warning("⚠️ Snowflake sync failed (data safe in PostgreSQL)")
                
                # Check if we've reached max messages (only in --once mode)
                if args.once and args.max_messages and messages_processed >= args.max_messages:
                    logger.info(f"🎯 Reached max messages limit ({args.max_messages})")
                    break
                
                # Progress logging
                if messages_processed % 100 == 0:
                    logger.info(
                        f"📈 Progress: {messages_processed} processed, "
                        f"{rows_written} written"
                    )
                    
            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON decode error: {e}")
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}", exc_info=True)
    
    except KeyboardInterrupt:
        logger.info("\n⚠️ Shutting down gracefully...")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}", exc_info=True)
    finally:
        # Final stats
        logger.info(
            f"\n{'='*60}\n"
            f"📊 Final stats:\n"
            f"   - Messages processed: {messages_processed}\n"
            f"   - Rows written: {rows_written}\n"
            f"   - Snowflake syncs: {batch_count}\n"
            f"{'='*60}"
        )
        
        # Final sync before shutdown (if there are unsynced rows)
        if rows_written > 0 and rows_written % BATCH_SIZE != 0:
            logger.info("🔄 Final Snowflake sync before shutdown...")
            sync_postgres_to_snowflake(no_state_mode=args.no_state)
        
        # Clean up Snowflake connection
        global _sf_connection
        if _sf_connection:
            try:
                _sf_connection.close()
                logger.info("🔌 Closed Snowflake connection")
            except:
                pass

        consumer.close()
        logger.info("🏁 Consumer shutdown complete")


if __name__ == "__main__":
    main()