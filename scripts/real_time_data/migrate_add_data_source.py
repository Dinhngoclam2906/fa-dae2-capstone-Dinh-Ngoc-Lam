"""
Migration script to add data_source column to existing staging.raw_data table.

Usage:
    python migrate_add_data_source.py
"""

import os
import psycopg2
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

def is_docker_environment():
    """Check if running inside Docker."""
    return os.path.exists('/.dockerenv') or os.path.exists('/run/.containerenv')

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

def migrate_add_data_source():
    """Add data_source column if it doesn't exist."""
    
    migration_sql = """
        -- Add data_source column if it doesn't exist
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'staging' 
                AND table_name = 'raw_data' 
                AND column_name = 'data_source'
            ) THEN
                ALTER TABLE staging.raw_data 
                ADD COLUMN data_source VARCHAR(20) DEFAULT 'realtime';
                
                -- Update existing rows to set data_source based on web_publication_date
                UPDATE staging.raw_data 
                SET data_source = CASE 
                    WHEN web_publication_date <= '2024-05-11 23:59:59' THEN 'batch'
                    ELSE 'realtime'
                END
                WHERE data_source IS NULL;
                
                -- Create index
                CREATE INDEX IF NOT EXISTS idx_data_source 
                    ON staging.raw_data(data_source);
                
                RAISE NOTICE 'Column data_source added successfully';
            ELSE
                RAISE NOTICE 'Column data_source already exists';
            END IF;
        END $$;
    """
    
    try:
        pg_config = get_postgres_config()
        logger.info(f"🔌 Connecting to PostgreSQL at {pg_config['host']}:{pg_config['port']}")
        
        with psycopg2.connect(**pg_config) as conn:
            with conn.cursor() as cur:
                logger.info("🔄 Running migration...")
                cur.execute(migration_sql)
                conn.commit()
                logger.info("✅ Migration completed successfully!")
                
                # Verify the column exists
                cur.execute("""
                    SELECT column_name, data_type, column_default 
                    FROM information_schema.columns 
                    WHERE table_schema = 'staging' 
                    AND table_name = 'raw_data'
                    ORDER BY ordinal_position
                """)
                
                logger.info("\n📊 Current table schema:")
                for row in cur.fetchall():
                    logger.info(f"   - {row[0]}: {row[1]} (default: {row[2]})")
                
                # Count rows
                cur.execute("SELECT COUNT(*) FROM staging.raw_data")
                count = cur.fetchone()[0]
                logger.info(f"\n📈 Total rows in staging.raw_data: {count:,}")
                
                # Count by data_source
                cur.execute("""
                    SELECT data_source, COUNT(*) 
                    FROM staging.raw_data 
                    GROUP BY data_source
                """)
                logger.info("\n📊 Rows by data_source:")
                for source, cnt in cur.fetchall():
                    emoji = "📚" if source == "batch" else "🔴" if source == "realtime" else "❓"
                    logger.info(f"   {emoji} {source}: {cnt:,}")
                
        return True
        
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    logger.info("🚀 Starting migration: Add data_source column")
    logger.info("="*60)
    
    success = migrate_add_data_source()
    
    logger.info("="*60)
    if success:
        logger.info("✅ Migration completed successfully!")
        logger.info("\n💡 You can now run your Kafka consumer:")
        logger.info("   python scripts/real_time_data/kafka_consumer.py --no-state --once")
    else:
        logger.info("❌ Migration failed! Check the errors above.")