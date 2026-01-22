"""
Guardian API → Kafka Producer (Continuous Real-Time)
Continuously crawls new articles from Guardian API and streams them to Kafka.
No intermediate file storage - true real-time streaming.

Usage:
    python guardian_kafka_producer.py
    
Environment Variables Required:
    - API_KEY: Guardian API key
    - KAFKA_BOOTSTRAP_SERVERS: Kafka broker address
    - KAFKA_TOPIC: Topic name for articles
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Set, Optional
from dataclasses import dataclass, asdict

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql

# Configure logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ==================== CONFIGURATION ====================

load_dotenv()

# Guardian API
API_URL = os.getenv("API_URL", "https://content.guardianapis.com")
API_KEY = os.getenv("API_KEY")
PAGE_SIZE = int(os.getenv("INGESTION_BATCH_SIZE", 50))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 300))  # 5 minutes

# Kafka
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

KAFKA_BOOTSTRAP_SERVERS = get_kafka_bootstrap_servers()
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_articles")


# ==================== DATA MODEL ====================

@dataclass
class GuardianArticle:
    """Article data model matching your PostgreSQL schema."""
    article_id: str
    crawl_timestamp: str
    web_publication_date: str
    web_title: str
    body_text: str
    web_url: str
    section_name: str
    
    def to_dict(self):
        """Convert to dictionary for Kafka serialization."""
        return asdict(self)


# ==================== STATE MANAGEMENT ====================

def ensure_state_tracking_table():
    """Create table to track processed articles (deduplication)."""
    create_sql = """
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE TABLE IF NOT EXISTS staging.producer_state (
            article_id VARCHAR(255) PRIMARY KEY,
            first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            sent_to_kafka_at TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_sent_to_kafka 
            ON staging.producer_state(sent_to_kafka_at);
    """
    
    try:
        pg_config = get_postgres_config()
        with psycopg2.connect(**pg_config) as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                conn.commit()
        logger.info("✅ State tracking table verified")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to create state table: {e}")
        return False


def get_processed_article_ids() -> Set[str]:
    """Get set of already processed article IDs from PostgreSQL."""
    try:
        pg_config = get_postgres_config()
        with psycopg2.connect(**pg_config) as conn:
            with conn.cursor() as cur:
                # Get articles from last 30 days to keep memory manageable
                cur.execute("""
                    SELECT article_id 
                    FROM staging.producer_state 
                    WHERE sent_to_kafka_at > NOW() - INTERVAL '30 days'
                """)
                article_ids = {row[0] for row in cur.fetchall()}
                logger.info(f"📚 Loaded {len(article_ids)} processed article IDs from state")
                return article_ids
    except Exception as e:
        logger.warning(f"⚠️ Failed to load state, starting fresh: {e}")
        return set()


def mark_article_as_sent(article_id: str):
    """Mark an article as sent to Kafka in state tracking."""
    try:
        pg_config = get_postgres_config()
        with psycopg2.connect(**pg_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO staging.producer_state (article_id, sent_to_kafka_at)
                    VALUES (%s, CURRENT_TIMESTAMP)
                    ON CONFLICT (article_id) DO UPDATE 
                    SET sent_to_kafka_at = CURRENT_TIMESTAMP
                """, (article_id,))
                conn.commit()
    except Exception as e:
        logger.warning(f"⚠️ Failed to update state for {article_id}: {e}")


# ==================== KAFKA SETUP ====================

def delivery_callback(err, msg):
    """Callback for Kafka message delivery confirmation."""
    if err:
        logger.error(f"❌ Delivery failed: {err}")
    else:
        # Extract article_id from message for state tracking
        try:
            data = json.loads(msg.value().decode('utf-8'))
            article_id = data.get('article_id')
            # Only update state if not in demo mode
            if article_id and not getattr(delivery_callback, 'no_state_mode', False):
                mark_article_as_sent(article_id)
            logger.debug(
                f"✅ Delivered: {msg.topic()}[{msg.partition()}] "
                f"@ offset {msg.offset()}"
            )
        except Exception as e:
            logger.warning(f"⚠️ Could not update state after delivery: {e}")


def create_kafka_topic(bootstrap_servers: str, topic_name: str) -> bool: # type: ignore
    """Create Kafka topic if it doesn't exist."""
    try:
        admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
        
        # Check if topic exists
        metadata = admin_client.list_topics(timeout=10)
        if topic_name in metadata.topics:
            logger.info(f"✅ Topic '{topic_name}' already exists")
            return True
        
        # Create new topic
        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=3,  # Multiple partitions for parallelism
            replication_factor=1
        )
        
        fs = admin_client.create_topics([new_topic])
        
        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f"✅ Topic '{topic}' created successfully")
                return True
            except Exception as e:
                logger.error(f"❌ Failed to create topic '{topic}': {e}")
                return False
                
    except Exception as e:
        logger.error(f"❌ Error creating topic: {e}")
        return False


# ==================== GUARDIAN API CRAWLER ====================

def fetch_recent_articles(
    from_date: str,
    processed_ids: Set[str],
    max_pages: int = 10,
    max_articles: int = 100
) -> list[GuardianArticle]:
    """
    Fetch recent articles from Guardian API.
    
    Args:
        from_date: Start date in YYYY-MM-DD format
        processed_ids: Set of already processed article IDs
        max_pages: Maximum pages to fetch per poll
        max_articles: Maximum number of new articles to fetch
    
    Returns:
        List of new articles not yet processed
    """
    if not API_KEY:
        raise ValueError("API_KEY not set in environment")
    
    new_articles = []
    page = 1
    
    while page <= max_pages and len(new_articles) < max_articles:
        params = {
            'api-key': API_KEY,
            'page': page,
            'page-size': PAGE_SIZE,
            'from-date': from_date,
            'show-fields': 'bodyText',
            'order-by': 'newest'
        }
        
        try:
            logger.info(f"📄 Fetching page {page} from {from_date}...")
            response = requests.get(
                f"{API_URL}/search",
                params=params,
                timeout=30
            )
            
            if response.status_code == 429:
                logger.warning("⏸️ Rate limited! Waiting 60 seconds...")
                time.sleep(60)
                continue
            
            # Handle 400 Bad Request (usually means no more pages)
            if response.status_code == 400:
                logger.info("✅ No more pages available")
                break
            
            response.raise_for_status()
            data = response.json()
            results = data['response']['results']
            
            if not results:
                logger.info("✅ No more articles found")
                break
            
            # Process results
            for item in results:
                # Stop if we've reached max articles
                if len(new_articles) >= max_articles:
                    logger.info(f"🎯 Reached max articles limit ({max_articles})")
                    break
                    
                article_id = item.get('id', '')
                
                # Skip if already processed
                if article_id in processed_ids:
                    logger.debug(f"⏭️ Skipping already processed: {article_id}")
                    continue
                
                article = GuardianArticle(
                    article_id=article_id,
                    crawl_timestamp=datetime.now().isoformat(),
                    web_publication_date=item.get('webPublicationDate', ''),
                    web_title=item.get('webTitle', ''),
                    body_text=item.get('fields', {}).get('bodyText', ''),
                    web_url=item.get('webUrl', ''),
                    section_name=item.get('sectionName', '')
                )
                
                new_articles.append(article)
            
            page += 1
            time.sleep(1)  # Rate limiting
            
        except Exception as e:
            logger.error(f"❌ Request failed: {e}")
            break
    
    return new_articles


# ==================== MAIN PRODUCER LOOP ====================

def main():
    """Main producer loop - continuous streaming."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Guardian Kafka Producer")
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run once and exit (for testing)'
    )
    parser.add_argument(
        '--max-articles',
        type=int,
        default=500,
        help='Maximum articles to fetch per poll (default: 500)'
    )
    parser.add_argument(
        '--from-days-ago',
        type=int,
        default=7,
        help='Fetch articles from N days ago (default: 7)'
    )
    parser.add_argument(
        '--no-state',
        action='store_true',
        help='Ignore state tracking - re-send all articles (DEMO MODE ONLY)'
    )

    args = parser.parse_args()
    
    logger.info("🚀 Guardian Kafka Producer Starting...")
    logger.info(f"📡 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"📰 Topic: {KAFKA_TOPIC}")
    if args.once:
        logger.info(f"🔄 Mode: One-time run")
    else:
        logger.info(f"⏱️ Poll interval: {POLL_INTERVAL_SECONDS}s")
    logger.info(f"📊 Max articles per poll: {args.max_articles}")
    logger.info(f"📅 Lookback period: {args.from_days_ago} days")
    
    # Validate configuration
    if not API_KEY:
        logger.error("❌ API_KEY not set in environment!")
        return
    
    # Ensure state tracking
    if not ensure_state_tracking_table():
        logger.error("❌ Failed to initialize state tracking!")
        return
    
    # Create Kafka topic
    if not create_kafka_topic(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC):
        logger.error("❌ Failed to create Kafka topic!")
        return
    
    # Configure Kafka producer
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "guardian-api-producer",
        "acks": "all",
        "retries": 5,
        "batch.size": 16384,
        "linger.ms": 10,
        "compression.type": "snappy"
    })
    
    # Load initial state (or skip for demo)
    if args.no_state:
        logger.info("⚠️  DEMO MODE: State tracking disabled - will re-send all articles!")
        processed_ids = set()  # Empty set = no articles marked as processed
    else:
        processed_ids = get_processed_article_ids()
    
    message_count = 0
    poll_count = 0
    
    try:
        logger.info("🔄 Starting polling loop...")
        
        while True:
            poll_count += 1
            logger.info(f"\n{'='*60}")
            logger.info(f"📊 Poll #{poll_count} - Total messages sent: {message_count}")
            logger.info(f"{'='*60}")
            
            # Calculate from_date based on lookback period
            from_date = (datetime.now() - timedelta(days=args.from_days_ago)).strftime("%Y-%m-%d")
            
            # Fetch new articles
            new_articles = fetch_recent_articles(
                from_date=from_date,
                processed_ids=processed_ids,
                max_pages=10,
                max_articles=args.max_articles
            )
            
            if not new_articles:
                logger.info("📭 No new articles in this poll")
            else:
                logger.info(f"📬 Found {len(new_articles)} new articles")
                
                # Send to Kafka
                for article in new_articles:
                    try:
                        producer.produce(
                            KAFKA_TOPIC,
                            key=article.article_id.encode('utf-8'),
                            value=json.dumps(article.to_dict()).encode('utf-8'),
                            callback=delivery_callback
                        )
                        
                        # Trigger delivery reports
                        producer.poll(0)
                        
                        # Add to processed set (will be persisted by callback)
                        processed_ids.add(article.article_id)
                        message_count += 1
                        
                        logger.info(
                            f"📤 Sent [{message_count}]: {article.article_id[:50]}..."
                        )
                        
                    except Exception as e:
                        logger.error(f"❌ Failed to send article: {e}")
                
                # Flush messages
                logger.info("⏳ Flushing producer...")
                producer.flush(timeout=30)
                logger.info(f"✅ Batch complete - {len(new_articles)} articles sent")
            
            # If running once, exit now
            if args.once:
                logger.info(f"✅ One-time run complete. Total sent: {message_count}")
                break
            
            # Wait before next poll
            logger.info(f"😴 Sleeping for {POLL_INTERVAL_SECONDS}s until next poll...")
            time.sleep(POLL_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        logger.info("\n⚠️ Shutting down gracefully...")
    except Exception as e:
        logger.error(f"\n❌ Unexpected error: {e}", exc_info=True)
    finally:
        logger.info("⏳ Flushing remaining messages...")
        producer.flush(timeout=30)
        logger.info(f"🏁 Producer shutdown complete. Total messages: {message_count}")


if __name__ == "__main__":
    main()