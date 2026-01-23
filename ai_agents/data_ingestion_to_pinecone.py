"""
Incremental ingestion pipeline with parallel NLTK chunking.
Only processes new/updated articles since last ingestion.

FIXES:
- Added batched updates to tracking table (100 articles per batch)
- Added timeout handling for search optimization check
- Improved logging and progress tracking
- Better error recovery
"""

import os
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import re
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing

from dotenv import load_dotenv
import snowflake.connector
from tenacity import retry, stop_after_attempt, wait_exponential
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from langchain_core.documents import Document
import nltk

# Download NLTK data (run once)
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global connection cache
_sf_conn = None


# ==================== SNOWFLAKE CONNECTION ====================

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_sf_connection():
    """Get or create Snowflake connection with retry logic."""
    global _sf_conn
    if _sf_conn is not None:
        try:
            _sf_conn.cursor().execute("SELECT 1")
            return _sf_conn
        except:
            logger.warning("⚠️  Cached connection is stale, reconnecting...")
            _sf_conn = None

    container_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE")
    local_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH")
    private_key_path = container_key_path or local_key_path

    if not private_key_path:
        raise ValueError("Snowflake private key path not set in environment")

    if not os.path.exists(private_key_path):
        raise FileNotFoundError(f"Snowflake private key not found: {private_key_path}")

    _sf_conn = snowflake.connector.connect(
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
    return _sf_conn


# ==================== FAST SEMANTIC CHUNKING (NLTK) ====================

def fast_semantic_chunk(text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
    """
    Fast semantic chunking using NLTK sentence tokenizer.
    ~3-4x faster than spaCy with good-enough quality for RAG.
    """
    sentences = nltk.sent_tokenize(text)
    
    chunks = []
    current_chunk = []
    current_length = 0
    
    for sentence in sentences:
        sentence_length = len(sentence)
        
        # Start new chunk if adding this sentence exceeds limit
        if current_length + sentence_length > chunk_size and current_chunk:
            chunks.append(" ".join(current_chunk))
            
            # Keep last N sentences for overlap
            if overlap > 0 and current_chunk:
                # Estimate how many sentences fit in overlap
                overlap_sentences = []
                overlap_len = 0
                for s in reversed(current_chunk):
                    if overlap_len + len(s) <= overlap:
                        overlap_sentences.insert(0, s)
                        overlap_len += len(s)
                    else:
                        break
                current_chunk = overlap_sentences
                current_length = overlap_len
            else:
                current_chunk = []
                current_length = 0
        
        current_chunk.append(sentence)
        current_length += sentence_length
    
    # Add remaining chunk
    if current_chunk:
        chunks.append(" ".join(current_chunk))
    
    return chunks


def improved_semantic_chunk(text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
    """
    Improved semantic chunking that respects document structure.
    
    Strategy:
    1. Split by paragraphs first (preserves article structure)
    2. If paragraph fits in chunk, add it whole
    3. If paragraph too large, fall back to sentence splitting
    4. Maintain overlap between chunks for context continuity
    """
    import re
    
    # Split into paragraphs (double newline or more)
    paragraphs = re.split(r'\n\s*\n', text)
    
    chunks = []
    current_chunk = []
    current_length = 0
    
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        
        para_length = len(para)
        
        # Case 1: Paragraph fits in current chunk
        if current_length + para_length <= chunk_size:
            current_chunk.append(para)
            current_length += para_length + 2  # +2 for paragraph spacing
        
        # Case 2: Paragraph too big for chunk, need to finalize current chunk
        elif current_chunk:
            # Save current chunk
            chunks.append("\n\n".join(current_chunk))
            
            # Handle overlap (keep last paragraph if it fits)
            if overlap > 0 and current_chunk:
                last_para = current_chunk[-1]
                if len(last_para) <= overlap:
                    current_chunk = [last_para]
                    current_length = len(last_para)
                else:
                    current_chunk = []
                    current_length = 0
            else:
                current_chunk = []
                current_length = 0
            
            # Now handle the paragraph that didn't fit
            if para_length <= chunk_size:
                # Paragraph fits in new chunk
                current_chunk.append(para)
                current_length = para_length
            else:
                # Paragraph is too large, need sentence-level splitting
                sentences = nltk.sent_tokenize(para)
                sentence_chunk = []
                sentence_length = 0
                
                for sent in sentences:
                    sent_len = len(sent)
                    
                    if sentence_length + sent_len <= chunk_size:
                        sentence_chunk.append(sent)
                        sentence_length += sent_len + 1  # +1 for space
                    else:
                        if sentence_chunk:
                            chunks.append(" ".join(sentence_chunk))
                            
                            # Overlap: keep last sentence if small enough
                            if overlap > 0 and len(sentence_chunk[-1]) <= overlap:
                                sentence_chunk = [sentence_chunk[-1]]
                                sentence_length = len(sentence_chunk[-1])
                            else:
                                sentence_chunk = []
                                sentence_length = 0
                        
                        # Add current sentence
                        if sent_len <= chunk_size:
                            sentence_chunk.append(sent)
                            sentence_length = sent_len
                        else:
                            # Sentence itself is too long (rare), force add it
                            chunks.append(sent[:chunk_size])
                            sentence_chunk = []
                            sentence_length = 0
                
                # Add remaining sentences
                if sentence_chunk:
                    current_chunk = [" ".join(sentence_chunk)]
                    current_length = sum(len(s) for s in sentence_chunk)
        
        # Case 3: First paragraph and it's too large
        else:
            if para_length <= chunk_size:
                current_chunk.append(para)
                current_length = para_length
            else:
                # Fall back to sentence splitting (same logic as above)
                sentences = nltk.sent_tokenize(para)
                sentence_chunk = []
                sentence_length = 0
                
                for sent in sentences:
                    sent_len = len(sent)
                    
                    if sentence_length + sent_len <= chunk_size:
                        sentence_chunk.append(sent)
                        sentence_length += sent_len + 1
                    else:
                        if sentence_chunk:
                            chunks.append(" ".join(sentence_chunk))
                            
                            if overlap > 0 and len(sentence_chunk[-1]) <= overlap:
                                sentence_chunk = [sentence_chunk[-1]]
                                sentence_length = len(sentence_chunk[-1])
                            else:
                                sentence_chunk = []
                                sentence_length = 0
                        
                        if sent_len <= chunk_size:
                            sentence_chunk.append(sent)
                            sentence_length = sent_len
                        else:
                            chunks.append(sent[:chunk_size])
                
                if sentence_chunk:
                    current_chunk = [" ".join(sentence_chunk)]
                    current_length = sum(len(s) for s in sentence_chunk)
    
    # Add final chunk
    if current_chunk:
        chunks.append("\n\n".join(current_chunk))
    
    return chunks


def adaptive_chunk(text: str, chunk_size: int = 1000, overlap: int = 200) -> tuple[List[str], str]:
    """
    Adaptive chunking: Choose strategy based on article characteristics.
    
    Strategy:
    - Simple articles (<3 paragraphs) → NLTK-only (fast)
    - Complex articles (≥3 paragraphs) → Paragraph-aware (quality)
    """
    paragraphs = re.split(r'\n\s*\n', text)
    
    # If article has few paragraphs, use fast NLTK approach
    if len(paragraphs) < 3:
        chunks = fast_semantic_chunk(text, chunk_size, overlap)
        return chunks, "nltk_fast"
    
    # Otherwise, use paragraph-aware for better structure preservation
    chunks = improved_semantic_chunk(text, chunk_size, overlap)
    return chunks, "paragraph_aware"


def clean_text(text: str) -> str:
    """Clean and normalize article text."""
    if not text:
        return ""
    
    # Remove excessive whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"\s{3,}", " ", text)
    
    # Remove Guardian footer text
    text = re.sub(r"© Guardian News & Media Limited.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"Sign up to.*?newsletter.*?\n", "", text, flags=re.IGNORECASE)
    
    # Remove URLs
    text = re.sub(r'http[s]?://\S+', '', text)
    
    return text.strip()


# ==================== PARALLEL CHUNKING ====================

def chunk_single_article(row_data: tuple) -> List[Dict[str, Any]]:
    """
    Worker function for parallel chunking.
    Processes one article and returns its chunks.
    """
    article_id, title, section, pub_date, url, body, data_source = row_data
    
    # Clean text
    cleaned = clean_text(body)
    if not cleaned or len(cleaned) < 50:
        return []
    
    # Adaptive chunking
    try:
        chunked_texts, chunking_method = adaptive_chunk(
            text=cleaned,
            chunk_size=1000,
            overlap=200
        )
    except Exception as e:
        logger.warning(f"⚠️  Chunking failed for {article_id}: {e}")
        return []
    
    # Create chunk objects with metadata
    chunks = []
    for i, text_chunk in enumerate(chunked_texts):
        stripped = text_chunk.strip()
        
        # Quality filter: skip very short chunks
        if len(stripped) < 100:
            continue
        
        chunks.append({
            "text": stripped,
            "metadata": {
                "article_id": str(article_id),
                "title": title or "Untitled",
                "section": section or "Unknown",
                "published_date": str(pub_date) if pub_date else "Unknown",
                "web_url": url or "",
                "data_source": data_source or "unknown",
                "chunk_index": i,
                "total_chunks": len(chunked_texts),
                "source_type": "guardian_article",
                "source": "The Guardian",
                "chunking_method": chunking_method,
                "ingestion_timestamp": datetime.now().isoformat()
            }
        })
    
    return chunks


# ==================== INCREMENTAL INGESTION ====================

def ensure_tracking_table():
    """Create tracking table if it doesn't exist."""
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.PINECONE_INGESTION_LOG (
            ARTICLE_ID VARCHAR(255) PRIMARY KEY,
            INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            CHUNK_COUNT INTEGER,
            PINECONE_NAMESPACE VARCHAR(100),
            INGESTION_STATUS VARCHAR(50) DEFAULT 'SUCCESS'
        )
    """
    
    conn = get_sf_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            conn.commit()
        logger.info("✅ Tracking table verified")
    except Exception as e:
        logger.error(f"❌ Failed to create tracking table: {e}")
        raise


def get_chunks_from_snowflake(
    incremental: bool = False,
    limit: Optional[int] = None,
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
    source_filter: str = 'all'
) -> List[Dict[str, Any]]:
    """
    Retrieve and chunk articles from Snowflake.
    """
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"

    source_clause = ""
    if source_filter != 'all':
        source_clause = f"AND a.DATA_SOURCE = '{source_filter}'"
    
    # Build query based on mode
    if incremental:
        logger.info("🔄 Incremental mode: Fetching only NEW/UPDATED articles")
        query = f"""
        SELECT 
            a.ARTICLE_ID, 
            a.WEB_TITLE, 
            s.SECTION_NAME,
            a.WEB_PUBLICATION_DATE, 
            a.WEB_URL, 
            a.BODY_TEXT,
            a.DATA_SOURCE
        FROM {database}.{schema}.FCT_ARTICLES a
        JOIN {database}.{schema}.DIM_SECTIONS s ON a.SECTION_KEY = s.SECTION_KEY
        LEFT JOIN {database}.{schema}.PINECONE_INGESTION_LOG p 
            ON a.ARTICLE_ID = p.ARTICLE_ID
        WHERE a.BODY_TEXT IS NOT NULL
          AND LENGTH(a.BODY_TEXT) > 1000
          AND a.IS_CURRENT = TRUE
          {source_clause}
          AND (
              p.ARTICLE_ID IS NULL
              OR a.DBT_UPDATED_AT > p.INGESTED_AT
          )
        ORDER BY a.WEB_PUBLICATION_DATE DESC
        """
    else:
        logger.info("📦 Full mode: Fetching ALL articles")
        query = f"""
        SELECT 
            a.ARTICLE_ID, 
            a.WEB_TITLE, 
            s.SECTION_NAME,
            a.WEB_PUBLICATION_DATE, 
            a.WEB_URL, 
            a.BODY_TEXT,
            a.DATA_SOURCE
        FROM {database}.{schema}.FCT_ARTICLES a
        JOIN {database}.{schema}.DIM_SECTIONS s ON a.SECTION_KEY = s.SECTION_KEY
        WHERE a.BODY_TEXT IS NOT NULL
          AND LENGTH(a.BODY_TEXT) > 1000
          AND a.IS_CURRENT = TRUE
        ORDER BY a.WEB_PUBLICATION_DATE DESC
        """
    
    if limit is not None:
        query += f" LIMIT {limit}"
    
    # Fetch articles
    conn = get_sf_connection()
    with conn.cursor() as cur:
        logger.info(f"🔍 Executing query...")
        cur.execute(query)
        rows = cur.fetchall()
    
    if not rows:
        logger.info("📭 No articles to process")
        return []
    
    logger.info(f"📄 Fetched {len(rows)} articles")
    
    # Parallel chunking
    num_workers = max(1, multiprocessing.cpu_count() - 1)
    logger.info(f"⚡ Processing with {num_workers} parallel workers (NLTK chunking)...")
    
    all_chunks = []
    
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        results = list(executor.map(chunk_single_article, rows))
    
    # Flatten results
    for chunks in results:
        all_chunks.extend(chunks)
    
    logger.info(f"✅ Created {len(all_chunks)} chunks from {len(rows)} articles")
    return all_chunks


def mark_articles_as_ingested(article_chunks: List[Dict[str, Any]], namespace: str = "guardian_articles"):
    """
    Update tracking table after successful Pinecone upload.
    
    IMPROVEMENTS:
    - Bulk INSERT with single MERGE operation per batch
    - Dramatically faster than row-by-row MERGE
    """
    
    if not article_chunks:
        return
    
    # Count chunks per article
    article_counts = {}
    for chunk in article_chunks:
        article_id = chunk['metadata']['article_id']
        article_counts[article_id] = article_counts.get(article_id, 0) + 1
    
    logger.info(f"📝 Updating tracking table for {len(article_counts)} articles...")
    
    conn = get_sf_connection()
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    try:
        # Use batched bulk inserts with temp table for much better performance
        batch_size = 1000  # Can handle larger batches with bulk insert
        article_items = list(article_counts.items())
        total_batches = (len(article_items) + batch_size - 1) // batch_size
        
        with conn.cursor() as cur:
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(article_items))
                batch_items = article_items[start_idx:end_idx]
                
                # Build VALUES clause for bulk insert
                values_list = []
                for article_id, chunk_count in batch_items:
                    values_list.append(f"('{article_id}', {chunk_count}, '{namespace}')")
                
                values_clause = ", ".join(values_list)
                
                # Single MERGE with temp CTE - much faster
                merge_sql = f"""
                MERGE INTO {database}.{schema}.PINECONE_INGESTION_LOG AS target
                USING (
                    SELECT column1 AS article_id, 
                           column2 AS chunk_count, 
                           column3 AS namespace
                    FROM VALUES {values_clause}
                ) AS source
                ON target.ARTICLE_ID = source.article_id
                WHEN MATCHED THEN
                    UPDATE SET 
                        INGESTED_AT = CURRENT_TIMESTAMP(),
                        CHUNK_COUNT = source.chunk_count,
                        PINECONE_NAMESPACE = source.namespace,
                        INGESTION_STATUS = 'SUCCESS'
                WHEN NOT MATCHED THEN
                    INSERT (ARTICLE_ID, INGESTED_AT, CHUNK_COUNT, PINECONE_NAMESPACE, INGESTION_STATUS)
                    VALUES (source.article_id, CURRENT_TIMESTAMP(), source.chunk_count, source.namespace, 'SUCCESS')
                """
                
                cur.execute(merge_sql)
                conn.commit()
                logger.info(f"   💾 Batch {batch_num + 1}/{total_batches} committed ({len(batch_items)} articles)")
        
        logger.info(f"✅ Marked {len(article_counts)} articles as ingested in tracking table")

        # Skip search optimization check if it takes too long
        try:
            with conn.cursor() as cur:
                cur.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 30")
                
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM TABLE(INFORMATION_SCHEMA.SEARCH_OPTIMIZATION_HISTORY('FCT_ARTICLES'))
                    WHERE STATUS = 'RUNNING'
                """)
                running_jobs = cur.fetchone()[0] # type: ignore
                
                if running_jobs > 0:
                    logger.info(f"🔄 {running_jobs} search optimization job(s) running in background")
                    logger.info("   New articles will be fully indexed in ~5-15 minutes")
                else:
                    logger.info("✅ All search optimizations up to date")
        except Exception as opt_error:
            logger.warning(f"⚠️  Could not check search optimization status: {opt_error}")
            logger.info("   (This is non-critical - search optimization may still be running)")
    
    except Exception as e:
        logger.error(f"❌ Failed to update tracking table: {e}")
        raise


# ==================== PINECONE UPLOAD ====================

def upload_to_pinecone_parallel(
    chunks: List[Dict[str, Any]], 
    namespace: str = "guardian_articles",
    batch_size: int = 300,
    max_workers: int = 3
) -> bool:
    """Parallel upload with retry logic."""
    
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
    index_name = os.getenv("PINECONE_INDEX", "guardian-capstone")
    
    total_chunks = len(chunks)
    batches = [chunks[i:i + batch_size] for i in range(0, total_chunks, batch_size)]
    
    logger.info(f"📤 Parallel upload: {len(batches)} batches, {max_workers} workers")
    
    def process_batch(batch_data):
        """Process a single batch with retry logic."""
        import time
        batch, batch_num, total_batches = batch_data
        
        time.sleep(0.5)

        docs = []
        ids = []
        
        for chunk in batch:
            chunk_id = f"{chunk['metadata']['article_id']}_chunk_{chunk['metadata']['chunk_index']}"
            doc = Document(page_content=chunk["text"], metadata=chunk["metadata"])
            docs.append(doc)
            ids.append(chunk_id)
        
        # Retry logic
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                PineconeVectorStore.from_documents(
                    documents=docs,
                    embedding=embeddings,
                    index_name=index_name,
                    namespace=namespace,
                    ids=ids
                )
                logger.info(f"   ✅ Batch {batch_num}/{total_batches} uploaded")
                return True
                
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    logger.warning(f"   ⚠️  Batch {batch_num} attempt {attempt + 1}/{max_retries} failed: {e}")
                    logger.warning(f"   ⏳ Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"   ❌ Batch {batch_num} failed after {max_retries} attempts: {e}")
                    return False
    
    # Create batch metadata
    batch_data = [(batch, i+1, len(batches)) for i, batch in enumerate(batches)]
    
    # Execute in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_batch, batch_data))
    
    # Report results
    successful = sum(1 for r in results if r is True)
    failed = sum(1 for r in results if r is False)
    
    logger.info(f"🎉 Upload complete!")
    logger.info(f"   ✅ Successful: {successful}/{len(batches)} batches")
    if failed > 0:
        logger.warning(f"   ⚠️  Failed: {failed}/{len(batches)} batches")
    
    return successful == len(batches)


def get_ingestion_stats(chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate statistics about the ingestion."""
    if not chunks:
        return {}
    
    chunk_sizes = [len(c["text"]) for c in chunks]
    sections = {}
    chunking_methods = {}
    data_sources = {}
    
    for chunk in chunks:
        section = chunk["metadata"].get("section", "Unknown")
        sections[section] = sections.get(section, 0) + 1
        
        method = chunk["metadata"].get("chunking_method", "unknown")
        chunking_methods[method] = chunking_methods.get(method, 0) + 1

        source = chunk["metadata"].get("data_source", "unknown")
        data_sources[source] = data_sources.get(source, 0) + 1
    
    return {
        "total_chunks": len(chunks),
        "unique_articles": len(set(c["metadata"]["article_id"] for c in chunks)),
        "avg_chunk_size": sum(chunk_sizes) / len(chunk_sizes),
        "min_chunk_size": min(chunk_sizes),
        "max_chunk_size": max(chunk_sizes),
        "sections": sections,
        "chunking_methods": chunking_methods,
        "data_sources": data_sources
    }


def close_sf_connection():
    """Close the global Snowflake connection."""
    global _sf_conn
    if _sf_conn is not None:
        try:
            _sf_conn.close()
            logger.info("🔌 Snowflake connection closed")
        except:
            pass
        _sf_conn = None


# ==================== MAIN ====================

if __name__ == "__main__":
    import argparse
    
    print("\n" + "="*60)
    print("Guardian Articles → Pinecone Ingestion Pipeline (FIXED)")
    print("="*60 + "\n")
    
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--incremental',
        action='store_true',
        help='Only process new/updated articles since last ingestion'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Max number of articles to process (default: unlimited)'
    )
    parser.add_argument(
        '--source',
        choices=['batch', 'realtime', 'all'],
        default='all',
        help='Filter by data source: batch (historical), realtime (live), or all (default: all)'
    )
    args = parser.parse_args()
    
    # Configuration
    CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1000"))
    CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "200"))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "300"))
    
    mode = "INCREMENTAL" if args.incremental else "FULL"
    limit_display = f"{args.limit}" if args.limit else "ALL"
    source_display = args.source.upper()
    
    print(f"Configuration:")
    print(f"  Mode: {mode}")
    print(f"  Data source filter: {source_display}")
    print(f"  Articles to process: {limit_display}")
    print(f"  Chunk size: {CHUNK_SIZE}")
    print(f"  Chunk overlap: {CHUNK_OVERLAP}")
    print(f"  Chunking strategy: NLTK (fast semantic)")
    print(f"  Upload batch size: {BATCH_SIZE}")
    print()
    
    try:
        # Ensure tracking table exists
        ensure_tracking_table()
        
        # Step 1: Fetch and chunk articles
        chunks = get_chunks_from_snowflake(
            incremental=args.incremental,
            limit=args.limit,
            chunk_size=CHUNK_SIZE,
            chunk_overlap=CHUNK_OVERLAP,
            source_filter=args.source
        )
        
        if not chunks:
            print("❌ No chunks generated. Either no new articles or check Snowflake data.")
            exit(0)
        
        # Step 2: Show statistics
        stats = get_ingestion_stats(chunks)
        print("\n" + "="*60)
        print("Ingestion Statistics:")
        print("="*60)
        print(f"  Total chunks: {stats['total_chunks']:,}")
        print(f"  Unique articles: {stats['unique_articles']:,}")
        print(f"  Avg chunk size: {stats['avg_chunk_size']:.0f} chars")
        print(f"  Min/Max chunk size: {stats['min_chunk_size']}/{stats['max_chunk_size']} chars")
        print(f"\n  Chunking methods used:")
        for method, count in sorted(stats['chunking_methods'].items(), key=lambda x: x[1], reverse=True):
            pct = (count / stats['total_chunks']) * 100
            print(f"    {method}: {count} chunks ({pct:.1f}%)")
        print(f"\n  Data sources:")
        for source, count in sorted(stats['data_sources'].items(), key=lambda x: x[1], reverse=True):
            pct = (count / stats['total_chunks']) * 100
            emoji = "📚" if source == "batch" else "🔴" if source == "realtime" else "❓"
            print(f"    {emoji} {source}: {count} chunks ({pct:.1f}%)")
        print(f"\n  Articles by section:")
        for section, count in sorted(stats['sections'].items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"    {section}: {count} chunks")
        print()
        
        # Step 3: Confirm before upload
        response = input(f"Upload {stats['total_chunks']:,} chunks to Pinecone? (yes/no): ").strip().lower()
        
        if response in ['yes', 'y']:
            upload_success = upload_to_pinecone_parallel(
                chunks, 
                namespace="guardian_articles",
                batch_size=BATCH_SIZE,
                max_workers=3
            )
            
            if upload_success:
                # Step 4: Mark as ingested in tracking table
                mark_articles_as_ingested(chunks, namespace="guardian_articles")
                
                print("\n" + "="*60)
                print("✅ Ingestion Complete!")
                print("="*60)
                print(f"Mode: {mode}")
                print(f"Articles processed: {stats['unique_articles']:,}")
                print(f"Chunks uploaded: {stats['total_chunks']:,}")
                print("\nNext steps:")
                print("  1. Check Pinecone dashboard to verify vectors")
                print("  2. Test retrieval with your RAG system")
                print("  3. Run incremental updates daily: --incremental")
                print()
            else:
                print("\n❌ Some batches failed to upload. Check logs above.")
                exit(1)
        else:
            print("\n❌ Upload cancelled by user.")
            exit(0)
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Ingestion interrupted by user.")
        exit(1)
    except Exception as e:
        logger.error(f"❌ Ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
    finally:
        close_sf_connection()