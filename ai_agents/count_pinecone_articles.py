"""
Count distinct article IDs in Pinecone namespace.

This script lists ALL vectors and extracts unique article IDs.
Works around Pinecone's query limitations.
"""

import os
import logging
from dotenv import load_dotenv
from pinecone import Pinecone
from collections import Counter

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def count_distinct_articles(namespace: str = "guardian_articles"):
    """
    Count distinct article IDs by listing ALL vector IDs.
    
    Pinecone vector IDs follow pattern: {article_id}_chunk_{chunk_index}
    We extract article_id from each vector ID.
    """
    logger.info(f"🔍 Connecting to Pinecone...")
    
    pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
    index_name = os.getenv("PINECONE_INDEX", "guardian-capstone")
    index = pc.Index(index_name)
    
    # Get stats
    stats = index.describe_index_stats()
    total_vectors = stats['namespaces'].get(namespace, {}).get('vector_count', 0)
    logger.info(f"📊 Total vectors in namespace '{namespace}': {total_vectors:,}")
    
    if total_vectors == 0:
        logger.warning("❌ No vectors found in this namespace")
        return
    
    logger.info(f"⏳ Listing all vector IDs (this may take 30-60 seconds)...")
    
    # List all vector IDs using pagination
    article_chunks = Counter()
    processed = 0
    
    try:
        # index.list() returns a generator that yields vector IDs
        for item in index.list(namespace=namespace):
            # Handle different return types (string, dict, list, object)
            if isinstance(item, str):
                vector_id = item
            elif isinstance(item, dict):
                vector_id = item.get('id', '')
            elif isinstance(item, list):
                # If it's a list, take the first element
                vector_id = item[0] if item else ''
            elif hasattr(item, 'id'):
                vector_id = item.id
            else:
                vector_id = str(item)
            
            if not vector_id:
                continue
            
            # Extract article_id from vector_id pattern: {article_id}_chunk_{index}
            if '_chunk_' in vector_id:
                article_id = vector_id.rsplit('_chunk_', 1)[0]
                article_chunks[article_id] += 1
            else:
                # Fallback if pattern is different
                article_chunks[vector_id] += 1
            
            processed += 1
            
            # Progress update every 10k vectors
            if processed % 10000 == 0:
                logger.info(f"   Processed {processed:,}/{total_vectors:,} vectors...")
                
    except Exception as e:
        logger.error(f"Error during listing: {e}")
        import traceback
        traceback.print_exc()
    
    logger.info(f"✅ Processed all {processed:,} vectors")
    
    # Results
    unique_articles = len(article_chunks)
    total_chunks = sum(article_chunks.values())
    avg_chunks = total_chunks / unique_articles if unique_articles > 0 else 0
    
    print("\n" + "="*60)
    print("Pinecone Article Count")
    print("="*60)
    print(f"Namespace: {namespace}")
    print(f"Total vectors: {total_chunks:,}")
    print(f"Unique articles: {unique_articles:,}")
    print(f"Avg chunks per article: {avg_chunks:.1f}")
    print()
    
    # Top 10 articles by chunk count
    print("Top 10 articles by chunk count:")
    for article_id, count in article_chunks.most_common(10):
        print(f"  {article_id}: {count} chunks")
    print("="*60 + "\n")
    
    return {
        'unique_articles': unique_articles,
        'total_vectors': total_chunks,
        'article_chunks': dict(article_chunks)
    }


if __name__ == "__main__":
    print("\n" + "="*60)
    print("Count Distinct Articles in Pinecone")
    print("="*60 + "\n")
    
    try:
        result = count_distinct_articles(namespace="guardian_articles")
        
        if result:
            print(f"✅ Found {result['unique_articles']:,} unique articles in Pinecone")
            
            # Optionally save to file
            save = input("\nSave article list to file? (yes/no): ").strip().lower()
            if save in ['yes', 'y']:
                import json
                with open('pinecone_articles.json', 'w') as f:
                    json.dump(result, f, indent=2)
                print("💾 Saved to pinecone_articles.json")
    
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()