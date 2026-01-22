# baseline_systems.py
"""
Baseline retrieval systems for comparing against your hybrid RAG + re-ranking approach.

✅ CLEANED VERSION: Removed SQL debug print statements

OPTIMIZATIONS:
- Parallel execution of dense + SQL searches
- Detailed timing diagnostics for Pinecone

Contains 4 retrieval methods:
1. Naive RAG (Dense vector search only)
2. SQL-only (Keyword search only)
3. Hybrid without re-ranking (Dense + SQL, date-sorted)
4. Hybrid with re-ranking (Your current system)
"""

import time
import os
import concurrent.futures
from typing import List, Dict, Any, Tuple, Callable
from langchain_core.messages import SystemMessage
from langchain_pinecone import PineconeVectorStore
from langchain_openai import OpenAIEmbeddings
from flashrank import Ranker, RerankRequest

# Import your existing functions
from capstone_tool_calling_agent import search_articles_by_multiple_keywords

def extract_keywords(query: str) -> List[str]:
    """
    Context-aware keyword extraction for SQL search.
    Preserves named entities and filters noise words.
    """
    import re
    
    # Comprehensive noise words (questions, common verbs, determiners)
    noise_words = {
        'what', 'how', 'why', 'when', 'where', 'who', 'which', 'whom',
        'tell', 'show', 'find', 'get', 'give', 'about',
        'happened', 'occurring', 'going', 'related',
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to',
        'of', 'for', 'with', 'from', 'by', 'as', 'into',
        'is', 'are', 'was', 'were', 'been', 'being',
        'have', 'has', 'had', 'do', 'does', 'did',
        'will', 'would', 'could', 'should', 'may', 'might',
    }
    
    # Important abbreviations/terms (keep short forms)
    important_terms = {
        'ai', 'uk', 'us', 'eu', 'un', 'nhs', 'fbi', 'cia',
        'cop', 'cop26', 'cop27', 'cop28',
        'g7', 'g20', 'gdp', 'ceo', 'pm',
        'war', 'ban', 'tax', 'law',
    }
    
    keywords = []
    
    # 1. Capture multi-word named entities (COP26, Donald Trump, etc.)
    # Pattern: Capitalized words next to each other or numbers
    entities = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b|\b[A-Z]+\d+\b', query)
    for entity in entities:
        # Split entity and add individual words
        for word in entity.split():
            word_lower = word.lower()
            if word_lower not in noise_words:
                keywords.append(word_lower)
    
    # 2. Extract significant content words
    all_words = re.findall(r'\b\w+\b', query.lower())
    
    for word in all_words:
        # Skip if already added or is noise
        if word in keywords or word in noise_words:
            continue
        
        # Include if: important term OR length >= 4
        if word in important_terms or len(word) >= 4:
            keywords.append(word)
    
    # 3. Remove redundant words (keep most specific)
    # Example: If we have both "trump" and "donald", keep "trump"
    final_keywords = []
    for kw in keywords:
        # Skip if it's a substring of another keyword
        if not any(kw != other and kw in other for other in keywords):
            final_keywords.append(kw)
    
    # Return top 4 (SQL performs better with fewer, more specific terms)
    return final_keywords[:4]

def deduplicate_results(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove duplicate results based on article_id or text similarity.
    Follows Pinecone's merge and deduplicate pattern.
    """
    seen_ids = set()
    seen_texts = set()
    unique = []
    
    for candidate in candidates:
        # Check article_id (primary deduplication)
        article_id = candidate["metadata"].get("article_id")
        if article_id and article_id in seen_ids:
            continue
        
        # Check text similarity (secondary deduplication)
        text_signature = candidate["text"][:100].lower().strip()
        if text_signature in seen_texts:
            continue
        
        # Add to unique list
        unique.append(candidate)
        if article_id:
            seen_ids.add(article_id)
        seen_texts.add(text_signature)
    
    return unique

class BaselineRetriever:
    """Collection of baseline retrieval methods for comparison."""
    
    def __init__(self):
        """Initialize shared resources (lazy loading for efficiency)."""
        self._vector_store = None
        self._reranker = None
        
        # Statistics tracking
        self.stats = {
            "naive_rag": [],
            "sql_only": [],
            "hybrid_no_rerank": [],
            "hybrid_with_rerank": []
        }
        
        # Timing diagnostics
        self.timing_diagnostics = {
            "pinecone_init_ms": None,
            "pinecone_search_times": []
        }
    
    @property
    def vector_store(self):
        """Lazy load vector store with timing."""
        if self._vector_store is None:
            print("🔄 Initializing Pinecone vector store...")
            init_start = time.time()
            
            embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
            self._vector_store = PineconeVectorStore(
                index_name=os.getenv("PINECONE_INDEX", "guardian-capstone"),
                embedding=embeddings,
                namespace="guardian_articles"
            )
            
            init_time = (time.time() - init_start) * 1000
            self.timing_diagnostics["pinecone_init_ms"] = init_time
            print(f"   ⏱️  Pinecone initialization: {init_time:.0f}ms")
        
        return self._vector_store
    
    @property
    def reranker(self):
        """Lazy load FlashRank reranker."""
        if self._reranker is None:
            print("🔄 Initializing FlashRank reranker...")
            self._reranker = Ranker(
                model_name="ms-marco-MiniLM-L-12-v2",
                cache_dir="/tmp"
            )
        return self._reranker
    
    def _search_dense_with_timing(self, query: str, k: int = 5) -> List:
        """Execute dense search with detailed timing."""
        search_start = time.time()
        docs = self.vector_store.similarity_search(query, k=k)
        search_time = (time.time() - search_start) * 1000
        
        # Track timing
        self.timing_diagnostics["pinecone_search_times"].append({
            "query": query[:50],
            "k": k,
            "time_ms": search_time,
            "results": len(docs)
        })
        
        print(f"   ⏱️  Dense search: {search_time:.0f}ms ({len(docs)} results)")
        
        return docs
    
    def retrieve_naive_rag(
        self, 
        query: str, 
        k: int = 10
    ) -> Tuple[List[Dict[str, Any]], float]:
        """
        Baseline 1: Naive RAG (Dense vector search only).
        
        Args:
            query: User query string
            k: Number of results to return
        
        Returns:
            Tuple of (results, latency_ms)
        """
        start_time = time.time()
        
        try:
            # Dense search with timing
            docs = self._search_dense_with_timing(query, k=k)
            
            results = []
            for doc in docs:
                article_id = doc.metadata.get("article_id")
                if article_id:
                    results.append({
                        "article_id": article_id,
                        "title": doc.metadata.get("title", "Unknown"),
                        "score": None,
                        "method": "dense_only",
                        "metadata": doc.metadata
                    })
            
            latency = (time.time() - start_time) * 1000
            
            # Track statistics
            self.stats["naive_rag"].append({
                "query": query,
                "num_results": len(results),
                "latency_ms": latency
            })
            
            return results, latency
        
        except Exception as e:
            print(f"❌ Naive RAG error: {e}")
            return [], 0
    
    def retrieve_sql_only(
        self, 
        query: str, 
        k: int = 10
    ) -> Tuple[List[Dict[str, Any]], float]:
        """
        Baseline 2: SQL keyword search only.
        
        Args:
            query: User query string
            k: Number of results to return
        
        Returns:
            Tuple of (results, latency_ms)
        """
        start_time = time.time()
        
        # Extract keywords
        keywords = extract_keywords(query)
        
        if not keywords:
            print(f"⚠️ No keywords extracted from query: {query}")
            return [], 0
        
        try:
            sql_response = search_articles_by_multiple_keywords.invoke({
                "keywords": keywords[:4],
                "limit": 10
            })
            
            results = []
            if "results" in sql_response and sql_response["results"]:
                for article in sql_response["results"]:
                    article_id = article.get("article_id")
                    if article_id:
                        results.append({
                            "article_id": article_id,
                            "title": article.get("web_title", "Unknown"),
                            "score": None,
                            "method": "sql_only",
                            "metadata": {
                                "article_id": article_id,
                                "title": article.get("web_title"),
                                "web_url": article.get("web_url", ""),
                                "published_date": str(article.get("web_publication_date", "")),
                            }
                        })
            
            latency = (time.time() - start_time) * 1000
            
            # Track statistics
            self.stats["sql_only"].append({
                "query": query,
                "keywords_used": keywords[:4],
                "num_results": len(results),
                "latency_ms": latency
            })
            
            return results, latency
        
        except Exception as e:
            print(f"❌ SQL-only error: {e}")
            return [], 0
    
    def retrieve_hybrid_no_rerank(
        self, 
        query: str, 
        k: int = 5
    ) -> Tuple[List[Dict[str, Any]], float]:
        """
        Baseline 3: Hybrid search without re-ranking.
        
        OPTIMIZED: Uses parallel execution for dense + SQL searches.
        
        Args:
            query: User query string
            k: Number of results to return
        
        Returns:
            Tuple of (results, latency_ms)
        """
        start_time = time.time()
        
        all_candidates = []
        
        # Extract keywords upfront
        keywords = extract_keywords(query)
        
        # ✅ PARALLEL EXECUTION - Dense + SQL simultaneously
        print("🚀 Running parallel searches (Dense + SQL)...")
        parallel_start = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            # Submit both searches in parallel
            dense_future = executor.submit(
                self._search_dense_with_timing,
                query,
                10
            )
            
            sql_future = executor.submit(
                search_articles_by_multiple_keywords.invoke,
                {"keywords": keywords[:4], "limit": 10}
            ) if keywords else None
            
            # Collect dense results
            try:
                dense_docs = dense_future.result()
                for doc in dense_docs:
                    all_candidates.append({
                        "article_id": doc.metadata.get("article_id"),
                        "title": doc.metadata.get("title", "Unknown"),
                        "published_date": doc.metadata.get("published_date", ""),
                        "text": doc.page_content,
                        "metadata": doc.metadata,
                        "source_type": "guardian_article",
                        "retrieval_method": "dense_vector"
                    })
            except Exception as e:
                print(f"⚠️ Dense search failed: {e}")
            
            # Collect SQL results
            if sql_future:
                try:
                    sql_response = sql_future.result()
                    if "results" in sql_response and sql_response["results"]:
                        for article in sql_response["results"]:
                            all_candidates.append({
                                "article_id": article.get("article_id"),
                                "title": article.get("web_title"),
                                "published_date": str(article.get("web_publication_date", "")),
                                "text": article.get("preview", ""),
                                "metadata": {
                                    "article_id": article.get("article_id"),
                                    "title": article.get("web_title"),
                                    "published_date": str(article.get("web_publication_date", "")),
                                    "web_url": article.get("web_url", ""),
                                },
                                "source_type": "guardian_article",
                                "retrieval_method": "sql_keyword"
                            })
                except Exception as e:
                    print(f"⚠️ SQL search failed: {e}")
        
        parallel_time = (time.time() - parallel_start) * 1000
        print(f"   ⏱️  Parallel execution total: {parallel_time:.0f}ms")
        
        # Deduplicate
        unique_candidates = deduplicate_results(all_candidates)
        print(f"   🔄 Dedup: {len(all_candidates)} candidates → {len(unique_candidates)} unique")
        
        def hybrid_sort_key(candidate):
            # Dense gets priority 1, SQL gets priority 0
            is_dense = 1 if candidate["retrieval_method"] == "dense_vector" else 0
            date = candidate.get("published_date", "")
            return (is_dense, date)

        # Sort by date (no re-ranking)
        sorted_candidates = sorted(
            unique_candidates,
            key=hybrid_sort_key,
            reverse=True
        )
        
        # Return top K
        results = []
        for candidate in sorted_candidates[:k]:
            results.append({
                "article_id": candidate.get("article_id"),
                "title": candidate.get("title", "Unknown"),
                "score": None,
                "method": "hybrid_no_rerank",
                "metadata": candidate.get("metadata", {})
            })
        
        latency = (time.time() - start_time) * 1000
        
        # Track statistics
        self.stats["hybrid_no_rerank"].append({
            "query": query,
            "dense_results": len([c for c in all_candidates if c["retrieval_method"] == "dense_vector"]),
            "sql_results": len([c for c in all_candidates if c["retrieval_method"] == "sql_keyword"]),
            "unique_after_dedup": len(unique_candidates),
            "num_results": len(results),
            "latency_ms": latency,
            "parallel_time_ms": parallel_time
        })

        dense_contributed = sum(1 for r in results if any(
            c['article_id'] == r['article_id'] and c['retrieval_method'] == 'dense_vector' 
            for c in all_candidates
        ))
        sql_contributed = sum(1 for r in results if any(
            c['article_id'] == r['article_id'] and c['retrieval_method'] == 'sql_keyword' 
            for c in all_candidates
        ))
        print(f"   📊 Final top-{k} sources: Dense={dense_contributed}, SQL={sql_contributed}")
        
        return results, latency
    
    def retrieve_hybrid_with_rerank(
        self, 
        query: str, 
        k: int = 5
    ) -> Tuple[List[Dict[str, Any]], float]:
        """
        Your current system: Hybrid search WITH re-ranking.
        
        OPTIMIZED: 
        - Parallel execution for dense + SQL
        - Reduced K from 10 → 5 for dense search
        
        Args:
            query: User query string
            k: Number of results to return
        
        Returns:
            Tuple of (results, latency_ms)
        """
        start_time = time.time()
        
        all_candidates = []
        
        # Extract keywords upfront
        keywords = extract_keywords(query)
        
        # ✅ PARALLEL EXECUTION - Dense + SQL simultaneously
        print("🚀 Running parallel searches (Dense + SQL)...")
        parallel_start = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            # Submit both searches in parallel
            dense_future = executor.submit(
                self._search_dense_with_timing,
                query,
                10
            )
            
            sql_future = executor.submit(
                search_articles_by_multiple_keywords.invoke,
                {"keywords": keywords[:4], "limit": 10}
            ) if keywords else None
            
            # Collect dense results
            try:
                dense_docs = dense_future.result()
                for doc in dense_docs:
                    all_candidates.append({
                        "article_id": doc.metadata.get("article_id"),
                        "title": doc.metadata.get("title", "Unknown"),
                        "published_date": doc.metadata.get("published_date", ""),
                        "text": doc.page_content,
                        "metadata": doc.metadata,
                        "source_type": "guardian_article",
                        "retrieval_method": "dense_vector"
                    })
            except Exception as e:
                print(f"⚠️ Dense search failed: {e}")
            
            # Collect SQL results
            if sql_future:
                try:
                    sql_response = sql_future.result()
                    if "results" in sql_response and sql_response["results"]:
                        for article in sql_response["results"]:
                            all_candidates.append({
                                "article_id": article.get("article_id"),
                                "title": article.get("web_title"),
                                "published_date": str(article.get("web_publication_date", "")),
                                "text": article.get("preview", ""),
                                "metadata": {
                                    "article_id": article.get("article_id"),
                                    "title": article.get("web_title"),
                                    "published_date": str(article.get("web_publication_date", "")),
                                    "web_url": article.get("web_url", ""),
                                },
                                "source_type": "guardian_article",
                                "retrieval_method": "sql_keyword"
                            })
                except Exception as e:
                    print(f"⚠️ SQL search failed: {e}")
        
        parallel_time = (time.time() - parallel_start) * 1000
        print(f"   ⏱️  Parallel execution total: {parallel_time:.0f}ms")
        
        # Deduplicate
        unique_candidates = deduplicate_results(all_candidates)
        print(f"   🔄 Dedup: {len(all_candidates)} candidates → {len(unique_candidates)} unique")
        
        # Re-rank with FlashRank
        rerank_start = time.time()
        if len(unique_candidates) >= 3:
            try:
                passages = [
                    {"id": i, "text": candidate["text"][:500]}
                    for i, candidate in enumerate(unique_candidates)
                ]
                
                rerank_request = RerankRequest(query=query, passages=passages)
                reranked = self.reranker.rerank(rerank_request)
                
                top_n = min(k, len(reranked))
                top_candidates = [unique_candidates[r["id"]] for r in reranked[:top_n]]
                rerank_scores = [r["score"] for r in reranked[:top_n]]
                
                rerank_used = True
            except Exception as e:
                print(f"⚠️ Re-ranking failed: {e}")
                top_candidates = unique_candidates[:k]
                rerank_scores = [None] * len(top_candidates)
                rerank_used = False
        else:
            top_candidates = unique_candidates[:k]
            rerank_scores = [None] * len(top_candidates)
            rerank_used = False
        
        rerank_time = (time.time() - rerank_start) * 1000
        print(f"   ⏱️  Re-ranking: {rerank_time:.0f}ms")
        
        # Format results
        results = []
        for i, candidate in enumerate(top_candidates):
            results.append({
                "article_id": candidate.get("article_id"),
                "title": candidate.get("title", "Unknown"),
                "score": rerank_scores[i] if i < len(rerank_scores) else None,
                "method": "hybrid_with_rerank",
                "metadata": candidate.get("metadata", {})
            })
        
        latency = (time.time() - start_time) * 1000
        
        # Track statistics
        self.stats["hybrid_with_rerank"].append({
            "query": query,
            "dense_results": len([c for c in all_candidates if c["retrieval_method"] == "dense_vector"]),
            "sql_results": len([c for c in all_candidates if c["retrieval_method"] == "sql_keyword"]),
            "unique_after_dedup": len(unique_candidates),
            "rerank_used": rerank_used,
            "num_results": len(results),
            "latency_ms": latency,
            "parallel_time_ms": parallel_time,
            "rerank_time_ms": rerank_time
        })

        dense_contributed = sum(1 for r in results if any(
            c['article_id'] == r['article_id'] and c['retrieval_method'] == 'dense_vector' 
            for c in all_candidates
        ))
        sql_contributed = sum(1 for r in results if any(
            c['article_id'] == r['article_id'] and c['retrieval_method'] == 'sql_keyword' 
            for c in all_candidates
        ))
        print(f"   📊 Final top-{k} sources: Dense={dense_contributed}, SQL={sql_contributed}")
        
        return results, latency
    
    def print_statistics_summary(self):
        """Print summary statistics for all methods."""
        print("\n" + "="*80)
        print("RETRIEVAL STATISTICS SUMMARY")
        print("="*80)
        
        for method_name, method_stats in self.stats.items():
            if not method_stats:
                continue
            
            print(f"\n{method_name.upper().replace('_', ' ')}:")
            print(f"  Total queries: {len(method_stats)}")
            
            # Average latency
            avg_latency = sum(s["latency_ms"] for s in method_stats) / len(method_stats)
            print(f"  Avg latency: {avg_latency:.1f}ms")
            
            # Average results
            avg_results = sum(s["num_results"] for s in method_stats) / len(method_stats)
            print(f"  Avg results returned: {avg_results:.1f}")
            
            # Method-specific stats
            if method_name == "sql_only":
                print(f"  Queries with keywords: {sum(1 for s in method_stats if s.get('keywords_used'))}")
            
            elif method_name in ["hybrid_no_rerank", "hybrid_with_rerank"]:
                avg_dense = sum(s.get("dense_results", 0) for s in method_stats) / len(method_stats)
                avg_sql = sum(s.get("sql_results", 0) for s in method_stats) / len(method_stats)
                avg_unique = sum(s.get("unique_after_dedup", 0) for s in method_stats) / len(method_stats)
                
                print(f"  Avg dense results: {avg_dense:.1f}")
                print(f"  Avg SQL results: {avg_sql:.1f}")
                print(f"  Avg unique (after dedup): {avg_unique:.1f}")
                
                # Parallel execution timing
                if "parallel_time_ms" in method_stats[0]:
                    avg_parallel = sum(s.get("parallel_time_ms", 0) for s in method_stats) / len(method_stats)
                    print(f"  Avg parallel execution: {avg_parallel:.1f}ms")
                
                if method_name == "hybrid_with_rerank":
                    rerank_success = sum(1 for s in method_stats if s.get("rerank_used", False))
                    print(f"  Re-ranking used: {rerank_success}/{len(method_stats)} queries")
                    
                    if "rerank_time_ms" in method_stats[0]:
                        avg_rerank = sum(s.get("rerank_time_ms", 0) for s in method_stats) / len(method_stats)
                        print(f"  Avg re-rank time: {avg_rerank:.1f}ms")
    
    def print_pinecone_diagnostics(self):
        """Print detailed Pinecone timing diagnostics."""
        print("\n" + "="*80)
        print("📊 PINECONE TIMING DIAGNOSTICS")
        print("="*80)
        
        if self.timing_diagnostics["pinecone_init_ms"]:
            print(f"\n🔧 Initialization: {self.timing_diagnostics['pinecone_init_ms']:.0f}ms")
        
        if self.timing_diagnostics["pinecone_search_times"]:
            print(f"\n🔍 Search Operations ({len(self.timing_diagnostics['pinecone_search_times'])} total):")
            
            times = [s["time_ms"] for s in self.timing_diagnostics["pinecone_search_times"]]
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            
            print(f"   Average: {avg_time:.0f}ms")
            print(f"   Min: {min_time:.0f}ms")
            print(f"   Max: {max_time:.0f}ms")
            
            # Performance assessment
            if avg_time < 1000:
                print(f"\n   ✅ EXCELLENT: Average search time is {avg_time:.0f}ms")
            elif avg_time < 3000:
                print(f"\n   ⚠️  FAIR: Average search time is {avg_time:.0f}ms (consider optimization)")
            else:
                print(f"\n   ❌ SLOW: Average search time is {avg_time:.0f}ms (needs optimization!)")
                print(f"   💡 Suggestions:")
                print(f"      - Check Pinecone index region (should be close to your location)")
                print(f"      - Verify embedding dimensions (should be 1536 for text-embedding-3-small)")
                print(f"      - Consider upgrading Pinecone tier")


# ==================== CONVENIENCE FUNCTIONS ====================

def compare_methods_on_query(query: str, k: int = 5) -> Dict[str, Any]:
    """
    Run all 4 methods on a single query and compare results.
    
    Args:
        query: Test query
        k: Number of results to return
    
    Returns:
        Dictionary with results from all 4 methods
    """
    retriever = BaselineRetriever()
    
    print(f"\n{'='*80}")
    print(f"QUERY: {query}")
    print(f"{'='*80}\n")
    
    methods = {
        "Naive RAG": retriever.retrieve_naive_rag,
        "SQL Only": retriever.retrieve_sql_only,
        "Hybrid (No Rerank)": retriever.retrieve_hybrid_no_rerank,
        "Hybrid + Rerank": retriever.retrieve_hybrid_with_rerank,
    }
    
    results = {}
    
    for method_name, method_func in methods.items():
        print(f"\nRunning {method_name}...")
        method_results, latency = method_func(query, k=k)
        
        results[method_name] = {
            "results": method_results,
            "latency_ms": latency,
            "num_results": len(method_results)
        }
        
        print(f"  ✅ Found {len(method_results)} results in {latency:.1f}ms")
        
        if method_results:
            top_ids = [r["article_id"] for r in method_results[:3]]
            print(f"  Top 3 IDs: {top_ids}")
    
    # Print diagnostics
    retriever.print_pinecone_diagnostics()
    
    return results


def get_retrieval_functions() -> Dict[str, Callable]:
    """
    Get dictionary of retrieval functions for use in evaluation.py.
    
    Returns:
        Dict mapping method name to retrieval function
    """
    retriever = BaselineRetriever()
    
    return {
        "naive_rag": lambda q, k=5: retriever.retrieve_naive_rag(q, k),
        "sql_only": lambda q, k=5: retriever.retrieve_sql_only(q, k),
        "hybrid_no_rerank": lambda q, k=5: retriever.retrieve_hybrid_no_rerank(q, k),
        "hybrid_with_rerank": lambda q, k=5: retriever.retrieve_hybrid_with_rerank(q, k),
    }


# ==================== DEMO / TESTING ====================

if __name__ == "__main__":
    """Demo script showing how to use the baseline retriever."""
    
    test_queries = [
        "Brexit referendum results 2016",
        "Climate change COP conferences",
        "AI regulation technology policy",
    ]
    
    print("\n" + "="*80)
    print("BASELINE RETRIEVAL SYSTEMS DEMO (OPTIMIZED + CLEANED)")
    print("="*80)
    
    retriever = BaselineRetriever()
    
    for query in test_queries:
        results = compare_methods_on_query(query, k=5)
        print()
    
    # Print overall statistics
    retriever.print_statistics_summary()
    retriever.print_pinecone_diagnostics()
    
    print("\n" + "="*80)
    print("DEMO COMPLETE")
    print("="*80)
    print("\n💡 Next steps:")
    print("  1. Run label_ground_truth.py to create ground truth labels")
    print("  2. Run evaluation.py to get quantitative metrics")
    print("  3. Run visualize_results.py to create comparison charts")