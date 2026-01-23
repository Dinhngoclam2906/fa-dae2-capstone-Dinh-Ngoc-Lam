# capstone_tool_calling_agent.py

import os
from typing import Annotated, Any, Dict, List, Optional, Generator
from contextlib import contextmanager
from datetime import datetime
from dotenv import load_dotenv

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, AIMessage 
from langchain_core.runnables import RunnableConfig
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

from langgraph.graph import StateGraph, START
from langgraph.prebuilt import ToolNode, tools_condition
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from pinecone import Pinecone
import threading

load_dotenv()


# ==================== SNOWFLAKE CONNECTION ====================
class SnowflakeConnectionPool:
    """Thread-safe connection pool for Snowflake."""
    
    def __init__(self, max_size: int = 5):
        self._pool: List[SnowflakeConnection] = []
        self._max_size = max_size
        self._lock = threading.Lock()
        
        # Read config once
        private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH")
        if not private_key_path or not os.path.exists(private_key_path):
            raise ValueError("SNOWFLAKE_PRIVATE_KEY_FILE_PATH missing or invalid")
        
        self._config = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "authenticator": "SNOWFLAKE_JWT",
            "private_key_file": private_key_path,
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
        }
    
    def get_connection(self) -> SnowflakeConnection:
        """Get a connection from pool or create new one."""
        with self._lock:
            # Reuse existing connection if available
            while self._pool:
                conn = self._pool.pop()
                try:
                    # Test if connection is alive
                    conn.cursor().execute("SELECT 1")
                    return conn
                except:
                    # Connection dead, discard
                    try:
                        conn.close()
                    except:
                        pass
            
            # Create new connection
            print("🔌 Creating new Snowflake connection...")
            return snowflake.connector.connect(**self._config)
    
    def return_connection(self, conn: SnowflakeConnection):
        """Return connection to pool."""
        with self._lock:
            if len(self._pool) < self._max_size:
                self._pool.append(conn)
            else:
                # Pool full, close connection
                try:
                    conn.close()
                except:
                    pass

# Global pool instance
_connection_pool: Optional[SnowflakeConnectionPool] = None

def get_pool() -> SnowflakeConnectionPool:
    """Get or create global connection pool."""
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = SnowflakeConnectionPool(max_size=5)
    return _connection_pool

@contextmanager
def snowflake_connection() -> Generator[SnowflakeConnection, None, None]:
    """Get connection from pool."""
    pool = get_pool()
    conn = pool.get_connection()
    try:
        yield conn
    finally:
        pool.return_connection(conn)

# ==================== TOOLS ====================

@tool
def get_data_source_info() -> Dict[str, Any]:
    """
    Get information about the data sources and their date boundaries.
    Use this when users ask about data coverage, sources, or date ranges.
    """
    return {
        "data_sources": {
            "batch": {
                "label": "📚 Historical Archive",
                "source": "HuggingFace dataset",
                "date_range": "All articles before May 11, 2024",
                "cutoff_date": "2024-05-11",
                "description": "Comprehensive historical archive of Guardian articles",
                "use_case": "Historical research, trend analysis, pre-2024 events"
            },
            "realtime": {
                "label": "🔴 Live Feed",
                "source": "Guardian API",
                "date_range": "All articles from May 11, 2024 onwards",
                "start_date": "2024-05-11",
                "description": "Live feed of current Guardian articles",
                "use_case": "Breaking news, current events, recent coverage"
            }
        },
        "boundary_date": "2024-05-11",
        "note": "The data sources are separated by publication date, not ingestion date. An article published on May 10, 2024 is in BATCH, while one published on May 11, 2024 is in REALTIME."
    }


@tool
def determine_data_source_by_date(date: str) -> Dict[str, Any]:
    """
    Determine which data source contains articles from a specific date.
    
    Args:
        date: Date in YYYY-MM-DD format
    
    Returns:
        Information about which data source to use
    """
    from datetime import datetime
    
    try:
        query_date = datetime.strptime(date, "%Y-%m-%d")
        boundary_date = datetime.strptime("2024-05-11", "%Y-%m-%d")
        
        if query_date < boundary_date:
            return {
                "date": date,
                "data_source": "batch",
                "label": "📚 Historical Archive",
                "reason": f"Articles from {date} are in the batch source (HuggingFace dataset)",
                "recommendation": "Use search_articles_by_data_source(data_source='batch', start_date='{date}', end_date='{date}')"
            }
        else:
            return {
                "date": date,
                "data_source": "realtime",
                "label": "🔴 Live Feed",
                "reason": f"Articles from {date} are in the realtime source (Guardian API)",
                "recommendation": "Use search_articles_by_data_source(data_source='realtime', start_date='{date}', end_date='{date}')"
            }
    except ValueError:
        return {"error": "Invalid date format. Use YYYY-MM-DD"}

@tool
def search_articles_by_keyword(keyword: str, limit: int = 10) -> Dict[str, Any]:
    """Search articles by keyword in title or body."""
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    query = f"""
    SELECT 
        ARTICLE_ID, 
        WEB_TITLE, 
        SECTION_NAME,
        WEB_PUBLICATION_DATE, 
        WEB_URL,
        DATA_SOURCE,
        preview || '...' AS preview
    FROM {database}.{schema}.VW_ARTICLES_SEARCH_OPTIMIZED
    WHERE (web_title_lower LIKE %s OR body_text_lower LIKE %s)
      AND WEB_PUBLICATION_DATE IS NOT NULL
    LIMIT %s
    """

    pattern = f"%{keyword.strip().lower()}%"
    try:
        with snowflake_connection() as conn:
            conn.cursor().execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 15")

            with conn.cursor() as cur:
                cur.execute(query, (pattern, pattern, limit))
                rows = cur.fetchall()
                columns = [col[0].lower() for col in cur.description]
                results = [dict(zip(columns, row)) for row in rows]

                for result in results:
                    source = result.get('data_source', 'unknown')
                    result['source_label'] = '📚 Historical' if source == 'batch' else '🔴 Live'
                    
                return {"keyword": keyword, "count": len(results), "results": results}
    except Exception as e:
        if "timeout" in str(e).lower():
            return {
                "error": f"Query timeout - searching for '{keyword}' took too long. Try a more specific search term.",
                "suggestion": "Use exact phrases or combine with section filters for faster results."
            }
        return {"error": str(e)}

@tool
def search_articles_by_multiple_keywords(keywords: List[str], limit: int = 15) -> Dict[str, Any]:
    """
    Search articles where MULTIPLE keywords match (not just one).
    Uses relevance scoring to rank by keyword coverage.
    """
    if not keywords:
        return {"error": "No keywords provided", "results": []}
    
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    # Build WHERE clause with AND logic for better precision
    where_conditions = []
    params = []
    
    # Require at least 2 keywords to match (configurable)
    min_keywords_required = min(2, len(keywords))
    
    # Create individual match columns for scoring
    match_columns = []
    for i, keyword in enumerate(keywords):
        clean_keyword = keyword.strip().lower()
        
        # Create a CASE expression to count this keyword match
        match_col = f"""
        CASE 
            WHEN CONTAINS(web_title_lower, %s) THEN 2  -- Title match worth more
            WHEN CONTAINS(body_text_lower, %s) THEN 1  -- Body match worth less
            ELSE 0 
        END AS match_{i}
        """
        match_columns.append(match_col)
        params.extend([clean_keyword, clean_keyword])
    
    # Calculate total relevance score
    relevance_score = " + ".join([f"match_{i}" for i in range(len(keywords))])
    
    query = f"""
    WITH scored_articles AS (
        SELECT 
            ARTICLE_ID, 
            WEB_TITLE, 
            SECTION_NAME,
            WEB_PUBLICATION_DATE, 
            WEB_URL,
            DATA_SOURCE,
            preview || '...' AS preview,
            {', '.join(match_columns)},
            ({relevance_score}) AS relevance_score
        FROM {database}.{schema}.VW_ARTICLES_SEARCH_OPTIMIZED
        WHERE WEB_PUBLICATION_DATE IS NOT NULL
    )
    SELECT 
        ARTICLE_ID, WEB_TITLE, SECTION_NAME, 
        WEB_PUBLICATION_DATE, WEB_URL, DATA_SOURCE, preview
    FROM scored_articles
    WHERE relevance_score >= %s  -- Require minimum keyword matches
    ORDER BY relevance_score DESC, WEB_PUBLICATION_DATE DESC
    LIMIT %s
    """
    
    params.append(min_keywords_required)  # Minimum score threshold
    params.append(limit)
    
    print(f"DEBUG - Keywords: {keywords}")
    print(f"DEBUG - Requiring at least {min_keywords_required} keyword matches")
    
    try:
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 20")
                cur.execute("ALTER SESSION SET USE_CACHED_RESULT = TRUE")
                
                import time
                start = time.time()
                print(f"   🔍 Executing SQL with relevance scoring...")
                
                cur.execute(query, params)
                rows = cur.fetchall()
                
                elapsed = (time.time() - start) * 1000
                print(f"   ✅ SQL completed in {elapsed:.0f}ms, found {len(rows)} results")
                
                columns = [col[0].lower() for col in cur.description]
                results = [dict(zip(columns, row)) for row in rows]

                for result in results:
                    source = result.get('data_source', 'unknown')
                    result['source_label'] = '📚 Historical' if source == 'batch' else '🔴 Live'

                return {
                    "keywords": keywords,
                    "count": len(results),
                    "results": results
                }
    except Exception as e:
        print(f"   ❌ SQL error: {str(e)}")
        return {"error": str(e), "results": []}

@tool
def search_articles_by_date(
    date: str,
    data_source: Optional[str] = None, 
    limit: int = 20
) -> Dict[str, Any]:
    """
    Search articles published on a specific date.
    
    Args:
        date: Date in YYYY-MM-DD format (e.g., '2026-01-22')
        limit: Maximum number of articles to return
    """
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    query = f"""
    SELECT 
        ARTICLE_ID, 
        WEB_TITLE, 
        SECTION_NAME,
        WEB_PUBLICATION_DATE, 
        WEB_URL,
        DATA_SOURCE,
        preview || '...' AS preview
    FROM {database}.{schema}.VW_ARTICLES_SEARCH_OPTIMIZED
    WHERE DATE(WEB_PUBLICATION_DATE) = %s
    ORDER BY WEB_PUBLICATION_DATE DESC
    LIMIT %s
    """
    
    try:
        # Validate date format
        datetime.strptime(date, "%Y-%m-%d")
        
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (date, limit))
                rows = cur.fetchall()
                columns = [col[0].lower() for col in cur.description]
                results = [dict(zip(columns, row)) for row in rows]
                
                for result in results:
                    source = result.get('data_source', 'unknown')
                    result['source_label'] = '📚 Historical' if source == 'batch' else '🔴 Live'
                
                return {
                    "date": date,
                    "count": len(results),
                    "results": results
                }
    except ValueError:
        return {"error": "Invalid date format. Use YYYY-MM-DD"}
    except Exception as e:
        return {"error": str(e)}


@tool
def search_articles_by_date_range(
    start_date: str,
    end_date: str,
    data_source: Optional[str] = None, 
    keyword: Optional[str] = None,
    limit: int = 50
) -> Dict[str, Any]:
    """
    Search articles published within a date range, optionally filtered by keyword.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        keyword: Optional keyword to filter results
        limit: Maximum number of articles to return
        
    Examples:
        - search_articles_by_date_range("2019-01-01", "2020-12-31", keyword="Putin")
        - search_articles_by_date_range("2020-03-01", "2021-06-30", keyword="COVID vaccine")
    """
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    query = f"""
    SELECT 
        ARTICLE_ID, 
        WEB_TITLE, 
        SECTION_NAME,
        WEB_PUBLICATION_DATE, 
        WEB_URL,
        DATA_SOURCE,
        preview || '...' AS preview
    FROM {database}.{schema}.VW_ARTICLES_SEARCH_OPTIMIZED
    WHERE DATE(WEB_PUBLICATION_DATE) BETWEEN %s AND %s
    ORDER BY WEB_PUBLICATION_DATE DESC
    LIMIT %s
    """
    
    try:
        # Validate dates
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
        
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (start_date, end_date, limit))
                rows = cur.fetchall()
                columns = [col[0].lower() for col in cur.description]
                results = [dict(zip(columns, row)) for row in rows]
                
                for result in results:
                    source = result.get('data_source', 'unknown')
                    result['source_label'] = '📚 Historical' if source == 'batch' else '🔴 Live'
                
                return {
                    "start_date": start_date,
                    "end_date": end_date,
                    "count": len(results),
                    "results": results
                }
    except ValueError:
        return {"error": "Invalid date format. Use YYYY-MM-DD"}
    except Exception as e:
        return {"error": str(e)}

@tool
def get_articles_by_section(section: str, days: int = 7, limit: int = 15) -> Dict[str, Any]:
    """Get recent articles from a section."""
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    query = f"""
    SELECT 
        ARTICLE_ID, 
        WEB_TITLE, 
        SECTION_NAME,
        WEB_PUBLICATION_DATE, 
        WEB_URL,
        preview || '...' AS preview
    FROM {database}.{schema}.VW_ARTICLES_SEARCH_OPTIMIZED
    WHERE (web_title_lower LIKE %s OR body_text_lower LIKE %s)
      AND WEB_PUBLICATION_DATE IS NOT NULL
    ORDER BY WEB_PUBLICATION_DATE DESC
    LIMIT %s
    """
    
    try:
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (section.strip(), -days, limit))
                rows = cur.fetchall()
                columns = [col[0].lower() for col in cur.description]
                results = [dict(zip(columns, row)) for row in rows]
                return {"section": section.strip(), "days": days, "articles": results}
    except Exception as e:
        return {"error": str(e)}


@tool
def get_trending_topics(days: int = 7, min_articles: int = 5) -> Dict[str, Any]:
    """Find trending sections."""
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    query = f"""
    SELECT s.SECTION_NAME, COUNT(*), MIN(a.WEB_PUBLICATION_DATE), MAX(a.WEB_PUBLICATION_DATE)
    FROM {database}.{schema}.FCT_ARTICLES a
    JOIN {database}.{schema}.DIM_SECTIONS s ON a.SECTION_KEY = s.SECTION_KEY
    WHERE a.WEB_PUBLICATION_DATE >= DATEADD(day, %s, CURRENT_DATE())
      AND a.IS_CURRENT = TRUE
    GROUP BY s.SECTION_NAME
    HAVING COUNT(*) >= %s
    ORDER BY COUNT(*) DESC
    LIMIT 10
    """
    try:
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (-days, min_articles))
                rows = cur.fetchall()
                return {
                    "period_days": days,
                    "trending": [
                        {"topic": r[0], "count": r[1], "from": str(r[2]), "to": str(r[3])}
                        for r in rows
                    ]
                }
    except Exception as e:
        return {"error": str(e)}


@tool
def get_article_by_id(article_id: str) -> Dict[str, Any]:
    """Get full article by ID."""
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    query = f"""
    SELECT a.WEB_TITLE, s.SECTION_NAME, a.WEB_PUBLICATION_DATE, a.WEB_URL, a.BODY_TEXT
    FROM {database}.{schema}.FCT_ARTICLES a
    JOIN {database}.{schema}.DIM_SECTIONS s ON a.SECTION_KEY = s.SECTION_KEY
    WHERE a.ARTICLE_ID = %s
      AND a.IS_CURRENT = TRUE
    """
    try:
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (article_id.strip(),))
                row = cur.fetchone()
                if not row:
                    return {"error": "Article not found"}
                return {
                    "title": row[0],
                    "section": row[1],
                    "published": str(row[2]),
                    "url": row[3],
                    "full_text": row[4]
                }
    except Exception as e:
        return {"error": str(e)}


@tool
def get_news_summary(date: Optional[str] = None) -> Dict[str, Any]:
    """Top 3 stories per section for a date."""
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    if date is None:
        date_filter = "DATE(a.WEB_PUBLICATION_DATE) = CURRENT_DATE()"
        display = "today"
    else:
        try:
            datetime.strptime(date, "%Y-%m-%d")
            date_filter = f"DATE(a.WEB_PUBLICATION_DATE) = '{date}'"
            display = date
        except ValueError:
            return {"error": "Date must be YYYY-MM-DD"}

    query = f"""
    WITH ranked AS (
        SELECT COALESCE(s.SECTION_NAME, 'Other') AS section, a.WEB_TITLE, a.WEB_URL,
               ROW_NUMBER() OVER (PARTITION BY COALESCE(s.SECTION_NAME, 'Other') ORDER BY a.WEB_PUBLICATION_DATE DESC) rn
        FROM {database}.{schema}.FCT_ARTICLES a
        JOIN {database}.{schema}.DIM_SECTIONS s ON a.SECTION_KEY = s.SECTION_KEY
        WHERE {date_filter}
          AND a.IS_CURRENT = TRUE
    )
    SELECT section, WEB_TITLE, WEB_URL FROM ranked WHERE rn <= 3 ORDER BY section, rn
    """
    try:
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                summary: Dict[str, List[Dict[str, str]]] = {}
                for sec, title, url in rows:
                    summary.setdefault(sec, []).append({"title": title, "url": url})
                return {"date": display, "summary": summary}
    except Exception as e:
        return {"error": str(e)}


# NEW TOOLS — Data Freshness
@tool
def get_latest_publication_date() -> Dict[str, Any]:
    """Return the most recent article publication date in the warehouse."""
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    query = f"""
    SELECT MAX(WEB_PUBLICATION_DATE) AS latest_date
    FROM {database}.{schema}.FCT_ARTICLES
    WHERE WEB_PUBLICATION_DATE IS NOT NULL
      AND IS_CURRENT = TRUE
    """
    try:
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                latest = row[0] if row and row[0] else None
                if latest:
                    return {
                        "latest_publication_date": latest.strftime("%B %d, %Y"),
                        "iso_date": latest.isoformat(),
                        "relative": "today" if latest.date() == datetime.now().date() else "recent"
                    }
                else:
                    return {"latest_publication_date": "No articles found"}
    except Exception as e:
        return {"error": str(e)}


@tool
def get_data_freshness() -> Dict[str, Any]:
    """How fresh is the data right now?"""
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    query = f"""
    SELECT 
        MAX(WEB_PUBLICATION_DATE) AS newest_article,
        COUNT(CASE WHEN DATE(WEB_PUBLICATION_DATE) = CURRENT_DATE THEN 1 END) AS articles_today,
        COUNT(CASE WHEN WEB_PUBLICATION_DATE >= CURRENT_DATE - 7 THEN 1 END) AS articles_last_7_days
    FROM {database}.{schema}.FCT_ARTICLES
    WHERE WEB_PUBLICATION_DATE IS NOT NULL
      AND IS_CURRENT = TRUE
    """
    try:
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()

                if not row or row[0] is None:
                    return {
                        "data_is_fresh": False,
                        "articles_today": 0,
                        "articles_last_week": 0,
                        "newest_article_date": "No articles yet",
                        "status": "Waiting for articles"
                    }

                newest = row[0]
                today_count = row[1] or 0
                week_count = row[2] or 0

                return {
                    "data_is_fresh": today_count > 0,
                    "articles_today": today_count,
                    "articles_last_week": week_count,
                    "newest_article_date": newest.strftime("%B %d, %Y"),
                    "status": "Live & up-to-date" if today_count > 0 else f"Latest article: {newest.strftime('%B %d, %Y')}"
                }
    except Exception as e:
        return {"error": str(e)}


@tool
def get_pinecone_index_stats() -> Dict[str, Any]:
    """Get statistics about the Guardian articles stored in the Pinecone vector database (RAG knowledge base)."""
    try:
        pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
        index_name = os.getenv("PINECONE_INDEX", "guardian-capstone")
        index = pc.Index(index_name)
        
        stats = index.describe_index_stats()
        namespace_stats = stats.namespaces.get("guardian_articles", {})
        vector_count = namespace_stats.get("vector_count", 0)
        
        return {
            "rag_knowledge_base": "Pinecone",
            "namespace": "guardian_articles",
            "total_chunks": vector_count,
            "index_total_vectors": stats.total_vector_count,
            "dimension": stats.dimension,
            "status": "healthy" if vector_count > 0 else "empty",
            "note": "This counts semantic chunks used for retrieval. Each article is typically split into multiple chunks for better accuracy."
        }
    except Exception as e:
        return {"error": f"Failed to retrieve Pinecone stats: {str(e)}"}


@tool
def get_approximate_unique_articles() -> Dict[str, Any]:
    """Estimate how many unique Guardian articles are represented in the Pinecone RAG database."""
    try:
        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        index_name = os.getenv("PINECONE_INDEX", "guardian-capstone")
        
        vector_store = PineconeVectorStore(
            index_name=index_name,
            embedding=embeddings,
            namespace="guardian_articles"
        )
        
        docs = vector_store.similarity_search("news", k=10000)  # dummy query
        
        unique_ids = {doc.metadata.get("article_id") for doc in docs if doc.metadata.get("article_id")}

        return {
            "estimated_unique_articles": len(unique_ids),
            "chunks_sampled": len(docs),
            "note": "Approximation based on sampling up to 10,000 chunks. Actual number may be slightly higher on large datasets."
        }
    except Exception as e:
        return {"error": f"Failed to estimate unique articles: {str(e)}"}


@tool
def get_article_full_text_and_summary(article_id: str) -> Dict[str, Any]:
    """Fetch full article body text and return an AI-generated summary."""
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    query = f"""
    SELECT a.WEB_TITLE, s.SECTION_NAME, a.WEB_PUBLICATION_DATE, a.WEB_URL, a.BODY_TEXT
    FROM {database}.{schema}.FCT_ARTICLES a 
    JOIN {database}.{schema}.DIM_SECTIONS s ON a.SECTION_KEY = s.SECTION_KEY
    WHERE a.ARTICLE_ID = %s 
      AND a.BODY_TEXT IS NOT NULL 
      AND LENGTH(a.BODY_TEXT) > 100
      AND a.IS_CURRENT = TRUE
    """
    try:
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (article_id.strip(),))
                row = cur.fetchone()
                if not row:
                    return {"error": "Article not found or has no body text"}
                
                title, section, pub_date, url, body_text = row
                
                summary_prompt = f"""
                Summarize the following Guardian article in 4–6 concise bullet points.
                Focus on key events, people, outcomes, and implications.

                Title: {title}
                Section: {section}
                Published: {pub_date.strftime("%B %d, %Y") if pub_date else "Unknown"}
                URL: {url}

                Article body:
                {body_text[:15000]}  # Safe limit
                """

                response: AIMessage = ChatOpenAI(model="gpt-4o-mini", temperature=0).invoke(summary_prompt)
                summary = str(response.content).strip()

                return {
                    "article_id": article_id,
                    "title": title,
                    "section": section,
                    "published": pub_date.strftime("%B %d, %Y") if pub_date else "Unknown",
                    "url": url,
                    "summary": summary,
                    "full_text_length": len(body_text),
                    "preview": body_text[:500] + "..." if len(body_text) > 500 else body_text
                }
    except Exception as e:
        return {"error": f"Failed to summarize article: {str(e)}"}


@tool
def get_priority_news(
    date: Optional[str] = None,
    days: int = 7,
    limit: int = 7
) -> Dict[str, Any]:
    """Prioritized, sentiment-aware editor's picks with explanation."""
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    # Initialize params once at the top — avoids shadowing and type conflicts
    params: List[Any] = []
    
    if date:
        date_filter = "DATE(a.WEB_PUBLICATION_DATE) = %s"
        params.append(date)
    else:
        date_filter = "a.WEB_PUBLICATION_DATE >= DATEADD(day, %s, CURRENT_DATE())"
        params.append(days)

    query = f"""
    WITH candidates AS (
        SELECT 
            a.ARTICLE_ID, a.WEB_TITLE, s.SECTION_NAME, a.WEB_PUBLICATION_DATE, a.WEB_URL, a.BODY_TEXT,
            CASE 
                WHEN LOWER(a.WEB_TITLE) LIKE '%%trump%%' OR LOWER(a.WEB_TITLE) LIKE '%%climate%%' 
                  OR LOWER(a.WEB_TITLE) LIKE '%%war%%' OR LOWER(a.WEB_TITLE) LIKE '%%ai%%' 
                  OR LOWER(a.WEB_TITLE) LIKE '%%election%%' THEN 10
                WHEN s.SECTION_NAME IN ('Politics', 'World news', 'US news', 'Environment') THEN 5
                ELSE 1
            END AS impact_score
        FROM {database}.{schema}.FCT_ARTICLES a
        JOIN {database}.{schema}.DIM_SECTIONS s ON a.SECTION_KEY = s.SECTION_KEY
        WHERE {date_filter}
          AND a.BODY_TEXT IS NOT NULL
          AND LENGTH(a.BODY_TEXT) > 500
          AND a.IS_CURRENT = TRUE
    )
    SELECT ARTICLE_ID, WEB_TITLE, SECTION_NAME, WEB_PUBLICATION_DATE, WEB_URL, BODY_TEXT, impact_score
    FROM candidates
    ORDER BY impact_score DESC, WEB_PUBLICATION_DATE DESC
    LIMIT %s
    """

    params.append(limit)  # Now safe: params is List[Any]

    try:
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                if not rows:
                    return {"message": f"No significant articles found for that period."}

                prioritized = []
                for i, row in enumerate(rows, 1):
                    article_id, title, section, pub_date, url, body = row[:6]
                    
                    response: AIMessage = ChatOpenAI(model="gpt-4o-mini", temperature=0).invoke(
                        f"In 2 sentences max, explain why this Guardian article matters today:\n\n"
                        f"Title: {title}\nSection: {section}\n"
                        f"First 1000 chars: {body[:1000]}"
                    )
                    why_it_matters = str(response.content).strip()

                    sentiment = "Neutral"
                    if any(w in why_it_matters.lower() for w in ["crisis", "threat", "warning", "controversy"]):
                        sentiment = "Negative"
                    elif any(w in why_it_matters.lower() for w in ["breakthrough", "hope", "success"]):
                        sentiment = "Positive"

                    prioritized.append({
                        "rank": i,
                        "title": title,
                        "section": section,
                        "published": pub_date.strftime("%B %d, %Y"),
                        "url": url,
                        "sentiment": sentiment,
                        "why_it_matters": why_it_matters
                    })

                return {
                    "period": date or f"last {days} days",
                    "total": len(prioritized),
                    "editors_picks": prioritized
                }
    except Exception as e:
        return {"error": f"Query failed: {str(e)}"}

@tool
def search_articles_by_data_source(
    data_source: str,
    keyword: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 20
) -> Dict[str, Any]:
    """
    Search articles filtered by data source.
    
    IMPORTANT DATE BOUNDARY:
    - BATCH source: Articles published BEFORE May 11, 2024 (HuggingFace dataset)
    - REALTIME source: Articles published FROM May 11, 2024 onwards (Guardian API)
    
    Args:
        data_source: 'batch' (pre-May 11, 2024) or 'realtime' (post-May 11, 2024)
        keyword: Optional keyword to search in title/body
        start_date: Optional YYYY-MM-DD (will be validated against source boundary)
        end_date: Optional YYYY-MM-DD (will be validated against source boundary)
        limit: Max results (default 20)
    """
    from datetime import datetime
    
    if data_source.lower() not in ['batch', 'realtime']:
        return {"error": "data_source must be 'batch' or 'realtime'"}
    
    # Validate date ranges against source boundary
    boundary_date = datetime.strptime("2024-05-11", "%Y-%m-%d")
    
    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        if data_source.lower() == 'batch' and start_dt >= boundary_date:
            return {
                "error": f"Date {start_date} is after May 11, 2024. Batch source only contains articles before this date.",
                "suggestion": "Use data_source='realtime' for dates from May 11, 2024 onwards"
            }
        if data_source.lower() == 'realtime' and start_dt < boundary_date:
            return {
                "error": f"Date {start_date} is before May 11, 2024. Realtime source only contains articles from this date onwards.",
                "suggestion": "Use data_source='batch' for dates before May 11, 2024"
            }
    
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    filters = ["DATA_SOURCE = %s"]
    params: List[Any] = [data_source.lower()]
    
    if keyword:
        filters.append("(web_title_lower LIKE %s OR body_text_lower LIKE %s)")
        pattern = f"%{keyword.lower()}%"
        params.extend([pattern, pattern])
    
    if start_date:
        filters.append("DATE(WEB_PUBLICATION_DATE) >= %s")
        params.append(start_date)
    
    if end_date:
        filters.append("DATE(WEB_PUBLICATION_DATE) <= %s")
        params.append(end_date)
    
    where_clause = " AND ".join(filters)
    params.append(limit)
    
    query = f"""
    SELECT 
        ARTICLE_ID, WEB_TITLE, SECTION_NAME,
        WEB_PUBLICATION_DATE, WEB_URL, DATA_SOURCE,
        preview || '...' AS preview
    FROM {database}.{schema}.VW_ARTICLES_SEARCH_OPTIMIZED
    WHERE {where_clause}
    ORDER BY WEB_PUBLICATION_DATE DESC
    LIMIT %s
    """
    
    try:
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                columns = [col[0].lower() for col in cur.description]
                results = [dict(zip(columns, row)) for row in rows]
                
                for result in results:
                    source = result.get('data_source', 'unknown')
                    pub_date = result.get('web_publication_date')
                    
                    # Enhanced labeling with date context
                    if source == 'batch':
                        result['source_label'] = '📚 Historical (pre-May 2024)'
                    else:
                        result['source_label'] = '🔴 Live (post-May 2024)'
                    
                    # Format publication date
                    if pub_date:
                        result['published_formatted'] = pub_date.strftime('%B %d, %Y')
                
                return {
                    "data_source": data_source,
                    "source_info": "📚 Pre-May 11, 2024" if data_source == "batch" else "🔴 Post-May 11, 2024",
                    "filters": {
                        "keyword": keyword,
                        "start_date": start_date,
                        "end_date": end_date
                    },
                    "count": len(results),
                    "results": results
                }
    except Exception as e:
        return {"error": str(e)}

@tool
def get_data_source_breakdown() -> Dict[str, Any]:
    """
    Get statistics about batch vs realtime data coverage.
    
    BATCH: Historical archive (before May 11, 2024) from HuggingFace
    REALTIME: Live feed (from May 11, 2024 onwards) from Guardian API
    """
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    query = f"""
    SELECT 
        DATA_SOURCE,
        COUNT(*) as article_count,
        MIN(WEB_PUBLICATION_DATE) as earliest_date,
        MAX(WEB_PUBLICATION_DATE) as latest_date,
        COUNT(CASE WHEN WEB_PUBLICATION_DATE >= CURRENT_DATE - 7 THEN 1 END) as last_7_days,
        COUNT(CASE WHEN DATE(WEB_PUBLICATION_DATE) = CURRENT_DATE THEN 1 END) as today
    FROM {database}.{schema}.VW_ARTICLES_SEARCH_OPTIMIZED
    WHERE DATA_SOURCE IS NOT NULL
    GROUP BY DATA_SOURCE
    ORDER BY DATA_SOURCE
    """
    
    try:
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                
                breakdown = {}
                for row in rows:
                    source = row[0]
                    breakdown[source] = {
                        "total_articles": row[1],
                        "earliest_date": row[2].strftime("%Y-%m-%d") if row[2] else None,
                        "latest_date": row[3].strftime("%Y-%m-%d") if row[3] else None,
                        "articles_last_7_days": row[4],
                        "articles_today": row[5],
                    }
                    
                    # Add source-specific metadata
                    if source == "batch":
                        breakdown[source].update({
                            "label": "📚 Historical Archive",
                            "source": "HuggingFace dataset",
                            "date_coverage": "Before May 11, 2024",
                            "boundary_date": "2024-05-11"
                        })
                    else:
                        breakdown[source].update({
                            "label": "🔴 Live Feed",
                            "source": "Guardian API",
                            "date_coverage": "From May 11, 2024 onwards",
                            "start_date": "2024-05-11"
                        })
                
                return {
                    "breakdown": breakdown,
                    "total_sources": len(breakdown),
                    "boundary_date": "2024-05-11",
                    "note": "Data sources are separated by publication date at May 11, 2024"
                }
    except Exception as e:
        return {"error": str(e)}

@tool
def compare_batch_vs_realtime_coverage(
    topic: str,
    days: int = 30
) -> Dict[str, Any]:
    """
    Compare how a topic is covered in batch (historical) vs realtime (live) data.
    Useful for understanding data freshness and coverage differences.
    
    Args:
        topic: Keyword or topic to compare (e.g., 'climate', 'trump', 'ai')
        days: Number of days to analyze (default 30)
    """
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    query = f"""
    SELECT 
        DATA_SOURCE,
        COUNT(*) as article_count,
        COUNT(DISTINCT SECTION_NAME) as sections_covered,
        MIN(WEB_PUBLICATION_DATE) as earliest,
        MAX(WEB_PUBLICATION_DATE) as latest
    FROM {database}.{schema}.VW_ARTICLES_SEARCH_OPTIMIZED
    WHERE (web_title_lower LIKE %s OR body_text_lower LIKE %s)
      AND WEB_PUBLICATION_DATE >= DATEADD(day, %s, CURRENT_DATE())
    GROUP BY DATA_SOURCE
    ORDER BY DATA_SOURCE
    """
    
    pattern = f"%{topic.lower()}%"
    
    try:
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (pattern, pattern, -days))
                rows = cur.fetchall()
                
                comparison = {}
                for row in rows:
                    source = row[0]
                    comparison[source] = {
                        "article_count": row[1],
                        "sections_covered": row[2],
                        "earliest": row[3].strftime("%Y-%m-%d %H:%M") if row[3] else None,
                        "latest": row[4].strftime("%Y-%m-%d %H:%M") if row[4] else None,
                        "label": "📚 Historical" if source == "batch" else "🔴 Live"
                    }
                
                return {
                    "topic": topic,
                    "period_days": days,
                    "comparison": comparison
                }
    except Exception as e:
        return {"error": str(e)}

@tool
def get_batch_data_info() -> Dict[str, Any]:
    """
    Get information about the batch (historical) data source.
    The batch source is a static HuggingFace dataset containing all Guardian articles before May 11, 2024.
    """
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    query = f"""
    SELECT 
        COUNT(*) as total_articles,
        MIN(WEB_PUBLICATION_DATE) as earliest_article,
        MAX(WEB_PUBLICATION_DATE) as latest_article,
        COUNT(DISTINCT SECTION_NAME) as sections_covered
    FROM {database}.{schema}.VW_ARTICLES_SEARCH_OPTIMIZED
    WHERE DATA_SOURCE = 'batch'
    """
    
    try:
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                
                if row is None:
                    return {
                        "source": "📚 HuggingFace Historical Dataset",
                        "type": "Static archive (not updated)",
                        "date_range": "Unknown",
                        "boundary_date": "2024-05-11",
                        "total_articles": 0,
                        "earliest_article": None,
                        "latest_article": None,
                        "sections_covered": 0,
                        "note": "No data available"
                    }

                return {
                    "source": "📚 HuggingFace Historical Dataset",
                    "type": "Static archive (not updated)",
                    "date_range": "All articles published before May 11, 2024",
                    "boundary_date": "2024-05-11",
                    "total_articles": row[0],
                    "earliest_article": row[1].strftime("%Y-%m-%d") if row[1] else None,
                    "latest_article": row[2].strftime("%Y-%m-%d") if row[2] else None,
                    "sections_covered": row[3],
                    "note": "This is a historical archive. For articles from May 11, 2024 onwards, use the realtime source."
                }
    except Exception as e:
        return {"error": str(e)}

@tool
def smart_search_articles(
    keyword: str,
    prefer_recent: bool = True,
    limit: int = 10,
    min_results_threshold: int = 5,
    use_semantic_search: bool = True
) -> Dict[str, Any]:
    """
    Intelligent hybrid search (Pinecone semantic + SQL keyword) that automatically falls back between sources.
    
    Args:
        keyword: Search term
        prefer_recent: If True, try realtime first. If False, try batch first.
        limit: Max results per source
        min_results_threshold: Minimum results before triggering fallback
        use_semantic_search: If True, use Pinecone semantic search + SQL. If False, SQL only.
    
    Returns:
        Articles from realtime and/or batch with smart fallback logic
    """
    from datetime import datetime
    
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    results = {
        "keyword": keyword,
        "realtime": [],
        "batch": [],
        "semantic": [],
        "strategy": "",
        "used_semantic": use_semantic_search
    }
    
    pattern = f"%{keyword.lower()}%"
    
    # STEP 1: Semantic Search (Pinecone) if enabled
    semantic_article_ids = set()
    if use_semantic_search:
        try:
            from langchain_pinecone import PineconeVectorStore
            from langchain_openai import OpenAIEmbeddings
            
            embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
            vector_store = PineconeVectorStore(
                index_name=os.getenv("PINECONE_INDEX", "guardian-capstone"),
                embedding=embeddings,
                namespace="guardian_articles"
            )
            
            # Semantic search - get more results for diversity
            semantic_docs = vector_store.similarity_search(keyword, k=limit * 2)
            
            # Extract article IDs and metadata
            for doc in semantic_docs:
                article_id = doc.metadata.get("article_id")
                if article_id:
                    semantic_article_ids.add(article_id)
                    results['semantic'].append({
                        "article_id": article_id,
                        "title": doc.metadata.get("title", "Unknown"),
                        "section": doc.metadata.get("section", "Unknown"),
                        "published_date": doc.metadata.get("published_date"),
                        "url": doc.metadata.get("web_url", ""),
                        "data_source": doc.metadata.get("data_source", "unknown"),
                        "preview": doc.page_content[:200] + "...",
                        "retrieval_method": "semantic"
                    })
            
            print(f"   🔍 Semantic search found {len(semantic_article_ids)} unique articles")
        except Exception as e:
            print(f"   ⚠️ Semantic search failed: {str(e)}, falling back to SQL-only")
            use_semantic_search = False
    
    # STEP 2: SQL Search with optional semantic boost
    def search_source(source: str) -> List[Dict]:
        """Search SQL with optional semantic article ID boosting."""
        
        if use_semantic_search and semantic_article_ids:
            # Boost articles found by semantic search
            article_ids_str = ", ".join([f"'{aid}'" for aid in list(semantic_article_ids)[:50]])
            
            query = f"""
            SELECT 
                ARTICLE_ID, WEB_TITLE, SECTION_NAME,
                WEB_PUBLICATION_DATE, WEB_URL, DATA_SOURCE,
                preview || '...' AS preview,
                CASE 
                    WHEN ARTICLE_ID IN ({article_ids_str}) THEN 1
                    ELSE 0
                END as semantic_boost
            FROM {database}.{schema}.VW_ARTICLES_SEARCH_OPTIMIZED
            WHERE DATA_SOURCE = %s
              AND (
                  ARTICLE_ID IN ({article_ids_str})
                  OR web_title_lower LIKE %s 
                  OR body_text_lower LIKE %s
              )
            ORDER BY semantic_boost DESC, WEB_PUBLICATION_DATE DESC
            LIMIT %s
            """
            params = (source, pattern, pattern, limit)
        else:
            # Standard SQL keyword search
            query = f"""
            SELECT 
                ARTICLE_ID, WEB_TITLE, SECTION_NAME,
                WEB_PUBLICATION_DATE, WEB_URL, DATA_SOURCE,
                preview || '...' AS preview
            FROM {database}.{schema}.VW_ARTICLES_SEARCH_OPTIMIZED
            WHERE DATA_SOURCE = %s
              AND (web_title_lower LIKE %s OR body_text_lower LIKE %s)
            ORDER BY WEB_PUBLICATION_DATE DESC
            LIMIT %s
            """
            params = (source, pattern, pattern, limit)
        
        try:
            with snowflake_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    rows = cur.fetchall()
                    columns = [col[0].lower() for col in cur.description]
                    articles = [dict(zip(columns, row)) for row in rows]
                    
                    for article in articles:
                        src = article.get('data_source', 'unknown')
                        article['source_label'] = '📚 Historical (pre-May 2024)' if src == 'batch' else '🔴 Live (post-May 2024)'
                        
                        if article.get('web_publication_date'):
                            article['published_formatted'] = article['web_publication_date'].strftime('%B %d, %Y')
                        
                        # Mark if found by semantic search
                        if use_semantic_search and article.get('article_id') in semantic_article_ids:
                            article['semantic_match'] = True
                    
                    return articles
        except Exception as e:
            print(f"   ❌ SQL search failed for {source}: {str(e)}")
            return []
    
    # STEP 3: Smart search strategy with fallback
    if prefer_recent:
        # Try realtime first
        print(f"   🔍 Searching realtime source for '{keyword}'...")
        results['realtime'] = search_source('realtime')
        
        if len(results['realtime']) < min_results_threshold:
            # Also get batch to supplement
            print(f"   🔍 Supplementing with batch source (found {len(results['realtime'])} realtime)...")
            results['batch'] = search_source('batch')
            
            if len(results['realtime']) == 0:
                results['strategy'] = "fallback_to_batch"
                results['message'] = f"No recent articles found about '{keyword}' (post-May 2024). Showing {len(results['batch'])} from historical archive (pre-May 2024)."
            else:
                results['strategy'] = "supplemented_with_batch"
                results['message'] = f"Found {len(results['realtime'])} recent article(s) about '{keyword}' (post-May 2024). Supplemented with {len(results['batch'])} from historical archive (pre-May 2024) for broader coverage."
        else:
            results['strategy'] = "realtime_success"
            results['message'] = f"Found {len(results['realtime'])} recent article(s) about '{keyword}' from our live feed."
    else:
        # Try batch first (for historical queries)
        print(f"   🔍 Searching batch source for '{keyword}'...")
        results['batch'] = search_source('batch')
        
        if len(results['batch']) < min_results_threshold:
            # Also get realtime to supplement
            print(f"   🔍 Supplementing with realtime source (found {len(results['batch'])} batch)...")
            results['realtime'] = search_source('realtime')
            
            if len(results['batch']) == 0:
                results['strategy'] = "fallback_to_realtime"
                results['message'] = f"No historical articles found about '{keyword}' (pre-May 2024). Showing {len(results['realtime'])} from live feed (post-May 2024)."
            else:
                results['strategy'] = "supplemented_with_realtime"
                results['message'] = f"Found {len(results['batch'])} historical article(s) about '{keyword}'. Supplemented with {len(results['realtime'])} from live feed (post-May 2024)."
        else:
            results['strategy'] = "batch_success"
            results['message'] = f"Found {len(results['batch'])} historical article(s) about '{keyword}' from our archive."
    
    # STEP 4: Combine and deduplicate results
    all_articles = results['realtime'] + results['batch']
    
    # Deduplicate by article_id
    seen_ids = set()
    unique_articles = []
    for article in all_articles:
        article_id = article.get('article_id')
        if article_id and article_id not in seen_ids:
            seen_ids.add(article_id)
            unique_articles.append(article)
        elif not article_id:
            unique_articles.append(article)
    
    if not unique_articles:
        results['strategy'] = "no_results"
        results['message'] = f"No articles found about '{keyword}' in either source. Try different search terms or broader topics."
    
    results['total_count'] = len(unique_articles)
    results['all_articles'] = unique_articles
    
    # Add search method info
    if use_semantic_search:
        semantic_count = sum(1 for a in unique_articles if a.get('semantic_match'))
        results['search_method'] = f"Hybrid (Pinecone + SQL) - {semantic_count}/{len(unique_articles)} boosted by semantic search"
    else:
        results['search_method'] = "SQL keyword search only"
    
    return results

@tool
def comprehensive_search(
    keyword: str,
    limit_per_source: int = 5
) -> Dict[str, Any]:
    """
    Search BOTH sources and show results from each separately.
    Use when you want complete coverage across time periods.
    
    Args:
        keyword: Search term
        limit_per_source: Max results from each source
    """
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = "SC_T26_MARTS"
    
    pattern = f"%{keyword.lower()}%"
    
    query = f"""
    SELECT 
        DATA_SOURCE,
        ARTICLE_ID, WEB_TITLE, SECTION_NAME,
        WEB_PUBLICATION_DATE, WEB_URL,
        preview || '...' AS preview
    FROM {database}.{schema}.VW_ARTICLES_SEARCH_OPTIMIZED
    WHERE (web_title_lower LIKE %s OR body_text_lower LIKE %s)
    ORDER BY DATA_SOURCE, WEB_PUBLICATION_DATE DESC
    """
    
    try:
        with snowflake_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (pattern, pattern))
                rows = cur.fetchall()
                columns = [col[0].lower() for col in cur.description]
                all_results = [dict(zip(columns, row)) for row in rows]
        
        # Separate by source
        batch_articles = [r for r in all_results if r.get('data_source') == 'batch'][:limit_per_source]
        realtime_articles = [r for r in all_results if r.get('data_source') == 'realtime'][:limit_per_source]
        
        # Add labels
        for article in batch_articles:
            article['source_label'] = '📚 Historical (pre-May 2024)'
            if article.get('web_publication_date'):
                article['published_formatted'] = article['web_publication_date'].strftime('%B %d, %Y')
        
        for article in realtime_articles:
            article['source_label'] = '🔴 Live (post-May 2024)'
            if article.get('web_publication_date'):
                article['published_formatted'] = article['web_publication_date'].strftime('%B %d, %Y')
        
        return {
            "keyword": keyword,
            "realtime": {
                "count": len(realtime_articles),
                "articles": realtime_articles,
                "label": "🔴 Recent Coverage (post-May 2024)"
            },
            "batch": {
                "count": len(batch_articles),
                "articles": batch_articles,
                "label": "📚 Historical Coverage (pre-May 2024)"
            },
            "total_count": len(batch_articles) + len(realtime_articles),
            "message": f"Found {len(realtime_articles)} recent and {len(batch_articles)} historical articles about '{keyword}'"
        }
    except Exception as e:
        return {"error": str(e)}

# Updated tools list
tools = [
    smart_search_articles,
    comprehensive_search,
    search_articles_by_keyword,
    search_articles_by_multiple_keywords,
    search_articles_by_data_source,
    get_articles_by_section,
    get_trending_topics,
    get_article_by_id,
    get_news_summary,
    get_latest_publication_date,
    get_data_freshness,
    get_article_full_text_and_summary,
    get_priority_news,
    get_pinecone_index_stats,
    get_approximate_unique_articles,
    get_data_source_breakdown, 
    compare_batch_vs_realtime_coverage,
    get_data_source_info, 
    determine_data_source_by_date,  
    get_batch_data_info, 
]

tool_node = ToolNode(tools)


# ==================== AGENT SETUP ====================
class AgentState(TypedDict):
    messages: Annotated[List[Any], add_messages]

model = ChatOpenAI(model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"), temperature=0)

system_prompt = """
You are a professional Guardian news analyst with access to a hybrid data architecture combining historical archives and live feeds.

## DATA ARCHITECTURE

**Two Data Sources:**
- 📚 **Batch** (data_source='batch'): Historical HuggingFace dataset archive
- 🔴 **Realtime** (data_source='realtime'): Live Guardian API feed

**Storage Layers:**
- Snowflake: Structured metadata, SQL querying, filtering, aggregations
- Pinecone: Semantic vector search for content retrieval

## TOOL SELECTION GUIDE

**Temporal Queries:**
- Specific date → search_articles_by_date(date="YYYY-MM-DD")
- Date range → search_articles_by_date_range(start_date, end_date)
- Recent articles → get_articles_by_section(section, days=7)
- Latest publication → get_latest_publication_date()
- Data freshness check → get_data_freshness()

**Content Search:**
- Single keyword → search_articles_by_keyword(keyword)
- Multiple keywords (AND logic) → search_articles_by_multiple_keywords(keywords=[...])
- Section-based → get_articles_by_section(section)
- Full article text → get_article_by_id(article_id)
- Article with AI summary → get_article_full_text_and_summary(article_id)

**Data Source Queries:**
- Filter by source → search_articles_by_data_source(data_source="batch"|"realtime")
- Source statistics → get_data_source_breakdown()
- Coverage comparison → compare_batch_vs_realtime_coverage(topic)
- Recently loaded batch → get_recently_loaded_batch_articles(hours=24)

**Analytics & Trends:**
- Trending topics → get_trending_topics(days=7)
- Daily summary → get_news_summary(date="YYYY-MM-DD")
- Priority news → get_priority_news(days=7, limit=7)

**Database Metadata:**
- Pinecone stats → get_pinecone_index_stats()
- Unique article count → get_approximate_unique_articles()
- Data freshness → get_data_freshness()

## RESPONSE GUIDELINES

**Source Attribution:**
- Always include data source badges in results: 📚 for batch, 🔴 for realtime
- Cite URLs when available
- Show loaded timestamps (LOADED_AT) when discussing data ingestion

**Conversational Context:**
- When users reference "that date" or "those articles", extract context from conversation history
- Use tools to verify - never assume or hallucinate based on prior responses
- If a query needs data you just provided, call the appropriate tool again

**Query Classification:**
- Metadata questions (data sources, timestamps, counts) → Use structured SQL tools directly
- Content questions (article text, topics) → May use semantic search OR SQL tools
- Temporal questions (dates, recency) → Use date-specific tools
- Analytical questions (trends, comparisons) → Use analytics tools

**Data Source Awareness:**
- Batch data typically covers historical periods
- Realtime data contains current/breaking news
- When freshness matters, check get_data_freshness() first
- For "latest" queries, prioritize realtime source

**Error Handling:**
- If no results found, suggest alternative search terms or broader date ranges
- For ambiguous queries, ask clarifying questions about date range or data source preference
- Never claim data doesn't exist without checking appropriate tools

**Citation Format:**
When presenting articles, use:
```
[Source Badge] Title
Section: [Section Name] | Published: [Date] | Source: [Batch/Realtime]
URL: [Link]
Loaded: [Timestamp if relevant]
```

Be accurate, cite sources with badges and URLs, acknowledge limitations, and never hallucinate.
"""

prompt = ChatPromptTemplate.from_messages([
    ("system", system_prompt),
    ("placeholder", "{messages}"),
])

model_with_tools = prompt | model.bind_tools(tools)

def chatbot(state: AgentState, config: RunnableConfig) -> Dict[str, Any]:
    result = model_with_tools.invoke({"messages": state["messages"]}, config=config)
    return {"messages": [result]}

workflow = StateGraph(AgentState)
workflow.add_node("chatbot", chatbot)
workflow.add_node("tools", tool_node)
workflow.add_edge(START, "chatbot")
workflow.add_conditional_edges("chatbot", tools_condition)
workflow.add_edge("tools", "chatbot")

memory = MemorySaver()
app = workflow.compile(checkpointer=memory)


# ==================== INTERACTIVE RUNNER ====================
if __name__ == "__main__":
    print("Guardian News AI Analyst — Live from Snowflake (Mart Layer) + Pinecone RAG")
    print("=" * 60)

    config: RunnableConfig = {"configurable": {"thread_id": "capstone_final_demo_v3"}}

    while True:
        try:
            user_input = input("\nYou: ").strip()
            if user_input.lower() in {"quit", "exit", "q", "bye"}:
                print("Goodbye!")
                break
            if not user_input:
                continue

            print("Thinking...\n")

            input_state: AgentState = {"messages": [HumanMessage(content=user_input)]}

            for event in app.stream(input_state, config, stream_mode="values"):
                msg = event["messages"][-1]
                if msg.content and not getattr(msg, "tool_calls", None):
                    print(f"Assistant: {msg.content}\n")
                elif getattr(msg, "tool_calls", None):
                    names = [tc["name"] for tc in msg.tool_calls]
                    print(f"Tools → {', '.join(names)}")

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")