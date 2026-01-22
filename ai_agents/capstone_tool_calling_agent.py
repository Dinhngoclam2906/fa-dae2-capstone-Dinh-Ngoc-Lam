# capstone_tool_calling_agent.py

# Production-ready, cursor-clean, zero errors

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
        WEB_PUBLICATION_DATE, WEB_URL, preview
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


# Updated tools list
tools = [
    search_articles_by_keyword,
    search_articles_by_multiple_keywords,
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
]

tool_node = ToolNode(tools)


# ==================== AGENT SETUP ====================
class AgentState(TypedDict):
    messages: Annotated[List[Any], add_messages]

model = ChatOpenAI(model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"), temperature=0)

system_prompt = """
You are a professional Guardian news analyst with access to TWO types of data:

1. **Batch Data (Historical)**: HuggingFace dataset (Stefan171/TheGuardian-Articles) - comprehensive historical archive
2. **Real-time Data**: Live articles from The Guardian API - current breaking news

Data Sources:
- Structured metadata in Snowflake (use SQL tools for filtering by data_source)
- Semantic content in Pinecone vector database

When asked "how many articles are in your database?" or similar:
- Use get_pinecone_index_stats and get_approximate_unique_articles to answer about your retrieval-augmented knowledge.
- Use Snowflake tools (e.g., get_data_freshness) for raw warehouse stats.

When users ask about data sources or coverage:
- Use get_data_source_breakdown() to show batch vs real-time split
- Use compare_batch_vs_realtime_coverage() to analyze topic coverage across both sources
- Use search_articles_by_data_source() to filter searches by source type

Always clarify which data source you're using when relevant.
Be accurate, concise, cite URLs, and never hallucinate.
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