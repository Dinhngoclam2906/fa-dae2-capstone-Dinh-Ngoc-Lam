# app.py - Optimized Hybrid RAG with Pinecone Best Practices
import time
import re
import streamlit as st
from dotenv import load_dotenv
import structlog
import logging
from pythonjsonlogger.json import JsonFormatter

# LangChain / LangGraph imports
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langchain_core.runnables import RunnableConfig
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_core.prompts import ChatPromptTemplate
from langchain_pinecone import PineconeVectorStore
from langchain_core.documents import Document
from langgraph.graph import START, END, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict
from typing import Annotated, List, Any, Dict, Optional
import psycopg
from psycopg import Connection
from psycopg.rows import DictRow, dict_row
import os
from flashrank import Ranker, RerankRequest
from pinecone import Pinecone
from keybert import KeyBERT
from sentence_transformers import SentenceTransformer
import nltk
from nltk.corpus import stopwords

# Your tool-calling module
from capstone_tool_calling_agent import tools

load_dotenv()

# ==================== STRUCTURED LOGGING SETUP ====================
def configure_logging():
    """
    Configure structured logging for production-ready observability.
    
    Logs include:
    - Timestamp
    - Log level
    - Event name
    - Contextual data (user, query, performance metrics)
    - Stack traces (for errors)
    """
    
    # ✅ Get root logger and clear existing handlers
    root_logger = logging.getLogger()
    
    # Clear ALL existing handlers to prevent duplicates
    root_logger.handlers.clear()
    
    # Configure standard library logging
    log_handler = logging.StreamHandler()
    
    # JSON formatter for structured output
    formatter = JsonFormatter(
        fmt='%(timestamp)s %(level)s %(event)s %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S'
    )
    log_handler.setFormatter(formatter)
    
    # Set up root logger
    root_logger.addHandler(log_handler)
    root_logger.setLevel(logging.INFO)
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

if "logging_configured" not in st.session_state:
    configure_logging()
    st.session_state.logging_configured = True

logger = structlog.get_logger()

# ==================== PAGE CONFIG & STYLE ====================
st.set_page_config(
    page_title="Lam's Capstone AI Agent",
    page_icon="📰",
    layout="centered",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .main { background-color: #0e1117; }
    [data-testid="stChatMessageUser"] { background: #2d5a8c; color: white; border-radius: 16px; padding: 12px; }
    [data-testid="stChatMessageAssistant"] { background: #16213e; border: 1px solid #4e9af1; border-radius: 16px; padding: 12px; }
    .stTextInput > div > div > input { background: #262730; color: white; border-radius: 12px; }
    .sidebar .sidebar-content { background: #16181d; }
    h1 { color: #4e9af1; text-align: center; }
</style>
""", unsafe_allow_html=True)

# ==================== VECTOR STORE SETUP (PINECONE) ====================
@st.cache_resource
def get_vector_store():
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
    index_name = os.getenv("PINECONE_INDEX", "guardian-capstone")
    
    try:
        vector_store = PineconeVectorStore(
            index_name=index_name,
            embedding=embeddings,
            namespace="guardian_articles"
        )
        # Test connection
        logger.info("Warming up Pinecone index...")
        vector_store.similarity_search("test", k=1)
        logger.info("✅ Pinecone index warmed up")
        return vector_store
    except Exception as e:
        st.warning(f"Vector store not available: {e}. Falling back to SQL-only mode.")
        return None

# vector_store = get_vector_store()

@st.cache_resource
def get_reranker():
    """Initialize FlashRank reranker (local, fast)."""
    return Ranker(model_name="ms-marco-MiniLM-L-12-v2", cache_dir="/tmp")

# reranker = get_reranker()

@st.cache_resource
def get_pinecone_client():
    """Get Pinecone client for potential future sparse index."""
    try:
        return Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
    except:
        return None

pinecone_client = get_pinecone_client()

# ==================== PERSISTENT CHECKPOINTER SETUP ====================
def create_checkpointer():
    try:
        host = os.getenv("LANGGRAPH_POSTGRES_HOST", "localhost")
        port = os.getenv("LANGGRAPH_POSTGRES_PORT", "5433")
        database = os.getenv("LANGGRAPH_POSTGRES_DB", "langgraph_memory")
        user = os.getenv("LANGGRAPH_POSTGRES_USER", "postgres")
        password = os.getenv("LANGGRAPH_POSTGRES_PASSWORD", "postgres")
        db_uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        connection = psycopg.connect(db_uri, autocommit=True)
        # conn: Connection[DictRow] = connection 
        checkpointer = PostgresSaver(connection) # type: ignore
        checkpointer.setup()
        return checkpointer
    except Exception as e:
        st.warning(f"PostgreSQL not available: {e}. Falling back to in-memory storage.")
        from langgraph.checkpoint.memory import MemorySaver
        return MemorySaver()

# ==================== HELPER FUNCTIONS FOR HYBRID SEARCH ====================
if "nltk_data_downloaded" not in st.session_state:
    try:
        nltk.data.find('corpora/stopwords')
        st.session_state.nltk_data_downloaded = True
    except LookupError:
        # Show friendly message to user (only on first run)
        with st.spinner("📦 One-time setup: Downloading NLTK data..."):
            nltk.download('stopwords', quiet=True)
        st.session_state.nltk_data_downloaded = True
        st.success("✅ Setup complete!")

_CACHED_STOPWORDS = None
def get_stopwords() -> set:
    """Get cached NLTK stopwords (loads only once)."""
    global _CACHED_STOPWORDS
    if _CACHED_STOPWORDS is None:
        base_stops = set(stopwords.words('english'))
        custom_stops = {
            'based', 'article', 'articles', 'news', 'compare', 'analyze',
            'summarize', 'describe', 'explain', 'show', 'find', 'give',
            'share', 'discuss', 'evaluate', 'review', 'examine', 'explore'
        }
        _CACHED_STOPWORDS = base_stops | custom_stops
    return _CACHED_STOPWORDS

def extract_keywords(query: str) -> List[str]:
    """
    Enhanced keyword extraction using NLTK stopwords.
    
    Improvements:
    1. Extracts ONLY surnames from multi-word names
    2. Filters out question verbs (compare, analyze, etc.)
    3. Prioritizes unique identifiers over generic terms
    """
    
    # Get NLTK stopwords
    stop_words = get_stopwords()
    
    # ✅ EXPANDED: Add question-specific stopwords
    stop_words.update({
        'based', 'article', 'articles', 'news', 'tell', 'according', 'say',
        'compare', 'analyze', 'summarize', 'describe', 'explain',
        'show', 'find', 'give', 'share', 'discuss', 'evaluate',
        'review', 'examine', 'explore', 'investigate', 'detail'
    })
    
    # Important short terms
    important_short_terms = {
        'ai', 'uk', 'us', 'eu', 'un', 'ceo', 'fbi', 'cia', 'gdp', 'nhs',
        'cop', 'g7', 'g20', 'ufo', 'nyc', 'la', 'qa', 'hr', 'it', 'pr',
        'q1', 'q2', 'q3', 'q4', 'gop', 'mlb', 'nba', 'nfl', 'ufc',
        'war', 'ban', 'law', 'tax', 'net'
    }
    
    keywords = []
    extracted_from_multiword = set()  # Track what we've extracted
    
    # ✅ STEP 1: Extract multi-word proper nouns (SURNAMES ONLY)
    multi_word_names = re.findall(r'\b([A-Z][a-z]+(?: [A-Z][a-z]+)+)\b', query)
    for name in multi_word_names:
        parts = name.split()
        if len(parts) >= 2:
            surname = parts[-1].lower()
            if surname not in stop_words:
                keywords.append(surname)
                extracted_from_multiword.add(surname)
                # ✅ Track first names too (to skip them later)
                for part in parts[:-1]:
                    extracted_from_multiword.add(part.lower())
    
    # ✅ STEP 2: Extract single capitalized words (ONLY if not from multi-word names)
    single_names = re.findall(r'\b[A-Z][a-z]{3,}\b', query)
    for name in single_names:
        name_lower = name.lower()
        # Skip if: stopword, already extracted, or was part of multi-word name
        if (name_lower not in stop_words and 
            name_lower not in keywords and
            name_lower not in extracted_from_multiword):
            keywords.append(name_lower)
    
    # ✅ STEP 3: Extract significant lowercase words
    all_words = re.findall(r'\b\w+\b', query.lower())
    
    for word in all_words:
        if word in keywords or word in extracted_from_multiword:
            continue
        
        if word in important_short_terms:
            keywords.append(word)
        elif word not in stop_words and len(word) >= 5:
            keywords.append(word)
    
    # ✅ STEP 4: Filter for SQL
    sql_keywords = [
        k for k in keywords 
        if len(k) >= 4 or k in important_short_terms
    ]
    
    # Remove duplicates while preserving order
    seen = set()
    sql_keywords = [k for k in sql_keywords if not (k in seen or seen.add(k))]  # type: ignore
    
    # Fallback
    if not sql_keywords and keywords:
        sql_keywords = [max(keywords, key=len)]
    
    return sql_keywords[:5]


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

# ==================== AGENT STATE & TOOLS ====================
class AgentState(TypedDict):
    messages: Annotated[List[Any], add_messages]
    user_name: str
    conversation_count: int

tool_node = ToolNode(tools)

llm = ChatOpenAI(
    model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
    temperature=0.7,
    model_kwargs={"max_tokens": 1500},
)

system_prompt = """
You are a professional Guardian news analyst with access to:
1. **Batch Historical Data** (HuggingFace dataset) - comprehensive archive for historical analysis
2. **Real-time Live Data** (Guardian API) - breaking news and current events
3. Dense semantic search (Pinecone vector database)
4. Structured data via Snowflake tools (SQL)
5. Uploaded user documents

When users ask about specific time periods or topics:
- Clarify if they want historical (batch) or current (real-time) coverage
- Use appropriate tools to filter by data_source
- Explain which data source provided the results

Context is retrieved using hybrid search (vector + SQL) and re-ranked for maximum relevance.

Always:
- Check data freshness first
- Cite URLs and sources
- Distinguish between Guardian articles (📰) and uploaded documents (📄)
- Never hallucinate - only use provided context

Be accurate, concise, and helpful.
"""

prompt = ChatPromptTemplate.from_messages([
    ("system", system_prompt),
    ("placeholder", "{messages}"),
])

# Bind tools
model_with_tools = prompt | llm.bind_tools(tools, tool_choice="auto")

def should_retrieve(query: str) -> bool:
    """
    Determine if a query needs retrieval from the knowledge base.
    
    Skip retrieval for:
    - Greetings and pleasantries
    - Meta questions about the assistant
    - Acknowledgments and feedback
    - Clarification requests without new information needs
    
    Returns:
        True if retrieval is needed, False otherwise
    """
    query_lower = query.lower().strip()
    
    # Pattern 1: Greetings and pleasantries
    greetings = {
        'hi', 'hello', 'hey', 'good morning', 'good afternoon', 
        'good evening', 'howdy', 'greetings'
    }
    if any(greeting in query_lower for greeting in greetings):
        return False
    
    # Pattern 2: Meta questions about the assistant (expanded)
    meta_patterns = [
        r'\bwho are you\b',
        r'\bwhat are you\b',
        r'\bwhat can you do\b',
        r'\bwhat do you do\b',
        r'\byour (name|capabilities|purpose)\b',
        r'\btell me about yourself\b',
        r'\bintroduce yourself\b',
        r'\bhow do you work\b',
        r'\bwhat\'s your (role|job|function)\b'
    ]
    if any(re.search(pattern, query_lower) for pattern in meta_patterns):
        return False
    
    # Pattern 3: Acknowledgments and feedback
    acknowledgments = {
        'thanks', 'thank you', 'ok', 'okay', 'got it', 'understood',
        'bye', 'goodbye', 'see you', 'cool', 'nice', 'great'
    }
    # If query is ONLY acknowledgment (≤3 words)
    if len(query_lower.split()) <= 3 and any(ack in query_lower for ack in acknowledgments):
        return False
    
    # Pattern 4: Clarification without new info needs
    clarification_only = [
        r'^(can you |could you |please )?(explain|clarify|tell me more|elaborate)\s*[?.!]*$',
        r'^what do you mean\??$',
        r'^how so\??$',
        r'^why\??$'  # Single "why?" without context
    ]
    if any(re.search(pattern, query_lower) for pattern in clarification_only):
        # These need context from conversation history, not new retrieval
        return False
    
    # Pattern 5: Very short queries (likely not factual)
    if len(query_lower.split()) <= 2 and '?' not in query_lower:
        # "yes", "no", "maybe", etc.
        return False
    
    # Default: Retrieve for everything else (factual queries, specific questions)
    return True

def get_instant_response(query: str) -> Optional[str]:
    """Return pre-defined responses for common queries."""
    query_lower = query.lower().strip()
    
    responses = {
        r'\bWho are you\b': """
            I'm a Guardian news analyst assistant! I have access to thousands of Guardian articles 
            stored in a hybrid database (vector search + SQL), and I can help you:

            📰 Find articles on specific topics
            📊 Analyze trends across sections
            🔍 Answer questions about recent events
            📅 Track coverage of ongoing stories

            Ask me about any news topic, and I'll search through the Guardian archives to find 
            relevant articles and insights!
        """.strip(),
        
        r'\bWhat can you do\b': """
            I can help you explore Guardian news articles! Here's what I'm good at:

            **Search & Discovery:**
            - Find articles by keyword, topic, or section
            - Search semantically (I understand meaning, not just exact words)

            **Analysis:**
            - Track trends over time
            - Compare coverage across sections
            - Identify priority stories

            **Smart Features:**
            - Hybrid search (combines semantic + keyword matching)
            - Re-ranked results for maximum relevance
            - Cites sources with URLs

            Try asking: "What are the latest articles about climate change?" or 
            "Show me trending topics this week"
        """.strip(),
    }
    
    for pattern, response in responses.items():
        if re.search(pattern, query_lower):
            return response
    
    return None

def retrieve_context(state: AgentState):
    """
    True Hybrid Retrieval Strategy (Always Dense + Lexical) (With smart routing):
    
    1. Dense Vector Search (Pinecone) - ALWAYS runs (semantic) - NOW PARALLEL
    2. Lexical Search (Snowflake SQL) - ALWAYS runs (keyword matching)
    3. Merge & Deduplicate - Remove redundant results
    4. Re-rank (FlashRank) - LAZY LOADED, ONLY if enough results to compare
    5. Return Top-K - Most relevant results
    
    Optimizations:
    - Lazy load vector_store and reranker (only on first use)
    - Parallel fetching of guardian + user namespaces
    - Efficient error handling
    """
    import concurrent.futures
    
    # ✅ TIMING: Overall start
    start_time = time.time()
    
    if not state["messages"]:
        return state

    last_message = state["messages"][-1]
    if not hasattr(last_message, "content") or not last_message.content.strip():
        return state

    query = last_message.content.strip()

    instant_response = get_instant_response(query)
    if instant_response:
        response_message = AIMessage(content=instant_response)
        return {"messages": state["messages"] + [response_message]}

    if not should_retrieve(query):
        logger.info(
            "retrieval_skipped",
            query=query[:100],
            reason="non_factual_query",
            user=st.session_state.get("user_name"),
            thread_id=st.session_state.get("thread_id")
        )
        
        # Add a lightweight system message instead
        context_message = SystemMessage(content=f"""
        Query: "{query}"
        
        This is a casual greeting or meta question - respond warmly and naturally!

        Guidelines:
        - Be friendly and conversational (this is a human saying hi!)
        - Keep it brief (2-3 sentences max)
        - Show personality while staying professional
        - Mention you're a Guardian news analyst if relevant
        - No need to list capabilities unless asked

        Example good responses:
        User: "Hi! How are you?"
        → "Hey! I'm doing great, thanks for asking! Ready to help you explore Guardian news on any topic. What are you curious about today?"

        User: "What can you do?"
        → "I'm your Guardian news analyst! I can search thousands of articles, track trends, compare coverage, and answer questions about current events. Try asking about any news topic and I'll find the most relevant stories!"
        """)
        
        new_messages = state["messages"][:-1] + [context_message] + [state["messages"][-1]]
        return {"messages": new_messages}

    logger.info(
        "retrieval_started",
        query=query[:100],
        user=st.session_state.get("user_name"),
        thread_id=st.session_state.get("thread_id")
    )

    all_candidates = []
    
    # Define demo mode first
    demo_mode = st.session_state.get("demo_mode", False)
    
    retrieval_stats = {
        "dense_guardian": 0,
        "dense_user": 0,
        "dense_total": 0,
        "sql": 0,
        "sql_keywords_used": [],
        "unique": 0,
        "reranked": 0,
        "sql_skipped": False,
        "rerank_skipped": False,
        "demo_mode": demo_mode,
        "query": query,
        "keywords_extracted": [],
        "query_time_ms": 0,
        "dense_time_ms": 0,      
        "sql_time_ms": 0,        
        "dedup_time_ms": 0,      
        "rerank_time_ms": 0    
    }

    checkpoint_setup = time.time()
    print(f"⏱️ Setup: {(checkpoint_setup - start_time) * 1000:.0f}ms")

    # ==================== LAZY LOAD VECTOR STORE ====================
    if st.session_state.vector_store is None:
        with st.spinner("🔄 Initializing vector search..."):
            st.session_state.vector_store = get_vector_store()
    
    vector_store = st.session_state.vector_store

    checkpoint_vectorstore = time.time()
    print(f"⏱️ Vector store loaded: +{(checkpoint_vectorstore - checkpoint_setup) * 1000:.0f}ms")

    # ==================== STEP 1: DENSE VECTOR SEARCH (PARALLEL) ====================
    # ✅ TIMING: Dense search start
    dense_start = time.time()
    
    if vector_store:
        guardian_k = 8 if demo_mode else 10
        user_k = 4 if demo_mode else 5
        
        # ✅ PARALLEL FETCHING - Both namespaces simultaneously
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            submit_start = time.time()

            # Submit both searches in parallel
            guardian_future = executor.submit(
                vector_store.similarity_search,
                query,
                k=guardian_k,
                namespace="guardian_articles"
            )
            
            user_future = executor.submit(
                vector_store.similarity_search,
                query,
                k=user_k,
                namespace="user_documents"
            )

            submit_time = (time.time() - submit_start) * 1000
            print(f"⏱️ Both searches submitted: {submit_time:.0f}ms")
            
            # Collect Guardian results
            guardian_wait_start = time.time()
            
            # ✅ Collect Guardian results
            try:
                guardian_docs = guardian_future.result(timeout=15)
                guardian_wait = (time.time() - guardian_wait_start) * 1000
                print(f"⏱️ Guardian wait: {guardian_wait:.0f}ms ({len(guardian_docs)} results)")
                retrieval_stats["dense_guardian"] = len(guardian_docs)
                
                for doc in guardian_docs:
                    all_candidates.append({
                        "text": doc.page_content,
                        "metadata": doc.metadata,
                        "source_type": doc.metadata.get("source_type", "guardian_article"),
                        "retrieval_method": "dense_vector_guardian"
                    })

                logger.debug(
                    "dense_search_completed",
                    namespace="guardian_articles",
                    results=len(guardian_docs),
                    time_ms=int((time.time() - dense_start) * 1000)
                )

            except concurrent.futures.TimeoutError:
                logger.warning(
                    "vector_search_timeout",
                    namespace="guardian_articles",
                    query=query[:100],
                    timeout_seconds=5
                )
            except Exception as e:
                logger.warning(
                    "vector_search_failed",
                    namespace="guardian_articles",
                    query=query[:100],
                    error_type=type(e).__name__,
                    error_message=str(e),
                    user=st.session_state.get("user_name", "unknown"),
                    thread_id=st.session_state.get("thread_id", "unknown"),
                    k=guardian_k
                )
            
            # ✅ Collect User documents results
            user_wait_start = time.time()
            try:
                user_docs = user_future.result(timeout=15)
                user_wait = (time.time() - user_wait_start) * 1000
                retrieval_stats["dense_user"] = len(user_docs)
                
                for doc in user_docs:
                    all_candidates.append({
                        "text": doc.page_content,
                        "metadata": doc.metadata,
                        "source_type": "uploaded_document",
                        "retrieval_method": "dense_vector_user"
                    })
                
                logger.debug(
                    "dense_search_completed",
                    namespace="user_documents",
                    results=len(user_docs),
                    time_ms=int((time.time() - dense_start) * 1000)
                )
                
            except concurrent.futures.TimeoutError:
                logger.warning(
                    "vector_search_timeout",
                    namespace="user_documents",
                    query=query[:100],
                    timeout_seconds=5
                )
            except Exception as e:
                # User namespace might not exist - only log if it's a different error
                if "Namespace 'user_documents' not found" not in str(e):
                    logger.warning(
                        "vector_search_failed",
                        namespace="user_documents",
                        error_type=type(e).__name__,
                        error_message=str(e)
                    )

    retrieval_stats["dense_total"] = len(all_candidates)
    
    # ✅ TIMING: Dense search end
    retrieval_stats["dense_time_ms"] = int((time.time() - dense_start) * 1000)

    checkpoint_dense = time.time()
    print(f"⏱️ Dense TOTAL: {(checkpoint_dense - dense_start) * 1000:.0f}ms")

    # ==================== STEP 2: LEXICAL SEARCH ====================
    # ✅ TIMING: SQL search start
    sql_start = time.time()
    
    keywords = extract_keywords(query)
    retrieval_stats["keywords_extracted"] = keywords

    keyword_time = (time.time() - sql_start) * 1000
    print(f"⏱️ Keyword extraction: {keyword_time:.0f}ms, keywords={keywords}")
    
    if keywords:
        # ✅ FILTER KEYWORDS FOR SQL PERFORMANCE
        # Important short terms that should always be used
        important_short_terms = {
            'ai', 'uk', 'us', 'eu', 'un', 'ceo', 'fbi', 'cia', 'gdp', 'nhs',
            'cop', 'g7', 'g20', 'ufo', 'nyc', 'la', 'qa', 'hr', 'it', 'pr',
            'q1', 'q2', 'q3', 'q4', 'gop', 'mlb', 'nba', 'nfl', 'ufc'
        }
        
        # Filter for SQL: length >= 5 OR important short term
        sql_keywords = [
            k for k in keywords 
            if len(k) >= 5 or k in important_short_terms
        ]
        
        # Fallback: use longest keyword if all were filtered out
        if not sql_keywords and keywords:
            sql_keywords = [max(keywords, key=len)]
        
        if sql_keywords:
            try:
                from capstone_tool_calling_agent import search_articles_by_multiple_keywords
                
                sql_limit = 15 if demo_mode else 10
                
                sql_invoke_start = time.time()
                sql_response = search_articles_by_multiple_keywords.invoke({
                    "keywords": sql_keywords,
                    "limit": sql_limit
                })
                sql_invoke_time = (time.time() - sql_invoke_start) * 1000
                print(f"⏱️ SQL invoke: {sql_invoke_time:.0f}ms")
                
                if "results" in sql_response and sql_response["results"]:
                    retrieval_stats["sql"] = len(sql_response["results"])
                    retrieval_stats["sql_keywords_used"] = sql_keywords
                    
                    for article in sql_response["results"]:
                        all_candidates.append({
                            "text": article.get("preview", ""),
                            "metadata": {
                                "article_id": article.get("article_id"),
                                "title": article.get("web_title"),
                                "section": article.get("section_name"),
                                "published_date": str(article.get("web_publication_date", "")),
                                "web_url": article.get("web_url", ""),
                                "source_type": "guardian_article"
                            },
                            "source_type": "guardian_article",
                            "retrieval_method": "sql_multi_keyword"
                        })
                    logger.debug(
                        "sql_search_completed",
                        keywords=sql_keywords,
                        results=len(sql_response["results"]),
                        time_ms=int((time.time() - sql_start) * 1000)
                    )

                else:
                    retrieval_stats["sql"] = 0
                    retrieval_stats["sql_keywords_used"] = sql_keywords
            
            except Exception as e:
                print(f"⚠️ Multi-keyword SQL search failed: {e}")
                logger.error(
                    "sql_search_failed",
                    keywords=sql_keywords,
                    query=query[:100],
                    error_type=type(e).__name__,
                    error_message=str(e)
                )
                retrieval_stats["sql"] = 0
                retrieval_stats["sql_keywords_used"] = sql_keywords
        else:
            retrieval_stats["sql"] = 0
            retrieval_stats["sql_keywords_used"] = []
    else:
        retrieval_stats["sql"] = 0
        retrieval_stats["sql_keywords_used"] = []
    
    retrieval_stats["sql_skipped"] = False
    
    # ✅ TIMING: SQL search end
    retrieval_stats["sql_time_ms"] = int((time.time() - sql_start) * 1000)

    checkpoint_sql = time.time()
    print(f"⏱️ SQL TOTAL: {(checkpoint_sql - sql_start) * 1000:.0f}ms")

    # ==================== STEP 3: MERGE & DEDUPLICATE ====================
    # ✅ TIMING: Deduplication start
    dedup_start = time.time()
    
    unique_candidates = deduplicate_results(all_candidates)
    retrieval_stats["unique"] = len(unique_candidates)
    
    # ✅ TIMING: Deduplication end
    retrieval_stats["dedup_time_ms"] = int((time.time() - dedup_start) * 1000)

    checkpoint_dedup = time.time()
    print(f"⏱️ Dedup TOTAL: {(checkpoint_dedup - dedup_start) * 1000:.0f}ms ({len(all_candidates)} → {len(unique_candidates)})")

    if not unique_candidates:
        no_results_message = SystemMessage(
            content="No relevant articles found for this query. Please try rephrasing or using different keywords."
        )
        new_messages = state["messages"][:-1] + [no_results_message] + [state["messages"][-1]]
        
        # Calculate total query time
        retrieval_stats["query_time_ms"] = int((time.time() - start_time) * 1000)
        
        # Update sidebar with stats
        if "retrieval_stats" not in st.session_state:
            st.session_state.retrieval_stats = []
        st.session_state.retrieval_stats.append(dict(retrieval_stats))
        
        return {"messages": new_messages}

    # ==================== STEP 4: RE-RANK (LAZY LOADED) ====================
    # ✅ TIMING: Re-ranking start
    rerank_start = time.time()
    
    MIN_RERANK_THRESHOLD = 3
    
    if len(unique_candidates) >= MIN_RERANK_THRESHOLD:
        # ✅ LAZY LOAD RERANKER (only when needed)
        if st.session_state.reranker is None:
            with st.spinner("🔄 Loading reranker..."):
                st.session_state.reranker = get_reranker()
        
        reranker = st.session_state.reranker
        
        try:
            passages = [
                {"id": i, "text": candidate["text"][:500]}
                for i, candidate in enumerate(unique_candidates)
            ]

            rerank_request = RerankRequest(query=query, passages=passages)
            reranked = reranker.rerank(rerank_request)
            
            top_n = min(5, len(reranked))
            top_candidates = [unique_candidates[r["id"]] for r in reranked[:top_n]]
            rerank_scores = [r["score"] for r in reranked[:top_n]]
            
            retrieval_stats["reranked"] = top_n
            retrieval_stats["rerank_skipped"] = False
            
        except Exception as e:
            print(f"⚠️ Re-ranking failed: {e}. Using original order.")
            logger.warning(
                "rerank_failed",
                error_type=type(e).__name__,
                error_message=str(e)
            )
            top_candidates = unique_candidates[:5]
            rerank_scores = [None] * len(top_candidates)
            retrieval_stats["reranked"] = len(top_candidates)
            retrieval_stats["rerank_skipped"] = False
    else:
        top_candidates = unique_candidates[:5]
        rerank_scores = [None] * len(top_candidates)
        retrieval_stats["reranked"] = len(top_candidates)
        retrieval_stats["rerank_skipped"] = True
    
    # ✅ TIMING: Re-ranking end
    retrieval_stats["rerank_time_ms"] = int((time.time() - rerank_start) * 1000)
    print(f"⏱️ Rerank TOTAL: {retrieval_stats['rerank_time_ms']}ms")

    checkpoint_1 = time.time()
    print(f"⏱️ Checkpoint after rerank: {(checkpoint_1 - start_time) * 1000:.0f}ms")

    # ==================== STEP 5: FORMAT CONTEXT ====================
    context_lines = []
    for i, candidate in enumerate(top_candidates, 1):
        md = candidate["metadata"]
        source_type = candidate["source_type"]
        retrieval_method = candidate["retrieval_method"]
        data_source = md.get("data_source", "unknown")

        if source_type == "uploaded_document":
            source_label = f"📄 Uploaded: {md.get('file_name', 'Unknown')}"
        else:
            if data_source == "batch":
                source_badge = "📚 Historical"
            elif data_source == "realtime":
                source_badge = "🔴 Live"
            else:
                source_badge = "❓ Unknown"
            
            source_label = f"📰 Guardian ({source_badge}): {md.get('title', 'Unknown Article')}"
        
        score = rerank_scores[i-1] if i-1 < len(rerank_scores) and rerank_scores[i-1] is not None else "N/A"
        score_display = f"{score:.3f}" if isinstance(score, (int, float)) else score
        
        if "vector" in retrieval_method:
            method_badge = "🔍 Vector"
        else:
            method_badge = "🗄️ SQL"
        
        context_lines.append(
            f"[{method_badge} Source {i} — {source_label}]\n"
            f"Relevance: {score_display} | Method: {retrieval_method}\n"
            f"Published: {md.get('published_date', 'Unknown')}\n"
            f"Section: {md.get('section', 'N/A')}\n"
            f"URL: {md.get('web_url', 'N/A')}\n"
            f"Content:\n{candidate['text'].strip()}\n"
            f"{'-' * 80}"
        )

    checkpoint_2 = time.time()
    print(f"⏱️ Context lines built: +{(checkpoint_2 - checkpoint_1) * 1000:.0f}ms")

    full_context = "\n\n".join(context_lines)

    checkpoint_3 = time.time()
    print(f"⏱️ Context joined: +{(checkpoint_3 - checkpoint_2) * 1000:.0f}ms")
    print(f"   📊 Context size: {len(full_context)} characters")
    
    # Build stats summary
    stats_parts = [
        f"Dense Guardian={retrieval_stats['dense_guardian']}",
        f"Dense User={retrieval_stats['dense_user']}",
    ]
    
    if retrieval_stats["sql"] > 0:
        keywords_str = ", ".join(retrieval_stats["sql_keywords_used"][:3])
        stats_parts.append(f"SQL={retrieval_stats['sql']} (keywords: {keywords_str})")
    else:
        stats_parts.append("SQL=0 (no matches)")
    
    stats_parts.append(f"Unique={retrieval_stats['unique']}")
    
    if retrieval_stats["rerank_skipped"]:
        stats_parts.append(f"Top-K={retrieval_stats['reranked']} (no rerank needed)")
    else:
        stats_parts.append(f"Reranked Top-K={retrieval_stats['reranked']}")
    
    stats_summary = f"\n[Retrieval Stats: {', '.join(stats_parts)}]\n"
    
    checkpoint_4 = time.time()
    print(f"⏱️ Stats summary: +{(checkpoint_4 - checkpoint_3) * 1000:.0f}ms")

    context_message = SystemMessage(content=f"""
    You are a professional Guardian news analyst assistant with expertise in current events and journalism.

    Your knowledge base:
    - Dense semantic search (Pinecone vector database) for finding related articles
    - Structured SQL database (Snowflake) for exact data queries
    - Access to Guardian articles spanning multiple years and topics

    When users ask about news, events, or topics:
    - Only call search tools if you need ADDITIONAL data clearly missing from the context. If all relevant articles are provided, \
    you do NOT need to call search tools. Your role is to SYNTHESIZE and CITE this information, not retrieve more.
    - If all relevant articles are not provided, search your database to find relevant Guardian articles
    - Cite sources clearly with [Source N - Title] format
    - Acknowledge when information is missing or uncertain
    - Present multiple perspectives when sources conflict

    When users ask about you or make conversation:
    - Respond naturally without searching the database
    - Be helpful, concise, and professional
    - Explain your capabilities when asked

    Core principles:
    - Always be factually accurate (cite sources or admit uncertainty)
    - Be conversational but professional
    - Never hallucinate information
    - Prioritize recent articles when relevant
    - Be concise** - Aim for 3-5 bullet points per topic, not full paragraphs
    - Use summary style** - Key facts first, details if asked
    - Offer follow-up** - "Want me to dive deeper into any of these stories?"

    Remember: You're a knowledgeable analyst, not a search engine. 
    Use your judgment to provide helpful, human-like responses.
    {full_context}
    """)

    checkpoint_5 = time.time()
    print(f"⏱️ SystemMessage created: +{(checkpoint_5 - checkpoint_4) * 1000:.0f}ms")
    print(f"   📊 Message size: {len(context_message.content)} characters")

    new_messages = state["messages"][:-1] + [context_message] + [state["messages"][-1]]

    checkpoint_6 = time.time()
    print(f"⏱️ Messages array updated: +{(checkpoint_6 - checkpoint_5) * 1000:.0f}ms")

    # ✅ TIMING: Calculate total query time
    retrieval_stats["query_time_ms"] = int((time.time() - start_time) * 1000)

    # Update sidebar with stats
    if "retrieval_stats" not in st.session_state:
        st.session_state.retrieval_stats = []
    
    stats_copy = dict(retrieval_stats)
    st.session_state.retrieval_stats.append(stats_copy)

    checkpoint_7 = time.time()
    print(f"⏱️ Session state updated: +{(checkpoint_7 - checkpoint_6) * 1000:.0f}ms")

    logger.info(
        "retrieval_completed",
        query=query[:100],
        dense_results=retrieval_stats["dense_total"],
        sql_results=retrieval_stats["sql"],
        unique_results=retrieval_stats["unique"],
        reranked_results=retrieval_stats["reranked"],
        total_time_ms=retrieval_stats["query_time_ms"],
        dense_time_ms=retrieval_stats["dense_time_ms"],
        sql_time_ms=retrieval_stats["sql_time_ms"],
        user=st.session_state.get("user_name"),
        thread_id=st.session_state.get("thread_id")
    )

    checkpoint_8 = time.time()
    print(f"⏱️ Logging: +{(checkpoint_8 - checkpoint_7) * 1000:.0f}ms")
    print(f"⏱️ TOTAL RETRIEVE_CONTEXT: {(checkpoint_8 - start_time) * 1000:.0f}ms")

    return {"messages": new_messages}


def chatbot(state: AgentState, config: RunnableConfig) -> Dict[str, Any]:
    response = model_with_tools.invoke({"messages": state["messages"]}, config=config)
    if isinstance(response.content, str):
       response.content = response.content.replace("$", "\\$")
    return {"messages": [response]}

# Build graph
workflow = StateGraph(AgentState)
workflow.add_node("retrieve", retrieve_context)
workflow.add_node("chatbot", chatbot)
workflow.add_node("tools", tool_node)

workflow.add_edge(START, "retrieve")
workflow.add_edge("retrieve", "chatbot")
workflow.add_conditional_edges("chatbot", tools_condition)
workflow.add_edge("tools", "chatbot")
workflow.add_edge("chatbot", END)

checkpointer = create_checkpointer()
app = workflow.compile(checkpointer=checkpointer)

# ==================== SESSION STATE INITIALIZATION ====================
def initialize_session_state():
    if "agent" not in st.session_state:
        st.session_state.agent = app

    if "messages" not in st.session_state:
        st.session_state.messages = []

    if "thread_id" not in st.session_state:
        st.session_state.thread_id = f"streamlit_thread_{int(time.time())}"

    if "user_name" not in st.session_state:
        st.session_state.user_name = "User"

    if "conversation_metadata" not in st.session_state:
        st.session_state.conversation_metadata = {
            st.session_state.thread_id: {
                "name": "Main Conversation",
                "created_at": time.time(),
                "message_count": 0,
            }
        }
    
    if "retrieval_stats" not in st.session_state:
        st.session_state.retrieval_stats = []
    
    if "reranker" not in st.session_state:
        st.session_state.reranker = None
    
    if "vector_store" not in st.session_state:
        st.session_state.vector_store = None

# ==================== THREAD MANAGEMENT ====================
def create_new_thread():
    new_thread_id = f"thread_{int(time.time())}"
    st.session_state.thread_id = new_thread_id
    st.session_state.messages = []
    st.session_state.retrieval_stats = []
    st.session_state.conversation_metadata[new_thread_id] = {
        "name": f"Conversation {len(st.session_state.conversation_metadata) + 1}",
        "created_at": time.time(),
        "message_count": 0,
    }
    st.rerun()

def switch_thread(thread_id: str):
    if thread_id in st.session_state.conversation_metadata:
        st.session_state.thread_id = thread_id
        
        try:
            config = {"configurable": {"thread_id": thread_id}}
            state = st.session_state.agent.get_state(config)
            
            if state and "messages" in state.values:
                loaded_messages = []
                for msg in state.values["messages"]:
                    if isinstance(msg, HumanMessage):
                        loaded_messages.append({"role": "user", "content": msg.content})
                    elif isinstance(msg, AIMessage):
                        loaded_messages.append({"role": "assistant", "content": msg.content})
                
                st.session_state.messages = loaded_messages
            else:
                st.session_state.messages = []
        except:
            st.session_state.messages = []
        
        st.session_state.retrieval_stats = []
        st.rerun()

# ==================== GET AGENT RESPONSE ====================
def process_showcase_query(query: str):
    """Process a showcase query automatically."""
    # Add user message
    st.session_state.messages.append({"role": "user", "content": query})
    
    # Create a placeholder for streaming response
    with st.spinner(f"🔍 Processing showcase query: {query}..."):
        st.session_state.processing = True
        
        try:
            response = get_agent_response(query)
            st.session_state.messages.append({"role": "assistant", "content": response})
            st.success(f"✅ Showcase query completed!")
            time.sleep(0.5)  # Brief pause to show success message
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
            st.session_state.messages.append({
                "role": "assistant", 
                "content": f"I encountered an error: {str(e)}"
            })
        finally:
            st.session_state.processing = False
    
    # Rerun to display
    st.rerun()

def get_agent_response(user_input: str) -> str:

    request_start = time.time()
    
    logger.info(
        "agent_request_started",
        user_input=user_input[:100],
        user=st.session_state.get("user_name"),
        thread_id=st.session_state.thread_id
    )

    st.session_state.processing = True

    config: RunnableConfig = {"configurable": {"thread_id": st.session_state.thread_id}}

    input_state = {
        "messages": [HumanMessage(content=user_input)],
        "user_name": st.session_state.user_name,
    }

    try:
        result = None
        for chunk in st.session_state.agent.stream(input_state, config, stream_mode="values"):
            result = chunk

        if result and "messages" in result and result["messages"]:
            final_msg = result["messages"][-1]
            response = final_msg.content if hasattr(final_msg, "content") else str(final_msg)
        else:
            response = "I'm not sure how to respond to that."

        meta = st.session_state.conversation_metadata[st.session_state.thread_id]
        meta["message_count"] = meta.get("message_count", 0) + 1

        response_time = (time.time() - request_start) * 1000
        
        logger.info(
            "agent_request_completed",
            user_input=user_input[:100],
            response_length=len(response),
            response_time_ms=response_time,
            user=st.session_state.get("user_name"),
            thread_id=st.session_state.thread_id
        )

        return response

    except Exception as e:
        logger.error(
            "agent_request_failed",
            user_input=user_input[:100],
            error_type=type(e).__name__,
            error_message=str(e),
            user=st.session_state.get("user_name"),
            thread_id=st.session_state.thread_id,
            exc_info=True
        )
        return f"Error: {str(e)}"
    finally:
        st.session_state.processing = False

# ==================== MAIN APP ====================
initialize_session_state()

st.markdown("<h1>Lam's Capstone AI Agent</h1>", unsafe_allow_html=True)
st.markdown("**📰 Guardian News Analyst • Hybrid RAG (Dense Vector + SQL) • Re-ranked with FlashRank**")

# Status indicators
cols = st.columns(4)
with cols[0]:
    st.caption(f"🟢 Dense Vector: {'ON'}")
with cols[1]:
    st.caption("🟢 SQL Tools: Active")
with cols[2]:
    st.caption("🟢 Re-ranking: FlashRank")
with cols[3]:
    st.caption("🟢 Model: GPT-4o-mini")

# Sidebar
with st.sidebar:
    st.markdown("### 👤 User")
    user_name = st.text_input("Your Name", value=st.session_state.user_name)
    if user_name != st.session_state.user_name:
        st.session_state.user_name = user_name

    st.divider()

    # ==================== DEMO MODE TOGGLE ====================
    st.markdown("### 🎓 Demo Mode")
    demo_mode = st.checkbox(
        "Enable Demo Mode",
        value=False,
        help="Reduces dense search results to showcase hybrid retrieval (Dense + SQL)"
    )
    
    if demo_mode:
        st.info("🎬 **Demo Mode Active**\n\nDense results limited to showcase SQL contribution")
        st.session_state.demo_mode = True
        
        st.markdown("**📝 Showcase Queries:**")
        st.caption("_Click any query to auto-execute and see the full hybrid pipeline_")

        showcase_queries = [
            ("Brexit referendum details", "🗳️"),
            ("Climate COP conferences", "🌍"),
            ("Australian regional news", "🦘"),
            ("Economic downturn analysis", "📉"),
            ("Technology AI regulation", "🤖"),
        ]
        
        for query_text, emoji in showcase_queries:
            if st.button(
                f"{emoji} {query_text}", 
                key=f"showcase_{query_text}",
                use_container_width=True,
                help=f"Click to auto-run: {query_text}"
            ):
                process_showcase_query(query_text)
    else:
        st.session_state.demo_mode = False
    
    st.divider()
    
    # ==================== SMART HYBRID STATS ====================
    st.markdown("### 🔍 Smart Hybrid Stats")
    
    if st.session_state.retrieval_stats:
        query_count = len(st.session_state.retrieval_stats)
        st.caption(f"📊 Showing results from query #{query_count}")
        latest_stats = st.session_state.retrieval_stats[-1]
        
        # ==================== DENSE VECTOR SEARCH ====================
        st.markdown("**Dense Vector Search:**")
        col1, col2 = st.columns(2)
        with col1:
            guardian_count = latest_stats.get("dense_guardian", 0)
            st.metric("📰 Guardian", guardian_count)
        with col2:
            user_count = latest_stats.get("dense_user", 0)
            st.metric("📄 User Docs", user_count)
        
        dense_total = guardian_count + user_count
        st.caption(f"_Total dense: {dense_total} results_")
        
        # ==================== SQL KEYWORD SEARCH (UPDATED!) ====================
        st.markdown("**SQL Keyword Search:**")
        sql_count = latest_stats.get("sql", 0)
        keywords_used = latest_stats.get("sql_keywords_used", [])

        # ✅ Show keywords if any were extracted
        if keywords_used:
            st.caption(f"🔍 _Keywords: {', '.join(keywords_used)}_")
        else:
            st.caption("⚠️ _No keywords extracted from query_")

        if sql_count > 0:
            # SQL found results
            st.metric("🗄️ SQL Results", sql_count)
            
            # Show SQL contribution
            with st.expander("ℹ️ Why SQL ran"):
                st.write(f"""
                **True Hybrid Search:**
        
                - SQL **always runs** alongside dense search
                - Found {sql_count} keyword-matched articles
                - Provides lexical/exact-match complementary results
        
                **Method:** Multi-keyword OR search  
                **Keywords searched:** {', '.join(keywords_used)}
        
                These results are merged with dense results, deduplicated, and re-ranked!
                """)
        else:
            # SQL found no results
            st.info("🔹 **SQL:** No matches found")
            
            # Explain why no results
            with st.expander("ℹ️ Why no SQL results?"):
                if keywords_used:
                    st.write(f"""
                    **SQL Search Attempted:**
                    
                    - Keywords extracted: {', '.join(keywords_used)}
                    - SQL searched Guardian articles in Snowflake
                    - No exact keyword matches found
                    
                    **This is normal!** Dense search already found semantically relevant results.
                    """)
                else:
                    st.write("""
                    **No Keywords Extracted:**
                    
                    - Query contained only stop words or very short terms
                    - SQL search requires extractable keywords
                    - Dense search is sufficient for this query
                    """)
        
        # ==================== DEDUPLICATION ====================
        st.markdown("**After Deduplication:**")
        total_before = dense_total + sql_count
        unique_after = latest_stats.get("unique", 0)
        duplicates = total_before - unique_after
        
        st.metric(
            "Unique Results", 
            unique_after,
            delta=f"-{duplicates} duplicate{'s' if duplicates != 1 else ''}" if duplicates > 0 else "No duplicates"
        )
        
        if duplicates > 0:
            st.caption(f"_Combined {total_before} results → {unique_after} unique_")
        
        # ==================== RE-RANKING ====================
        st.markdown("**Re-ranking:**")
        if latest_stats.get("rerank_skipped"):
            reranked_count = latest_stats.get("reranked", 0)
            st.warning(f"⚠️ **Skipped** - Only {reranked_count} result{'s' if reranked_count != 1 else ''}")
            st.caption("_Need ≥3 results to rerank_")
        else:
            reranked_count = latest_stats.get("reranked", 0)
            st.metric("🎯 Top-K Results", reranked_count)
            st.caption("_Re-ranked by FlashRank_")

        # ==================== QUERY TIMING ====================
        query_time = latest_stats.get("query_time_ms", 0)
        if query_time > 0:
            if query_time < 1000:
                st.success(f"⚡ **Retrieval Time:** {query_time}ms")
            elif query_time < 2000:
                st.info(f"⏱️ **Retrieval Time:** {query_time}ms")
            else:
                st.warning(f"⏱️ **Retrieval Time:** {query_time}ms")
                
            # ✅ ACTUAL TIME BREAKDOWN (not estimated!)
            dense_time = latest_stats.get("dense_time_ms", 0)
            sql_time = latest_stats.get("sql_time_ms", 0)
            dedup_time = latest_stats.get("dedup_time_ms", 0)
            rerank_time = latest_stats.get("rerank_time_ms", 0)

            with st.expander("⏱️ Time breakdown (measured)", expanded=False):
                # Show each step with percentage
        
                if dense_time > 0:
                    dense_pct = (dense_time / query_time * 100)
                    st.caption(f"🔍 Dense search: {dense_time}ms ({dense_pct:.1f}%)")
                
                if sql_time > 0:
                    sql_pct = (sql_time / query_time * 100)
                    st.caption(f"🗄️ SQL search: {sql_time}ms ({sql_pct:.1f}%)")
        
                if dedup_time > 0:
                    dedup_pct = (dedup_time / query_time * 100)
                    st.caption(f"🔗 Deduplication: {dedup_time}ms ({dedup_pct:.1f}%)")
        
                if rerank_time > 0:
                    rerank_pct = (rerank_time / query_time * 100)
                    st.caption(f"🎯 Re-ranking: {rerank_time}ms ({rerank_pct:.1f}%)")

                # Show any overhead
                measured_total = dense_time + sql_time + dedup_time + rerank_time
                overhead = query_time - measured_total
                if overhead > 0:
                    overhead_pct = (overhead / query_time * 100)
                    st.caption(f"⚙️ Overhead: {overhead}ms ({overhead_pct:.1f}%)")

                st.caption(f"**Measured:** {measured_total}ms")
                st.caption(f"**Total Query:** {query_time}ms")
                if overhead > measured_total * 0.5:
                    st.warning(f"⚠️ High overhead detected! {overhead}ms is {overhead_pct:.1f}% of total time.") # type: ignore
        
        st.divider()
        
        # ==================== PIPELINE SUMMARY ====================
        # Show what happened in this retrieval
        pipeline_steps = []
        
        if dense_total > 0:
            pipeline_steps.append(f"✅ Dense search: {dense_total} results")
        
        if sql_count > 0:
            pipeline_steps.append(f"✅ SQL search: {sql_count} results")
        elif keywords_used:
            pipeline_steps.append(f"⚪ SQL search: 0 results")
        
        if duplicates > 0:
            pipeline_steps.append(f"✅ Deduplication: -{duplicates} duplicates")
        
        if not latest_stats.get("rerank_skipped"):
            pipeline_steps.append(f"✅ Re-ranking: Top {reranked_count}")
        
        if pipeline_steps:
            st.markdown("**Pipeline Summary:**")
            for step in pipeline_steps:
                st.caption(step)
        
        # ==================== EFFICIENCY INSIGHTS ====================
        st.divider()
        
        # Calculate SQL contribution percentage
        if total_before > 0:
            sql_contribution_pct = (sql_count / total_before) * 100
            
            if sql_contribution_pct > 0:
                st.success(
                    f"🎯 **True Hybrid Active**\n\n"
                    f"SQL contributed {sql_contribution_pct:.1f}% of total results "
                    f"({sql_count}/{total_before})"
                )
            else:
                st.info(
                    f"ℹ️ **Dense-Only Retrieval**\n\n"
                    f"Query fully satisfied by semantic search. "
                    f"SQL attempted but found no additional matches."
                )
        
    else:
        st.info("👋 _No searches yet. Ask a question to see stats!_")

    st.divider()
    
    # ==================== THREAD MANAGEMENT ====================
    st.markdown("### 💬 Thread Management")
    if st.button("New Conversation", type="primary", use_container_width=True):
        create_new_thread()

    if st.button("🛑 Interrupt & Reset", use_container_width=True):
        st.session_state.messages = []
        st.session_state.retrieval_stats = []
        st.rerun()

    thread_input = st.text_input("Switch to Thread ID")
    if st.button("Switch Thread", use_container_width=True):
        if thread_input.strip():
            switch_thread(thread_input.strip())

    st.divider()
    
    # ==================== CONVERSATIONS ====================
    st.markdown("### 📂 Conversations")
    for tid, meta in st.session_state.conversation_metadata.items():
        status = "🟢 Current" if tid == st.session_state.thread_id else "⚪"
        st.write(f"{status} `{tid[:12]}...` — {meta['name']} ({meta.get('message_count', 0)} msgs)")

    st.divider()
    
    # ==================== ARCHITECTURE INFO ====================
    st.caption("**Architecture:**")
    st.caption("🔹 Dense: Pinecone vector DB")
    st.caption("🔹 Lexical: Snowflake SQL (multi-keyword)")
    st.caption("🔹 Rerank: FlashRank (local)")
    st.caption("🔹 LLM: GPT-4o-mini")
    st.caption("🔹 Mode: True Hybrid (both always run)")

# ==================== CHAT INTERFACE ====================

# Display chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Chat input (ALWAYS visible at bottom)
prompt = st.chat_input(
    "💬 Ask about Guardian news, articles, trends, or uploaded documents...",
    disabled=st.session_state.get("processing", False),
    key="main_chat_input"
)

if prompt:
    # Check for goodbye phrases
    if re.search(r'\b(bye|goodbye|end|tạm biệt|hẹn gặp lại|done|xong|see you|that.?s all)\b', prompt, re.IGNORECASE):
        st.session_state.messages.append({"role": "user", "content": prompt})
        st.session_state.messages.append({
            "role": "assistant",
            "content": "**Take care! Have a wonderful day! See you next time! 👋**"
        })
        st.rerun()

    # Add user message to history
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Display user message
    with st.chat_message("user"):
        st.markdown(prompt)

    # Generate and display assistant response
    with st.chat_message("assistant"):
        with st.spinner("🔍 Searching (Dense + SQL) → Re-ranking → Generating..."):
            try:
                response = get_agent_response(prompt)
            except Exception as e:
                response = f"❌ Error: {str(e)}"
        
        st.markdown(response)

    # Add assistant response to history
    st.session_state.messages.append({"role": "assistant", "content": response})
    
    # Rerun to update display
    st.rerun()

# ==================== FOOTER ====================
st.divider()
st.markdown(
    "<p style='text-align: center; color: #666;'>"
    "Built by Dinh Ngoc Lam • Foundry AI Academy Capstone 2025<br>"
    "Hybrid RAG: Pinecone (Dense) + Snowflake (SQL) + FlashRank (Rerank)</p>",
    unsafe_allow_html=True,
)