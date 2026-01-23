# cli_agent.py - CLI-Only Hybrid RAG Agent (No Streamlit)
import time
import re
import os
from dotenv import load_dotenv
import structlog
import logging
from pythonjsonlogger.json import JsonFormatter
from typing import cast

# LangChain / LangGraph imports
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langchain_core.runnables import RunnableConfig
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_core.prompts import ChatPromptTemplate
from langchain_pinecone import PineconeVectorStore
from langgraph.graph import START, END, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict
from typing import Annotated, List, Any, Dict, Optional
import psycopg
from flashrank import Ranker, RerankRequest
from pinecone import Pinecone
import nltk
from nltk.corpus import stopwords

# Your tool-calling module
from capstone_tool_calling_agent import tools

load_dotenv()

# ==================== STRUCTURED LOGGING SETUP ====================
def configure_logging():
    """Configure structured logging for production-ready observability."""
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    
    log_handler = logging.StreamHandler()
    formatter = JsonFormatter(
        fmt='%(timestamp)s %(level)s %(event)s %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S'
    )
    log_handler.setFormatter(formatter)
    root_logger.addHandler(log_handler)
    root_logger.setLevel(logging.INFO)
    
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

configure_logging()
logger = structlog.get_logger()

# ==================== GLOBAL STATE ====================
class SessionState:
    def __init__(self):
        self.vector_store: Optional[PineconeVectorStore] = None
        self.reranker: Optional[Any] = None
        self.thread_id = f"cli_thread_{int(time.time())}"
        self.user_name = "CLI_User"
        self.retrieval_stats = []
        self.demo_mode = False
        self.nltk_data_downloaded = False

session = SessionState()

# ==================== VECTOR STORE SETUP (PINECONE) ====================
def get_vector_store():
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
    index_name = os.getenv("PINECONE_INDEX", "guardian-capstone")
    
    try:
        vector_store = PineconeVectorStore(
            index_name=index_name,
            embedding=embeddings,
            namespace="guardian_articles"
        )
        logger.info("Warming up Pinecone index...")
        vector_store.similarity_search("test", k=1)
        logger.info("✅ Pinecone index warmed up")
        return vector_store
    except Exception as e:
        print(f"⚠️ Vector store not available: {e}. Falling back to SQL-only mode.")
        return None

def get_reranker():
    """Initialize FlashRank reranker (local, fast)."""
    return Ranker(model_name="ms-marco-MiniLM-L-12-v2", cache_dir="/tmp")

def get_pinecone_client():
    """Get Pinecone client for potential future sparse index."""
    try:
        return Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
    except:
        return None

# ==================== PERSISTENT CHECKPOINTER SETUP ====================
def create_checkpointer():
    try:
        host = os.getenv("LANGGRAPH_POSTGRES_HOST", "localhost")
        port = os.getenv("LANGGRAPH_POSTGRES_PORT", "5433")
        database = os.getenv("LANGGRAPH_POSTGRES_DB", "langgraph_memory")
        user = os.getenv("LANGGRAPH_POSTGRES_USER", "postgres")
        password = os.getenv("LANGGRAPH_POSTGRES_PASSWORD")
        if not password:
            raise ValueError("LANGGRAPH_POSTGRES_PASSWORD environment variable is required")
        db_uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        connection = psycopg.connect(db_uri, autocommit=True)
        checkpointer = PostgresSaver(connection) # type: ignore
        checkpointer.setup()
        return checkpointer
    except Exception as e:
        print(f"⚠️ PostgreSQL not available: {e}. Using in-memory storage.")
        from langgraph.checkpoint.memory import MemorySaver
        return MemorySaver()

# ==================== HELPER FUNCTIONS FOR HYBRID SEARCH ====================
def download_nltk_data():
    """Download NLTK stopwords if not already available."""
    if not session.nltk_data_downloaded:
        try:
            nltk.data.find('corpora/stopwords')
            session.nltk_data_downloaded = True
        except LookupError:
            print("📦 One-time setup: Downloading NLTK data...")
            nltk.download('stopwords', quiet=True)
            session.nltk_data_downloaded = True
            print("✅ Setup complete!")

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
    """Enhanced keyword extraction - SKIP if query is about metadata."""
    query_lower = query.lower()
    
    # ✅ Don't extract keywords for metadata queries
    if any(word in query_lower for word in ['batch', 'realtime', 'data source', 'loaded']):
        return []  # Return empty - let tools handle it
    
    # ✅ Don't extract keywords for date queries
    if any(word in query_lower for word in ['published on', 'last week', 'today', 'yesterday']):
        return []  # Return empty - let tools handle it

    stop_words = get_stopwords()
    stop_words.update({
        'based', 'article', 'articles', 'news', 'tell', 'according', 'say',
        'compare', 'analyze', 'summarize', 'describe', 'explain',
        'show', 'find', 'give', 'share', 'discuss', 'evaluate',
        'review', 'examine', 'explore', 'investigate', 'detail'
    })
    
    important_short_terms = {
        'ai', 'uk', 'us', 'eu', 'un', 'ceo', 'fbi', 'cia', 'gdp', 'nhs',
        'cop', 'g7', 'g20', 'ufo', 'nyc', 'la', 'qa', 'hr', 'it', 'pr',
        'q1', 'q2', 'q3', 'q4', 'gop', 'mlb', 'nba', 'nfl', 'ufc',
        'war', 'ban', 'law', 'tax', 'net'
    }
    
    keywords = []
    extracted_from_multiword = set()
    
    # Extract multi-word proper nouns (SURNAMES ONLY)
    multi_word_names = re.findall(r'\b([A-Z][a-z]+(?: [A-Z][a-z]+)+)\b', query)
    for name in multi_word_names:
        parts = name.split()
        if len(parts) >= 2:
            surname = parts[-1].lower()
            if surname not in stop_words:
                keywords.append(surname)
                extracted_from_multiword.add(surname)
                for part in parts[:-1]:
                    extracted_from_multiword.add(part.lower())
    
    # Extract single capitalized words
    single_names = re.findall(r'\b[A-Z][a-z]{3,}\b', query)
    for name in single_names:
        name_lower = name.lower()
        if (name_lower not in stop_words and 
            name_lower not in keywords and
            name_lower not in extracted_from_multiword):
            keywords.append(name_lower)
    
    # Extract significant lowercase words
    all_words = re.findall(r'\b\w+\b', query.lower())
    for word in all_words:
        if word in keywords or word in extracted_from_multiword:
            continue
        if word in important_short_terms:
            keywords.append(word)
        elif word not in stop_words and len(word) >= 5:
            keywords.append(word)
    
    # Meta-words to exclude (question structure, not content)
    meta_stopwords = {
        'check', 'whether', 'discussing', 'articles', 'news',
        'find', 'search', 'look', 'show', 'give', 'tell',
        'want', 'need', 'could', 'would', 'should',
    }
    
    # Filter out meta-words from SQL keywords
    sql_keywords = [
        k for k in keywords 
        if k not in meta_stopwords and (len(k) >= 4 or k in important_short_terms)
    ]
    
    # Remove duplicates while preserving order
    seen = set()
    sql_keywords = [k for k in sql_keywords if not (k in seen or seen.add(k))]  # type: ignore
    
    if not sql_keywords and keywords:
        sql_keywords = [max(keywords, key=len)]
    
    return sql_keywords[:5]

def deduplicate_results(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicate results based on article_id or text similarity."""
    seen_ids = set()
    seen_texts = set()
    unique = []
    
    for candidate in candidates:
        article_id = candidate["metadata"].get("article_id")
        if article_id and article_id in seen_ids:
            continue
        
        text_signature = candidate["text"][:100].lower().strip()
        if text_signature in seen_texts:
            continue
        
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
You are a professional Guardian news analyst with hybrid data architecture.

## DATA SOURCES

📚 **Batch** (HuggingFace historical dataset): All articles before May 11, 2024
🔴 **Realtime** (Guardian API live feed): All articles from May 11, 2024 onwards
📄 **User Documents**: Uploaded PDFs and files (HIGHEST PRIORITY when present)

## YOUR CAPABILITIES

**Retrieval System:**
- Hybrid search (Pinecone semantic + Snowflake SQL)
- Results are pre-filtered, deduplicated, and re-ranked
- Context provided includes the most relevant sources

**Available Tools:**
- Smart search with automatic fallback between sources
- Date-based filtering and range queries
- Data source statistics and freshness checks
- Full article retrieval and summarization
- Trend analysis and priority news detection

## CORE PRINCIPLES

**1. Context First**
- The retrieval system has already provided relevant context
- Use this context to answer questions whenever possible
- Only call tools if you need ADDITIONAL information clearly missing from context

**2. User Documents Priority**
- Uploaded documents (📄) always take precedence over Guardian articles
- When both are present, start with user documents, then supplement with news coverage

**3. Source Transparency**
- Always indicate which source data comes from:
  - 📄 Uploaded documents
  - 📚 Batch (historical, pre-May 2024)
  - 🔴 Realtime (current, post-May 2024)
- Cite URLs when available
- Explain date boundaries when relevant

**4. Conversational Intelligence**
**When to search vs. when to chat:**

DO NOT search for:
- Acknowledgments: "That's interesting", "I agree", "Cool!", "Makes sense"
- Follow-up reactions: "I found it interesting too", "Good point"
- Simple affirmations: "Yes", "Okay", "Got it"
- Casual conversation: "How are you?", "Thanks!"

DO search for:
- Factual questions: "What happened with X?", "Do you have articles about Y?"
- Analysis requests: "Can you elaborate?", "What themes appear?"
- New topics: "Tell me about climate change" (different from previous topic)

**Conversational tone:**
- Be warm and natural, not robotic
- Match the user's energy level
- Don't over-explain or be overly formal
- Keep acknowledgment responses SHORT (1-2 sentences max)

**Examples:**

User: "That's interesting!"
❌ BAD: "I'm glad you find it interesting! The ongoing conversation is crucial. If you'd like to explore further..."
✅ GOOD: "Glad you found it interesting! Anything else you'd like to know about climate change?"

User: "I agree"
❌ BAD: "I appreciate your agreement. This topic is important and..."
✅ GOOD: "Great minds think alike! 😊 What else can I help with?"

User: "Thanks!"
❌ BAD: "You're welcome! I'm here to help with any questions about Guardian news..."
✅ GOOD: "You're welcome! Happy to help anytime."

**5. Accuracy Over Volume**
- Never hallucinate information
- Admit when information is missing
- Suggest alternatives if no results found
- Be honest about data limitations

## RESPONSE STYLE

**Be concise:**
- 3-5 bullet points for summaries
- Key facts first, details if asked
- Offer to dive deeper

**Be helpful:**
- Suggest related queries when appropriate
- Explain data source differences when relevant
- Provide context for temporal queries

**Be accurate:**
- Only cite sources you can verify
- Distinguish facts from analysis
- Acknowledge uncertainty

## EXAMPLE WORKFLOWS

**Simple query:**
User: "News about climate change?"
You: [Check context → If sufficient, cite and explain → If not, call smart_search_articles()]

**Follow-up query:**
User: "Can you get older articles?"
You: [Check conversation history → Infer topic and source → Call appropriate tool with historical filter]

**Data source query:**
User: "Show me batch articles about X"
You: [Call search_articles_by_data_source(data_source='batch', keyword='X')]

**User document + news:**
User: "Compare this report with news coverage"
You: [Prioritize uploaded document → Then search Guardian articles → Synthesize both]

Remember: You're a knowledgeable analyst, not just a search engine. Use your judgment to provide helpful, accurate, human-like responses.
"""

prompt = ChatPromptTemplate.from_messages([
    ("system", system_prompt),
    ("placeholder", "{messages}"),
])

model_with_tools = prompt | llm.bind_tools(tools, tool_choice="auto")

def should_retrieve(query: str) -> bool:
    """
    Determine if a query needs retrieval from the knowledge base.
    
    Skip retrieval for:
    - Greetings and pleasantries
    - Meta questions about the assistant
    - Acknowledgments and feedback (EXPANDED)
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
    
    # Pattern 2: Meta questions about the assistant
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
    
    # Pattern 3: Acknowledgments and feedback (EXPANDED!)
    acknowledgments = {
        'thanks', 'thank you', 'ok', 'okay', 'got it', 'understood',
        'bye', 'goodbye', 'see you', 'cool', 'nice', 'great', 'perfect',
        'interesting', 'fascinating', 'agree', 'makes sense', 'i see'
    }
    
    # ✅ NEW: More sophisticated acknowledgment detection
    acknowledgment_patterns = [
        r'^(that\'?s?|it\'?s?|this is) (interesting|fascinating|cool|great|nice|good|helpful|useful)',
        r'^i (find|found|think|agree|see)',
        r'^(yes|yep|yeah|no|nope),?\s*(that\'?s?|it\'?s?)?',
        r'^sounds (good|great|interesting|cool)',
        r'^(makes sense|got it|understood|i understand)',
    ]
    
    # If query matches acknowledgment patterns OR is short acknowledgment
    if any(re.search(pattern, query_lower) for pattern in acknowledgment_patterns):
        return False
    
    if len(query_lower.split()) <= 5 and any(ack in query_lower for ack in acknowledgments):
        return False
    
    # Pattern 4: Clarification without new info needs
    clarification_only = [
        r'^(can you |could you |please )?(explain|clarify|tell me more|elaborate)\s*[?.!]*$',
        r'^what do you mean\??$',
        r'^how so\??$',
        r'^why\??$'  # Single "why?" without context
    ]
    if any(re.search(pattern, query_lower) for pattern in clarification_only):
        return False
    
    # Pattern 5: Very short queries (likely not factual)
    if len(query_lower.split()) <= 2 and '?' not in query_lower:
        return False

    # Pattern 6 - Analytical requests needing evidence
    analytical_requests = [
        r'\binfer from (his|her|their|the) (past|actions|behavior|record)\b',
        r'\bbased on (his|her|their|the) (past|previous|prior|history)\b',
        r'\blooking at (his|her|their|the) (history|record|track record)\b',
        r'\b(give|show|find) (me )?(examples?|evidence|instances)\b',
        r'\bwhat (did|has|have) (he|she|they|it) (done|said|do)\b',
    ]
    
    if any(re.search(pattern, query_lower) for pattern in analytical_requests):
        return True  # ✅ Retrieve for evidence-based analysis
    
    # Default: Retrieve for everything else (factual queries, specific questions)
    return True

def get_instant_response(query: str) -> Optional[str]:
    """Return pre-defined responses for common queries."""
    query_lower = query.lower().strip()
    
    if re.search(r'\bWho are you\b', query_lower):
        return """I'm a Guardian news analyst assistant! I have access to thousands of Guardian articles 
stored in a hybrid database (vector search + SQL), and I can help you find articles, analyze trends, 
and answer questions about recent events."""
    
    if re.search(r'\bWhat can you do\b', query_lower):
        return """I can help you explore Guardian news articles! I can:
- Find articles by keyword, topic, or section
- Track trends over time
- Compare coverage across sections
- Answer questions using hybrid search (semantic + keyword matching)

Try asking: "What are the latest articles about climate change?" """
    
    return None

def is_conversational_followup(query: str, messages: List[Any]) -> bool:
    """
    Detect if query is a conversational reaction to previous response.
    
    Examples:
    - "That's interesting"
    - "I found it interesting as well"
    - "Cool!"
    - "Makes sense"
    """
    query_lower = query.lower().strip()
    
    # Pattern 1: Direct acknowledgments
    acknowledgment_phrases = [
        'interesting', 'fascinating', 'cool', 'great', 'nice',
        'i agree', 'i see', 'makes sense', 'good point',
        'i found', 'i think', 'that\'s', 'it\'s'
    ]
    
    # If query is short AND contains acknowledgment phrase
    if len(query_lower.split()) <= 7:
        if any(phrase in query_lower for phrase in acknowledgment_phrases):
            # Check if it's NOT a question (questions need retrieval)
            if '?' not in query_lower:
                return True
    
    # Pattern 2: Simple affirmations
    simple_affirmations = {
        'yes', 'yep', 'yeah', 'ok', 'okay', 'sure',
        'no', 'nope', 'not really', 'i disagree'
    }
    
    first_word = query_lower.split()[0] if query_lower.split() else ""
    if first_word in simple_affirmations and len(query_lower.split()) <= 4:
        return True
    
    return False

def retrieve_context(state: AgentState):
    """True Hybrid Retrieval Strategy (Dense + Lexical + Rerank)."""
    import concurrent.futures
    
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

    if is_conversational_followup(query, state["messages"]):
        logger.info("retrieval_skipped", query=query[:100], reason="conversational_followup")
        
        # Let LLM respond naturally without context
        context_message = SystemMessage(content=f"""
        Query: "{query}"
        
        This is a conversational acknowledgment or reaction.
        Respond warmly and briefly (1-2 sentences max).
        DO NOT over-explain or provide unsolicited information.
        Match the user's casual tone.
        """)
        
        new_messages = state["messages"][:-1] + [context_message] + [state["messages"][-1]]
        return {"messages": new_messages}

    if not should_retrieve(query):
        logger.info("retrieval_skipped", query=query[:100], reason="non_factual_query")
        context_message = SystemMessage(content=f'Query: "{query}"\n\nThis is a casual greeting - respond warmly!')
        new_messages = state["messages"][:-1] + [context_message] + [state["messages"][-1]]
        return {"messages": new_messages}

    logger.info("retrieval_started", query=query[:100])

    all_candidates = []
    demo_mode = session.demo_mode
    
    retrieval_stats = {
        "dense_guardian": 0,
        "dense_user": 0,
        "dense_total": 0,
        "sql": 0,
        "unique": 0,
        "reranked": 0,
        "query_time_ms": 0,
    }

    # Lazy load vector store
    if session.vector_store is None:
        print("🔄 Initializing vector search...")
        session.vector_store = get_vector_store()
    
    vector_store = session.vector_store

    # STEP 1: Dense Vector Search (Parallel)
    dense_start = time.time()
    if vector_store:
        guardian_k = 8 if demo_mode else 10
        user_k = 4 if demo_mode else 5
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            guardian_future = executor.submit(
                vector_store.similarity_search, query, k=guardian_k, namespace="guardian_articles"
            )
            user_future = executor.submit(
                vector_store.similarity_search, query, k=user_k, namespace="user_documents"
            )
            
            try:
                guardian_docs = guardian_future.result(timeout=15)
                retrieval_stats["dense_guardian"] = len(guardian_docs)
                for doc in guardian_docs:
                    all_candidates.append({
                        "text": doc.page_content,
                        "metadata": doc.metadata,
                        "source_type": doc.metadata.get("source_type", "guardian_article"),
                        "retrieval_method": "dense_vector_guardian"
                    })
            except Exception as e:
                logger.warning("vector_search_failed", namespace="guardian_articles", error=str(e))
            
            try:
                user_docs = user_future.result(timeout=15)
                retrieval_stats["dense_user"] = len(user_docs)
                for doc in user_docs:
                    all_candidates.append({
                        "text": doc.page_content,
                        "metadata": doc.metadata,
                        "source_type": "uploaded_document",
                        "retrieval_method": "dense_vector_user"
                    })
            except Exception:
                pass  # User namespace might not exist

    retrieval_stats["dense_total"] = len(all_candidates)

    # STEP 2: Lexical Search (SQL)
    sql_start = time.time()
    keywords = extract_keywords(query)
    
    if keywords:
        important_short_terms = {'ai', 'uk', 'us', 'eu', 'un', 'ceo', 'fbi', 'cia', 'gdp', 'nhs'}
        sql_keywords = [k for k in keywords if len(k) >= 5 or k in important_short_terms]
        
        if not sql_keywords and keywords:
            sql_keywords = [max(keywords, key=len)]
        
        if sql_keywords:
            try:
                from capstone_tool_calling_agent import search_articles_by_multiple_keywords
                sql_limit = 15 if demo_mode else 10
                sql_response = search_articles_by_multiple_keywords.invoke({
                    "keywords": sql_keywords,
                    "limit": sql_limit
                })
                
                if "results" in sql_response and sql_response["results"]:
                    retrieval_stats["sql"] = len(sql_response["results"])
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
            except Exception as e:
                logger.error("sql_search_failed", error=str(e))

    # STEP 3: Deduplicate
    unique_candidates = deduplicate_results(all_candidates)
    retrieval_stats["unique"] = len(unique_candidates)

    if not unique_candidates:
        no_results_message = SystemMessage(
            content="No relevant articles found. Try different keywords."
        )
        return {"messages": state["messages"][:-1] + [no_results_message] + [state["messages"][-1]]}

    # STEP 4: Re-rank
    MIN_RERANK_THRESHOLD = 3
    if len(unique_candidates) >= MIN_RERANK_THRESHOLD:
        if session.reranker is None:
            print("🔄 Loading reranker...")
            session.reranker = get_reranker()
        
        reranker = session.reranker
        try:
            passages = [{"id": i, "text": c["text"][:500]} for i, c in enumerate(unique_candidates)]
            rerank_request = RerankRequest(query=query, passages=passages)
            reranked = reranker.rerank(rerank_request)
            top_n = min(5, len(reranked))
            top_candidates = [unique_candidates[r["id"]] for r in reranked[:top_n]]
            rerank_scores = [r["score"] for r in reranked[:top_n]]
            retrieval_stats["reranked"] = top_n
        except Exception:
            top_candidates = unique_candidates[:5]
            rerank_scores = [None] * len(top_candidates)
    else:
        top_candidates = unique_candidates[:5]
        rerank_scores = [None] * len(top_candidates)
        retrieval_stats["reranked"] = len(top_candidates)

    # STEP 5: Format Context
    context_lines = []
    for i, candidate in enumerate(top_candidates, 1):
        md = candidate["metadata"]
        source_type = candidate["source_type"]
        data_source = md.get("data_source", "unknown")
        
        if source_type == "uploaded_document":
            source_label = f"📄 Uploaded: {md.get('file_name', 'Unknown')}"
        else:
            # ← ADD DATA SOURCE BADGE
            if data_source == "batch":
                source_badge = "📚 Historical"
            elif data_source == "realtime":
                source_badge = "🔴 Live"
            else:
                source_badge = "❓ Unknown"
            
            source_label = f"📰 Guardian ({source_badge}): {md.get('title', 'Unknown Article')}"
        
        score = rerank_scores[i-1] if i-1 < len(rerank_scores) else "N/A"
        context_lines.append(
            f"[Source {i} — {source_label}]\n"
            f"Relevance: {score}\n"
            f"Data Source: {data_source}\n"
            f"URL: {md.get('web_url', 'N/A')}\n"
            f"Content:\n{candidate['text'].strip()}\n"
            f"{'-' * 80}"
        )

    full_context = "\n\n".join(context_lines)
    
    context_message = SystemMessage(content=f"""
    You are a professional Guardian news analyst assistant.

    When users ask about news: search your database, cite sources with [Source N - Title], acknowledge uncertainty.
    When users make conversation: respond naturally without searching.

    Core principles: Be accurate, conversational, concise. Never hallucinate.

    {full_context}
    """)

    retrieval_stats["query_time_ms"] = int((time.time() - start_time) * 1000)
    session.retrieval_stats.append(retrieval_stats)

    logger.info("retrieval_completed", **retrieval_stats)

    return {"messages": state["messages"][:-1] + [context_message] + [state["messages"][-1]]}

def chatbot(state: AgentState, config: RunnableConfig) -> Dict[str, Any]:
    """Agent decision node."""
    
    last_message = state["messages"][-1]
    if hasattr(last_message, "content"):
        query_lower = last_message.content.lower()
        
        # Detect requests for more/different articles
        wants_more = any(word in query_lower for word in [
            'other', 'more', 'different', 'else', 'additional', 
            'another', 'further', 'expand'
        ])
        
        # Check if we previously returned few results
        prev_was_limited = False
        if len(state["messages"]) >= 2:
            prev_msg = state["messages"][-2]
            if hasattr(prev_msg, "content") and isinstance(prev_msg.content, str):
                # Check if previous response mentioned only 1-3 articles
                if any(phrase in prev_msg.content.lower() for phrase in ['2 recent', '1 recent', '3 recent']):
                    prev_was_limited = True
        
        if wants_more and prev_was_limited:
            # Force agent to check batch source
            enhanced_message = HumanMessage(content=f"""
            {last_message.content}
            
            CRITICAL INSTRUCTION:
            The user wants MORE articles than the 2-3 recent ones already shown.
            You MUST call comprehensive_search(keyword="<topic>", limit_per_source=10) 
            OR search_articles_by_data_source(data_source="batch", keyword="<topic>", limit=15)
            to get historical articles (pre-May 2024).
            
            DO NOT call smart_search_articles again - you'll get the same results.
            DO NOT just repeat what you already showed.
            """)
            
            modified_messages = state["messages"][:-1] + [enhanced_message]
            response = model_with_tools.invoke({"messages": modified_messages}, config=config)
        else:
            # Normal processing
            response = model_with_tools.invoke({"messages": state["messages"]}, config=config)
    else:
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

# ==================== CLI INTERFACE ====================
def get_agent_response(user_input: str) -> str:
    """Get response from the agent."""
    request_start = time.time()
    logger.info("agent_request_started", user_input=user_input[:100])

    config: RunnableConfig = {"configurable": {"thread_id": session.thread_id}}
    input_state = {
        "messages": [HumanMessage(content=user_input)],
        "user_name": session.user_name,
    }

    try:
        result = None
        for chunk in app.stream(cast(AgentState, input_state), config, stream_mode="values"):
            result = chunk

        if result and "messages" in result and result["messages"]:
            final_msg = result["messages"][-1]
            response = final_msg.content if hasattr(final_msg, "content") else str(final_msg)
        else:
            response = "I'm not sure how to respond to that."

        response_time = (time.time() - request_start) * 1000
        logger.info("agent_request_completed", response_time_ms=response_time)
        return response

    except Exception as e:
        logger.error("agent_request_failed", error=str(e), exc_info=True)
        return f"Error: {str(e)}"

def print_stats():
    """Print retrieval statistics."""
    if session.retrieval_stats:
        latest = session.retrieval_stats[-1]
        print("\n" + "="*60)
        print("📊 RETRIEVAL STATS")
        print("="*60)
        print(f"Dense (Guardian): {latest['dense_guardian']}")
        print(f"Dense (User Docs): {latest['dense_user']}")
        print(f"SQL Results: {latest['sql']}")
        print(f"Unique Results: {latest['unique']}")
        print(f"Top-K Reranked: {latest['reranked']}")
        print(f"Total Time: {latest['query_time_ms']}ms")
        print("="*60 + "\n")

def main():
    """Main CLI loop."""
    # Download NLTK data on startup
    # download_nltk_data()
    
    print("\n" + "="*60)
    print("🤖 Guardian News Analyst - CLI Mode")
    print("="*60)
    print("Hybrid RAG: Pinecone (Dense) + Snowflake (SQL) + FlashRank")
    print("Commands: 'quit' to exit, 'stats' to show retrieval stats")
    print("="*60 + "\n")

    while True:
        try:
            user_input = input("\n💬 You: ").strip()
            
            if not user_input:
                continue
            
            if user_input.lower() in ['quit', 'exit', 'q']:
                print("\n👋 Goodbye! Have a great day!")
                break
            
            if user_input.lower() == 'stats':
                print_stats()
                continue
            
            print("\n🔍 Processing...\n")
            response = get_agent_response(user_input)
            print(f"\n🤖 Assistant: \n{response}\n")
            
        except KeyboardInterrupt:
            print("\n\n👋 Interrupted. Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {str(e)}\n")

if __name__ == "__main__":
    main()