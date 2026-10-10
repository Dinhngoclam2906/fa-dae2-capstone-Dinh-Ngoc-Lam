# app.py - Optimized Hybrid RAG with Pinecone Best Practices
import time
import re
import streamlit as st
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
import nltk
from nltk.corpus import stopwords

# Your tool-calling module
from capstone_tool_calling_agent import tools

load_dotenv()

# ==================== STRUCTURED LOGGING SETUP ====================
def configure_logging():
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
        logger.info("Warming up Pinecone index...")
        vector_store.similarity_search("test", k=1)
        logger.info("✅ Pinecone index warmed up")
        return vector_store
    except Exception as e:
        st.warning(f"Vector store not available: {e}. Falling back to SQL-only mode.")
        return None

@st.cache_resource
def get_reranker():
    """Initialize FlashRank reranker (local, fast)."""
    return Ranker(model_name="ms-marco-MiniLM-L-12-v2", cache_dir="/tmp")

@st.cache_resource
def get_pinecone_client():
    try:
        return Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
    except:
        return None

pinecone_client = get_pinecone_client()

def process_and_upload_pdf(uploaded_file) -> bool:
    """Process uploaded PDF using existing DocumentProcessor pipeline."""
    import tempfile
    from services.document_processor import DocumentProcessor

    try:
        # Save uploaded file to temp path
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(uploaded_file.read())
            tmp_path = tmp.name

        # Use existing pipeline
        processor = DocumentProcessor(
            chunk_size=int(os.getenv("CHUNK_SIZE", 1000)),
            chunk_overlap=int(os.getenv("CHUNK_OVERLAP", 200)),
            chunking_strategy="hybrid"
        )
        chunks = processor.process_document(tmp_path)

        if not chunks:
            st.error("No text could be extracted from this PDF.")
            return False

        # Convert to LangChain Documents with file_name metadata
        from langchain_core.documents import Document
        docs = []
        for chunk in chunks:
            chunk["metadata"]["file_name"] = uploaded_file.name
            chunk["metadata"]["source_type"] = "uploaded_document"
            docs.append(Document(page_content=chunk["text"], metadata=chunk["metadata"]))

        # Upload to Pinecone user_documents namespace
        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        PineconeVectorStore.from_documents(
            documents=docs,
            embedding=embeddings,
            index_name=os.getenv("PINECONE_INDEX", "guardian-capstone"),
            namespace="user_documents"
        )

        os.unlink(tmp_path)
        logger.info("pdf_uploaded", file=uploaded_file.name, chunks=len(docs))
        return True

    except Exception as e:
        st.error(f"❌ Failed to process PDF: {e}")
        return False

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
        checkpointer = PostgresSaver(connection)  # type: ignore
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
        with st.spinner("📦 One-time setup: Downloading NLTK data..."):
            nltk.download('stopwords', quiet=True)
        st.session_state.nltk_data_downloaded = True
        st.success("✅ Setup complete!")

_CACHED_STOPWORDS = None
def get_stopwords() -> set:
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

    # Don't extract keywords for metadata queries
    if any(word in query_lower for word in ['batch', 'realtime', 'data source', 'loaded']):
        return []

    # Don't extract keywords for date queries
    if any(word in query_lower for word in ['published on', 'last week', 'today', 'yesterday']):
        return []

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

    single_names = re.findall(r'\b[A-Z][a-z]{3,}\b', query)
    for name in single_names:
        name_lower = name.lower()
        if (name_lower not in stop_words and
                name_lower not in keywords and
                name_lower not in extracted_from_multiword):
            keywords.append(name_lower)

    all_words = re.findall(r'\b\w+\b', query.lower())
    for word in all_words:
        if word in keywords or word in extracted_from_multiword:
            continue
        if word in important_short_terms:
            keywords.append(word)
        elif word not in stop_words and len(word) >= 5:
            keywords.append(word)

    meta_stopwords = {
        'check', 'whether', 'discussing', 'articles', 'news',
        'find', 'search', 'look', 'show', 'give', 'tell',
        'want', 'need', 'could', 'would', 'should',
    }

    sql_keywords = [
        k for k in keywords
        if k not in meta_stopwords and (len(k) >= 4 or k in important_short_terms)
    ]

    seen = set()
    sql_keywords = [k for k in sql_keywords if not (k in seen or seen.add(k))]  # type: ignore

    if not sql_keywords and keywords:
        sql_keywords = [max(keywords, key=len)]

    return sql_keywords[:5]


def deduplicate_results(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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

# ==================== IMPROVED SYSTEM PROMPT (FROM CLI) ====================
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
- When answering about uploaded documents, ONLY use information explicitly 
  present in the document. Do NOT supplement with general background knowledge
  without clearly labeling it as such with: "(from general knowledge, not the document)"
- If the document contains a negative review or critical opinion, reflect that 
  accurately — do not present it positively based on prior knowledge of the subject

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

# ==================== IMPROVED SHOULD_RETRIEVE (FROM CLI) ====================
def should_retrieve(query: str) -> bool:
    """
    Determine if a query needs retrieval from the knowledge base.
    Ported from CLI version with factual indicator priority logic.
    """
    query_lower = query.lower().strip()

    # PRIORITY 1: Always retrieve if it's clearly a factual query
    factual_indicators = [
        r'\bwhat (do|does|did|are|is|was|were)\b',
        r'\bhow (do|does|did|can|could|should|would)\b',
        r'\b(why|when|where|which|who)\b',
        r'\b(medical|experts?|doctors?|scientists?|researchers?)\b',
        r'\b(guardian|article|news|coverage|report)\b',
        r'\b(show|find|search|get|fetch|retrieve)\b',
        r'\b(compare|analyze|summarize)\b',
    ]

    if any(re.search(pattern, query_lower) for pattern in factual_indicators):
        return True

    if '?' in query:
        if len(query_lower.split()) > 3:
            return True

    # PRIORITY 2: Check for NON-factual patterns

    # Pattern 1: Greetings
    greetings = {
        'hi', 'hello', 'hey', 'good morning', 'good afternoon',
        'good evening', 'howdy', 'greetings'
    }
    greeting_patterns = [rf'\b{g}\b' for g in greetings]
    if len(query_lower.split()) <= 4:
        if any(re.match(pattern, query_lower) for pattern in greeting_patterns):
            return False

    # Pattern 2: Meta questions about the assistant
    meta_patterns = [
        r'^\s*(who|what) are you\b',
        r'^\s*what can you do\b',
        r'^\s*what do you do\b',
        r'^\s*tell me about yourself\b',
        r'^\s*introduce yourself\b',
    ]
    if any(re.search(pattern, query_lower) for pattern in meta_patterns):
        return False

    # Pattern 3: Pure acknowledgments (short + no question mark)
    acknowledgment_phrases = [
        'thanks', 'thank you', 'ok', 'okay', 'got it', 'understood',
        'bye', 'goodbye', 'see you', 'cool', 'nice', 'great', 'perfect'
    ]
    if len(query_lower.split()) <= 5 and '?' not in query:
        first_two_words = ' '.join(query_lower.split()[:2])
        if any(ack in first_two_words for ack in acknowledgment_phrases):
            return False

    # Pattern 4: Conversational reactions (short + no question mark)
    reaction_patterns = [
        r'^(that\'?s?|it\'?s?|this is) (interesting|fascinating|cool|great|nice|good)\s*[!.]*$',
        r'^i (agree|see|understand)\s*[!.]*$',
        r'^(makes sense|got it|understood)\s*[!.]*$',
        r'^(yes|yep|yeah|no|nope),?\s*[!.]*$',
    ]
    if len(query_lower.split()) <= 6:
        if any(re.match(pattern, query_lower) for pattern in reaction_patterns):
            return False

    # Pattern 5: Very short non-questions
    if len(query_lower.split()) <= 2 and '?' not in query_lower:
        important_entities = ['trump', 'brexit', 'covid', 'ukraine', 'climate']
        if not any(entity in query_lower for entity in important_entities):
            return False

    return True


def get_instant_response(query: str) -> Optional[str]:
    """Return pre-defined responses for common queries."""
    query_lower = query.lower().strip()

    if re.search(r'\bwho are you\b', query_lower):
        return """I'm a Guardian news analyst assistant! I have access to thousands of Guardian articles 
stored in a hybrid database (vector search + SQL), and I can help you find articles, analyze trends, 
and answer questions about recent events."""

    if re.search(r'\bwhat can you do\b', query_lower):
        return """I can help you explore Guardian news articles! I can:
- Find articles by keyword, topic, or section
- Track trends over time
- Compare coverage across sections
- Answer questions using hybrid search (semantic + keyword matching)

Try asking: "What are the latest articles about climate change?" """

    return None


# ==================== IS_CONVERSATIONAL_FOLLOWUP (FROM CLI) ====================
def is_conversational_followup(query: str, messages: List[Any]) -> bool:
    """
    Detect if query is a conversational reaction to previous response.
    Ported from CLI version.
    """
    query_lower = query.lower().strip()

    acknowledgment_phrases = [
        'interesting', 'fascinating', 'cool', 'great', 'nice',
        'i agree', 'i see', 'makes sense', 'good point',
        'i found', 'i think', 'that\'s', 'it\'s'
    ]

    if len(query_lower.split()) <= 7:
        if any(phrase in query_lower for phrase in acknowledgment_phrases):
            if '?' not in query_lower:
                return True

    simple_affirmations = {
        'yes', 'yep', 'yeah', 'ok', 'okay', 'sure',
        'no', 'nope', 'not really', 'i disagree'
    }

    first_word = query_lower.split()[0] if query_lower.split() else ""
    if first_word in simple_affirmations and len(query_lower.split()) <= 4:
        return True

    return False


def retrieve_context(state: AgentState):
    """True Hybrid Retrieval Strategy (Dense + Lexical + Rerank) — optimized from CLI."""
    import concurrent.futures

    start_time = time.time()

    if not state["messages"]:
        return state

    last_message = state["messages"][-1]
    if not hasattr(last_message, "content") or not last_message.content.strip():
        return state

    query = last_message.content.strip()

    # Instant responses for meta queries
    instant_response = get_instant_response(query)
    if instant_response:
        response_message = AIMessage(content=instant_response)
        return {"messages": state["messages"] + [response_message]}

    # Conversational followup — respond naturally
    if is_conversational_followup(query, state["messages"]):
        logger.info("retrieval_skipped", query=query[:100], reason="conversational_followup")
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
        logger.info("retrieval_skipped", query=query[:100], reason="non_factual_query",
                    user=st.session_state.get("user_name"),
                    thread_id=st.session_state.get("thread_id"))
        context_message = SystemMessage(content=f"""
        Query: "{query}"
        
        This is a casual greeting or meta question - respond warmly and naturally!
        Be friendly and conversational (this is a human saying hi!)
        Keep it brief (2-3 sentences max).
        """)
        new_messages = state["messages"][:-1] + [context_message] + [state["messages"][-1]]
        return {"messages": new_messages}

    logger.info("retrieval_started", query=query[:100],
                user=st.session_state.get("user_name"),
                thread_id=st.session_state.get("thread_id"))

    all_candidates = []
    demo_mode = st.session_state.get("demo_mode", False)

    retrieval_stats = {
        "dense_guardian": 0, "dense_user": 0, "dense_total": 0,
        "sql": 0, "sql_keywords_used": [], "unique": 0, "reranked": 0,
        "sql_skipped": False, "rerank_skipped": False, "demo_mode": demo_mode,
        "query": query, "keywords_extracted": [], "query_time_ms": 0,
        "dense_time_ms": 0, "sql_time_ms": 0, "dedup_time_ms": 0, "rerank_time_ms": 0
    }

    # Lazy load vector store
    if st.session_state.vector_store is None:
        with st.spinner("🔄 Initializing vector search..."):
            st.session_state.vector_store = get_vector_store()

    vector_store = st.session_state.vector_store

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
                pass

    retrieval_stats["dense_total"] = len(all_candidates)
    retrieval_stats["dense_time_ms"] = int((time.time() - dense_start) * 1000)

    # STEP 2: Lexical Search (SQL)
    sql_start = time.time()
    keywords = extract_keywords(query)
    retrieval_stats["keywords_extracted"] = keywords

    if keywords:
        important_short_terms = {
            'ai', 'uk', 'us', 'eu', 'un', 'ceo', 'fbi', 'cia', 'gdp', 'nhs',
            'cop', 'g7', 'g20', 'ufo', 'nyc', 'la', 'qa', 'hr', 'it', 'pr',
            'q1', 'q2', 'q3', 'q4', 'gop', 'mlb', 'nba', 'nfl', 'ufc'
        }
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
            except Exception as e:
                logger.error("sql_search_failed", error=str(e))
                retrieval_stats["sql"] = 0
                retrieval_stats["sql_keywords_used"] = sql_keywords

    retrieval_stats["sql_time_ms"] = int((time.time() - sql_start) * 1000)

    # STEP 3: Deduplicate
    dedup_start = time.time()
    unique_candidates = deduplicate_results(all_candidates)
    retrieval_stats["unique"] = len(unique_candidates)
    retrieval_stats["dedup_time_ms"] = int((time.time() - dedup_start) * 1000)

    if not unique_candidates:
        no_results_message = SystemMessage(
            content="No relevant articles found for this query. Please try rephrasing or using different keywords."
        )
        new_messages = state["messages"][:-1] + [no_results_message] + [state["messages"][-1]]
        retrieval_stats["query_time_ms"] = int((time.time() - start_time) * 1000)
        if "retrieval_stats" not in st.session_state:
            st.session_state.retrieval_stats = []
        st.session_state.retrieval_stats.append(dict(retrieval_stats))
        return {"messages": new_messages}

    # STEP 4: Re-rank (Lazy loaded)
    rerank_start = time.time()
    MIN_RERANK_THRESHOLD = 3

    if len(unique_candidates) >= MIN_RERANK_THRESHOLD:
        if st.session_state.reranker is None:
            with st.spinner("🔄 Loading reranker..."):
                st.session_state.reranker = get_reranker()
        reranker = st.session_state.reranker
        try:
            passages = [{"id": i, "text": c["text"][:500]} for i, c in enumerate(unique_candidates)]
            rerank_request = RerankRequest(query=query, passages=passages)
            reranked = reranker.rerank(rerank_request)
            top_n = min(5, len(reranked))
            top_candidates = [unique_candidates[r["id"]] for r in reranked[:top_n]]
            rerank_scores = [r["score"] for r in reranked[:top_n]]
            retrieval_stats["reranked"] = top_n
            retrieval_stats["rerank_skipped"] = False
        except Exception as e:
            logger.warning("rerank_failed", error=str(e))
            top_candidates = unique_candidates[:5]
            rerank_scores = [None] * len(top_candidates)
            retrieval_stats["reranked"] = len(top_candidates)
            retrieval_stats["rerank_skipped"] = False
    else:
        top_candidates = unique_candidates[:5]
        rerank_scores = [None] * len(top_candidates)
        retrieval_stats["reranked"] = len(top_candidates)
        retrieval_stats["rerank_skipped"] = True

    retrieval_stats["rerank_time_ms"] = int((time.time() - rerank_start) * 1000)

    # STEP 5: Format Context
    context_lines = []
    for i, candidate in enumerate(top_candidates, 1):
        md = candidate["metadata"]
        source_type = candidate["source_type"]
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

        score = rerank_scores[i-1] if i-1 < len(rerank_scores) else "N/A"
        score_display = f"{score:.3f}" if isinstance(score, (int, float)) else score

        context_lines.append(
            f"[Source {i} — {source_label}]\n"
            f"Relevance: {score_display}\n"
            f"Data Source: {data_source}\n"
            f"Published: {md.get('published_date', 'Unknown')}\n"
            f"Section: {md.get('section', 'N/A')}\n"
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
    if "retrieval_stats" not in st.session_state:
        st.session_state.retrieval_stats = []
    st.session_state.retrieval_stats.append(dict(retrieval_stats))

    logger.info(
        "retrieval_completed",
        query=query[:100],
        dense_results=retrieval_stats["dense_total"],
        sql_results=retrieval_stats["sql"],
        unique_results=retrieval_stats["unique"],
        reranked_results=retrieval_stats["reranked"],
        total_time_ms=retrieval_stats["query_time_ms"],
        user=st.session_state.get("user_name"),
        thread_id=st.session_state.get("thread_id")
    )

    return {"messages": state["messages"][:-1] + [context_message] + [state["messages"][-1]]}


# ==================== IMPROVED CHATBOT NODE (FROM CLI) ====================
def chatbot(state: AgentState, config: RunnableConfig) -> Dict[str, Any]:
    """Agent decision node — with 'wants more' detection from CLI."""

    last_message = state["messages"][-1]
    if hasattr(last_message, "content"):
        query_lower = last_message.content.lower()

        wants_more = any(word in query_lower for word in [
            'other', 'more', 'different', 'else', 'additional',
            'another', 'further', 'expand'
        ])

        prev_was_limited = False
        if len(state["messages"]) >= 2:
            prev_msg = state["messages"][-2]
            if hasattr(prev_msg, "content") and isinstance(prev_msg.content, str):
                if any(phrase in prev_msg.content.lower() for phrase in ['2 recent', '1 recent', '3 recent']):
                    prev_was_limited = True

        if wants_more and prev_was_limited:
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
    if "demo_mode" not in st.session_state:
        st.session_state.demo_mode = False

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
    st.session_state.messages.append({"role": "user", "content": query})
    with st.spinner(f"🔍 Processing showcase query: {query}..."):
        st.session_state.processing = True
        try:
            response = get_agent_response(query)
            st.session_state.messages.append({"role": "assistant", "content": response})
            st.success(f"✅ Showcase query completed!")
            time.sleep(0.5)
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"I encountered an error: {str(e)}"
            })
        finally:
            st.session_state.processing = False
    st.rerun()

def get_agent_response(user_input: str) -> str:
    request_start = time.time()
    logger.info("agent_request_started", user_input=user_input[:100],
                user=st.session_state.get("user_name"),
                thread_id=st.session_state.thread_id)
    st.session_state.processing = True
    config: RunnableConfig = {"configurable": {"thread_id": st.session_state.thread_id}}
    input_state = {
        "messages": [HumanMessage(content=user_input)],
        "user_name": st.session_state.user_name,
    }
    try:
        result = None
        for chunk in st.session_state.agent.stream(cast(AgentState, input_state), config, stream_mode="values"):
            result = chunk
        if result and "messages" in result and result["messages"]:
            final_msg = result["messages"][-1]
            response = final_msg.content if hasattr(final_msg, "content") else str(final_msg)
        else:
            response = "I'm not sure how to respond to that."
        meta = st.session_state.conversation_metadata[st.session_state.thread_id]
        meta["message_count"] = meta.get("message_count", 0) + 1
        response_time = (time.time() - request_start) * 1000
        logger.info("agent_request_completed", user_input=user_input[:100],
                    response_length=len(response), response_time_ms=response_time,
                    user=st.session_state.get("user_name"),
                    thread_id=st.session_state.thread_id)
        return response
    except Exception as e:
        logger.error("agent_request_failed", user_input=user_input[:100],
                     error_type=type(e).__name__, error_message=str(e),
                     user=st.session_state.get("user_name"),
                     thread_id=st.session_state.thread_id, exc_info=True)
        return f"Error: {str(e)}"
    finally:
        st.session_state.processing = False

# ==================== MAIN APP ====================
initialize_session_state()

st.markdown("<h1>Daily News AI Assistant</h1>", unsafe_allow_html=True)
st.markdown("**📰 The Guardian AI Assistant • Hybrid RAG (Dense Vector + SQL) • Re-ranked with FlashRank**")

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
    st.markdown("### 📄 Upload Document")
    uploaded_file = st.file_uploader("Upload a PDF to ask questions about", type=["pdf"])
    if uploaded_file:
        file_key = f"uploaded_{uploaded_file.name}"
        if file_key not in st.session_state:
            with st.spinner(f"📄 Processing {uploaded_file.name}..."):
                success = process_and_upload_pdf(uploaded_file)
                if success:
                    st.session_state[file_key] = True
                    st.success(f"✅ {uploaded_file.name} ready! Ask questions about it.")
                else:
                    st.error("Failed to process PDF.")
        else:
            st.success(f"✅ {uploaded_file.name} already loaded!")

    st.divider()

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
            if st.button(f"{emoji} {query_text}", key=f"showcase_{query_text}",
                         use_container_width=True, help=f"Click to auto-run: {query_text}"):
                process_showcase_query(query_text)
    else:
        st.session_state.demo_mode = False

    st.divider()

    st.markdown("### 🔍 Smart Hybrid Stats")

    if st.session_state.retrieval_stats:
        query_count = len(st.session_state.retrieval_stats)
        st.caption(f"📊 Showing results from query #{query_count}")
        latest_stats = st.session_state.retrieval_stats[-1]

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

        st.markdown("**SQL Keyword Search:**")
        sql_count = latest_stats.get("sql", 0)
        keywords_used = latest_stats.get("sql_keywords_used", [])
        if keywords_used:
            st.caption(f"🔍 _Keywords: {', '.join(keywords_used)}_")
        else:
            st.caption("⚠️ _No keywords extracted from query_")

        if sql_count > 0:
            st.metric("🗄️ SQL Results", sql_count)
            with st.expander("ℹ️ Why SQL ran"):
                st.write(f"""
                **True Hybrid Search:**
                - SQL **always runs** alongside dense search
                - Found {sql_count} keyword-matched articles
                **Keywords searched:** {', '.join(keywords_used)}
                """)
        else:
            st.info("🔹 **SQL:** No matches found")
            with st.expander("ℹ️ Why no SQL results?"):
                if keywords_used:
                    st.write(f"Keywords extracted: {', '.join(keywords_used)} — no exact matches found.")
                else:
                    st.write("No keywords extractable from query. Dense search is sufficient.")

        st.markdown("**After Deduplication:**")
        total_before = dense_total + sql_count
        unique_after = latest_stats.get("unique", 0)
        duplicates = total_before - unique_after
        st.metric(
            "Unique Results", unique_after,
            delta=f"-{duplicates} duplicate{'s' if duplicates != 1 else ''}" if duplicates > 0 else "No duplicates"
        )

        st.markdown("**Re-ranking:**")
        if latest_stats.get("rerank_skipped"):
            reranked_count = latest_stats.get("reranked", 0)
            st.warning(f"⚠️ **Skipped** - Only {reranked_count} result{'s' if reranked_count != 1 else ''}")
            st.caption("_Need ≥3 results to rerank_")
        else:
            reranked_count = latest_stats.get("reranked", 0)
            st.metric("🎯 Top-K Results", reranked_count)
            st.caption("_Re-ranked by FlashRank_")

        query_time = latest_stats.get("query_time_ms", 0)
        if query_time > 0:
            if query_time < 1000:
                st.success(f"⚡ **Retrieval Time:** {query_time}ms")
            elif query_time < 2000:
                st.info(f"⏱️ **Retrieval Time:** {query_time}ms")
            else:
                st.warning(f"⏱️ **Retrieval Time:** {query_time}ms")

            dense_time = latest_stats.get("dense_time_ms", 0)
            sql_time = latest_stats.get("sql_time_ms", 0)
            dedup_time = latest_stats.get("dedup_time_ms", 0)
            rerank_time = latest_stats.get("rerank_time_ms", 0)

            with st.expander("⏱️ Time breakdown", expanded=False):
                if dense_time > 0:
                    st.caption(f"🔍 Dense search: {dense_time}ms ({dense_time/query_time*100:.1f}%)")
                if sql_time > 0:
                    st.caption(f"🗄️ SQL search: {sql_time}ms ({sql_time/query_time*100:.1f}%)")
                if dedup_time > 0:
                    st.caption(f"🔗 Deduplication: {dedup_time}ms ({dedup_time/query_time*100:.1f}%)")
                if rerank_time > 0:
                    st.caption(f"🎯 Re-ranking: {rerank_time}ms ({rerank_time/query_time*100:.1f}%)")
                measured_total = dense_time + sql_time + dedup_time + rerank_time
                overhead = query_time - measured_total
                if overhead > 0:
                    st.caption(f"⚙️ Overhead: {overhead}ms ({overhead/query_time*100:.1f}%)")
                st.caption(f"**Total Query:** {query_time}ms")

        st.divider()

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
                    f"Query fully satisfied by semantic search."
                )
    else:
        st.info("👋 _No searches yet. Ask a question to see stats!_")

    st.divider()

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

    st.markdown("### 📂 Conversations")
    for tid, meta in st.session_state.conversation_metadata.items():
        status = "🟢 Current" if tid == st.session_state.thread_id else "⚪"
        st.write(f"{status} `{tid[:12]}...` — {meta['name']} ({meta.get('message_count', 0)} msgs)")

    st.divider()

    st.caption("**Architecture:**")
    st.caption("🔹 Dense: Pinecone vector DB")
    st.caption("🔹 Lexical: Snowflake SQL (multi-keyword)")
    st.caption("🔹 Rerank: FlashRank (local)")
    st.caption("🔹 LLM: GPT-4o-mini")
    st.caption("🔹 Mode: True Hybrid (both always run)")

# ==================== CHAT INTERFACE ====================
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

prompt = st.chat_input(
    "💬 Ask about Guardian news, articles, trends, or uploaded documents...",
    disabled=st.session_state.get("processing", False),
    key="main_chat_input"
)

if prompt:
    if re.search(r'\b(bye|goodbye|end|tạm biệt|hẹn gặp lại|done|xong|see you|that.?s all)\b', prompt, re.IGNORECASE):
        st.session_state.messages.append({"role": "user", "content": prompt})
        st.session_state.messages.append({
            "role": "assistant",
            "content": "**Take care! Have a wonderful day! See you next time! 👋**"
        })
        st.rerun()

    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("🔍 Searching (Dense + SQL) → Re-ranking → Generating..."):
            try:
                response = get_agent_response(prompt)
            except Exception as e:
                response = f"❌ Error: {str(e)}"
        st.markdown(response)

    st.session_state.messages.append({"role": "assistant", "content": response})
    st.rerun()

# ==================== FOOTER ====================
st.divider()
st.markdown(
    "<p style='text-align: center; color: #666;'>"
    "Built by Dinh Ngoc Lam • Foundry AI Academy Capstone 2025<br>"
    "Hybrid RAG: Pinecone (Dense) + Snowflake (SQL) + FlashRank (Rerank)</p>",
    unsafe_allow_html=True,
)