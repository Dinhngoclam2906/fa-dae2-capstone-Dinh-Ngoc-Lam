"""Shared core of the Daily News AI Assistant: one implementation behind the Streamlit
app (`app.py`) and the terminal agent (`cli_agent.py`).

    config      settings from env (models, limits, BigQuery / Pinecone names)
    bq          BigQuery access with a bytes-billed cap on every query
    articles    plain data-access functions over marts.obt_articles
    text        query heuristics (keywords, retrieve-or-chat, Vietnamese detection)
    retrieval   hybrid retrieval: dense (Pinecone) + keyword (BigQuery) -> rerank
    tools       the LangChain tools the agent can call
    agent       the LangGraph graph (retrieve -> chatbot <-> tools)
    memory      conversation checkpointer (Postgres if configured, else in-memory)
    uploads     per-session PDF upload into the vector store
"""
