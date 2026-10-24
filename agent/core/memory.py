"""Conversation memory (LangGraph checkpointer).

Postgres when LANGGRAPH_POSTGRES_PASSWORD is set and the database answers (local:
`make memory-db`, i.e. `docker compose --project-directory . -f infra/local/docker-compose.yml up -d`), otherwise in-memory: history then lasts
for the process lifetime, which is fine for a demo and costs nothing.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def create_checkpointer():
    """Returns (checkpointer, kind) where kind is 'postgres' or 'memory'."""
    password = os.getenv("LANGGRAPH_POSTGRES_PASSWORD")
    if password:
        try:
            import psycopg
            from langgraph.checkpoint.postgres import PostgresSaver
            uri = (f"postgresql://{os.getenv('LANGGRAPH_POSTGRES_USER', 'postgres')}:{password}"
                   f"@{os.getenv('LANGGRAPH_POSTGRES_HOST', 'localhost')}:{os.getenv('LANGGRAPH_POSTGRES_PORT', '5433')}"
                   f"/{os.getenv('LANGGRAPH_POSTGRES_DB', 'langgraph_memory')}")
            conn = psycopg.connect(uri, autocommit=True, connect_timeout=3)
            saver = PostgresSaver(conn)  # type: ignore[arg-type]
            saver.setup()
            return saver, "postgres"
        except Exception as e:  # noqa: BLE001
            logger.warning("Postgres checkpointer unavailable (%s); using in-memory history", e)
    from langgraph.checkpoint.memory import MemorySaver
    return MemorySaver(), "memory"
