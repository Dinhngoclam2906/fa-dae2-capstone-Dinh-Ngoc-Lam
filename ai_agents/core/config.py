"""Settings, all from environment variables (`.env` locally, job/service env on GCP)."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

SOURCES = {
    # source_name: (site, language)
    "guardian": ("The Guardian", "en"),
    "tuoitre": ("Tuổi Trẻ", "vi"),
    "thanhnien": ("Thanh Niên", "vi"),
    "vneconomy": ("VnEconomy", "vi"),
    "vietnamnet": ("VietNamNet", "vi"),
    "vnexpress": ("VnExpress", "vi"),
    "tuoitre_en": ("Tuoi Tre News (English)", "en"),
    "vneconomy_en": ("VnEconomy (English)", "en"),
    "vietnamnet_en": ("VietNamNet Global (English)", "en"),
    "vnexpress_en": ("VnExpress International (English)", "en"),
}

SECTION_GROUPS = [
    "Sports", "Opinion", "National News", "Global & UK", "Economy & Business", "Politics",
    "Technology", "Science & Environment", "Culture & Arts", "Lifestyle & Travel",
    "Society & Education", "Other",
]

LANGUAGES = ["en", "vi"]


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


@dataclass(frozen=True)
class Settings:
    gcp_project: str = os.getenv("GCP_PROJECT", "")
    bq_location: str = os.getenv("BQ_LOCATION", "asia-southeast1")
    marts_dataset: str = os.getenv("BQ_DATASET_MARTS", "marts")

    # Hard cap per BigQuery query. 600 MB allows a 90-day full-body search (~0.5 GB)
    # but refuses full-archive body scans (~2.5 GB).
    max_bytes_per_query: int = _int("AGENT_MAX_BYTES_PER_QUERY", 600_000_000)
    # Keyword search / browsing default to the last N days when no dates are given.
    default_window_days: int = _int("AGENT_DEFAULT_WINDOW_DAYS", 90)

    llm_model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    llm_max_tokens: int = _int("AGENT_MAX_TOKENS", 1500)
    embedding_model: str = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")

    pinecone_index: str = os.getenv("PINECONE_INDEX", "guardian-capstone")
    articles_namespace: str = os.getenv("PINECONE_ARTICLES_NAMESPACE", "guardian_articles")
    user_docs_namespace: str = os.getenv("PINECONE_USER_DOCS_NAMESPACE", "user_documents")

    # Conversation turns sent to the LLM (older turns stay in memory, not in the prompt).
    history_turns: int = _int("AGENT_HISTORY_TURNS", 6)
    # Questions per session per hour (public demo guard).
    rate_limit_per_hour: int = _int("AGENT_RATE_LIMIT_PER_HOUR", 30)

    @property
    def articles_table(self) -> str:
        return f"`{self.gcp_project}.{self.marts_dataset}.obt_articles`"


settings = Settings()
