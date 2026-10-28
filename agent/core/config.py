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
    "dantri_en": ("DTiNews, Dân Trí (English)", "en"),
    "vir": ("Vietnam Investment Review (English)", "en"),
    "vietnamfinance": ("VietnamFinance", "vi"),
    "congthuong": ("Công Thương", "vi"),
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

    # gpt-5.4-mini since 2026-10-20 (docs/llm_compare.md): the best answers of 5 models
    # tested, ~$7 per 1k turns vs ~$2 for gpt-4o-mini.
    llm_model: str = os.getenv("OPENAI_MODEL", "gpt-5.4-mini")
    llm_reasoning_effort: str = os.getenv("AGENT_REASONING_EFFORT", "low")  # reasoning models only
    llm_max_tokens: int = _int("AGENT_MAX_TOKENS", 1500)

    # Embeddings v2: gemini-embedding-2 article vectors (BigQuery vectors.article_embeddings,
    # 768 dims) served from this index at 512 dims. The v1 index (Guardian-only OpenAI
    # chunks, also home of the PDF uploads) was removed on 2026-10-24.
    pinecone_index_v2: str = os.getenv("PINECONE_INDEX_V2", "news-articles-v2")
    articles_namespace_v2: str = os.getenv("PINECONE_ARTICLES_NAMESPACE_V2", "articles")
    # Automatic retrieval. "dense": v2 dense only, ranked by similarity (the 2026-10-19 eval
    # winner, docs/eval_refresh.md). "hybrid": dense + keyword interleave + FlashRank, the
    # pre-eval pipeline (demo mode runs it, for comparison).
    retrieval_mode: str = os.getenv("AGENT_RETRIEVAL_MODE", "dense")
    query_embedding_model: str = os.getenv("QUERY_EMBEDDING_MODEL", "gemini-embedding-2")
    # Passage rescue (dense mode, 2026-10-21, core/passages.py, docs/hard_eval.md): the top
    # `passage_pool` dense hits' full bodies are scored passage by passage with BM25; the
    # dense top `passage_keep` stay, the next slots go to the best passages. Finds details
    # beyond the ~1,500 chars the vectors see. AGENT_PASSAGE_POOL=0 turns it off.
    passage_pool: int = _int("AGENT_PASSAGE_POOL", 30)
    passage_keep: int = _int("AGENT_PASSAGE_KEEP", 4)
    vectors_dataset: str = os.getenv("BQ_DATASET_VECTORS", "vectors")

    # Conversation turns sent to the LLM (older turns stay in memory, not in the prompt).
    history_turns: int = _int("AGENT_HISTORY_TURNS", 6)
    # Questions per session per hour (public demo guard).
    rate_limit_per_hour: int = _int("AGENT_RATE_LIMIT_PER_HOUR", 30)

    @property
    def articles_table(self) -> str:
        return f"`{self.gcp_project}.{self.marts_dataset}.obt_articles`"

    @property
    def bodies_table(self) -> str:
        return f"`{self.gcp_project}.{self.vectors_dataset}.article_bodies`"


settings = Settings()
