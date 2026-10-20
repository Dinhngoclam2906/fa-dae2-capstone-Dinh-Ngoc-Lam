"""Automatic retrieval: v2 dense search (Pinecone `news-articles-v2`, gemini-embedding-2).

Default since 2026-10-20 (`settings.retrieval_mode == "dense"`): one query embedding,
v2 articles ranked by similarity, after this session's uploads (still on the v1 index). The
2026-10-19 eval (docs/eval_refresh.md) found v2 dense alone beats every hybrid on every
question group; the FlashRank rerank and the keyword interleave both pushed good hits out
of the top 5. The old hybrid pipeline (dense + keyword + FlashRank, the winner of the
2026-01 English-only eval) stays as mode "hybrid": demo mode and the eval use it. Keyword
search is still an LLM tool (exact names, numbers, articles not yet embedded).

Uploaded documents live in one namespace but are filtered by `session_id`, so a user
only ever retrieves their own uploads.
"""

from __future__ import annotations

import concurrent.futures
import logging
import os
import tempfile
import threading
import time
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

from core import articles
from core.config import settings
from core.text import extract_keywords, is_vietnamese, recency_days

logger = logging.getLogger(__name__)

MIN_RECENT = 3  # fewer hits than this inside a recency window -> ignore the window
RECENT_FIRST = 3  # recent-window hits placed ahead of the all-dates ranking
RERANK_EN = "ms-marco-MiniLM-L-12-v2"
RERANK_MULTILINGUAL = "ms-marco-MultiBERT-L-12"  # used when Vietnamese is involved
MIN_RERANK = 3


@dataclass
class Candidate:
    text: str
    article_id: str | None = None
    title: str = ""
    section: str = ""
    section_group: str = ""
    source_name: str = ""
    language: str = ""
    published_date: str = ""
    url: str = ""
    source_type: str = "article"  # "article" | "uploaded_document"
    method: str = ""
    file_name: str = ""
    score: float | None = None

    def as_dict(self) -> dict[str, Any]:
        d = dict(self.__dict__)
        d["text"] = self.text[:500]
        return d


@dataclass
class RetrievalResult:
    candidates: list[Candidate] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)


@lru_cache(maxsize=1)
def _embeddings():
    from langchain_openai import OpenAIEmbeddings
    return OpenAIEmbeddings(model=settings.embedding_model)


@lru_cache(maxsize=1)
def vector_store():
    """The Pinecone store, or None if unavailable (retrieval falls back to keyword-only)."""
    try:
        from langchain_pinecone import PineconeVectorStore
        return PineconeVectorStore(index_name=settings.pinecone_index, embedding=_embeddings(),
                                   namespace=settings.articles_namespace)
    except Exception as e:  # noqa: BLE001
        logger.warning("vector store unavailable: %s", e)
        return None


@lru_cache(maxsize=2)
def reranker(model_name: str):
    from flashrank import Ranker
    return Ranker(model_name=model_name, cache_dir=os.path.join(tempfile.gettempdir(), "flashrank"))


def _dense_articles(query: str, k: int) -> list[Candidate]:
    vs = vector_store()
    if vs is None:
        return []
    out = []
    for doc in vs.similarity_search(query, k=k, namespace=settings.articles_namespace):
        md = doc.metadata
        out.append(Candidate(
            text=doc.page_content, article_id=md.get("article_id"), title=md.get("title", ""),
            section=md.get("section", ""), source_name=md.get("source_name", "guardian"),
            language=md.get("language", "en"), published_date=str(md.get("published_date", ""))[:10],
            url=md.get("web_url", ""), method="dense",
        ))
    return out


# lru_cache alone isn't enough here: threads that miss the cache together each build a
# client, and the uncached ones are garbage-collected mid-request, which closes their
# HTTP connection ("client has been closed", seen with parallel embeds 2026-10-20).
_client_lock = threading.Lock()


@lru_cache(maxsize=1)
def _genai_client_unlocked():
    from google import genai
    from google.genai import types
    # gemini-embedding-2 is served from Vertex AI's `global` location only.
    return genai.Client(vertexai=True, project=settings.gcp_project, location="global",
                        http_options=types.HttpOptions(timeout=15_000))


def _genai_client():
    with _client_lock:
        return _genai_client_unlocked()


@lru_cache(maxsize=1)
def _index_v2_unlocked():
    from pinecone import Pinecone
    return Pinecone(api_key=os.environ["PINECONE_API_KEY"]).Index(settings.pinecone_index_v2)


def _index_v2():
    with _client_lock:
        return _index_v2_unlocked()


def embed_query(query: str, dims: int = 512) -> list[float]:
    """The question as gemini-embedding-2 expects it (task in the text, no task_type),
    truncated to the index's dims and re-normalised."""
    import numpy as np
    from google.genai import types
    r = _genai_client().models.embed_content(
        model=settings.query_embedding_model, contents=[f"task: search result | query: {query}"],
        config=types.EmbedContentConfig(output_dimensionality=768))
    v = np.asarray(r.embeddings[0].values[:dims], dtype=np.float64)
    return (v / np.linalg.norm(v)).tolist()


def _pinecone_filter(filters: dict) -> dict | None:
    """Source / language / section / date filters, applied inside Pinecone (v2 metadata)."""
    from datetime import datetime, timedelta, timezone
    f: dict = {}
    for key, field in (("source", "source_name"), ("language", "language"), ("section_group", "section_group")):
        if filters.get(key):
            f[field] = {"$eq": filters[key]}
    if filters.get("start_date") or filters.get("end_date") or filters.get("days"):
        start, end = articles.window(filters.get("start_date"), filters.get("end_date"), filters.get("days"))
        lo = datetime.combine(start, datetime.min.time(), timezone.utc) - timedelta(hours=7)
        hi = datetime.combine(end + timedelta(days=1), datetime.min.time(), timezone.utc) - timedelta(hours=7)
        f["pub_ts"] = {"$gte": int(lo.timestamp()), "$lt": int(hi.timestamp())}
    return f or None


def _dense_articles_v2(query: str, k: int, filters: dict, vec: list[float] | None = None) -> list[Candidate]:
    res = _index_v2().query(vector=vec or embed_query(query), top_k=k, include_metadata=True,
                            namespace=settings.articles_namespace_v2, filter=_pinecone_filter(filters))
    out = []
    for m in res.matches:
        md = m.metadata or {}
        out.append(Candidate(
            text=f"{md.get('title', '')}. {md.get('snippet', '')}", article_id=m.id,
            title=md.get("title", ""), section_group=md.get("section_group", ""),
            source_name=md.get("source_name", ""), language=md.get("language", ""),
            published_date=md.get("pub_date", ""), url=md.get("url", ""), method="dense_v2",
            score=m.score,
        ))
    return out


def _dense_user_docs(query: str, k: int, session_id: str | None) -> list[Candidate]:
    # Check the session BEFORE touching the store: initialising the v1 store is slow
    # (and was pure waste with no session: a 100 s first retrieval, 2026-10-18).
    if not session_id:
        return []
    vs = vector_store()
    if vs is None:
        return []
    out = []
    for doc in vs.similarity_search(query, k=k, namespace=settings.user_docs_namespace,
                                    filter={"session_id": session_id}):
        md = doc.metadata
        out.append(Candidate(text=doc.page_content, title=md.get("file_name", "Uploaded document"),
                             source_type="uploaded_document", method="dense_user",
                             file_name=md.get("file_name", "")))
    return out


def _dense_v2_recent(query: str, k: int, filters: dict, recent_days: int | None) -> list[Candidate]:
    """v2 articles. With `recent_days`, the best RECENT_FIRST hits from that window go
    first, then the best of all dates. A hard window lost 8/100 eval questions whose
    "recent" / "last week" meant the article's own time, not today's (2026-10-20)."""
    vec = embed_query(query)
    everything = _dense_articles_v2(query, k, filters, vec=vec)
    if not recent_days:
        return everything
    recent = _dense_articles_v2(query, k, {**filters, "days": recent_days}, vec=vec)
    if len(recent) < MIN_RECENT:
        return everything
    return _dedupe(recent[:RECENT_FIRST] + everything)[:k]


def _keyword(keywords: list[str], limit: int, filters: dict) -> list[Candidate]:
    rows = articles.keyword_search(keywords, limit=limit, **filters)
    return [Candidate(
        text=f"{r['web_title']}. {r.get('preview') or ''}", article_id=r["article_id"],
        title=r["web_title"], section=r.get("section_name") or "", section_group=r["section_group"],
        source_name=r["source_name"], language=r["language"],
        published_date=str(r["web_publication_date_local"])[:10], url=r["web_url"], method="keyword",
    ) for r in rows]


def _interleave(*lists: list[Candidate]) -> list[Candidate]:
    """a1, b1, a2, b2, ... so truncating the list keeps a fair share of each search."""
    out: list[Candidate] = []
    for i in range(max((len(x) for x in lists), default=0)):
        out.extend(x[i] for x in lists if i < len(x))
    return out


def _dedupe(cands: list[Candidate]) -> list[Candidate]:
    seen_ids, seen_text, out = set(), set(), []
    for c in cands:
        sig = c.text[:100].lower().strip()
        if (c.article_id and c.article_id in seen_ids) or sig in seen_text:
            continue
        out.append(c)
        if c.article_id:
            seen_ids.add(c.article_id)
        seen_text.add(sig)
    return out


def hybrid_retrieve(query: str, *, session_id: str | None = None, demo_mode: bool = False,
                    top_n: int = 5, include_user_docs: bool = True, article_vectors: str | None = None,
                    keyword: bool | None = None, rerank: bool | None = None, **filters) -> RetrievalResult:
    """The retrieval for one question -> top_n candidates.
    Mode "dense" (default): this session's upload chunks (v1 index), then v2 dense articles.
    Mode "hybrid" (and demo mode): also the keyword search (interleaved) and FlashRank.
    `filters` (source, language, section_group, start_date, end_date, days) apply inside
    Pinecone and to the keyword search (v1 post-filters source/language).
    `article_vectors` ("v1" / "v2"), `keyword` and `rerank` override the mode (eval)."""
    t0 = time.time()
    mode = "hybrid" if demo_mode else settings.retrieval_mode
    vectors = article_vectors or settings.article_vectors
    use_keyword = (mode == "hybrid") if keyword is None else keyword
    use_rerank = (mode == "hybrid") if rerank is None else rerank
    keywords = extract_keywords(query) if use_keyword else []
    k_dense, k_user, k_sql = (8, 4, 15) if demo_mode else (10, 5, 10)
    sid = session_id if include_user_docs else None
    # Explicit dates win; otherwise "latest ..." / "tuần này" asks for recent articles.
    has_dates = any(filters.get(k) for k in ("start_date", "end_date", "days"))
    recent = None if has_dates else recency_days(query)
    stats: dict[str, Any] = {"query": query, "keywords": keywords, "demo_mode": demo_mode,
                             "mode": mode, "article_vectors": vectors, "recency_days": recent}

    # Not a `with` block: its exit waits for every thread, so one hung leg (a network call
    # that never returns, seen on v1 Pinecone 2026-10-19) would hang the whole question
    # despite the 20 s timeout. shutdown(wait=False) abandons a hung leg instead.
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=3)
    try:
        jobs = {
            "dense": (ex.submit(_dense_v2_recent, query, k_dense, filters, recent) if vectors == "v2"
                      else ex.submit(_dense_articles, query, k_dense)),
            "user_docs": ex.submit(_dense_user_docs, query, k_user, sid),
            "keyword": ex.submit(_keyword, keywords, k_sql, filters) if keywords else None,
        }
        found: dict[str, list[Candidate]] = {}
        deadline = time.time() + 20
        for name, fut in jobs.items():
            try:
                found[name] = fut.result(timeout=max(0.1, deadline - time.time())) if fut else []
            except Exception as e:  # noqa: BLE001 - one failed leg must not kill retrieval
                logger.warning("retrieval leg %s failed: %r", name, e)
                stats[f"{name}_error"] = repr(e)[:200]
                found[name] = []
    finally:
        ex.shutdown(wait=False, cancel_futures=True)
    for name, cands in found.items():
        stats[name] = len(cands)
    stats["retrieve_ms"] = int((time.time() - t0) * 1000)

    dense = found["dense"]

    for key in ("source", "language"):  # v2 already filtered inside Pinecone
        if filters.get(key) and vectors != "v2":
            attr = "source_name" if key == "source" else "language"
            dense = [c for c in dense if getattr(c, attr) == filters[key]]
    cands = _dedupe(found["user_docs"] + _interleave(dense, found["keyword"]))
    stats["unique"] = len(cands)

    t1 = time.time()
    if use_rerank and len(cands) >= MIN_RERANK:
        from flashrank import RerankRequest
        multilingual = is_vietnamese(query) or any(c.language == "vi" for c in cands)
        model = RERANK_MULTILINGUAL if multilingual else RERANK_EN
        if multilingual:
            # The multilingual model is ~5x slower on CPU (6.2 s for 19 x 500 chars,
            # measured 2026-10-18), so rerank fewer, shorter passages with it.
            cands, chars = cands[:12], 400
        else:
            chars = 500
        try:
            passages = [{"id": i, "text": c.text[:chars]} for i, c in enumerate(cands)]
            ranked = reranker(model).rerank(RerankRequest(query=query, passages=passages))
            top = []
            for r in ranked[:top_n]:
                c = cands[r["id"]]
                c.score = float(r["score"])
                top.append(c)
            cands = top
            stats["rerank_model"] = model
        except Exception as e:  # noqa: BLE001
            logger.warning("rerank failed: %s", e)
            cands = cands[:top_n]
            stats["rerank_error"] = str(e)[:200]
    else:
        cands = cands[:top_n]
        stats["rerank_skipped"] = "too_few" if use_rerank else "off"
    stats["rerank_ms"] = int((time.time() - t1) * 1000)
    stats["returned"] = len(cands)
    stats["total_ms"] = int((time.time() - t0) * 1000)
    return RetrievalResult(cands, stats)


def warm_up() -> None:
    """Open the Vertex and Pinecone clients and embed once (~14 s cold, measured
    2026-10-20), so the first question doesn't hit the 20 s retrieval deadline."""
    try:
        _index_v2().query(vector=embed_query("warm up"), top_k=1, namespace=settings.articles_namespace_v2)
    except Exception as e:  # noqa: BLE001 - the first question just pays the cost instead
        logger.warning("retrieval warm-up failed: %r", e)


def format_context(cands: list[Candidate]) -> str:
    """The retrieved sources as numbered blocks for the system prompt."""
    blocks = []
    for i, c in enumerate(cands, 1):
        if c.source_type == "uploaded_document":
            label = f"📄 Uploaded document: {c.file_name or c.title}"
            meta = ""
        else:
            label = f"📰 {c.source_name or 'unknown source'} ({c.language or '?'}): {c.title}"
            meta = (f"Published: {c.published_date or 'unknown'} | Section: {c.section or c.section_group or 'n/a'}"
                    f" | article_id: {c.article_id} | URL: {c.url or 'n/a'}\n")
        score = f"{c.score:.3f}" if c.score is not None else "n/a"
        blocks.append(f"[Source {i} — {label}]\nRelevance: {score}\n{meta}Content:\n{c.text.strip()}")
    return "\n\n".join(blocks)
