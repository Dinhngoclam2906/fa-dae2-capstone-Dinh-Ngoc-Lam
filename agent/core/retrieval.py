"""Automatic retrieval: v2 dense search (Pinecone `news-articles-v2`, gemini-embedding-2).

Default since 2026-10-20 (`settings.retrieval_mode == "dense"`): one query embedding,
v2 articles ranked by similarity. The 2026-10-19 eval (docs/eval_refresh.md) found v2 dense alone beats every hybrid on every
question group; the FlashRank rerank and the keyword interleave both pushed good hits out
of the top 5. The old hybrid pipeline (dense + keyword + FlashRank, the winner of the
2026-01 English-only eval) stays as mode "hybrid": demo mode and the eval use it. Keyword
search is still an LLM tool (exact names, numbers, articles not yet embedded).
The v1 index (Guardian-only OpenAI chunks) and the session-scoped PDF uploads it held
were removed on 2026-10-24.
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

from core import articles, passages
from core.config import settings
from core.text import extract_keywords, is_vietnamese, query_filters, recency_days

logger = logging.getLogger(__name__)

MIN_RECENT = 3  # fewer hits than this inside a recency window -> ignore the window
RECENT_FIRST = 3  # recent-window hits placed ahead of the all-dates ranking ("latest", "this month")
# Short calendar windows ("today", "this week", "mấy ngày qua") mean today's calendar, so they
# get every top-5 slot: in-window results 0.53 -> 0.64 (eval/time_eval.py, 2026-10-22).
RECENT_FIRST_SHORT = 5
SHORT_WINDOW_DAYS = 7
# Per network call, then one retry. Warm calls take <= 0.65 s (embed p90 0.49, Pinecone 0.59,
# 2026-10-23); ~1% hung until the 20 s leg deadline that day and left the question without
# context. 3 s x 2 attempts x 3 sequential calls (embed + 2 queries) stays under 20 s.
CALL_TIMEOUT_S = 3.0
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
    method: str = ""
    score: float | None = None

    def as_dict(self) -> dict[str, Any]:
        d = dict(self.__dict__)
        d["text"] = self.text[:500]
        return d


@dataclass
class RetrievalResult:
    candidates: list[Candidate] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)


@lru_cache(maxsize=2)
def reranker(model_name: str):
    from flashrank import Ranker
    return Ranker(model_name=model_name, cache_dir=os.path.join(tempfile.gettempdir(), "flashrank"))


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
    from pinecone.openapi_support.retry_urllib3 import JitterRetry
    pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
    # The SDK's default urllib3 retry is total=5 with backoff, so one hung query took 6 x the
    # timeout (a 1 ms timeout measured 16.8 s). One retry, set where Index() reads it: there is
    # no public setting in pinecone 7.3.0 (uv.lock). If a later SDK renames it, keep its default.
    if hasattr(pc, "_openapi_config"):
        pc._openapi_config.retries = JitterRetry(total=1, backoff_factor=0.25,
                                                 status_forcelist=(500, 502, 503, 504), allowed_methods=None)
    else:
        logger.warning("pinecone SDK changed: default retries (up to 6 attempts per query)")
    return pc.Index(settings.pinecone_index_v2)


def _index_v2():
    with _client_lock:
        return _index_v2_unlocked()


def _retry_once(what: str, call):
    """Run `call`; after a timeout or any other failure, run it once more."""
    try:
        return call()
    except Exception as e:  # noqa: BLE001 - mostly timeouts; a real error fails again, fast
        logger.warning("%s failed, retrying once: %r", what, e)
        return call()


def embed_query(query: str, dims: int = 512, timeout: float = CALL_TIMEOUT_S) -> list[float]:
    """The question as gemini-embedding-2 expects it (task in the text, no task_type),
    truncated to the index's dims and re-normalised."""
    import numpy as np
    from google.genai import types
    cfg = types.EmbedContentConfig(output_dimensionality=768,
                                   http_options=types.HttpOptions(timeout=int(timeout * 1000)))
    r = _retry_once("query embedding", lambda: _genai_client().models.embed_content(
        model=settings.query_embedding_model, contents=[f"task: search result | query: {query}"], config=cfg))
    v = np.asarray(r.embeddings[0].values[:dims], dtype=np.float64)
    return (v / np.linalg.norm(v)).tolist()


def _pinecone_filter(filters: dict) -> dict | None:
    """Source / language / section / date filters, applied inside Pinecone (v2 metadata)."""
    from datetime import datetime, timedelta, timezone
    f: dict = {}
    for key, field in (("source", "source_name"), ("language", "language"), ("section_group", "section_group")):
        v = filters.get(key)
        if v:  # a list (parsed from the question, e.g. both VnExpress editions) -> $in
            f[field] = {"$in": list(v)} if isinstance(v, (list, tuple)) else {"$eq": v}
    if filters.get("start_date") or filters.get("end_date") or filters.get("days"):
        start, end = articles.window(filters.get("start_date"), filters.get("end_date"), filters.get("days"))
        lo = datetime.combine(start, datetime.min.time(), timezone.utc) - timedelta(hours=7)
        hi = datetime.combine(end + timedelta(days=1), datetime.min.time(), timezone.utc) - timedelta(hours=7)
        f["pub_ts"] = {"$gte": int(lo.timestamp()), "$lt": int(hi.timestamp())}
    return f or None


def _dense_articles_v2(query: str, k: int, filters: dict, vec: list[float] | None = None) -> list[Candidate]:
    vec = vec or embed_query(query)
    res = _index_v2().query(vector=vec, top_k=k, include_metadata=True, namespace=settings.articles_namespace_v2,
                            filter=_pinecone_filter(filters), _request_timeout=CALL_TIMEOUT_S)  # retried once by urllib3
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


def _dense_v2_recent(query: str, k: int, filters: dict, recent_days: int | None) -> list[Candidate]:
    """v2 articles. With `recent_days`, the best RECENT_FIRST hits from that window go
    first (RECENT_FIRST_SHORT for windows up to SHORT_WINDOW_DAYS), then the best of all
    dates. Not a hard window: one lost 8/100 eval questions whose "recent" meant the
    article's own time, not today's (2026-10-20)."""
    vec = embed_query(query)
    everything = _dense_articles_v2(query, k, filters, vec=vec)
    if not recent_days:
        return everything
    recent = _dense_articles_v2(query, k, {**filters, "days": recent_days}, vec=vec)
    if len(recent) < MIN_RECENT:
        return everything
    first = RECENT_FIRST_SHORT if recent_days <= SHORT_WINDOW_DAYS else RECENT_FIRST
    return _dedupe(recent[:first] + everything)[:k]


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


def _passage_rerank(query: str, dense: list[Candidate], stats: dict) -> list[Candidate]:
    """Fill the slots after the dense top `passage_keep` with the best body passages
    (core/passages.py); on any failure keep the dense order."""
    t = time.time()
    try:
        bodies = articles.get_bodies([c.article_id for c in dense])
        ranked = passages.rescue(query, [(c.title, bodies.get(c.article_id, "")) for c in dense],
                                 settings.passage_keep)
    except Exception as e:  # noqa: BLE001 - the dense ranking is still a good answer
        logger.warning("passage rerank failed: %r", e)
        stats["passage_error"] = repr(e)[:200]
        return dense
    out = []
    for i, passage in ranked:
        c = dense[i]
        chunk = passage[len(c.title) + 2:].strip()
        if chunk and chunk[:200] not in c.text:  # a detail the snippet doesn't show
            c.text = f"{c.text}\n…{chunk}…"
        out.append(c)
    stats["passage_bodies"] = len(bodies)
    stats["passage_ms"] = int((time.time() - t) * 1000)
    return out


def scope_filter(sources: list[str] | None) -> dict:
    """The user's chosen sources as a default source filter ({} = all sources)."""
    return {"source": list(sources)} if sources else {}


def build_filters(query: str, filters: dict, sources: list[str] | None = None,
                  parse: bool = True, today=None) -> tuple[dict, dict, dict]:
    """(dense_filters, keyword_filters, parsed) for one question. Precedence: explicit
    `filters` (a tool call's arguments), then a source / period stated in the question
    (`text.query_filters`, only without explicit filters), then the user's chosen `sources`
    as the default source scope. A source named in the question wins over the scope."""
    filters = {k: v for k, v in filters.items() if v}  # search_news passes every key, mostly None
    parsed = query_filters(query, today or articles._today_local()) if parse and not filters else {}
    dense = {**parsed, **filters}
    if "source" not in dense:
        dense.update(scope_filter(sources))
    return dense, dict(dense), parsed  # the keyword search takes the same filters (incl. a source list)


def hybrid_retrieve(query: str, *, demo_mode: bool = False, top_n: int = 5,
                    sources: list[str] | None = None,
                    keyword: bool | None = None, rerank: bool | None = None,
                    parse_filters: bool = True, passage_rerank: bool | None = None,
                    **filters) -> RetrievalResult:
    """The retrieval for one question -> top_n candidates.
    Mode "dense" (default): v2 dense articles.
    Mode "hybrid" (and demo mode): also the keyword search (interleaved) and FlashRank.
    `filters` (source, language, section_group, start_date, end_date, days) apply inside
    Pinecone and to the keyword search. Without explicit
    filters, a source or month / year stated in the question (`text.query_filters`) becomes
    a hard filter on the v2 dense search, dropped again if it finds nothing.
    Mode "dense" then fills the slots after the dense top `settings.passage_keep` from the
    top `settings.passage_pool` articles by their best body passage (core/passages.py), and
    adds each article's best passage to its text.
    `sources` (the user's chosen sources) scope every search unless the question or an
    explicit filter names a source (`build_filters`).
    `keyword`, `rerank`, `parse_filters` and
    `passage_rerank` override the defaults (eval)."""
    t0 = time.time()
    mode = "hybrid" if demo_mode else settings.retrieval_mode
    use_keyword = (mode == "hybrid") if keyword is None else keyword
    use_rerank = (mode == "hybrid") if rerank is None else rerank
    keywords = extract_keywords(query) if use_keyword else []
    k_dense, k_sql = (8, 15) if demo_mode else (10, 10)
    use_passages = (mode == "dense" and settings.passage_pool > 0
                    if passage_rerank is None else passage_rerank)
    if use_passages:
        k_dense = max(k_dense, settings.passage_pool)
    filters = {k: v for k, v in filters.items() if v}
    dense_filters, kw_filters, parsed = build_filters(query, filters, sources, parse_filters)
    # Explicit dates win; otherwise "latest ..." / "tuần này" asks for recent articles.
    has_dates = any(dense_filters.get(k) for k in ("start_date", "end_date", "days"))
    recent = None if has_dates else recency_days(query)
    stats: dict[str, Any] = {"query": query, "keywords": keywords, "demo_mode": demo_mode,
                             "mode": mode, "recency_days": recent,
                             "parsed_filters": parsed, "scope": list(sources or []),
                             "scope_overridden": bool(sources) and dense_filters.get("source") != list(sources)}

    # Not a `with` block: its exit waits for every thread, so one hung leg (a network call
    # that never returns, seen on Pinecone 2026-10-19) would hang the whole question
    # despite the 20 s timeout. shutdown(wait=False) abandons a hung leg instead.
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    try:
        jobs = {
            "dense": ex.submit(_dense_v2_recent, query, k_dense, dense_filters, recent),
            "keyword": ex.submit(_keyword, keywords, k_sql, kw_filters) if keywords else None,
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
    if parsed and not found["dense"] and "dense_error" not in stats:
        # Nothing matches the stated source / period: search without it, so the LLM still
        # has context and can say there is no exact match.
        fallback = {**filters, **({} if "source" in filters else scope_filter(sources))}
        found["dense"] = _dense_v2_recent(query, k_dense, fallback, recency_days(query))
        stats["parsed_filters_dropped"] = True
    for name, cands in found.items():
        stats[name] = len(cands)
    stats["retrieve_ms"] = int((time.time() - t0) * 1000)

    dense = found["dense"]
    if use_passages and dense:
        dense = _passage_rerank(query, dense, stats)
    cands = _dedupe(_interleave(dense, found["keyword"]))
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
        _index_v2().query(vector=embed_query("warm up", timeout=30), top_k=1,
                          namespace=settings.articles_namespace_v2, _request_timeout=30)
    except Exception as e:  # noqa: BLE001 - the first question just pays the cost instead
        logger.warning("retrieval warm-up failed: %r", e)


def format_context(cands: list[Candidate]) -> str:
    """The retrieved sources as numbered blocks for the system prompt."""
    blocks = []
    for i, c in enumerate(cands, 1):
        label = f"📰 {c.source_name or 'unknown source'} ({c.language or '?'}): {c.title}"
        meta = (f"Published: {c.published_date or 'unknown'} | Section: {c.section or c.section_group or 'n/a'}"
                f" | article_id: {c.article_id} | URL: {c.url or 'n/a'}\n")
        score = f"{c.score:.3f}" if c.score is not None else "n/a"
        blocks.append(f"[Source {i} — {label}]\nRelevance: {score}\n{meta}Content:\n{c.text.strip()}")
    return "\n\n".join(blocks)
