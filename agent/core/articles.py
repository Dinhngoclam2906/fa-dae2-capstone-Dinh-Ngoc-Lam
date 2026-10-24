"""Data access over marts.obt_articles (one row per article, all 10 sources).

Plain functions, shared by the agent's tools and the hybrid retriever. Cost rules
(BigQuery bills scanned bytes per column per partition; dry-run figures, 2026-10-18):

  * Every query filters on `web_publication_date` (the partition column), so it reads
    only the months in its window. Keyword search defaults to the last
    `settings.default_window_days` days: title + preview ~82 MB, full body ~0.5 GB
    (vs ~2.5 GB for a full-archive body scan).
  * Article lookups take the publication date, so they read one or two months instead
    of the whole table (~84 MB vs ~1.2 GB).
  * Dates are Vietnam local dates (the `*_local` columns); storage stays UTC.
"""

from __future__ import annotations

import hashlib
import unicodedata
from datetime import date, datetime, timedelta, timezone

from google.cloud import bigquery

from core import bq
from core.config import LANGUAGES, SECTION_GROUPS, SOURCES, settings

COLUMNS = (
    "article_id, web_title, section_name, section_group, source_name, language, "
    "web_publication_date, web_publication_date_local, web_url, preview"
)
MAX_LIMIT = 30


def _parse_day(value: str | None, name: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value.strip()[:10])
    except ValueError as e:
        raise ValueError(f"{name} must be YYYY-MM-DD, got {value!r}") from e


def _today_local() -> date:
    return (datetime.now(timezone.utc) + timedelta(hours=7)).date()


def window(start_date: str | None = None, end_date: str | None = None,
           days: int | None = None) -> tuple[date, date]:
    """Inclusive local-date window. Defaults to the last `default_window_days` days."""
    end = _parse_day(end_date, "end_date") or _today_local()
    start = _parse_day(start_date, "start_date")
    if start is None:
        start = end - timedelta(days=(days or settings.default_window_days) - 1)
    if start > end:
        raise ValueError("start_date is after end_date")
    return start, end


def _window_params(start: date, end: date) -> list:
    # UTC partition bounds padded by a day on each side (local = UTC+7), then the exact
    # local-date filter. The padding keeps pruning correct at month boundaries.
    lo = datetime.combine(start - timedelta(days=1), datetime.min.time(), timezone.utc)
    hi = datetime.combine(end + timedelta(days=2), datetime.min.time(), timezone.utc)
    return [
        bq.p("lo", lo, "TIMESTAMP"), bq.p("hi", hi, "TIMESTAMP"),
        bq.p("start_day", start, "DATE"), bq.p("end_day", end, "DATE"),
    ]


WINDOW_SQL = (
    "web_publication_date >= @lo AND web_publication_date < @hi "
    "AND DATE(web_publication_date_local) BETWEEN @start_day AND @end_day"
)


def source_list(source: str | list[str] | tuple[str, ...] | None) -> list[str]:
    """One source, several (the user's chosen sources) or none (= all) as a validated list."""
    sources = [source] if isinstance(source, str) else list(source or [])
    for s in sources:
        if s not in SOURCES:
            raise ValueError(f"unknown source {s!r}; valid: {', '.join(SOURCES)}")
    return sources


def _filters(source: str | list[str] | None, language: str | None,
             section_group: str | None) -> tuple[str, list]:
    sources = source_list(source)
    if language and language not in LANGUAGES:
        raise ValueError(f"language must be one of {LANGUAGES}")
    if section_group and section_group not in SECTION_GROUPS:
        raise ValueError(f"unknown section_group {section_group!r}; valid: {', '.join(SECTION_GROUPS)}")
    sql = (" AND (IFNULL(ARRAY_LENGTH(@sources), 0) = 0 OR source_name IN UNNEST(@sources))"  # an empty array param arrives as NULL
           " AND (@language IS NULL OR language = @language)"
           " AND (@section_group IS NULL OR section_group = @section_group)")
    return sql, [bq.p_array("sources", sources), bq.p("language", language),
                 bq.p("section_group", section_group)]


def _limit(limit: int | None, default: int = 15) -> int:
    return max(1, min(int(limit or default), MAX_LIMIT))


def keyword_search(keywords: list[str], *, start_date: str | None = None,
                   end_date: str | None = None, days: int | None = None,
                   source: str | list[str] | None = None, language: str | None = None,
                   section_group: str | None = None, full_text: bool = False,
                   limit: int = 15) -> list[dict]:
    """Articles matching the keywords, ranked by coverage (title hit = 2, text hit = 1).
    With 2+ keywords an article must score >= 2. Searches title + preview unless
    full_text=True (5-6x the bytes)."""
    # NFC on both sides: some sources store Vietnamese with combining diacritics.
    kws = [unicodedata.normalize("NFC", k.strip().lower()) for k in keywords if k and len(k.strip()) >= 2][:6]
    if not kws:
        return []
    start, end = window(start_date, end_date, days)
    filt_sql, filt_params = _filters(source, language, section_group)
    text_col = "body_text" if full_text else "preview"  # fixed choice, never user text
    sql = f"""
    SELECT * FROM (
      SELECT {COLUMNS},
        (SELECT SUM(CASE WHEN STRPOS(NORMALIZE(LOWER(web_title), NFC), kw) > 0 THEN 2
                         WHEN STRPOS(NORMALIZE(LOWER({text_col}), NFC), kw) > 0 THEN 1 ELSE 0 END)
         FROM UNNEST(@keywords) AS kw) AS relevance
      FROM {settings.articles_table}
      WHERE {WINDOW_SQL}{filt_sql}
    )
    WHERE relevance >= @min_relevance
    ORDER BY relevance DESC, web_publication_date DESC
    LIMIT @limit
    """
    params = _window_params(start, end) + filt_params + [
        bq.p_array("keywords", kws),
        bq.p("min_relevance", min(2, len(kws)), "INT64"),
        bq.p("limit", _limit(limit), "INT64"),
    ]
    return bq.query(sql, params)


def browse(*, start_date: str | None = None, end_date: str | None = None,
           days: int | None = 7, source: str | list[str] | None = None, language: str | None = None,
           section_group: str | None = None, limit: int = 15) -> list[dict]:
    """Most recent articles in a window, optionally by source / language / section group."""
    start, end = window(start_date, end_date, days)
    filt_sql, filt_params = _filters(source, language, section_group)
    sql = f"""
    SELECT {COLUMNS} FROM {settings.articles_table}
    WHERE {WINDOW_SQL}{filt_sql}
    ORDER BY web_publication_date DESC
    LIMIT @limit
    """
    return bq.query(sql, _window_params(start, end) + filt_params + [bq.p("limit", _limit(limit), "INT64")])


def get_article(article_id: str, published_date: str, source: str | None = None) -> dict | None:
    """One article with its full body. `published_date` (YYYY-MM-DD, from a search
    result) lets BigQuery read one month instead of the whole table."""
    day = _parse_day(published_date, "published_date")
    if day is None:
        raise ValueError("published_date is required (copy it from the search result)")
    sql = f"""
    SELECT {COLUMNS}, body_text, word_count FROM {settings.articles_table}
    WHERE web_publication_date >= @lo AND web_publication_date < @hi
      AND article_id = @article_id
      AND (@source IS NULL OR source_name = @source)
    LIMIT 1
    """
    lo = datetime.combine(day - timedelta(days=2), datetime.min.time(), timezone.utc)
    hi = datetime.combine(day + timedelta(days=2), datetime.min.time(), timezone.utc)
    rows = bq.query(sql, [bq.p("lo", lo, "TIMESTAMP"), bq.p("hi", hi, "TIMESTAMP"),
                          bq.p("article_id", article_id.strip()), bq.p("source", source)])
    return rows[0] if rows else None


# Full bodies by id, for passage reranking. obt_articles is partitioned by month, so 30
# scattered ids read ~1.3 GB; clustering on article_id doesn't prune scattered ids
# (2-2.5 GB). `vectors.article_bodies` is integer-range partitioned on a hash of the id
# instead: 30 ids touch at most 30 small partitions, ~25 MB (measured 2026-10-21).
# The two expressions below must agree (BigQuery MD5 hashes the UTF-8 bytes).
BODY_BUCKETS = 3000
BODY_BUCKET_SQL = "MOD(CAST(CONCAT('0x', SUBSTR(TO_HEX(MD5({col})), 1, 8)) AS INT64), %d)" % BODY_BUCKETS


def body_bucket(article_id: str) -> int:
    return int(hashlib.md5(article_id.encode("utf-8")).hexdigest()[:8], 16) % BODY_BUCKETS


def get_bodies(article_ids: list[str], timeout: float = 10) -> dict[str, str]:
    """article_id -> full body (latest saved version). Missing ids are left out."""
    ids = sorted({i for i in article_ids if i})
    if not ids:
        return {}
    sql = f"""
    SELECT article_id, body_text FROM {settings.bodies_table}
    WHERE bucket IN UNNEST(@buckets) AND article_id IN UNNEST(@ids)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY saved_at DESC) = 1
    """
    buckets = bigquery.ArrayQueryParameter("buckets", "INT64", sorted({body_bucket(i) for i in ids}))
    rows = bq.query(sql, [buckets, bq.p_array("ids", ids)], timeout=timeout)
    return {r["article_id"]: r["body_text"] or "" for r in rows}


def news_summary(day: str | None = None, *, language: str | None = None,
                 source: str | list[str] | None = None, per_group: int = 3) -> list[dict]:
    """Latest `per_group` stories per section group on one local day (default: today)."""
    d = _parse_day(day, "date") or _today_local()
    filt_sql, filt_params = _filters(source, language, None)
    sql = f"""
    SELECT * EXCEPT(rn) FROM (
      SELECT section_group, web_title, web_url, source_name, language, web_publication_date_local,
             ROW_NUMBER() OVER (PARTITION BY section_group ORDER BY web_publication_date DESC) AS rn
      FROM {settings.articles_table}
      WHERE {WINDOW_SQL}{filt_sql}
    )
    WHERE rn <= @per_group
    ORDER BY section_group, rn
    """
    return bq.query(sql, _window_params(d, d) + filt_params + [bq.p("per_group", per_group, "INT64")])


def section_counts(days: int = 7, *, language: str | None = None,
                   source: str | list[str] | None = None) -> list[dict]:
    """Article counts per section group (and top raw sections) over the last N days."""
    start, end = window(None, None, days)
    filt_sql, filt_params = _filters(source, language, None)
    sql = f"""
    SELECT section_group, COUNT(*) AS articles,
           ARRAY_AGG(DISTINCT section_name IGNORE NULLS LIMIT 5) AS example_sections,
           MAX(web_publication_date_local) AS latest
    FROM {settings.articles_table}
    WHERE {WINDOW_SQL}{filt_sql}
    GROUP BY section_group
    ORDER BY articles DESC
    """
    return bq.query(sql, _window_params(start, end) + filt_params)


def freshness() -> list[dict]:
    """Per source: newest article, articles today and in the last 7 days (local dates)."""
    today = _today_local()
    sql = f"""
    SELECT source_name, language,
           MAX(web_publication_date_local) AS newest_article,
           COUNTIF(DATE(web_publication_date_local) = @today) AS articles_today,
           COUNTIF(DATE(web_publication_date_local) > DATE_SUB(@today, INTERVAL 7 DAY)) AS articles_last_7_days
    FROM {settings.articles_table}
    WHERE web_publication_date >= TIMESTAMP(DATE_SUB(@today, INTERVAL 40 DAY))
    GROUP BY source_name, language
    ORDER BY source_name
    """
    return bq.query(sql, [bq.p("today", today, "DATE")])


def coverage() -> list[dict]:
    """Per source: total articles and the date range covered."""
    sql = f"""
    SELECT source_name, language, COUNT(*) AS articles,
           DATE(MIN(web_publication_date_local)) AS earliest,
           DATE(MAX(web_publication_date_local)) AS latest
    FROM {settings.articles_table}
    GROUP BY source_name, language
    ORDER BY articles DESC
    """
    rows = bq.query(sql)
    for r in rows:
        r["site"] = SOURCES.get(r["source_name"], (r["source_name"], ""))[0]
    return rows
