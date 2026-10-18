"""
embed_articles.py - article-level embeddings v2 (agent phase, step 2b).

Embeds every article in marts.obt_articles with gemini-embedding-2 (768 dims) into
BigQuery `vectors.article_embeddings`, the source of truth for vectors. Replaces the old
Snowflake -> Pinecone chunk pipeline (data_ingestion_to_pinecone.py).

    uv run --env-file .env python ai_agents/embed_articles.py status
    uv run --env-file .env python ai_agents/embed_articles.py embed --limit 50   # smoke test
    uv run --env-file .env python ai_agents/embed_articles.py embed --full       # backfill
    uv run --env-file .env python ai_agents/embed_articles.py embed              # daily: last 40 days

Incremental by design: an article is pending when (article_id, content_hash) has no row
for this model, so new articles, new sources and edited articles are picked up by the
next run, and a crashed or interrupted run simply continues. Months are processed newest
first; each month reads only its own partition. Rows are saved every --flush rows with a
BigQuery load job (free), so a crash loses at most one batch of embedding cost.

Text: "title: {title} | text: {first 1,500 chars of the body}" - gemini-embedding-2's
documented document format (it rejects task_type; queries use
"task: search result | query: {question}", see core/retrieval.py). Bake-off and model
choice: docs/embedding_bakeoff.md. The model is served from Vertex AI's `global`
location only, one text per request, so throughput comes from --workers parallel
requests (16 measured best: ~36 docs/s; more stalled on connection limits).
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import random
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone

import numpy as np
import polars as pl
from google.cloud import bigquery

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # for core.*
from core import bq  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("embed_articles")
for noisy in ("httpx", "google_genai.models", "google_genai._api_client"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

MODEL = "gemini-embedding-2"
LOCATION = "global"
DIMS = 768
LEAD_CHARS = 1500
SNIPPET_CHARS = 240  # stored for the vector index's metadata (context without a BigQuery lookup)
DATASET = "vectors"
TABLE = "article_embeddings"

SCHEMA = [
    bigquery.SchemaField("article_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("content_hash", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_name", "STRING"),
    bigquery.SchemaField("language", "STRING"),
    bigquery.SchemaField("section_group", "STRING"),
    bigquery.SchemaField("web_publication_date", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("web_title", "STRING"),
    bigquery.SchemaField("web_url", "STRING"),
    bigquery.SchemaField("snippet", "STRING"),
    bigquery.SchemaField("model", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("dims", "INT64"),
    bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
    bigquery.SchemaField("input_tokens", "INT64"),
    bigquery.SchemaField("embedded_at", "TIMESTAMP", mode="REQUIRED"),
]


def _project() -> str:
    return os.environ["GCP_PROJECT"]


def table_ref() -> str:
    return f"{_project()}.{DATASET}.{TABLE}"


def ensure_table():
    """Partitioned like obt_articles (publication month), so a month's pending query prunes
    both sides; clustered on embedded_at so the vector-index sync reads only new rows."""
    c = bq.client()
    ds = bigquery.Dataset(f"{_project()}.{DATASET}")
    ds.location = os.getenv("BQ_LOCATION", "asia-southeast1")
    ds.description = "Article embeddings (source of truth for the vector index). Written by ai_agents/embed_articles.py."
    c.create_dataset(ds, exists_ok=True)
    t = bigquery.Table(table_ref(), schema=SCHEMA)
    t.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.MONTH,
                                                    field="web_publication_date")
    t.clustering_fields = ["embedded_at", "article_id"]
    t.description = (f"One row per (article_id, content_hash, model). {MODEL} @{DIMS} of "
                     "'title: … | text: <first 1,500 chars>'. Latest row per article_id wins.")
    c.create_table(t, exists_ok=True)


def _run(sql: str, params: list | None = None, max_bytes: int = 2_000_000_000):
    cfg = bigquery.QueryJobConfig(query_parameters=params or [], maximum_bytes_billed=max_bytes,
                                  labels={"app": "embed-articles"})
    job = bq.client().query(sql, job_config=cfg)
    rows = list(job.result())
    bq.usage["bytes_billed"] += job.total_bytes_billed or 0
    return rows


def _window_sql(full: bool, days: int) -> str:
    return "TRUE" if full else (
        f"o.web_publication_date >= TIMESTAMP(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {int(days)} DAY), MONTH))")


def pending_months(full: bool, days: int) -> list[tuple[datetime, int]]:
    sql = f"""
    SELECT TIMESTAMP_TRUNC(o.web_publication_date, MONTH) AS m, COUNT(*) AS n
    FROM `{_project()}.marts.obt_articles` o
    LEFT JOIN (SELECT DISTINCT article_id, content_hash FROM `{table_ref()}` WHERE model = @model) e
      USING (article_id, content_hash)
    WHERE e.article_id IS NULL AND {_window_sql(full, days)}
    GROUP BY m ORDER BY m DESC
    """
    return [(r["m"], r["n"]) for r in _run(sql, [bq.p("model", MODEL)])]


def pending_rows(month: datetime, limit: int | None) -> list[dict]:
    sql = f"""
    SELECT o.article_id, o.content_hash, o.source_name, o.language, o.section_group,
           o.web_publication_date, o.web_title, o.web_url, LEFT(o.body_text, {LEAD_CHARS}) AS lead
    FROM `{_project()}.marts.obt_articles` o
    LEFT JOIN (SELECT DISTINCT article_id, content_hash FROM `{table_ref()}`
               WHERE model = @model AND web_publication_date >= @m AND web_publication_date < @m_next) e
      USING (article_id, content_hash)
    WHERE e.article_id IS NULL AND o.web_publication_date >= @m AND o.web_publication_date < @m_next
    {"LIMIT @limit" if limit else ""}
    """
    m_next = datetime(month.year + (month.month // 12), month.month % 12 + 1, 1, tzinfo=timezone.utc)
    params = [bq.p("model", MODEL), bq.p("m", month, "TIMESTAMP"), bq.p("m_next", m_next, "TIMESTAMP")]
    if limit:
        params.append(bq.p("limit", limit, "INT64"))
    return [dict(r.items()) for r in _run(sql, params)]


_CLIENT = None


def _genai():
    global _CLIENT
    if _CLIENT is None:
        from google import genai
        from google.genai import types
        _CLIENT = genai.Client(vertexai=True, project=_project(), location=LOCATION,
                               http_options=types.HttpOptions(timeout=30_000))
    return _CLIENT


def doc_text(row: dict) -> str:
    return f"title: {row['web_title']} | text: {row['lead'] or ''}"


def embed_one(text: str) -> tuple[list[float], int]:
    from google.genai import types
    cfg = types.EmbedContentConfig(output_dimensionality=DIMS)
    for attempt in range(6):
        try:
            r = _genai().models.embed_content(model=MODEL, contents=[text], config=cfg)
            e = r.embeddings[0]
            return e.values, int(getattr(e.statistics, "token_count", 0) or 0)
        except Exception as ex:  # noqa: BLE001 - timeouts / 429 / transient 5xx
            if attempt == 5:
                raise
            time.sleep(min(30, 2 ** attempt) + random.random())
            logger.debug("retry %s: %s", attempt + 1, type(ex).__name__)


def load_rows(rows: list[dict]):
    if not rows:
        return
    df = pl.DataFrame(rows, schema={
        "article_id": pl.Utf8, "content_hash": pl.Utf8, "source_name": pl.Utf8, "language": pl.Utf8,
        "section_group": pl.Utf8, "web_publication_date": pl.Datetime("us", "UTC"), "web_title": pl.Utf8,
        "web_url": pl.Utf8, "snippet": pl.Utf8, "model": pl.Utf8, "dims": pl.Int64,
        "embedding": pl.List(pl.Float64), "input_tokens": pl.Int64, "embedded_at": pl.Datetime("us", "UTC"),
    })
    # Polars writes lists as Arrow large_list, which BigQuery's Parquet list inference reads
    # as EMPTY arrays (found in the smoke test); write a standard LIST via pyarrow instead.
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = df.to_arrow()
    table = table.set_column(table.schema.get_field_index("embedding"), "embedding",
                             table.column("embedding").cast(pa.list_(pa.float64())))
    buf = io.BytesIO()
    pq.write_table(table, buf, use_compliant_nested_type=True)
    buf.seek(0)
    cfg = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET, schema=SCHEMA,
                                 write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    # Build the options first, then assign: `cfg.parquet_options` returns a copy, so setting
    # the flag on it afterwards is silently lost (arrays then load as EMPTY).
    po = bigquery.ParquetOptions()
    po.enable_list_inference = True
    cfg.parquet_options = po
    bq.client().load_table_from_file(buf, table_ref(), job_config=cfg).result()


def cmd_embed(args):
    ensure_table()
    months = pending_months(args.full, args.window_days)
    total = sum(n for _, n in months)
    logger.info("pending: %d articles in %d months (%s)", total, len(months),
                "full corpus" if args.full else f"last ~{args.window_days} days")
    if args.limit:
        months = months[:1]
    done = tokens = failed = 0
    started = time.time()
    batch: list[dict] = []
    for month, n in months:
        rows = pending_rows(month, args.limit)
        logger.info("%s: %d pending", month.strftime("%Y-%m"), len(rows))
        ex = ThreadPoolExecutor(args.workers)
        futs = {ex.submit(embed_one, doc_text(r)): r for r in rows}
        try:
            # Stall watchdog: on 2026-10-18 a flaky network left all 16 requests hung (no CPU,
            # no progress for 26 min) despite the per-request timeout. If nothing completes
            # for --stall-seconds, save what we have and exit non-zero; a rerun resumes.
            # (Not as_completed(timeout=...): that bounds the WHOLE loop, so it cut every
            # month off 300 s after it started - the first Cloud Run attempt.)
            pending = set(futs)
            completed: list = []
            while pending or completed:
                if not completed:
                    finished, pending = wait(pending, timeout=args.stall_seconds, return_when=FIRST_COMPLETED)
                    if not finished:
                        raise TimeoutError
                    completed = list(finished)
                fut = completed.pop()
                r = futs.pop(fut)  # pop: don't keep every finished vector of the month in memory
                try:
                    vec, ntok = fut.result()
                except Exception as e:  # noqa: BLE001 - left pending; the next run retries it
                    failed += 1
                    logger.warning("failed %s: %s", r["article_id"], str(e)[:150])
                    continue
                v = np.asarray(vec, dtype=np.float64)
                v /= np.linalg.norm(v)
                batch.append({
                    "article_id": r["article_id"], "content_hash": r["content_hash"],
                    "source_name": r["source_name"], "language": r["language"],
                    "section_group": r["section_group"], "web_publication_date": r["web_publication_date"],
                    "web_title": r["web_title"], "web_url": r["web_url"],
                    "snippet": (r["lead"] or "")[:SNIPPET_CHARS], "model": MODEL, "dims": DIMS,
                    "embedding": v.tolist(), "input_tokens": ntok,
                    "embedded_at": datetime.now(timezone.utc),
                })
                tokens += ntok
                if len(batch) >= args.flush:
                    load_rows(batch)
                    done += len(batch)
                    batch = []
                    rate = done / (time.time() - started)
                    logger.info("saved %d/%d (%.0f/s, ETA %.0f min) | %.1fM tokens ≈ $%.2f | failed %d",
                                done, total, rate, (total - done) / max(rate, 1e-9) / 60,
                                tokens / 1e6, tokens / 1e6 * args.price_per_m, failed)
                if tokens > args.max_tokens:
                    load_rows(batch)
                    done += len(batch)
                    logger.error("token budget %.0fM reached - stopping (rerun to continue)", args.max_tokens / 1e6)
                    ex.shutdown(wait=False, cancel_futures=True)
                    return 2
        except TimeoutError:
            load_rows(batch)
            logger.error("STALLED: nothing completed for %ds; saved %d rows, exiting (rerun to resume)",
                         args.stall_seconds, done + len(batch))
            logging.shutdown()
            os._exit(3)  # hung worker threads would block a normal exit
        ex.shutdown(wait=True)
    load_rows(batch)
    done += len(batch)
    logger.info("DONE: %d embedded, %d failed (left pending), %.1fM tokens ≈ $%.2f, BigQuery %.2f GB billed, %.0f min",
                done, failed, tokens / 1e6, tokens / 1e6 * args.price_per_m, bq.usage["bytes_billed"] / 1e9,
                (time.time() - started) / 60)
    return 0


def cmd_status(_):
    ensure_table()
    sql = f"""
    SELECT model, COUNT(*) AS n_rows, COUNT(DISTINCT article_id) AS articles,
           SUM(input_tokens) AS tokens, MIN(embedded_at) AS first, MAX(embedded_at) AS last
    FROM `{table_ref()}` GROUP BY model
    """
    for r in _run(sql, max_bytes=200_000_000):
        print(dict(r.items()))
    months = pending_months(True, 0)
    print(f"pending (full corpus): {sum(n for _, n in months)} articles in {len(months)} months")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("embed")
    e.add_argument("--full", action="store_true", help="whole corpus (default: publication months of the last --window-days)")
    e.add_argument("--window-days", type=int, default=40)
    e.add_argument("--limit", type=int, default=None, help="smoke test: at most N articles from the newest month")
    e.add_argument("--workers", type=int, default=16)
    e.add_argument("--flush", type=int, default=2000)
    e.add_argument("--stall-seconds", type=int, default=300, help="exit (resumably) if nothing completes this long")
    e.add_argument("--max-tokens", type=float, default=200e6, help="stop once this many tokens are used (cost guard)")
    e.add_argument("--price-per-m", type=float, default=0.20, help="USD per 1M tokens, for progress estimates")
    sub.add_parser("status")
    args = ap.parse_args()
    return {"embed": cmd_embed, "status": cmd_status}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
