"""
sync_vector_index.py - copy article embeddings from BigQuery into the Pinecone index.

BigQuery `vectors.article_embeddings` (768 dims) is the source of truth; Pinecone is a
serving copy holding the first 512 dims, re-normalised (gemini-embedding-2 is trained
for truncation; the bake-off scored 512 ≈ 768). Rebuildable at any time with no
embedding cost.

    uv run --env-file .env python ai_agents/sync_vector_index.py status
    uv run --env-file .env python ai_agents/sync_vector_index.py sync          # new rows only

Per vector: id = article_id; metadata = source_name, language, section_group, pub_date
(YYYY-MM-DD), pub_ts (unix seconds, for range filters), title, url and a 240-char
snippet. The text fields let the agent rerank and cite dense hits without a per-question
BigQuery lookup (~0.1-0.3 GB each); they add ~0.3 KB per vector.

Pinecone Starter limits (pinecone.io, 2026-10-18): 2 GB storage, 2M write units/month,
1 WU per KB upserted. A full load of ~485k x ~2.7 KB ≈ 1.3M WU fits ONCE per month
(overwrites cost the old record's size too), so the sync is incremental (`embedded_at`
watermark in `vectors.sync_state`) and resumable (per-month checkpoint file), and it
refuses to start if the estimate exceeds --max-write-units.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # for core.*
from core import bq  # noqa: E402
from core.config import settings  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("sync_vector_index")

DIMS = 512
MODEL = "gemini-embedding-2"
CHECKPOINT = Path("data/vectors/sync_checkpoint.json")


def _p() -> str:
    return os.environ["GCP_PROJECT"]


def _index(create: bool = False):
    from pinecone import Pinecone, ServerlessSpec
    pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
    name = settings.pinecone_index_v2
    if create and name not in [i.name for i in pc.list_indexes()]:
        logger.info("creating index %s (%d dims, cosine, serverless aws us-east-1)", name, DIMS)
        pc.create_index(name=name, dimension=DIMS, metric="cosine",
                        spec=ServerlessSpec(cloud="aws", region="us-east-1"))
        while not pc.describe_index(name).status["ready"]:
            time.sleep(2)
    return pc.Index(name)


def _q(sql: str, params: list | None = None, max_bytes: int = 1_000_000_000):
    from google.cloud import bigquery
    cfg = bigquery.QueryJobConfig(query_parameters=params or [], maximum_bytes_billed=max_bytes,
                                  labels={"app": "sync-vector-index"})
    job = bq.client().query(sql, job_config=cfg)
    rows = list(job.result())
    bq.usage["bytes_billed"] += job.total_bytes_billed or 0
    return rows


def _q_stream(sql: str, params: list, page_size: int = 1000, max_bytes: int = 1_000_000_000):
    """Yield rows page by page: a whole month of 768-float rows as Python objects can
    exceed the Cloud Run job's 2 GiB (it OOM-killed the first full sync)."""
    from google.cloud import bigquery
    cfg = bigquery.QueryJobConfig(query_parameters=params, maximum_bytes_billed=max_bytes,
                                  labels={"app": "sync-vector-index"})
    job = bq.client().query(sql, job_config=cfg)
    yield from job.result(page_size=page_size)
    bq.usage["bytes_billed"] += job.total_bytes_billed or 0


def _upsert(index, chunk: list[dict]) -> None:
    for attempt in range(5):
        try:
            index.upsert(vectors=chunk, namespace=settings.articles_namespace_v2)
            return
        except Exception as e:  # noqa: BLE001
            if attempt == 4:
                raise
            logger.warning("upsert retry %d: %s", attempt + 1, str(e)[:150])
            time.sleep(2 ** attempt)


def _state_table() -> str:
    return f"{_p()}.vectors.sync_state"


def watermark() -> datetime | None:
    _q(f"CREATE TABLE IF NOT EXISTS `{_state_table()}` (index_name STRING, synced_through TIMESTAMP, updated_at TIMESTAMP)")
    rows = _q(f"SELECT MAX(synced_through) AS w FROM `{_state_table()}` WHERE index_name = @i",
              [bq.p("i", settings.pinecone_index_v2)])
    return rows[0]["w"] if rows else None


def to_vector(row) -> dict:
    v = np.asarray(row["embedding"][:DIMS], dtype=np.float64)
    v /= np.linalg.norm(v)
    pub: datetime = row["web_publication_date"]
    md = {
        "source_name": row["source_name"] or "", "language": row["language"] or "",
        "section_group": row["section_group"] or "Other", "pub_date": pub.strftime("%Y-%m-%d"),
        "pub_ts": int(pub.timestamp()), "title": (row["web_title"] or "")[:200],
        "url": row["web_url"] or "", "snippet": row["snippet"] or "",
    }
    return {"id": row["article_id"], "values": v.astype(np.float32).tolist(), "metadata": md}


def _record_kb(vec: dict) -> float:
    return (len(vec["id"].encode()) + DIMS * 4 + len(json.dumps(vec["metadata"], ensure_ascii=False).encode())) / 1024


def cmd_sync(args):
    index = _index(create=True)
    since = watermark()
    cutoff = datetime.now(timezone.utc)
    cp = json.loads(CHECKPOINT.read_text()) if CHECKPOINT.exists() else {}
    if cp.get("index") == settings.pinecone_index_v2 and cp.get("since") == (since.isoformat() if since else None):
        cutoff = datetime.fromisoformat(cp["cutoff"])
        logger.info("resuming the sync that started %s (%d months already done)", cp["cutoff"], len(cp["months"]))
    else:
        cp = {"index": settings.pinecone_index_v2, "since": since.isoformat() if since else None,
              "cutoff": cutoff.isoformat(), "months": []}
    where = "model = @model AND embedded_at <= @cutoff" + (" AND embedded_at > @since" if since else "")
    # A re-embedded article keeps its older versions in the table, possibly in another month
    # (re-dated, incident 2026-10-20). Skip them: months go newest first and the Pinecone id is
    # the article_id, so a full sync would otherwise upsert the stale vector over the current one.
    where += (f" AND NOT EXISTS (SELECT 1 FROM `{_p()}.vectors.article_embeddings` n"
              " WHERE n.article_id = v.article_id AND n.model = v.model AND n.embedded_at > v.embedded_at)")
    params = [bq.p("model", MODEL), bq.p("cutoff", cutoff, "TIMESTAMP")]
    if since:
        params.append(bq.p("since", since, "TIMESTAMP"))
    months = _q(f"""SELECT TIMESTAMP_TRUNC(web_publication_date, MONTH) AS m, COUNT(*) AS n
                    FROM `{_p()}.vectors.article_embeddings` v WHERE {where} GROUP BY m ORDER BY m DESC""", params)
    todo = [(r["m"], r["n"]) for r in months if r["m"].isoformat() not in cp["months"]]
    if args.before:  # resume a run whose checkpoint was lost (newer months already upserted)
        b = datetime.strptime(args.before, "%Y-%m").replace(tzinfo=timezone.utc)
        todo = [(m, n) for m, n in todo if m < b]
    n_total = sum(n for _, n in todo)
    est_wu = n_total * 2.8  # ~2.7 KB per record measured; the "at least 1 WU per KB" rule
    logger.info("to sync: %d vectors in %d months (since %s), ≈ %.2fM write units", n_total, len(todo), since, est_wu / 1e6)
    if est_wu > args.max_write_units:
        logger.error("estimate exceeds --max-write-units %.1fM; refusing (Pinecone Starter has 2M/month)",
                     args.max_write_units / 1e6)
        return 2
    CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
    done, kb = 0, 0.0
    for m, n in todo:
        m_next = datetime(m.year + (m.month // 12), m.month % 12 + 1, 1, tzinfo=timezone.utc)
        rows = _q_stream(f"""SELECT article_id, source_name, language, section_group, web_publication_date,
                             web_title, web_url, snippet, embedding
                      FROM `{_p()}.vectors.article_embeddings` v
                      WHERE {where} AND web_publication_date >= @m AND web_publication_date < @m_next
                      ORDER BY embedded_at""",
                  params + [bq.p("m", m, "TIMESTAMP"), bq.p("m_next", m_next, "TIMESTAMP")])
        chunk, n_month = [], 0
        for r in rows:
            chunk.append(to_vector(r))
            if len(chunk) == args.batch:
                _upsert(index, chunk)
                kb += sum(_record_kb(v) for v in chunk)
                n_month += len(chunk)
                chunk = []
        if chunk:
            _upsert(index, chunk)
            kb += sum(_record_kb(v) for v in chunk)
            n_month += len(chunk)
        done += n_month
        cp["months"].append(m.isoformat())
        CHECKPOINT.write_text(json.dumps(cp))
        logger.info("%s: %d upserted (%d/%d, ≈ %.2fM WU so far)", m.strftime("%Y-%m"), n_month, done, n_total, kb / 1e6)
    _q(f"INSERT INTO `{_state_table()}` VALUES (@i, @w, CURRENT_TIMESTAMP())",
       [bq.p("i", settings.pinecone_index_v2), bq.p("w", cutoff, "TIMESTAMP")])
    CHECKPOINT.unlink(missing_ok=True)
    logger.info("DONE: %d vectors, ≈ %.0f MB upserted, watermark -> %s, BigQuery %.2f GB billed",
                done, kb / 1024, cutoff.isoformat(), bq.usage["bytes_billed"] / 1e9)
    return 0


def cmd_status(_):
    from pinecone import Pinecone
    pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
    for ix in pc.list_indexes():
        s = pc.Index(ix.name).describe_index_stats()
        print(f"{ix.name}: dim={ix.dimension} vectors={s.total_vector_count} "
              f"namespaces={ {k: v.vector_count for k, v in s.namespaces.items()} }")
    print("watermark:", watermark())


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sync")
    s.add_argument("--batch", type=int, default=100)
    s.add_argument("--max-write-units", type=float, default=1.6e6)
    s.add_argument("--before", metavar="YYYY-MM",
                   help="only months before this one; resumes a sync that died after upserting the newer months")
    sub.add_parser("status")
    args = ap.parse_args()
    return {"sync": cmd_sync, "status": cmd_status}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
