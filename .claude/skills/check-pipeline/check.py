"""Pipeline health by rows, not exit codes (used by the check-pipeline skill).

    uv run --env-file .env python .claude/skills/check-pipeline/check.py [--runs 6]

Prints: the last workflow runs, each job's last execution, raw rows loaded per source
in the last 24 h (+ newest loaded_at), OBT rows without a vector, and Pinecone vs OBT.
Read-only; every BigQuery query is byte-capped (~0.1 GB in total).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess

from google.cloud import bigquery

REGION = "asia-southeast1"
JOBS = ["guardian-ingestion", "vn-ingestion", "dbt-build", "embed-sync"]
CAP = 300_000_000


def gcloud(*args: str) -> list[dict]:
    exe = shutil.which("gcloud") or shutil.which("gcloud.cmd")
    out = subprocess.run([exe, *args, "--format=json"], capture_output=True, text=True, check=True)
    return json.loads(out.stdout or "[]")


def q(c: bigquery.Client, sql: str) -> list[dict]:
    job = c.query(sql, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=CAP))
    return [dict(r.items()) for r in job.result()]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=6)
    args = ap.parse_args()
    project = os.environ["GCP_PROJECT"]

    print("== Workflow news-pipeline (newest first)")
    for e in gcloud("workflows", "executions", "list", "news-pipeline", f"--location={REGION}",
                    f"--project={project}", f"--limit={args.runs}"):
        print(f"  {e['startTime'][:16]}  {e['state']}")

    print("== Last execution per job")
    for j in JOBS:
        ex = gcloud("run", "jobs", "executions", "list", f"--job={j}", f"--region={REGION}",
                    f"--project={project}", "--limit=1")
        if not ex:
            print(f"  {j}: no executions")
            continue
        st = ex[0].get("status", {})
        ok = "OK" if st.get("succeededCount") else ("FAILED" if st.get("failedCount") else "running?")
        print(f"  {j:<19} {ex[0]['metadata']['name']:<28} {st.get('startTime', '')[:16]}  {ok}")

    c = bigquery.Client(project=project, location=REGION)
    print("== raw.raw_data: rows loaded in the last 24 h per source (0 = look closer)")
    for r in q(c, f"""
        SELECT source_name, COUNTIF(loaded_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) AS last_24h,
               MAX(loaded_at) AS newest
        FROM `{project}.raw.raw_data` GROUP BY source_name ORDER BY source_name"""):
        print(f"  {r['source_name']:<14} {r['last_24h']:>6}  newest {str(r['newest'])[:16]}")

    obt = q(c, f"SELECT COUNT(*) AS n FROM `{project}.marts.obt_articles`")[0]["n"]
    missing = q(c, f"""
        SELECT COUNT(*) AS n FROM `{project}.marts.obt_articles` o
        LEFT JOIN (SELECT DISTINCT article_id, content_hash FROM `{project}.vectors.article_embeddings`) e
          USING (article_id, content_hash)
        WHERE e.article_id IS NULL""")[0]["n"]
    print(f"== OBT rows: {obt:,}   without a current vector: {missing:,}")

    if os.getenv("PINECONE_API_KEY"):
        from pinecone import Pinecone
        index = os.getenv("PINECONE_INDEX_V2", "news-articles-v2")
        n = Pinecone(api_key=os.environ["PINECONE_API_KEY"]).Index(index).describe_index_stats().total_vector_count
        print(f"== Pinecone {index}: {n:,} vectors ({n - obt:+,} vs OBT)")
    else:
        print("== Pinecone: PINECONE_API_KEY not set, skipped")


if __name__ == "__main__":
    main()
