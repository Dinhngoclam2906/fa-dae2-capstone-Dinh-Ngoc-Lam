"""
load_raw_to_bigquery.py

Phase 1 / Phase 2 loader: partitioned Parquet -> GCS landing bucket -> BigQuery
`raw.raw_data`.

Replaces `batch_data_ingestion.py` (Snowflake) for the BigQuery migration. Auth is
Application Default Credentials (`gcloud auth application-default login`) - there is no
service-account key (org policy blocks key creation; see docs/phase0_baseline.md).

Pipeline:
    1. upload  data/batch/<source>_parquet/**.parquet
               -> gs://<bucket>/<prefix>/<source>/pub_year=/pub_month=/part-*.parquet
    2. load    that GCS tree -> raw._stage_<source>   (PARQUET, WRITE_TRUNCATE)
    3. MERGE   raw._stage_<source> -> raw.raw_data    (on article_id, adds loaded_at)
    4. report  row counts / date range / per-year / storage bytes

`raw.raw_data` is created on first run: MONTH-partitioned on `web_publication_date`,
clustered on (section_name, data_source), no required partition filter (dbt staging
full-scans it).

Config (env / .env), with defaults:
    GCP_PROJECT           (required)
    BQ_LOCATION           asia-southeast1
    BQ_DATASET_RAW        raw
    BQ_RAW_TABLE          raw_data
    GCS_LANDING_BUCKET    (required)
    GCS_LANDING_PREFIX    guardian

Usage:
    uv run python scripts/batch_data/load_raw_to_bigquery.py                 # hf, full run
    uv run python scripts/batch_data/load_raw_to_bigquery.py --truncate      # clean reload
    uv run python scripts/batch_data/load_raw_to_bigquery.py --source api    # crawler output
    uv run python scripts/batch_data/load_raw_to_bigquery.py --skip-upload --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery, storage

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

RAW_DATA_SCHEMA = [
    bigquery.SchemaField("article_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("crawl_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("web_publication_date", "TIMESTAMP"),
    bigquery.SchemaField("web_title", "STRING"),
    bigquery.SchemaField("body_text", "STRING"),
    bigquery.SchemaField("web_url", "STRING"),
    bigquery.SchemaField("section_name", "STRING"),
    bigquery.SchemaField("data_source", "STRING"),
    bigquery.SchemaField("source_name", "STRING"),
    bigquery.SchemaField("language", "STRING"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]

# Columns the MERGE writes, in order (everything except loaded_at, which it sets).
STAGE_COLUMNS = [f.name for f in RAW_DATA_SCHEMA if f.name != "loaded_at"]

# The 8 columns every source's Parquet physically carries.
CORE_COLUMNS = [
    "article_id",
    "crawl_timestamp",
    "web_publication_date",
    "web_title",
    "body_text",
    "web_url",
    "section_name",
    "data_source",
]

# source_name / language: present in the Parquet for "vn"; injected as literals for
# the Guardian sources, whose Parquet predates these two columns.
SOURCE_META = {
    "hf": {"source_name": "guardian", "language": "en"},
    "api": {"source_name": "guardian", "language": "en"},
    "vn": None,  # carried per-row in the crawler output
}

# Columns added to an existing raw_data table by ensure_columns (metadata-only).
NEW_COLUMNS = ["source_name", "language"]

SOURCE_DEFAULT_DIR = {
    "hf": "data/batch/hf_guardian_parquet",
    "api": "data/batch/guardian_api_parquet",
    "vn": "data/batch/vn_news_parquet",  # crawler writes <dir>/<source>/pub_year=/...
}


def parquet_columns(source: str) -> list[str]:
    """Columns actually in this source's Parquet files."""
    if SOURCE_META.get(source):  # Guardian: 8 core columns only
        return CORE_COLUMNS
    return CORE_COLUMNS + NEW_COLUMNS  # vn: core + source_name + language


class Config:
    def __init__(self) -> None:
        self.project = os.environ.get("GCP_PROJECT", "")
        self.location = os.environ.get("BQ_LOCATION", "asia-southeast1")
        self.dataset = os.environ.get("BQ_DATASET_RAW", "raw")
        self.table = os.environ.get("BQ_RAW_TABLE", "raw_data")
        self.bucket = os.environ.get("GCS_LANDING_BUCKET", "")
        self.prefix = os.environ.get("GCS_LANDING_PREFIX", "guardian").strip("/")
        missing = [
            k
            for k, v in {"GCP_PROJECT": self.project, "GCS_LANDING_BUCKET": self.bucket}.items()
            if not v
        ]
        if missing:
            raise SystemExit(f"Missing required env var(s): {', '.join(missing)}")

    @property
    def table_fqn(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table}"

    def stage_fqn(self, source: str) -> str:
        return f"{self.project}.{self.dataset}._stage_{source}"


# --------------------------------------------------------------------------- upload


def upload_tree(cfg: Config, local_dir: Path, source: str) -> tuple[str, list[str]]:
    """Upload local *.parquet to gs://bucket/prefix/source/... Skips blobs whose
    size already matches. Returns (gs:// wildcard URI, blob names uploaded this run) -
    the wildcard is kept for --skip-upload/--truncate's full-reload path; the blob
    list is what lets the caller stage+merge only what's new instead of everything
    ever uploaded for this source (see pending-merge tracking below)."""
    client = storage.Client(project=cfg.project)
    bucket = client.bucket(cfg.bucket)
    base_prefix = f"{cfg.prefix}/{source}"

    files = sorted(local_dir.rglob("*.parquet"))
    if not files:
        raise SystemExit(f"No .parquet files under {local_dir}")

    existing = {b.name: b.size for b in client.list_blobs(bucket, prefix=base_prefix + "/")}
    uploaded = skipped = uploaded_bytes = 0
    new_blobs: list[str] = []
    for f in files:
        rel = f.relative_to(local_dir).as_posix()
        blob_name = f"{base_prefix}/{rel}"
        size = f.stat().st_size
        if existing.get(blob_name) == size:
            skipped += 1
            continue
        bucket.blob(blob_name).upload_from_filename(str(f), content_type="application/octet-stream")
        uploaded += 1
        uploaded_bytes += size
        new_blobs.append(blob_name)

    logger.info(
        "GCS upload: %d uploaded (%.1f MB), %d already current -> gs://%s/%s/",
        uploaded,
        uploaded_bytes / 1_048_576,
        skipped,
        cfg.bucket,
        base_prefix,
    )
    return f"gs://{cfg.bucket}/{base_prefix}/*.parquet", new_blobs


# -------------------------------------------------------------------- pending merge


def pending_path(source: str) -> Path:
    """Blobs uploaded to GCS but not yet confirmed MERGEd into raw.raw_data. Entries
    persist across runs - a merge failure (e.g. the --max-bytes-billed errors hit
    repeatedly during development) must not drop them, so the next run retries
    exactly those files instead of either silently losing them or re-staging the
    source's entire multi-month Parquet history (which is what made every MERGE scan
    - and bill for - every previously-matched row, not just new ones)."""
    return Path(f"data/batch/.pending_merge.{source}.json")


def load_pending(source: str) -> set[str]:
    p = pending_path(source)
    return set(json.loads(p.read_text())) if p.exists() else set()


def save_pending(source: str, blobs: set[str]) -> None:
    p = pending_path(source)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(sorted(blobs)))


# ------------------------------------------------------------------------ bigquery


def ensure_table(bq: bigquery.Client, cfg: Config) -> None:
    try:
        bq.get_table(cfg.table_fqn)
        return
    except Exception:
        pass
    table = bigquery.Table(cfg.table_fqn, schema=RAW_DATA_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH, field="web_publication_date"
    )
    table.clustering_fields = ["section_name", "data_source"]
    bq.create_table(table)
    logger.info(
        "Created %s (MONTH partition on web_publication_date, cluster on section_name,data_source)",
        cfg.table_fqn,
    )


def ensure_columns(bq: bigquery.Client, cfg: Config) -> None:
    """Idempotently add the multi-source columns to an existing raw_data table.
    ensure_table only *creates*; a table made before the schema grew needs this.
    ALTER TABLE ADD COLUMN is a metadata op — no bytes scanned or billed."""
    clauses = ", ".join(f"ADD COLUMN IF NOT EXISTS {c} STRING" for c in NEW_COLUMNS)
    bq.query(f"ALTER TABLE `{cfg.table_fqn}` {clauses}").result()
    logger.info("Ensured columns present on %s: %s", cfg.table_fqn, ", ".join(NEW_COLUMNS))


def load_stage(bq: bigquery.Client, cfg: Config, source: str, source_uris: str | list[str]) -> int:
    stage = cfg.stage_fqn(source)
    cols = parquet_columns(source)
    job_cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[f for f in RAW_DATA_SCHEMA if f.name in cols],
    )
    logger.info(
        "Loading %s -> %s (%d cols)",
        source_uris if isinstance(source_uris, str) else f"{len(source_uris)} file(s)",
        stage, len(cols),
    )
    job = bq.load_table_from_uri(source_uris, stage, job_config=job_cfg)
    job.result()
    n = bq.get_table(stage).num_rows or 0
    logger.info("Staged %s rows into %s", f"{n:,}", stage)
    return n


def merge_stage(bq: bigquery.Client, cfg: Config, source: str, max_bytes: int) -> int:
    stage = cfg.stage_fqn(source)
    meta = SOURCE_META.get(source)
    if meta:
        # Guardian Parquet has no source_name / language — supply them as literals.
        source_select = ", ".join(CORE_COLUMNS) + (
            f", '{meta['source_name']}' AS source_name, '{meta['language']}' AS language"
        )
    else:
        source_select = ", ".join(CORE_COLUMNS + NEW_COLUMNS)

    set_clause = ",\n        ".join(
        f"{c} = S.{c}" for c in STAGE_COLUMNS if c != "article_id"
    )
    insert_cols = ", ".join(STAGE_COLUMNS + ["loaded_at"])
    insert_vals = ", ".join(f"S.{c}" for c in STAGE_COLUMNS) + ", S.loaded_at"
    # Dedupe the source on article_id — BigQuery MERGE errors if >1 source row
    # matches a target row. The crawler can emit dupes across resumed runs.
    sql = f"""
    MERGE `{cfg.table_fqn}` T
    USING (
        SELECT {source_select}, CURRENT_TIMESTAMP() AS loaded_at
        FROM `{stage}`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY article_id ORDER BY crawl_timestamp DESC
        ) = 1
    ) S
    ON T.article_id = S.article_id
    WHEN MATCHED THEN UPDATE SET
        {set_clause},
        loaded_at = S.loaded_at
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    job = bq.query(
        sql, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=max_bytes)
    )
    job.result()
    affected = job.num_dml_affected_rows or 0
    logger.info(
        "MERGE %s -> %s: %s rows affected, %.1f MB billed",
        stage,
        cfg.table_fqn,
        f"{affected:,}",
        (job.total_bytes_billed or 0) / 1_048_576,
    )
    return affected


def truncate_table(bq: bigquery.Client, cfg: Config, max_bytes: int) -> None:
    bq.query(
        f"TRUNCATE TABLE `{cfg.table_fqn}`",
        job_config=bigquery.QueryJobConfig(maximum_bytes_billed=max_bytes),
    ).result()
    logger.info("Truncated %s", cfg.table_fqn)


def report(bq: bigquery.Client, cfg: Config, max_bytes: int) -> None:
    qcfg = bigquery.QueryJobConfig(maximum_bytes_billed=max_bytes)
    logger.info("=" * 64)
    logger.info("raw.raw_data REPORT")
    logger.info("=" * 64)

    row = list(
        bq.query(
            f"""
        SELECT COUNT(*) n, COUNT(DISTINCT article_id) n_ids,
               MIN(web_publication_date) dmin, MAX(web_publication_date) dmax
        FROM `{cfg.table_fqn}`
        """,
            job_config=qcfg,
        ).result()
    )[0]
    logger.info("rows: %s   distinct article_id: %s", f"{row.n:,}", f"{row.n_ids:,}")
    logger.info("web_publication_date: %s -> %s", row.dmin, row.dmax)

    for r in bq.query(
        f"SELECT data_source, COUNT(*) n FROM `{cfg.table_fqn}` GROUP BY 1 ORDER BY 1",
        job_config=qcfg,
    ).result():
        logger.info("  data_source=%-9s %s", r.data_source, f"{r.n:,}")

    for r in bq.query(
        f"""
        SELECT COALESCE(source_name, '(null)') src, COALESCE(language, '?') lang, COUNT(*) n
        FROM `{cfg.table_fqn}` GROUP BY 1, 2 ORDER BY n DESC
        """,
        job_config=qcfg,
    ).result():
        logger.info("  source_name=%-10s lang=%-3s %s", r.src, r.lang, f"{r.n:,}")

    logger.info("rows per publication year:")
    for r in bq.query(
        f"""
        SELECT EXTRACT(YEAR FROM web_publication_date) yr, COUNT(*) n
        FROM `{cfg.table_fqn}` GROUP BY 1 ORDER BY 1
        """,
        job_config=qcfg,
    ).result():
        logger.info("  %d  %s", r.yr, f"{r.n:,}")

    t = bq.get_table(cfg.table_fqn)
    logger.info(
        "  storage: %s rows | %.1f MB logical (uncompressed)",
        f"{t.num_rows:,}",
        (t.num_bytes or 0) / 1_048_576,
    )
    logger.info("=" * 64)


# ----------------------------------------------------------------------------- cli


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source", choices=["hf", "api", "vn"], default="hf")
    ap.add_argument("--local-dir", type=Path, default=None)
    ap.add_argument(
        "--skip-upload", action="store_true",
        help="load everything currently in GCS as-is (full reload, bypasses pending-merge tracking)",
    )
    ap.add_argument(
        "--truncate", action="store_true",
        help="empty raw_data before the MERGE (full reload - the whole GCS history must come back, not just what's pending)",
    )
    ap.add_argument("--dry-run", action="store_true", help="upload + stage, no MERGE")
    ap.add_argument("--max-bytes-billed", type=int, default=2_000_000_000)
    args = ap.parse_args()

    cfg = Config()
    local_dir = args.local_dir or Path(SOURCE_DEFAULT_DIR[args.source])
    logger.info(
        "source=%s  project=%s  location=%s  target=%s",
        args.source,
        cfg.project,
        cfg.location,
        cfg.table_fqn,
    )

    source_uris: str | list[str]
    if args.skip_upload:
        gcs_uri = f"gs://{cfg.bucket}/{cfg.prefix}/{args.source}/*.parquet"
        logger.info("Skipping upload; loading %s", gcs_uri)
        source_uris = gcs_uri
    else:
        if not local_dir.is_dir():
            # Not an error: the crawler only creates this directory when it actually
            # has new rows to write. On an ephemeral filesystem (e.g. a Cloud Run Job,
            # which starts from a clean container every invocation) a fully-caught-up
            # run legitimately produces zero output and the directory never gets
            # created at all - this is the normal "nothing new" case, same as the
            # empty-pending-set check below, not a misconfiguration.
            logger.info("Local dir not found (nothing new crawled this run): %s", local_dir)
            return 0
        gcs_uri, new_blobs = upload_tree(cfg, local_dir, args.source)
        if args.truncate:
            # Target is about to be emptied - the full history must come back, not
            # just what's pending.
            source_uris = gcs_uri
        else:
            pending = load_pending(args.source) | set(new_blobs)
            if not pending:
                logger.info("Nothing new to merge for source=%s", args.source)
                return 0
            save_pending(args.source, pending)
            logger.info("%d file(s) pending merge for source=%s", len(pending), args.source)
            source_uris = [f"gs://{cfg.bucket}/{b}" for b in sorted(pending)]

    bq = bigquery.Client(project=cfg.project, location=cfg.location)
    ensure_table(bq, cfg)
    ensure_columns(bq, cfg)
    load_stage(bq, cfg, args.source, source_uris)

    if args.dry_run:
        logger.info("--dry-run: staged only, skipping MERGE (pending list, if any, left untouched)")
        return 0

    if args.truncate:
        truncate_table(bq, cfg, args.max_bytes_billed)
    merge_stage(bq, cfg, args.source, args.max_bytes_billed)
    # Merge succeeded (merge_stage raises/exits nonzero otherwise) - whatever was
    # staged (the pending set, or everything on a full reload) is now safely in
    # raw.raw_data, so nothing is left pending.
    save_pending(args.source, set())
    report(bq, cfg, args.max_bytes_billed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
