"""
raw_archive.py

Bronze capture: keep every fetched payload exactly as received, so a parser fix is a
re-parse of the archive instead of a re-crawl of the site (2026-10-18: fixing the
vietnamnet section parser meant re-fetching 2,708 pages from the web).

The crawlers call `write_records()` next to each Parquet flush. It writes one gzipped
JSON Lines file per flush, laid out Hive-style:

    data/batch/raw_archive/source=<source>/fetch_date=YYYY-MM-DD/<tag>.jsonl.gz

One line per article:

    {"source_name", "article_id", "url", "fetched_at" (UTC ISO-8601),
     "payload_format" ("html" | "json"), "payload_sha256", "payload", ...extra}

`upload` copies those files to gs://$GCS_RAWHTML_BUCKET/raw/<same path> (Coldline)
and deletes each local file once its blob is confirmed with the same size:

    uv run python scripts/batch_data/raw_archive.py upload

Append-only by design: nothing is ever overwritten, so repeat fetches (the Guardian
crawler re-polls its last 2 days every run) are kept as version history. Only pages
that parsed into an article are archived. Pages that fail to parse are re-fetched on
every run, so archiving them would store the same page over and over.

Environment:
    GCP_PROJECT          GCP project for the storage client
    GCS_RAWHTML_BUCKET   archive bucket (no default; the upload is skipped if unset)
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_DIR = Path("data/batch/raw_archive")
GCS_PREFIX = "raw"


def record(source_name: str, article_id: str, url: str, payload: str,
           payload_format: str, fetched_at: datetime | None = None, **extra) -> dict:
    """One archive line. `payload` is the response body as received (decoded text)."""
    fetched_at = fetched_at or datetime.now(timezone.utc)
    return {
        "source_name": source_name,
        "article_id": article_id,
        "url": url,
        "fetched_at": fetched_at.astimezone(timezone.utc).isoformat(),
        "payload_format": payload_format,
        "payload_sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        "payload": payload,
        **extra,
    }


def write_records(records: list[dict], source_name: str, tag: str,
                  base_dir: Path | None = DEFAULT_DIR) -> Path | None:
    """Write one .jsonl.gz for these records; returns its path. `base_dir=None`
    disables the archive (the crawlers' --raw-archive-dir '' maps to that)."""
    if not records or base_dir is None:
        return None
    day = datetime.now(timezone.utc).date().isoformat()
    d = base_dir / f"source={source_name}" / f"fetch_date={day}"
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{tag}.jsonl.gz"
    tmp = path.with_suffix(".gz.tmp")
    with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=6) as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
    tmp.replace(path)  # never leave a half-written file for the uploader to pick up
    return path


def upload(base_dir: Path = DEFAULT_DIR) -> int:
    """Upload every finished archive file, deleting each local copy once the blob
    exists with the same size. Returns the number of files uploaded."""
    bucket_name = os.environ.get("GCS_RAWHTML_BUCKET", "")
    files = sorted(base_dir.rglob("*.jsonl.gz")) if base_dir.exists() else []
    if not files:
        logger.info("raw archive: nothing to upload under %s", base_dir)
        return 0
    if not bucket_name:
        logger.warning("raw archive: GCS_RAWHTML_BUCKET unset - keeping %d local files", len(files))
        return 0

    from google.cloud import storage  # only the upload step needs it

    client = storage.Client(project=os.environ.get("GCP_PROJECT") or None)
    bucket = client.bucket(bucket_name)
    n = skipped = total = 0
    for f in files:
        name = f"{GCS_PREFIX}/{f.relative_to(base_dir).as_posix()}"
        blob = bucket.blob(name)
        # if_generation_match=0: create only, so an archived object is never overwritten.
        try:
            blob.upload_from_filename(str(f), content_type="application/gzip", if_generation_match=0)
            n += 1
            total += f.stat().st_size
        except Exception as e:  # noqa: BLE001
            if getattr(e, "code", None) != 412:  # 412 = already uploaded by an earlier run
                raise
            skipped += 1
        blob.reload()
        if blob.size != f.stat().st_size:
            raise RuntimeError(f"size mismatch for gs://{bucket_name}/{name}")
        f.unlink()
    logger.info("raw archive: %d files (%.1f MB) uploaded, %d already archived -> gs://%s/%s/",
                n, total / 1_048_576, skipped, bucket_name, GCS_PREFIX)
    return n


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["upload"])
    ap.add_argument("--dir", type=Path, default=DEFAULT_DIR)
    args = ap.parse_args()
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    upload(args.dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
