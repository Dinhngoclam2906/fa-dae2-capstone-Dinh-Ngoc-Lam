"""
sync_job_state.py - pull/push the small persistent state files a Cloud Run Job needs
across otherwise-ephemeral invocations: guardian_api_state.json (the crawler's
resume cursor) and .pending_merge.api.json (the loader's pending-merge tracking,
see load_raw_to_bigquery.py). Cloud Run Jobs have no durable local disk between
runs - without this, every invocation would start cold, with guardian_api_backfill.py
falling back to its --from-date default (2024-05-08) and re-crawling years of
history every 4 hours. The bulk Parquet data itself does NOT need this treatment -
each run's crawl output gets uploaded to GCS by load_raw_to_bigquery.py's own
upload_tree() regardless, so only these two small JSON files need to round-trip.

Usage:
    python sync_job_state.py pull   # before running the crawl+load
    python sync_job_state.py push   # after, regardless of crawl/load exit status
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

STATE_FILES = [
    "data/batch/guardian_api_state.json",
    "data/batch/.pending_merge.api.json",
]
GCS_STATE_PREFIX = "job-state/guardian-ingestion"


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in ("pull", "push"):
        print("usage: sync_job_state.py pull|push", file=sys.stderr)
        return 1
    mode = sys.argv[1]

    project = os.environ["GCP_PROJECT"]
    bucket_name = os.environ["GCS_LANDING_BUCKET"]
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)

    for f in STATE_FILES:
        local = Path(f)
        blob = bucket.blob(f"{GCS_STATE_PREFIX}/{local.name}")
        if mode == "pull":
            local.parent.mkdir(parents=True, exist_ok=True)
            if blob.exists():
                blob.download_to_filename(str(local))
                print(f"pulled {f}")
            else:
                print(f"no remote state yet for {f} (first run)")
        else:  # push
            if local.exists():
                blob.upload_from_filename(str(local))
                print(f"pushed {f}")
            else:
                print(f"nothing to push for {f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
