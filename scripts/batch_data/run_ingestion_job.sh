#!/bin/sh
# Entrypoint for the guardian-ingestion Cloud Run Job.
#
# State is ALWAYS pushed back to GCS at the end, even if the crawl or load step
# failed - a failed load must still persist the crawler's advanced cursor (so next
# run doesn't re-crawl a window it already fetched) and the loader's own
# pending-merge list (so next run retries exactly the unmerged files, same as a
# local failure already behaves - see load_raw_to_bigquery.py). The job's own exit
# code still reflects failure either way, so Cloud Run's execution history and any
# configured retries/alerts work normally.
set -u

python scripts/batch_data/sync_job_state.py pull

rc=0
python scripts/batch_data/guardian_api_backfill.py --max-calls 60 || rc=1
python scripts/batch_data/load_raw_to_bigquery.py --source api --max-bytes-billed 6000000000 || rc=1

python scripts/batch_data/sync_job_state.py push

exit "$rc"
