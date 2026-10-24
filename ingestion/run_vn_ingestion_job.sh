#!/bin/sh
# Entrypoint for the vn-ingestion Cloud Run Job (same image as guardian-ingestion).
#
# Runs the Tuoi Tre / Thanh Nien / VnEconomy crawlers, plus the English editions of
# Tuoi Tre (news.tuoitre.vn) and VnEconomy (en.vneconomy.vn), and VietNamNet (vi + the
# forward-only English edition) and VnExpress (RSS-only, vi + en), in parallel (independent
# per-source state, lock, .seen and output - see vn_news_backfill.py), then one
# BigQuery load for all three. Like run_ingestion_job.sh, state is ALWAYS pushed
# back to GCS, even after a failure, and the exit code still reflects it.
#
# --concurrency 6 / --sleep 0.75 is the low-error setting the 2026-10 backfill
# settled on; a steady-state 4-hourly run is only ~100 new articles per source.
set -u

python ingestion/sync_job_state.py pull vn-ingestion

rc=0
pids=""
for s in tuoitre thanhnien vneconomy tuoitre_en vneconomy_en vietnamnet vietnamnet_en vnexpress vnexpress_en; do
  # VnExpress (RSS-only, by policy) gets a gentler rate: 2 workers, 1.5 s apart.
  case "$s" in
    vnexpress*) rate="--concurrency 2 --sleep 1.5" ;;
    *)          rate="--concurrency 6 --sleep 0.75" ;;
  esac
  python ingestion/vn_news_backfill.py --source "$s" --from-date 2026-01-01 $rate &
  pids="$pids $!"
done
for p in $pids; do
  wait "$p" || rc=1
done

python ingestion/load_raw_to_bigquery.py --source vn --max-bytes-billed 6000000000 || rc=1
# Bronze: raw HTML -> gs://$GCS_RAWHTML_BUCKET/raw/ (see raw_archive.py).
python ingestion/raw_archive.py upload || rc=1

python ingestion/sync_job_state.py push vn-ingestion

exit "$rc"
