#!/usr/bin/env bash
#
# Run the Tuoi Tre / Thanh Nien / VnEconomy crawlers in PARALLEL.
#
# Each source has its own state file, lock, .seen sidecar and Parquet subdir
# (data/batch/vn_news_parquet/<source>/), so the three processes are fully
# independent - no shared cursor, no coordination.
#
# Usage:
#   scripts/batch_data/run_vn_backfill_all.sh [FROM_DATE] [CONCURRENCY] [SLEEP] [MAX_ARTICLES] [TO_DATE]
#
#   FROM_DATE      first month to crawl              (default 2024-01-01)
#   CONCURRENCY    parallel fetches per source       (default 8)
#   SLEEP          per-worker delay, seconds         (default 0.5)
#   MAX_ARTICLES   per-source cap, 0=unlimited       (default 0 - run to completion)
#   TO_DATE        last month to crawl (inclusive)   (default: today - i.e. no cap)
#
# Batching by date range: run a year at a time, verify + load each batch into
# BigQuery, then continue with the next range (each source's cursor/seen state
# persists, so a later run with a later FROM_DATE just continues on):
#   scripts/batch_data/run_vn_backfill_all.sh 2024-01-01 8 0.5 0 2024-12-31   # batch 1: 2024
#   scripts/batch_data/run_vn_backfill_all.sh 2025-01-01 8 0.5 0 2025-12-31   # batch 2: 2025
#   scripts/batch_data/run_vn_backfill_all.sh 2026-01-01 8 0.5 0             # batch 3: 2026 -> today
#
# Measured throughput is ~5-6 articles/s PER SOURCE and does NOT improve past
# CONCURRENCY 8 - the sites throttle per-IP, so higher concurrency just adds
# connection resets and gets slower. 8 is the knee; leave it. The three sources
# run against three different hosts, so aggregate is ~16 art/s.
#
#   ~520k articles (2024-01 -> now) / ~16 art/s  ~=  9 hours, unattended.
#   Run it on the asia-southeast1 (Singapore) VM - closer to VN, fewer timeouts,
#   and it doesn't tie up your laptop.
#
# Examples:
#   scripts/batch_data/run_vn_backfill_all.sh                       # full 2024-01 -> today (~9h)
#   scripts/batch_data/run_vn_backfill_all.sh 2020-01-01            # deeper backfill
#   scripts/batch_data/run_vn_backfill_all.sh 2024-01-01 8 0.5 500  # short test slice
#
# Re-running is safe: each crawler resumes from its own cursor and the .seen
# sidecar skips already-fetched articles. Kill with Ctrl-C (state is saved at the
# last completed month) or `kill $(cat data/batch/vn_news_state.*.json.lock ...)`.
#
# When done (or in between), load what has landed so far:
#   uv run python scripts/batch_data/load_raw_to_bigquery.py --source vn --max-bytes-billed 5000000000

set -u

FROM="${1:-2024-01-01}"
CONCURRENCY="${2:-8}"
SLEEP="${3:-0.5}"
MAX="${4:-0}"
TO="${5:-}"
SOURCES=(tuoitre thanhnien vneconomy tuoitre_en vneconomy_en vietnamnet vietnamnet_en vnexpress vnexpress_en)

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

LOGDIR="data/batch/vn_crawl_logs"
mkdir -p "$LOGDIR"

echo "repo:   $REPO_ROOT"
echo "from:   $FROM   to: ${TO:-today}   concurrency: $CONCURRENCY   sleep: ${SLEEP}s   (~$(awk "BEGIN{printf \"%.0f\", $CONCURRENCY/$SLEEP}") req/s/source)   max/source: $MAX (0=unlimited)"
echo "logs:   $LOGDIR/<source>.log"
echo

TO_ARGS=()
if [ -n "$TO" ]; then
  TO_ARGS=(--to-date "$TO")
fi

pids=()
for s in "${SOURCES[@]}"; do
  echo "  launching $s"
  uv run python scripts/batch_data/vn_news_backfill.py \
      --source "$s" --from-date "$FROM" "${TO_ARGS[@]}" \
      --concurrency "$CONCURRENCY" --sleep "$SLEEP" --max-articles "$MAX" \
      >"$LOGDIR/$s.log" 2>&1 &
  pids+=("$!")
done

echo
echo "pids: ${pids[*]}"
echo "follow: tail -f $LOGDIR/*.log"
echo

trap 'echo; echo "signal received - stopping crawlers"; kill "${pids[@]}" 2>/dev/null' INT TERM

rc=0
for p in "${pids[@]}"; do
  wait "$p" || rc=1
done

echo
if [ "$rc" -eq 0 ]; then
  echo "all three finished cleanly"
else
  echo "at least one crawler exited non-zero - check $LOGDIR/*.log (re-run to resume)"
fi

echo
echo "landed so far:"
find data/batch/vn_news_parquet -name '*.parquet' 2>/dev/null \
  | awk -F/ '{print $4}' | sort | uniq -c | sed 's/^/  /'

exit "$rc"
