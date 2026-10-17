"""
guardian_api_backfill.py

Resumable Guardian Content API crawler. Fills the gap the Hugging Face archive does not
cover (it ends 2024-05-07) and, optionally, sparse pre-2024 years.

Output is the SAME layout as `hf_archive_to_parquet.py`, so the existing loader ingests it:

    uv run python scripts/batch_data/guardian_api_backfill.py --from-date 2024-05-08
    uv run python scripts/batch_data/load_raw_to_bigquery.py --source api

Design:
  * `order-by=oldest` + fixed date windows (default 7 days) instead of deep pagination.
  * State file records the next un-crawled date; a crash loses at most one window.
  * Each finished window is flushed to Parquet immediately.
  * `--max-calls` stops cleanly before the dev-key cap; just re-run the next UTC day.
  * A lock file (`<state>.lock`) refuses a second concurrent run - two crawlers racing
    on the same state file corrupts the cursor and doubles quota burn.
  * Downstream MERGE de-dupes on `article_id`, so overlapping re-runs are safe.
  * Once caught up, the cursor rewinds to the last `--tail-days` (default today +
    yesterday) instead of moving past `--to-date`, so a scheduled job keeps picking
    up articles published later the same day.

Guardian developer key limits (measured 2026-09-10): **500 requests / day**
(`X-RateLimit-Limit-Day`), 60 / minute, resetting at midnight UTC. A ~7-call window
means ~65 real windows/day, so 2024-05 -> present is roughly a 2-day crawl.

Environment:
    API_KEY   Guardian developer key   (https://open-platform.theguardian.com/access/)
    API_URL   default https://content.guardianapis.com
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import requests
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

API_URL = os.getenv("API_URL", "https://content.guardianapis.com").rstrip("/")
API_KEY = os.getenv("API_KEY", "")

CUTOFF = date(2024, 5, 11)  # <= CUTOFF -> data_source 'batch', else 'realtime'
HF_ARCHIVE_END = "2024-05-08"  # day after the HF archive's last article

OUTPUT_COLUMNS = [
    "article_id",
    "crawl_timestamp",
    "web_publication_date",
    "web_title",
    "body_text",
    "web_url",
    "section_name",
    "data_source",
]


class RateLimit(Exception):
    """429 from the per-minute burst limit - a short sleep clears it."""


class DailyQuotaExhausted(Exception):
    """429 with X-RateLimit-Remaining-Day: 0 - stop until the midnight-UTC reset."""


class AuthError(Exception):
    pass


def fetch_page(
    session: requests.Session,
    from_date: str,
    to_date: str,
    page: int,
    page_size: int,
    timeout: int = 30,
) -> dict:
    """One /search call. Raises AuthError on 401, DailyQuotaExhausted / RateLimit
    on 429, requests exceptions otherwise."""
    params = {
        "api-key": API_KEY,
        "from-date": from_date,
        "to-date": to_date,
        "page": page,
        "page-size": page_size,
        "order-by": "oldest",
        "show-fields": "bodyText",
    }
    r = session.get(f"{API_URL}/search", params=params, timeout=timeout)
    if r.status_code == 401:
        raise AuthError("Guardian API returned 401 - API_KEY missing/invalid/expired")
    if r.status_code == 429:
        if r.headers.get("X-RateLimit-Remaining-Day") == "0":
            reset = r.headers.get("Retry-After", "?")
            raise DailyQuotaExhausted(
                f"daily 500-call quota exhausted; resets in ~{reset}s (midnight UTC)"
            )
        raise RateLimit()
    r.raise_for_status()
    body = r.json()
    resp = body.get("response", {})
    if resp.get("status") != "ok":
        raise RuntimeError(f"Guardian API status={resp.get('status')}: {body}")
    return resp


def parse_results(results: list[dict], crawl_ts: datetime) -> list[dict]:
    rows = []
    for it in results:
        body_text = (it.get("fields") or {}).get("bodyText") or ""
        wpd_raw = it.get("webPublicationDate") or ""
        if not it.get("id") or not body_text or not wpd_raw:
            continue
        try:
            wpd = datetime.strptime(wpd_raw, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
        rows.append(
            {
                "article_id": it["id"],
                "crawl_timestamp": crawl_ts,
                "web_publication_date": wpd,
                "web_title": it.get("webTitle") or "",
                "body_text": body_text,
                "web_url": it.get("webUrl") or "",
                "section_name": it.get("sectionName") or "",
                "data_source": "batch" if wpd.date() <= CUTOFF else "realtime",
            }
        )
    return rows


def crawl_window(
    session: requests.Session,
    win_from: date,
    win_to: date,
    page_size: int,
    sleep_s: float,
    max_pages: int,
) -> tuple[list[dict], int, bool]:
    """Return (rows, calls_made, ok) for one date window (inclusive both ends).
    ok is False if a page could not be fetched or max_pages was hit - the caller
    must NOT advance the cursor past a not-ok window. DailyQuotaExhausted
    propagates (stop the whole crawl)."""
    crawl_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    f, t = win_from.isoformat(), win_to.isoformat()
    rows: list[dict] = []
    calls = 0
    page = 1
    while page <= max_pages:
        for attempt in range(4):
            try:
                resp = fetch_page(session, f, t, page, page_size)
                calls += 1
                break
            except RateLimit:
                calls += 1
                logger.warning("429 (per-minute burst); sleeping 60s")
                time.sleep(60)
            except (requests.Timeout, requests.ConnectionError) as e:
                logger.warning("network error (%s), retry %d/3", e, attempt + 1)
                time.sleep(2 ** attempt)
        else:
            logger.error("window %s..%s page %d failed after retries", f, t, page)
            return rows, calls, False

        rows.extend(parse_results(resp.get("results", []), crawl_ts))
        pages = resp.get("pages", 1)
        if page >= pages:
            return rows, calls, True
        page += 1
        time.sleep(sleep_s)
    logger.warning("window %s..%s hit max_pages=%d - narrow --window-days", f, t, max_pages)
    return rows, calls, False


def write_window(rows: list[dict], output_dir: Path, run_tag: str) -> int:
    if not rows:
        return 0
    df = pl.DataFrame(rows, schema_overrides={"crawl_timestamp": pl.Datetime("us"),
                                              "web_publication_date": pl.Datetime("us")})
    df = df.select(OUTPUT_COLUMNS).unique(subset=["article_id"], keep="last")
    parted = df.with_columns(
        pl.col("web_publication_date").dt.year().alias("_y"),
        pl.col("web_publication_date").dt.month().alias("_m"),
    )
    for row in parted.select("_y", "_m").unique().iter_rows(named=True):
        y, m = row["_y"], row["_m"]
        sub = parted.filter((pl.col("_y") == y) & (pl.col("_m") == m)).drop("_y", "_m")
        d = output_dir / f"pub_year={y}" / f"pub_month={m:02d}"
        d.mkdir(parents=True, exist_ok=True)
        sub.write_parquet(d / f"part-{run_tag}.parquet", compression="zstd")
    return df.height


def load_state(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text())
    return {}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, default=str))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from-date", default=HF_ARCHIVE_END, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--to-date", default=datetime.now(timezone.utc).date().isoformat(),
                    help="YYYY-MM-DD (inclusive), default today in UTC")
    ap.add_argument("--tail-days", type=int, default=2,
                    help="once caught up, re-poll this many trailing days each run")
    ap.add_argument("--window-days", type=int, default=7)
    ap.add_argument("--page-size", type=int, default=200)
    ap.add_argument("--max-pages", type=int, default=60, help="safety cap per window")
    ap.add_argument("--max-calls", type=int, default=450, help="stop before the 500/day dev cap")
    ap.add_argument("--sleep", type=float, default=0.2, help="seconds between page calls")
    ap.add_argument("--output-dir", type=Path, default=Path("data/batch/guardian_api_parquet"))
    ap.add_argument("--state-file", type=Path, default=Path("data/batch/guardian_api_state.json"))
    ap.add_argument("--reset", action="store_true", help="ignore saved state, start at --from-date")
    args = ap.parse_args()

    if not API_KEY:
        logger.error("API_KEY not set (see https://open-platform.theguardian.com/access/)")
        return 1

    start = date.fromisoformat(args.from_date)
    end = date.fromisoformat(args.to_date)

    # Refuse a second concurrent run - two crawlers racing on the state file
    # corrupts the cursor and doubles quota burn.
    lock = args.state_file.with_suffix(args.state_file.suffix + ".lock")
    if lock.exists():
        logger.error(
            "lock file %s exists - another crawl is running (or crashed). "
            "Delete it if you are sure none is.", lock,
        )
        return 1
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text(f"pid={os.getpid()} started={datetime.now(timezone.utc).isoformat()}\n")

    state = {} if args.reset else load_state(args.state_file)
    cursor = date.fromisoformat(state["cursor_date"]) if state.get("cursor_date") else start
    tail_start = max(end - timedelta(days=args.tail_days - 1), start)
    if cursor > tail_start:
        # State written before the tail re-poll existed parked the cursor past `end`.
        logger.info("Saved cursor %s is inside/after the tail; re-polling from %s",
                    cursor, tail_start)
        cursor = tail_start
    if cursor > start:
        logger.info("Resuming from saved cursor %s (--from-date was %s)", cursor, start)

    session = requests.Session()
    run_tag = datetime.now().strftime("%Y%m%dT%H%M%S")
    total_calls = state.get("calls_total", 0)
    total_rows = state.get("rows_total", 0)
    windows = state.get("windows_done", 0)

    logger.info(
        "Backfill %s -> %s | window=%dd page-size=%d max-calls=%d",
        cursor, end, args.window_days, args.page_size, args.max_calls,
    )

    calls_this_run = 0
    windows_this_run = 0
    stop_reason = ""
    caught_up = False
    try:
        while cursor <= end:
            win_to = min(cursor + timedelta(days=args.window_days - 1), end)
            rows, calls, ok = crawl_window(
                session, cursor, win_to, args.page_size, args.sleep, args.max_pages
            )
            calls_this_run += calls
            total_calls += calls

            if not ok:
                # Window incomplete - do NOT advance the cursor past it, or those
                # articles are silently lost. Stop; re-run continues from here.
                stop_reason = f"window {cursor}..{win_to} incomplete"
                logger.warning("%s - stopping without advancing cursor", stop_reason)
                break

            n = write_window(rows, args.output_dir, f"{run_tag}-{windows:04d}")
            total_rows += n
            windows += 1
            windows_this_run += 1
            logger.info(
                "window %s..%s: %d articles, %d calls (run total %d/%d calls)",
                cursor, win_to, n, calls, calls_this_run, args.max_calls,
            )

            caught_up = win_to >= end
            if caught_up:
                # Never park the cursor past `end`: the cursor is day-granular, and
                # `end` (today) is still filling up. Rewinding to the tail keeps the
                # last --tail-days re-polled on every run; MERGE de-dupes on article_id.
                cursor = tail_start
            else:
                cursor = win_to + timedelta(days=1)
            state.update(
                cursor_date=cursor.isoformat(),
                windows_done=windows,
                rows_total=total_rows,
                calls_total=total_calls,
                last_run=datetime.now(timezone.utc).isoformat(),
            )
            save_state(args.state_file, state)

            if caught_up:
                break
            if calls_this_run >= args.max_calls:
                stop_reason = f"hit --max-calls ({args.max_calls})"
                logger.warning("%s - state saved at cursor %s", stop_reason, cursor)
                break
    except DailyQuotaExhausted as e:
        stop_reason = str(e)
        logger.warning("%s - state saved at cursor %s; re-run after reset", e, cursor)
    except AuthError as e:
        logger.error("%s", e)
        save_state(args.state_file, state)
        lock.unlink(missing_ok=True)
        return 1
    except KeyboardInterrupt:
        logger.warning("Interrupted - state saved at cursor %s", state.get("cursor_date"))
        save_state(args.state_file, state)
        lock.unlink(missing_ok=True)
        return 130
    finally:
        lock.unlink(missing_ok=True)

    done = caught_up
    logger.info("=" * 64)
    logger.info("Backfill %s | cursor now %s", "COMPLETE" if done else "PAUSED", cursor)
    if not done:
        logger.info("stopped because: %s", stop_reason or "loop ended")
    logger.info(
        "this run: %d calls, %d windows | lifetime: %d rows, %d calls, %d windows",
        calls_this_run, windows_this_run, total_rows, total_calls, windows,
    )
    logger.info("Parquet under %s  ->  load with: "
                "uv run python scripts/batch_data/load_raw_to_bigquery.py --source api",
                args.output_dir)
    logger.info("=" * 64)
    return 0


if __name__ == "__main__":
    sys.exit(main())
