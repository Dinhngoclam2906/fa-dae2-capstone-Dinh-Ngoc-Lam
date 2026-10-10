# Phase 1 — Real Data Load Report

Companion to `PROGRESS_LOG.md` → *Cloud migration roadmap*. Covers the bulk load of the
Hugging Face Guardian archive into BigQuery `raw.raw_data`. The Guardian Content API
backfill crawler (also Phase 1) is tracked separately — see the bottom of this file.

---

## 0. Headline finding — the archive is ~138k articles, not 2.1M

`docs/phase0_baseline.md` recorded **2,133,117 articles**. That number is a **`wc -l`
line count**, not a row count: Guardian article bodies contain many newlines inside
quoted CSV fields, so each article spans ~15 physical lines.

| Measure | Value |
|---|---|
| `wc -l` across the 6 CSV parts | 2,133,123 (≈ the bogus figure) |
| **Actual CSV rows** (`polars`, full schema inference) | **138,236** |
| `Data Quality = Full` | 87,575 |
| `Data Quality = Partial` | 50,661 |
| Non-null body text | 98,200 |

**Parsing note:** a single `pl.scan_csv(glob)` mis-splits on the embedded newlines and
reports ~1.69M rows. Reading each part with `pl.read_csv(..., infer_schema_length=None)`
and concatenating is stable at 138,236. The transform script does the latter and could
add a hard assert if the count drifts.

---

## 1. What was loaded

Source: `data/batch/TheGuardian-Articles/articledb_part{1..6}.csv` (6 parts, ~551 MB CSV,
already on disk). The byte-identical `Stefan171_TheGuardian-Articles/` copy was deleted.

Pipeline (both scripts new, under `scripts/batch_data/`):

```
hf_archive_to_parquet.py   CSV → clean, pub-date-partitioned Parquet
load_raw_to_bigquery.py    Parquet → GCS landing → raw._stage_hf → MERGE → raw.raw_data
```

### Transform (`hf_archive_to_parquet.py`)

| Step | Rows |
|---|---|
| Raw CSV rows read | 138,236 |
| After `Data Quality = Full` | 87,575 |
| After non-null/non-empty `article_id, web_publication_date, web_title, body_text` | 87,575 |
| After de-dupe on `article_id` (keep latest `web_publication_date`) | **87,461** (114 dupes dropped) |

- `article_id` = URL path after `theguardian.com/`
- `section_name` = title-cased `Article Category` (96 distinct)
- `data_source` = `batch` if `web_publication_date` ≤ 2024-05-11 else `realtime` → **all 87,461 are `batch`** (archive ends 2024-05-07)
- Output: `data/batch/hf_guardian_parquet/pub_year=YYYY/pub_month=MM/part-0.parquet` — 328 files, 207 MB (zstd)

### Load (`load_raw_to_bigquery.py`)

- GCS landing: `gs://guardian-capstone-landing-dnl/guardian/hf/…` — 328 objects, 217 MB
- `raw._stage_hf` (8 cols, `WRITE_TRUNCATE`) ← Parquet load job (free — no bytes scanned)
- `MERGE raw._stage_hf → raw.raw_data ON article_id`, adds `loaded_at = CURRENT_TIMESTAMP()`
- Idempotent: re-run keeps the table at exactly 87,461 rows (0 inserts, 87,461 updates)
- `maximum_bytes_billed = 2 GB` guard on the MERGE (used ~0.5–1 GB)

---

## 2. `raw.raw_data` — state after HF + full API load (2026-09-10)

| Property | Value |
|---|---|
| Rows | **257,441** (87,461 HF + 170,000 API, boundary overlaps merged) |
| Distinct `article_id` | 257,441 (no dupes) |
| `data_source` | `batch` 88,328 · `realtime` 169,113 |
| `web_publication_date` range | 1964-06-25 → **2026-09-10** |
| Logical size | **~1.56 GB** (uncompressed) |
| Physical size | ~0.6 GB (PHYSICAL billing, compressed; storage stats lag a few hours) |
| Partitioning | `MONTH` on `web_publication_date` |
| Clustering | `section_name, data_source` |
| Required partition filter | no (dbt staging full-scans `raw`) |

Schema (9 cols): `article_id` STRING REQUIRED · `crawl_timestamp` TIMESTAMP ·
`web_publication_date` TIMESTAMP · `web_title` STRING · `body_text` STRING · `web_url`
STRING · `section_name` STRING · `data_source` STRING · `loaded_at` TIMESTAMP

### Rows per publication year (combined, after full API load)

| Year | Rows | | Year | Rows |
|---|---|---|---|---|
| ≤2007 | 2,397 | | 2017 | 5,963 |
| 2008 | 993 | | 2018 | 5,047 |
| 2009 | 1,287 | | 2019 | 5,219 |
| 2010 | 1,717 | | 2020 | 4,789 |
| 2011 | 2,185 | | 2021 | 5,020 |
| 2012 | 2,730 | | 2022 | 6,927 |
| 2013 | 3,368 | | 2023 | 12,422 |
| 2014 | 4,933 | | **2024** (HF to 05-07 + API from 05-08) | **61,718** |
| 2015 | 4,777 | | **2025** (API) | **71,644** |
| 2016 | 6,110 | | **2026** (API, to 09-10) | **48,195** |
| | | | **Total** | **257,441** |

Pre-2024 is still HF-only (sparse); 2024+ shows the API density (~6k/month). The
`_stage_hf` / `_stage_api` tables persist in `raw` (~1 GB logical combined, free) —
the loader truncates and reuses them.

---

## 3. Impact on the roadmap

- **Phase 1 deliverable** revised: "~2M+ rows" → **257k real rows** (87k HF + 170k
  Guardian API, 2024-05-08 → 2026-09-10). The gap the HF archive left (mid-2024 onward) is
  now fully closed; the API crawl is caught up to today.
- **Phase 4 base-scale premise (HF archive alone)** is void, but the Guardian *API*
  archive is genuinely ~2.4M articles — see §6. A full pre-2024 backfill would restore
  Guardian as a primary scale corpus; that deeper crawl is parked
  (`PROGRESS_LOG.md` → Parking lot).
- **Scale for Phase 4 currently rests on:** the Phase 3.5 Vietnamese crawl, the (2024→now)
  Guardian API corpus, and bounded synthetic amplification.

## 4. Legal note

The HF dataset README bars use "for LLMs / ML / AI-related purposes / any commercial
purposes." This is a **private, non-commercial learning capstone** with no redistribution
of the corpus — the same deliberate judgement call already recorded for the Phase 3.5
VnExpress crawl.

---

## 5. Guardian Content API backfill crawler — COMPLETE (2024-05-08 → 2026-09-10)

`scripts/batch_data/guardian_api_backfill.py`. New key `9f7…` works.

**Loaded:** 170,000 distinct API articles → `raw.raw_data` (MERGE, 2.2 GB billed on the
final load). Coverage **2024-05-08 → 2026-09-10** (~28 months), ~5.5–6k/month.

### Final catch-up run (2026-09-10)

The 2026-01-21 → 2026-09-10 gap was crawled in a single session: **34 weekly windows,
~44,600 new articles, 242 / 450 API calls** — well under the 500/day dev-key cap, one
transient network timeout auto-retried. Cursor now `2026-09-11` (past today); the lock
file cleared on the clean exit. Loader run:
`load_raw_to_bigquery.py --source api` staged 223,952 rows (new crawl + leftover dupes),
the QUALIFY dedupe in `merge_stage` collapsed them, MERGE affected 169,999 rows.
`raw.raw_data` → **257,441 rows** (see §2).

### Two problems hit during the run

1. **Guardian dev key = 500 requests/day, not 5,000.** Measured from
   `X-RateLimit-Limit-Day: 500` (60/min, resets midnight UTC). The crawl exhausted the
   day's quota partway through.
2. **Two crawler processes ran in parallel** (a `nohup` background run survived a harness
   kill and kept running next to a fresh one). They raced on the state file — the cursor
   desynced from real coverage, and quota burned 2×. On persistent 429 the old code
   *advanced the cursor past un-crawled windows*, silently dropping them.

### Fixes applied to the crawler

- `--max-calls` default 4500 → **450** (under the real cap).
- **Lock file** (`<state>.lock`) — a second concurrent run refuses to start.
- New `DailyQuotaExhausted` (429 + `Remaining-Day: 0`) vs `RateLimit` (per-minute burst).
- `crawl_window` returns an `ok` flag; on an incomplete window the crawl **stops without
  advancing the cursor** — re-run resumes exactly there.
- State file reset to `cursor_date = 2026-01-21` (first fully-missing day).

### Keeping it current (incremental going forward)

```
uv run python scripts/batch_data/guardian_api_backfill.py          # resumes at the saved cursor
uv run python scripts/batch_data/load_raw_to_bigquery.py --source api --max-bytes-billed 5000000000
```

Re-running picks up from `guardian_api_state.json` (`cursor_date`), crawls only new days,
and the MERGE de-dupes on `article_id` — safe to run repeatedly. Same transform
(`article_id` from `id`, cutoff `data_source`, drop empty bodies); output
`data/batch/guardian_api_parquet/…`.

**Cleanup pending:** `data/batch/guardian_api_parquet/` holds ~54k duplicate rows in
~150 files from the earlier parallel-run mess plus the catch-up run's overlaps (harmless —
MERGE collapses them; the QUALIFY dedupe in `load_raw_to_bigquery.merge_stage` handles it).
Could be consolidated to one file/month.

## 6. Guardian's true archive size (for a future deep backfill)

Live counts from the Content API (`type=article`), 2026-09-10:

| Period | Articles |
|---|---|
| recent year (2018–2024) | ~74–77k |
| 2014 | 115,240 |
| **2014 → 2024** | **953,748** |
| **1999 → 2024 (full archive)** | **2,441,663** |

So the true Guardian archive really is ~2.4M — the phase-0 "2.13M" was a `wc -l`
coincidence, but the real number is that big. The HF dataset captured ~4 %. A full
1999→now backfill (~2.6M articles ≈ ~15M chunk rows, ~16 GB logical / ~7 GB physical)
would meet the Phase 4 "≥5M real rows" target on its own. Parked pending the always-on VM
(the blocker is ~3 quota-days / ~27 h crawl wall-clock, not cost) —
`PROGRESS_LOG.md` → Parking lot.
