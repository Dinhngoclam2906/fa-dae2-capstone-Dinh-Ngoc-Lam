# Phase 3.5 — Vietnamese News Crawler (build log)

Companion to `PROGRESS_LOG.md` → *Cloud migration roadmap* → Phase 3.5. Covers the
multi-source Vietnamese news crawler: source selection, the crawler + loader changes,
parser selectors, throughput benchmarks, and the exact run procedure.

**Status (2026-09-10):** crawler + loader **built and tested**, dry-run volume measured.
The full crawl has **not been started** — that is a ~9 h unattended job to run later
(ideally on the `asia-southeast1` VM). dbt-side changes (`source_name` dimension etc.)
not started.

---

## 1. Source selection

### Chosen: Tuổi Trẻ + Thanh Niên + VnEconomy

| Source | Archive depth | Sitemap path | Article URL / id | JSON-LD | robots (generic UA) |
|---|---|---|---|---|---|
| **tuoitre** | 2011 → now | `/sitemaps/index.rss` → `/StaticSitemaps/sitemaps-YYYY-M-D1-D2.xml` (5-day chunks) | `slug-<digits>.htm`; 18-digit id on recent posts encodes a near-publish timestamp | `NewsArticle` (rich) | `Allow: /`, blocks `/print/`, `/tim-kiem.htm`, SSO/js |
| **thanhnien** | 2016 → now | `/sitemap.xml` → `/sitemaps/sitemaps-YYYY-M-D1-D2.xml` (5-day chunks) | same CMS as tuoitre | `NewsArticle` (rich, incl. `articleSection`) | `Allow: /`, blocks `/tim-kiem.htm`, `/news-*.htm` |
| **vneconomy** | 2007 → now | `/sitemap.xml` → `/sitemap/news-YYYY-MM.xml` (monthly) | `slug.htm`, no numeric id | none — but rich `article:*` meta | `Allow: /`, blocks `/api/`, `/v1/search` |

- **Tuổi Trẻ and Thanh Niên run the same CMS** → one parser class (`VccorpStyleSource`)
  covers both. Two sources for ~1× parser effort, which still exercises the
  source-abstraction goal.
- All three are **server-rendered** (no headless browser), expose **date-bucketed archive
  sitemaps** (no listing-page scraping), and carry a full publish timestamp + section +
  URL per article — the metadata the pipeline needs (`web_publication_date` is the BQ
  MONTH partition key).
- VnEconomy is a genuinely different CMS and reaches back to 2007 — good third source.

### Rejected / deferred

| Candidate | Why not |
|---|---|
| **`ademax/binhvq-news-corpus`** (HF, 13.95M rows) | Only `title` / `summary` / `category` / `content`. **No publication date** (= our partition key), **no URL**, **no source name**. Cannot land in `raw.raw_data` without fabricating the partition key. Only viable as a bulk *text* payload for a future vector-scale tier, loaded to a separate table/namespace. |
| **VnExpress** | Mapped (`-<7digit>.html` URLs, sitemaps, an undocumented JSON list API) but not implemented — needs listing-page or JSON-API enumeration rather than clean archive sitemaps. Deprioritised behind the two shared-CMS sources; easy to add later as a 4th `NewsSource` subclass. |
| **worldnewsapi.com** live feed | Optional paid ($39/mo); archive only from 2022. Not a backfill source. Parked. |

### Legal note

Private, non-commercial learning capstone; no redistribution of the crawled corpus.
Same deliberate judgement call already recorded for the original VnExpress plan and the
HF Guardian archive (`docs/phase1_load_report.md` §4).

---

## 2. Measured volume (dry-run, 2024-01-01 → 2026-09-10, 33 months)

Counts are **sitemap `<loc>` URLs** — before dedupe and before parse-rejection of
video/photo posts. Expect ~5–10 % fewer landed rows.

| Year | Tuổi Trẻ | Thanh Niên | VnEconomy | Total |
|---|---:|---:|---:|---:|
| 2024 | 66,328 | 97,455 | 19,434 | 183,217 |
| 2025 | 79,282 | 95,613 | 24,014 | 198,909 |
| 2026 (to 09-10) | 54,719 | 66,741 | 15,809 | 137,269 |
| **Total** | **~200,300** | **~259,800** | **~59,300** | **~519,400** |

- Stable monthly volume: Tuổi Trẻ ~6–7k, Thanh Niên ~8k, VnEconomy ~2k. February dips
  every year (Tết).
- Expected landed: **~470–500k rows**, ~2 GB logical raw text (~1 GB physical).
- At RAG chunk grain (~3–5 chunks/article) → **~1.5–2.5M chunk rows** from this 2.7-year
  slice — with the 257k Guardian rows, a real path toward the Phase 4 ≥5M-chunk target
  without heavy synthetic amplification.
- Deeper backfill available for free: `--from-date 2016-01-01` (Tuổi Trẻ/Thanh Niên) or
  `2007-01-01` (VnEconomy) — same code, just more months.

---

## 3. Crawler — `scripts/batch_data/vn_news_backfill.py`

Mirrors `guardian_api_backfill.py`. Writes the **same Parquet contract** so
`load_raw_to_bigquery.py --source vn` ingests it.

### Design

- **Enumeration:** walk the archive sitemaps for each month from a month-granularity
  cursor. No listing-page scraping.
- **Per-source everything:** state file `data/batch/vn_news_state.<source>.json`, its
  `.lock`, its `.seen` sidecar (every written `article_id` — re-runs skip fetched
  articles), and output subdir `data/batch/vn_news_parquet/<source>/`. → the three
  sources run **in parallel** with zero coordination.
- **Resumable:** a crash loses at most one month; an incomplete month (budget hit or
  sitemap fetch failure) does **not** advance the cursor.
- **Output:** partitioned Parquet `…/<source>/pub_year=YYYY/pub_month=MM/part-*.parquet`,
  10 columns = the 8 Guardian columns + `source_name` + `language`.
- **`data_source`:** `realtime` if within `--realtime-cutoff-days` (7) of now, else
  `batch`. Timestamps converted `+07:00` → naive UTC.
- **Politeness:** shared `Session` + browser UA; robots.txt for all three is `Allow: /`
  with no `Crawl-delay`; only search/print/api paths disallowed, which the crawler never
  touches. `fetch()` retries any `requests.RequestException` (Timeout, ConnectionError,
  **ChunkedEncodingError** — tuoitre/thanhnien serve truncated responses intermittently)
  with exponential backoff; connection pool sized to the worker count.

### Parsing (`HtmlNewsSource.parse_article`)

Per field: **JSON-LD `NewsArticle` → `og:` / `article:` meta → DOM**, first hit wins.
Verified against 6 sample pages (2015–2026) for all three sources + live crawls.

| Field | Tuổi Trẻ / Thanh Niên (`VccorpStyleSource`) | VnEconomy |
|---|---|---|
| `article_id` | trailing `\d{8,20}` in the URL (`185260805203614782`); slug fallback | slug (`nhu-cau-chip-nho-toan-cau-lon-the-nao`) |
| `web_title` | `h1.detail-title` → `og:title` → ld `headline` (ld is entity-encoded → `html.unescape`) | `og:title` |
| `web_publication_date` | ld `datePublished` → `article:published_time` (ISO + `+07:00`) | `article:published_time` — a **`name=`** meta (not `property=`), with the `+` double-encoded as `&#x2B;` → unescape before parse |
| `section_name` | ld `articleSection` (thanhnien only) → `article:section` (thanhnien) → `div.detail-cate a` (tuoitre has neither) | `article:section` → `[class*='breadcrumb'] a` |
| `body_text` | `div.detail-content` / `div[itemprop='articleBody']`; **raw-text fallback** for the pre-2019 template where paragraphs are not `<p>` | `main.article-editor` (site's own class typo `ct-edtior-web`) / `div.article-detail-section__main` |

The 18-digit VCCorp id embeds a timestamp (`<prefix><YYMMDD><HHMMSS><seq>`) but it is
*creation* time, ~minutes before publish — used only as a fallback when the meta is
missing.

### CLI

```
--source {tuoitre,thanhnien,vneconomy}   (required)
--from-date 2024-01-01                    month floor
--to-date   <today>
--concurrency 8                           parallel article fetches per source (see §5 — do NOT raise)
--sleep 0.5                               per-worker inter-request delay
--max-articles 5000                       per-run cap (0 = unlimited; re-run to continue)
--realtime-cutoff-days 7
--output-dir data/batch/vn_news_parquet
--state-file data/batch/vn_news_state.<source>.json   (default is per-source)
--dry-run                                 walk sitemaps + count URLs only
--reset                                   ignore saved cursor + seen set
```

---

## 4. Loader changes — `scripts/batch_data/load_raw_to_bigquery.py`

| Change | Detail |
|---|---|
| `--source vn` | added to choices; `SOURCE_DEFAULT_DIR["vn"] = "data/batch/vn_news_parquet"`. The `*.parquet` wildcard recurses through the per-source subdirs. |
| Schema +2 columns | `RAW_DATA_SCHEMA` gains `source_name` STRING, `language` STRING (before `loaded_at`). |
| `ensure_columns()` | new — runs `ALTER TABLE raw_data ADD COLUMN IF NOT EXISTS source_name/language` every run. Metadata-only, no bytes billed, idempotent. **The first `--source vn` (or any) run applies this to the live table.** |
| `parquet_columns(source)` | vn Parquet carries all 10 columns; Guardian (`hf`/`api`) Parquet carries the original 8. `load_stage` picks the right stage schema per source. |
| `merge_stage()` | for `vn` reads `source_name`/`language` from the stage; for `hf`/`api` injects literals `'guardian'` / `'en'` in the MERGE `SELECT`. → **re-running the `hf`/`api` loader backfills `source_name='guardian', language='en'` onto the existing 257 k Guardian rows.** |
| `report()` | added a `source_name` / `language` breakdown line. |

Deps: `beautifulsoup4` added (`uv add`); `lxml` was already present.

---

## 5. Throughput — benchmarks

200 Thanh Niên articles, one month, after the connection-pool fix:

| `--concurrency` | throughput | net errors |
|---|---:|---:|
| 8 | **5.6 art/s** | 0 |
| 16 | 4.5 art/s | 1 |
| 24 | 3.2 art/s | 5 |

**More concurrency is slower.** The sites throttle per-IP; past ~8 in-flight requests you
get connection resets whose backoff costs more than the added parallelism saves. `8` is
the knee and the default — do not raise it.

- Per source ≈ 5–6 art/s. Three source-processes in parallel (separate hosts) ≈ **16 art/s**.
- **~520k articles ÷ 16 ≈ ~9 hours** for the 2024→now backfill, unattended.
- The remaining lever is **latency**, not client code: run from the `asia-southeast1`
  (Singapore) VM — closer to Vietnam, far fewer of the timeouts seen from a local machine.
  A parser rewrite (e.g. `selectolax`) would not help; the bottleneck is the network/servers.

---

## 6. Run procedure (later)

### 1. Crawl — `scripts/batch_data/run_vn_backfill_all.sh`

```bash
# args: [FROM_DATE] [CONCURRENCY] [SLEEP] [MAX_ARTICLES]
scripts/batch_data/run_vn_backfill_all.sh 2024-01-01          # full, ~9 h, unattended
tail -f data/batch/vn_crawl_logs/*.log                         # follow
```

Launches all three crawlers as background processes, per-source logs in
`data/batch/vn_crawl_logs/`, waits for all three, prints a landed-row count. Ctrl-C
stops all three (state saved at the last completed month). **Re-run to resume** — each
source picks up from its own cursor and the `.seen` sidecar skips fetched articles.

Deeper backfill: `scripts/batch_data/run_vn_backfill_all.sh 2016-01-01`.

### 2. Load to BigQuery (any time, incremental)

```bash
uv run python scripts/batch_data/load_raw_to_bigquery.py --source vn --max-bytes-billed 5000000000
```

Safe to run mid-crawl and repeatedly (MERGE de-dupes on `article_id`). The first run
also `ALTER`s `raw.raw_data` to add `source_name` / `language`.

### 3. Backfill Guardian's `source_name` (once, optional)

```bash
uv run python scripts/batch_data/load_raw_to_bigquery.py --source api --max-bytes-billed 5000000000
```

Sets `source_name='guardian', language='en'` on the existing ~257 k Guardian rows.

---

## 7. Still to do (not started)

- Run the crawl + load (§6).
- **dbt Phase 3.5:** `source_name` dimension, `language` column on staging/marts,
  per-source section mapping (VN sections are Vietnamese strings — `section_utils` macro
  needs per-source buckets or a passthrough).
- Decide whether the RAG agent treats VN + EN as one corpus or filters by `language` /
  `source_name` at retrieval.
- Optional: add VnExpress as a 4th `NewsSource`; deeper (pre-2024) backfill.
