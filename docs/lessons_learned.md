# Lessons learned: bugs, incidents and gotchas

One knowledge base for everything that broke, surprised us or cost time on this project,
and what we do differently because of it. **Read the golden rules before touching the
pipeline or the agent.** Entries are grouped by area; each has the symptom, the cause, the
fix, and the lesson. Dates are when it happened; commits are where it was fixed. Detailed
incident reports sit in the appendices at the end.

This file replaced `docs/incident_2026-10-20_redated_article_duplicate.md` (now Appendix A)
and the "Lessons learned" section of `PROGRESS_LOG.md` on 2026-10-20. The day-by-day
narrative stays in `PROGRESS_LOG.full-history.md`.

**Adding an entry:** put it under its area, using this shape:

```
### <short title> (YYYY-MM-DD, `commit`)
- **Symptom:** what we saw.
- **Cause:** why, verified (not guessed).
- **Fix:** what changed.
- **Lesson:** the rule to keep.
```

---

## Golden rules

1. **Verify by rows, not exit codes.** A "nothing to do" run exits 0. Check `COUNT(*)` /
   `MAX(loaded_at)` per source, duplicate ids, vectors vs articles. (Hid a week of missing
   data, 2026-10-12.)
2. **Date cursors never park past the live tail.** Re-poll the tail instead (hit both the
   Guardian and the VN crawlers).
3. **Never run two writers on one state.** That means two crawlers on one state file, a
   manual job over a scheduled one, or a vector sync during an embed.
4. **A publication date is not stable.** Publishers re-date old articles, so partition
   logic must expect rows to move between months.
5. **A `WHERE` clause doesn't cut the BigQuery bill; partition pruning does.** Estimate DML
   high, and use `--dry_run` / `maximum_bytes_billed`.
6. **Prefer tables derivable from raw.** `--full-refresh` must always be safe. Never
   re-enable the frozen star schema.
7. **Measure, don't assume:** storage, costs, latency, model quality. Several estimates and
   one "data loss" alarm were wrong until measured.
8. **Run the real thing:** real `dbt build`s against real data, the real agent on real
   questions. Reading code missed most bugs.
9. **Size timeouts for the job at hand.** A one-off backfill isn't the daily run.
10. **Fail soft in user paths:** one failed retrieval leg, a hung client or a step limit
    must still produce an answer.
11. **An eval only measures what its questions ask.** LLM-written questions flatter dense
    search and borrow the article's own time words, so build hard sets and re-run baselines
    on the same day.
12. **Production changes need the user's go:** deletes, IAM, model or index switches. The
    auto-mode classifier enforces this; ask instead of working around it.

---

## Index

| Date | Area | Entry |
|---|---|---|
| 2026-09-10 | Ingestion | Guardian dev key is 500 calls/day; two crawler processes raced |
| 2026-10-09 | Ingestion | Tuổi Trẻ section selector would have emptied 97% of sections |
| 2026-10-10 | dbt | Snowflake → BigQuery port bugs that only a real build showed |
| 2026-10-10 | BigQuery | Loader re-staged the whole history on every run (70× cost) |
| 2026-10-10 | Airflow | DAG cron never ran (no scheduler process) |
| 2026-10-12 | Ingestion | **Silent cursor bug: a week of "successful" runs landed nothing** |
| 2026-10-17 | Ingestion | Same cursor bug class in the VN crawler |
| 2026-10-17 | Ingestion | False "truncated sitemap" alarm (curl exit code ignored) |
| 2026-10-17 | dbt | SQLFluff still on the Snowflake dialect; salt verification |
| 2026-10-18 | Ingestion | vietnamnet: missing sections, 40% "Other", numeric ids |
| 2026-10-18 | dbt | Star schema silently dropped rows and couldn't be rebuilt → OBT |
| 2026-10-18 | Tooling | dbt Power User used a stale `~/.dbt` profile |
| 2026-10-18 | Embeddings | Laptop network stall, watchdog bug, sync OOM, task timeout |
| 2026-10-18 | Embeddings | **302 vectors lost to a watermark race** |
| 2026-10-19 | Agent | `hybrid_retrieve` timeout never fired (executor `with` block) |
| 2026-10-19 | Cloud | Workflows first run failed in 0.3 s (IAM propagation) |
| 2026-10-19 | dbt | `processed_at` test warned on 436k rows daily |
| 2026-10-19 | Eval | FlashRank + keyword interleave hurt on the multilingual corpus |
| 2026-10-20 | dbt | **Re-dated article → duplicate id → 5 failed dbt-builds** (Appendix A) |
| 2026-10-20 | Embeddings | Full Pinecone re-sync would overwrite current vectors with stale ones |
| 2026-10-20 | Agent | "Latest" questions returned old articles; cold start; client race |
| 2026-10-20 | Agent | Vietnamese questions answered in English |
| 2026-10-20 | Agent | gpt-5.x needs the Responses API; content blocks; inherited env var |
| 2026-10-20 | Agent | `GraphRecursionError` shown to users (4/95 hard questions) |
| 2026-10-20 | Eval | Hard-set pitfalls: leaks, drift, rollover, shared counters |
| 2026-10-20 | Tooling | Shell quoting, grep over `.venv`, git-ignored files |
| 2026-10-21 | BigQuery | Fetching 30 scattered ids: month partitions 1.3 GB, clustering 2.5 GB, hash buckets 25 MB |
| 2026-10-21 | Agent | A full passage rerank hurt "latest" questions; one rescue slot didn't |
| 2026-10-21 | Eval | Prototype pool was 10 deep, not 30: an ablation flag changed the pool size |
| 2026-10-22 | Eval | Time-relative sets expire, and one written after midnight can't test "today" |
| 2026-10-22 | Embeddings | Stale-version guard disabled pruning: Pinecone sync needed 3.3 GB, failed 6 runs |
| 2026-10-23 | Agent | Hung calls cost 20 s + the context; Pinecone SDK retried 6×; 3 s timeout + 1 retry |
| 2026-10-23 | Tooling | `uv sync` would have uninstalled pinecone (not in pyproject / uv.lock) |

---

## 1. Ingestion and crawlers

### Guardian dev key quota and racing crawlers (2026-09-10)
- **Symptom:** the backfill stopped early, and the state file was corrupted.
- **Cause:** the dev key allows **500 calls/day** (60/min, resets at midnight UTC), not
  5,000. A `nohup` survivor and a new run wrote the same state file.
- **Fix:** a lock file, a 450-call default, no cursor advance on an incomplete window, and
  a `DailyQuotaExhausted` stop.
- **Lesson:** one writer per state file. On Windows, `TaskStop` on a `uv run` task doesn't
  kill the `uv.exe → python.exe` tree. Confirm with `Get-CimInstance Win32_Process -Filter
  "Name='python.exe'"` (PowerShell tool; Bash mangles `$_`) before deleting a `.lock`.

### Tuổi Trẻ section selector (2026-10-09)
- **Symptom (caught before launch):** ~97% of Tuổi Trẻ rows would have had an empty
  `section_name`.
- **Cause:** its breadcrumb is `<nav class="detail-cate">`; the selector only matched
  Thanh Niên's `<div>`.
- **Fix:** `.detail-cate a`. Later, the English editions needed `.list-cate`, and
  vietnamnet needed JSON-LD.
- **Lesson:** test every parser on real articles of each site, and check the result per
  source.

### Silent cursor bug: a week of "successful" runs landed nothing (2026-10-12, fixed 10-17)
- **Symptom:** all scheduled `guardian-ingestion` executions `succeeded`, but `raw.raw_data`
  had no new rows since 2026-10-10.
- **Cause:** the day cursor moved to `win_to + 1 day`. After fetching today it pointed at
  tomorrow, so every later run logged "Backfill COMPLETE" and exited 0.
- **Fix:** once caught up, rewind to a `--tail-days` (2) re-poll window; clamp stale
  cursors; `--to-date` defaults to today (UTC). The gap was re-crawled (the API serves
  history).
- **Lesson:** golden rules 1 and 2. Re-polls are safe because the raw MERGE upserts on
  `article_id`.

### Same bug class in the VN crawler (2026-10-17)
- **Symptom:** all three VN states said `cursor_month: 2026-11` in October.
- **Fix:** tail-month clamping before scheduling `vn-ingestion`.
- **Lesson:** any new crawler with a date cursor needs the tail rule.

### False "truncated sitemap" alarm (2026-10-17)
- **Symptom:** I reported silent data loss from truncated sitemaps.
- **Cause:** my curl probe ignored curl's exit status, so partial downloads looked like
  short files. The real crawler (Python `requests`) was fine.
- **Lesson:** probe with the crawler's own client, check exit codes, and fetch several
  times before calling something data loss. (A real tuoitre gap, 2026-05-13..15, is
  source-side: its own sitemap lists only 358 URLs. Accepted.)

### vietnamnet: missing sections, "Other" share, numeric ids (2026-10-18, `0454bf8`)
- **Symptoms:** ~5% of vietnamnet pages had no section; 40% of vietnamnet landed in "Other".
- **Causes:** some pages lack NewsArticle JSON-LD. Sub-sections (*Tin nóng*, *Tuyển sinh*,
  *Quốc hội*) matched no keyword.
- **Fixes:** a BreadcrumbList fallback plus a one-off raw repair; VN sub-section keywords
  (Other 40 → 8%), anchoring generic words (`^tin tức$`).
- **Lessons:**
  - Never leave `section_name` empty.
  - Check raw vs OBT counts and the "Other" share per source after every new source.
  - **Prefix numeric ids with the source name** (`vietnamnet-2565868`): raw merges on
    `article_id` across all sources.

### Site access rules (2026-10-17/18)
- news.tuoitre.vn 404s about every other request, so sitemap reads get 8 attempts.
- VnExpress sitemaps are served to search engines only (302), and robots.txt blocks
  dataset bots. So VnExpress is **RSS-only and forward-only**: never spoof Googlebot or
  crawl listing pages for history.
- Crawl throughput from this connection: ~2–4 articles/s per source at concurrency 6, with
  no gain past 8 (per-IP throttle).

### Cloud Run executions don't lock each other
- The crawler's `.lock` is local to each container. A manual run overlapping a scheduled
  one races on the GCS state: last push wins, and the cursor can regress (the MERGE keeps
  the data correct). Since 2026-10-19 the Workflows DAG runs jobs strictly one after
  another; keep manual runs away from the schedule.

### Bronze storage estimates (2026-10-18)
- My storage estimates for the raw archive were too low, twice. A tuoitre page is ~183 KB
  gzipped, so the archive grows ~90 MB/day (~33 GB/yr).
- **Lesson:** measure real pages before quoting storage.

---

## 2. BigQuery and dbt

### Port bugs only a real build showed (2026-10-10)
Reading the SQL found none of these:
- `cluster_by` with a trailing `ORDER BY` fails ("Result of ORDER BY queries cannot be
  clustered").
- A `WHERE` can't reference an alias from the same `SELECT` (Snowflake allows it): split
  into CTEs.
- `dbt_utils.expression_is_true` *prepends* the column: write the bare predicate
  (`"LIKE 'http%'"`).
- The default `generate_schema_name` prefixes `<target.schema>_`, so it's overridden in
  `macros/`.
- `MD5` returns BYTES (wrap in `TO_HEX`). `ARRAY_TO_STRING` needs a `null_text` argument.
  Recursive CTEs have an iteration cap (use `GENERATE_DATE_ARRAY`).
- A TIMESTAMP compared with a bare date string means midnight: compare `DATE(col)`.
- There were 3 `profiles.yml` copies, not 2. Now there's one:
  `capstone_dbt_project/.dbt/profiles.yml`; always pass `--profiles-dir .dbt`.

### Loader re-staged the whole history every run (2026-10-10)
- **Symptom:** "Staged 229,521 rows" for 5,540 new ones; the MERGE billed in proportion to
  all history.
- **Cause:** `load_stage()` loaded `*.parquet` (every file ever) from GCS.
- **Fix:** a pending-merge list per source, cleared only after a successful MERGE. A
  96-article run went from 2,583 MB to 37 MB.
- **Lesson:** a wildcard load into a staging table grows with history; track what's new.

### Cost mechanics (2026-10-17)
- **A `WHERE` filter doesn't reduce the bill; partition pruning does.** Filter on the
  partition column with a constant expression (`CURRENT_DATE()`-based), not a subquery.
  That's why staging and the OBT are `insert_overwrite` on publication month.
- Don't widen `incremental_lookback_days`: a wide `insert_overwrite` rewrites every
  partition (~20 GB vs ~8). A deep backfill means `dbt build --full-refresh` instead.
- dbt's log shows only the final MERGE. The temp-table build before it can cost far more
  (log said 35 MiB, real 2.4 GB). Use `INFORMATION_SCHEMA.JOBS_BY_PROJECT` by `node_id`.
- **DML bills far more than the columns it references:** a 4-column UPDATE billed 2.43 GB.
- Loads into a growing table need `--max-bytes-billed` headroom (the jobs use 6 GB).

### Point lookups of scattered ids (2026-10-21)
- **Symptom:** reading the bodies of the dense top 30 from `obt_articles` billed ~1.3 GB per
  question (twice the agent's 600 MB cap): 30 ids spread over ~15 month partitions.
- **Cause:** month partitions prune by date, not id. A copy **clustered** on `article_id` was
  worse (2.0–2.5 GB): 30 scattered ids touch nearly every storage block, so clustering
  prunes almost nothing.
- **Fix:** `vectors.article_bodies`, **integer-range partitioned on a hash bucket** of the id
  (3,000 buckets, ~0.8 MB each); the query passes `bucket IN UNNEST(@buckets)` computed in
  Python with the same hash. 25 MB per lookup (10 MB minimum per table). Kept current by
  free load jobs from the embed job, not by a dbt MERGE (which would scan the target).
- **Lesson:** clustering helps range scans and a few ids, not dozens of scattered ones. For
  key lookups, partition on a hash of the key. Measure with uncached queries (a repeated
  identical query bills 0).

### SQLFluff dialect and the hash salt (2026-10-17)
- **Dialect:** SQLFluff was still set to `snowflake` after the port, so parse errors hid
  the real lint results. Fixed to `bigquery`; now lints clean.
- **Salt:** `news_salt`'s "placeholder" value was really the production salt behind every
  surrogate key. Before moving it to `NEWS_SALT`, all 418,310 stored keys were recomputed
  and matched.
- **Lesson:** before changing any input that affects keys, recompute and compare; 100%
  must match. `NEWS_SALT` must never change.

### Star schema silently dropped rows and couldn't be rebuilt (2026-10-18, `7047fb9`)
- **Symptom:** empty-section articles vanished. A full refresh would have lost history.
- **Cause:** `fct_articles` inner-joined `dim_sections`. The dims and facts held crawl /
  version history that raw (upserts) no longer has.
- **Fix:** an OBT, `marts.obt_articles` (empty section → "Other"). The star schema is
  frozen (`enabled: false`, copies in `marts_archive`).
- **Lesson:** golden rule 6. Never re-enable the frozen models: they'd merge into stale
  tables.

### `processed_at` test warned on 436k rows daily (2026-10-19, `e29e990`)
- **Cause:** the "processed in the last 24 h" test assumed a full rebuild. Incremental
  staging leaves old partitions with old `processed_at` values.
- **Fix:** scope the test to the rewritten months.
- **Lesson:** after making a model incremental, re-check every test that assumed a full
  rebuild.

### Re-dated article → duplicate id → 5 failed dbt-builds (2026-10-20, `ab34d4a`)
- **Symptom:** ~10 alert emails; `unique_obt_articles_article_id` failed in 5 runs in a row.
- **Cause:** the Guardian moved an old review from July to October. `insert_overwrite`
  wrote October and never touched July, so two rows existed.
- **Fix:** a `delete_moved_articles` post-hook on staging and the OBT.
- **Follow-ups** (`4166f21`): a test that catches articles moved into an *older* month (they
  would silently vanish; fix with a full refresh), and `unique(article_id)` on staging.
- **Lesson:** golden rule 4. Full report: **Appendix A**.

### dbt Power User used a stale profile (2026-10-18)
- **Symptom:** in Cursor, "Could not find profile named 'capstone_dbt_project'".
- **Cause:** the extension looks for `DBT_PROFILES_DIR`, then `<project>/profiles.yml`,
  then `~/.dbt`. Ours is in `.dbt/`, so it fell back to a stale per-user Snowflake profile.
  The `dbt.profilesDirOverride` setting isn't supported by that version.
- **Fix:** `DBT_PROFILES_DIR=.dbt` in `.env`, read through `python.envFile`, then a window
  reload.

---

## 3. Cloud Run, Scheduler, Workflows, Build, IAM

- **Scheduler → Run `:run` with no body returns 411.** Always send `--message-body='{}'` plus
  `Content-Type: application/json`.
- **Cloud Run Jobs have an ephemeral filesystem.** Persist state in GCS
  (`sync_job_state.py`), seed it before the first run, and treat a missing local output
  directory as "nothing new". Checkpoints written inside the container are lost when it
  dies (bit us in the 10-18 sync resume).
- Jobs resolve `:latest` at each execution, so a rebuilt image applies to the next run. One
  image can serve several jobs (`--command sh --args <entrypoint>`).
- **Moving an env var to a secret:** `--remove-env-vars X --update-secrets X=secret:latest`
  in one update. Pipe values with `--data-file=-` so they never hit the terminal.
- **Repo root has no `.gcloudignore`:** building there uploads the on-disk
  `capstone_dbt_project/.venv`, which was once committed (8,335 files, removed 2026-10-17).
  Build ingestion and embed images from a minimal copy of the files they `COPY`, and build
  dbt from `capstone_dbt_project/`.
- Cloud Build runs as the compute default SA, not the Cloud Build service agent.
- gcloud `alpha` / `beta` aren't installed: Monitoring channels and policies were created
  via REST (`gcloud auth print-access-token` + `x-goog-user-project`).
- **WIF for GitHub:** always set an `attribute-condition` (repo lock), bind
  `workloadIdentityUser` to the repo's `principalSet`, give CI its own read-only SA, and set
  `permissions: id-token: write`.
- **A required status check must always run:** a `paths:` filter on `PR_CI` would block PRs
  forever.
- **Airflow cron never ran (2026-10-10):** the DAG's `0 */4 * * *` was inert because no
  scheduler process was left running. Production moved to Cloud Scheduler + Run Jobs, then
  Workflows.
- **Workflows first test failed in 0.3 s (2026-10-19):** IAM propagation lag on
  `logging.logEntries.create`; a retry 2 minutes later succeeded.
- **The auto-mode classifier blocks some actions** (IAM grants, a production `DELETE`), so
  they wait for the user. That is intended: ask, don't route around it.
- **Alert noise (2026-10-20):** both alert policies use 5-minute metric windows, so a
  persistent failure opens a new incident per run on each policy (2+ mails every 4 h).
  **Accepted by the user**; widening the window would hide a second failure.
- **Concurrent MERGEs into one table conflict.** Schedules were once offset (:00 / :15); now
  the DAG runs jobs strictly in sequence.

---

## 4. Embeddings and Pinecone

### Gemini embedding throughput (2026-10-18)
- `gemini-embedding-2` accepts **one content per request**, so throughput comes from
  parallel requests. 16 workers gave 35.6 req/s; 32 gave 7.8 req/s and 64 gave 16 req/s,
  with timeouts.
- **Lesson:** measure the concurrency sweet spot; more threads can be much slower.

### Laptop backfill stalled (2026-10-18)
- **Symptom:** no save for 26 minutes, 0 CPU, 1.6 GB of memory.
- **Cause:** the laptop's network degraded (a Gemini call took 16 s instead of 0.4 s), so
  all in-flight requests hung. The memory was finished futures (768 floats each) kept for
  the whole month.
- **Fix:** a stall watchdog (save, log, `os._exit(3)`), drop finished futures, and move the
  backfill to Cloud Run.
- **Lesson:** long jobs belong in the cloud; watch for no-progress, not just errors.

### My watchdog bug (2026-10-18, `d591621`)
- **Symptom:** a healthy Cloud Run backfill exited "STALLED" 10 s after a save.
- **Cause:** `as_completed(fs, timeout)` times out when the *whole* iteration isn't done
  within `timeout`, not after a quiet gap.
- **Fix:** loop on `wait(pending, timeout, return_when=FIRST_COMPLETED)`.
- **Lesson:** read the timeout semantics of concurrency APIs; test a watchdog with a short
  stall window.

### Sync out-of-memory, and a task timeout sized for the wrong job (2026-10-18, `f7649f9`, `a63db9f`)
- **OOM:** a whole month of BigQuery rows (768 floats each) plus vector dicts was held in
  memory; the container was killed. Fix: stream pages (`page_size=1000`) and upsert in
  batches as they arrive.
- **Timeout:** the job's 3,600 s limit was sized for the daily run, so it killed the one-off
  resync. Its checkpoint lived in the container. Fix: `--before YYYY-MM` resume and a
  10,800 s limit. Re-upserting would also have spent Pinecone write units (Starter:
  2M/month).
- **Watcher glitch:** my script couldn't parse the execution name because gcloud printed
  extra lines. Parse structured output (`--format`), not human text.

### 302 vectors lost to a watermark race (2026-10-18, `e234a0f`)
- **Symptom:** Pinecone had 484,592 vectors vs 484,894 articles (a stable gap).
- **Cause:** `embedded_at` was stamped at embed time, but rows reach BigQuery in
  2,000-row flushes. A sync during the backfill set its watermark past rows not yet
  visible, and later syncs read only `embedded_at > watermark`.
- **Fix:** stamp rows at save time; repair the missing 302.
- **Lesson:** **never run a sync during an embed**; a watermark must be a time when every
  earlier row is already visible.

### A full re-sync would overwrite current vectors with stale ones (2026-10-20, `4166f21`)
- **Cause:** old versions of an article stay in `vectors.article_embeddings`. A full sync
  walks months newest first, and the Pinecone id is the `article_id`. So a re-dated
  article's stale July vector would be upserted over its October one.
- **Fix:** the sync skips rows with a newer `embedded_at` for the same article (+45 MB scan).
- **Lesson:** when an id can have several versions, every reader must pick the latest.

### The stale-version guard broke every daily sync (2026-10-22, `ba2feee`)
- **Symptom:** `embed-sync` failed 6 runs in a row (10-21 20:00 → 10-22 16:00 UTC): the embed
  step was green, then `sync_vector_index.py sync` died with `bytesBilledLimitExceeded: 3310354432
  or higher required` (cap 1 GB). Pinecone stopped at 10-21 16:04; 1,560 vectors backed up.
- **Cause:** the `NOT EXISTS` self-join from `4166f21` sat inside the per-month row query. Any
  self-reference there (`NOT EXISTS`, a join to `MAX(embedded_at)`, an uncorrelated `NOT IN`;
  all dry-run 3.07 GB vs 0.16 GB without) disables partition pruning, so the whole embedding
  column is read. The "+45 MB" in the entry above was measured on a COUNT over small columns,
  not on the query that reads `embedding`. It stayed latent until the 10-21 passage-rescue
  rebuild shipped it in the image.
- **Fix:** one query over the small columns lists the stale `(article_id, embedded_at)` pairs
  (~41 MB, 85 rows); the per-month query is back to plain partition filters and the rows are
  skipped in Python. Local catch-up sync 0.07 GB, image rebuilt, cloud run green; Pinecone =
  OBT = 491,403.
- **Lesson:** dry-run the exact query that ships (same columns), not a COUNT stand-in; and a
  fix to a job's code isn't live until its image is rebuilt, so test it then.

---

## 5. The agent

### A hung call cost 20 s and the whole context; the SDK retried 6× (2026-10-23)
- **Symptom:** ~1% of retrieval calls in the 10-23 evals (5 of ~420) timed out. Each waited
  for the 20 s leg deadline, and in dense-only mode the question then had no context at all.
- **Cause:** no per-request timeout on Pinecone, 15 s on the query embedding, no retry. And
  pinecone 7.3.0's default urllib3 retry is `total=5` with backoff: a 1 ms timeout took
  16.8 s, so a plain timeout would have been multiplied by 6.
- **Fix:** `CALL_TIMEOUT_S = 3` (warm calls ≤ 0.65 s measured) per call; the embedding is
  retried once in code, Pinecone once via its own retry (set on the private
  `_openapi_config`, guarded). 3 s × 2 × 3 sequential calls < 20 s. The 1 ms test now fails
  in 0.29 s; warm retrieval unchanged (~3 s). `warm_up()` keeps 30 s for the cold start.
- **Lesson:** a timeout means nothing until you know the client's own retry policy; test
  the failure path with a tiny timeout and time it.

### `uv sync` would have uninstalled Pinecone (2026-10-23)
- **Symptom:** `pinecone` and `langchain-pinecone` were installed but in neither
  `pyproject.toml` nor `uv.lock`; `uv sync --dry-run` listed both for removal (plus 20 other
  stray packages, e.g. matplotlib for `visualize_results.py`, dbt-snowflake).
- **Cause:** installed outside the lock at some point; only `requirements-embed.txt` (the
  embed-sync image) pinned pinecone. A fresh clone, or the documented `uv sync`, breaks retrieval.
- **Fix:** `uv add pinecone==7.3.0 langchain-pinecone==0.2.13` (moved `packaging` 25.0 → 24.2;
  dbt parse and retrieval re-checked).
- **Lesson:** before publishing, `uv sync --dry-run` must show nothing the app imports.

### Retrieval leg timeout never fired (2026-10-19, `fe3ff4f`)
- **Symptom:** one v1 Pinecone call hung the eval for 12 minutes.
- **Cause:** leaving a `ThreadPoolExecutor` `with` block waits for every thread, so the
  20 s per-leg timeout couldn't end the question.
- **Fix:** one deadline plus `shutdown(wait=False, cancel_futures=True)`.
- **Lesson:** the context manager defeats your timeout.

### Slow first retrieval (2026-10-18)
- Initialising the v1 vector store took ~100 s on the first retrieval even with no uploads.
  Fix: check for a session before touching the store.

### Hybrid search stopped winning (eval 2026-10-19, cut-over 2026-10-20)
- In 2026-01 (English Guardian only), hybrid + FlashRank won (P@5 0.87 vs 0.61 dense).
- On today's corpus (~85% Vietnamese or cross-lingual, gemini-embedding-2), dense alone wins
  everything. The multilingual reranker (MultiBERT) cut English→Vietnamese recall from 0.80
  to 0.15. Keyword search can't cross languages or reach past its 90-day window. A 50/50
  interleave ignores scores.
- **Lesson:** re-evaluate the recipe when the corpus or the embedding model changes.

### "Latest" questions returned old articles (2026-10-20, `85b9662`)
- **Symptom:** "Tin mới nhất về giá vàng" (latest gold price) ranked January articles first.
- **Cause:** dense search has no sense of time. The old hybrid got recency for free from the
  keyword leg's 90-day window, which dropping that leg removed.
- **Fix:** a soft recency rule (`recency_days`): the top 3 hits from the window, then the
  all-dates ranking. A hard window lost 8/100 eval questions whose "recent" / "last week"
  meant the article's own time.
- **Lesson:** when removing a component, list what it did implicitly.

### Cold start and a client race (2026-10-20)
- **Cold start:** ~14 s to the first result (Vertex client, first embed, Pinecone), close to
  the 20 s deadline. Fix: `warm_up()` in a background thread at app / CLI start.
- **Race:** threads missing an `lru_cache` together each built a client; the uncached one
  was garbage-collected mid-request ("client has been closed"). Fix: a lock around client
  creation.

### Uploads ranked by score lost to articles (2026-10-20, reverted)
- Upload chunks ranked by similarity scored below generic articles for "what is this
  document about?". Pinning them first worked, but the user plans to **remove uploads**, so
  the v2 migration was reverted. Uploads stay on the v1 index; delete v1 after removal.

### Vietnamese questions answered in English (2026-10-20, `dd406f6`)
- **Cause:** "Answer in the user's language" was abstract, inside an English prompt, ahead of
  a long mostly-Vietnamese context. gpt-4o-mini drifted to English (0/3), in both retrieval
  modes.
- **Fix:** `_language_note`, a concrete "Write your reply in Vietnamese…", added as the
  **last** message on every LLM call and never stored in memory. Now 16/16.
- **Lesson:** state constraints concretely, and last.

### Switching to gpt-5.4-mini (2026-10-20, `1e73498`)
- **Model comparison** (`docs/llm_compare.md`):
  - `gpt-4.1-mini` rarely gives URLs.
  - `gemini-3.5-flash-lite` mangled 3/23 URLs.
  - `gemini-3.8-flash` took up to 412 s a turn.
  - `gpt-5.4-mini` gave the best answers at ~3.5× gpt-4o-mini's cost.
- **gpt-5.x with tools + a reasoning effort returns 400 on chat/completions.** Use the
  Responses API (`use_responses_api=True`); there's no `temperature` either. Built in
  `core/llm.py`.
- Responses API replies are content blocks: read `message.text`, never `str(content)`.
- **`uv run --env-file` doesn't override variables already in the shell.** The terminal had
  inherited `OPENAI_MODEL=gpt-4o-mini` from its parent process, so the first check still ran
  the old model. Verify the model name in the response metadata, and open a new terminal.
- Cost is pay-per-token on the OpenAI account (prepaid credit or monthly invoice), separate
  from GCP.

### `GraphRecursionError` shown to users (2026-10-20, `e5a0c17`)
- **Symptom:** 4/95 hard-set questions made the agent loop on tools until LangGraph's
  recursion limit (12) raised an error.
- **Fix:** `ask()` catches it and answers once, without tools, from what was found. A
  dangling tool call gets a "not run" ToolMessage so the stored history stays valid.
  Tested with a forced `recursion_limit=4` and a follow-up in the same thread.
- **Lesson:** every loop needs a graceful exit, and the state after it must still be usable.

### A full passage rerank hurt "latest" questions (2026-10-21)
- **Symptom:** BM25 passage scores fused into the whole top 30 (reciprocal rank fusion)
  lifted deep_detail 0.50 → 0.70, but time_relative strict fell 0.23 → 0.15 and constraint
  compliance 0.52 → 0.35.
- **Cause:** lexical overlap favours older articles that share the topic's words (and words
  like "week"); the dense order, with its recent-first slots, was already right for topical
  questions.
- **Fix:** keep the dense top 4, give only the later slots to the best passages, and drop
  time words from the BM25 query (`core/passages.py`). Same known-item gains, topical
  unchanged.
- **Lesson:** a reranker that wins on known-item questions can lose on topical ones; always
  check both groups, and prefer a rescue slot over reordering a good head.

### Question filter parser (2026-10-20, `e5a0c17`)
- "tuổi trẻ" also means "youth" and "thanh niên" "young people", so those source names need
  capitals or "báo" (newspaper) in front. A bare "may" is a verb, so English months need a
  year or a preposition.
- `search_news` passes every filter key, mostly as `None`, so "no explicit filters" must mean
  no filter *value*.

---

## 6. Evaluation methodology

- **LLM-written questions flatter dense search.** Paraphrases of an article's lead are
  close to what the article vectors embed. The 1c set saturated (~0.95); the hard set
  (`docs/hard_eval.md`) exposed deep details (0.50).
- **Generated questions borrow the article's time words** ("recent", "last week"), which
  punished a hard recency window unfairly.
- **Generator leaks:** questions that only make sense after reading the article ("the
  author", "Ông…"), and multi-line outputs. Fix: a standalone rule, keep the first line,
  hand-edit the rest (`"edited": true`).
- **Index drift:** +1,426 articles between two runs moved `v2_dense` scores. Re-run the
  baseline on the same day as the candidate.
- **Time-relative questions expire.** Record `asked_on`. Vietnam's date rolls over at 17:00
  UTC, mid-run.
- **Check the checker:** the first citation check flagged follow-up turns' URLs (cited from
  the previous turn); every flagged URL had to be verified against `obt_articles.web_url`.
  The language detector misread English answers with *italic* Vietnamese titles.
- **"Agent cited the seed" is a lower bound:** it often answered from another outlet's copy
  of the same story.
- **Shared counters lie under parallelism:** the per-process BigQuery byte counter can't
  attribute bytes to systems running side by side.
- **Check what an ablation flag also changes.** The first variant replay collected the pool
  with `passage_rerank=False`, which also left the dense pool at 10 instead of 30, so every
  variant looked weak (deep_detail 0.55 instead of 0.70). Compare a replay's baseline with
  the official run before trusting it.
- **Time-relative questions expire, and their timing matters** (2026-10-22). Re-asked two
  days later, the hard set's "this week" answers moved out of their windows (11 cited
  articles newer than the window). `eval/time_eval.py` writes a fresh dated set instead.
  Written at 00:20 Vietnam time, every "today" seed was from yesterday, so "hôm nay" scored
  0.00: generate the set in the afternoon, Vietnam time.
- **An eval process can hang after its last write**: a retrieval leg abandoned on timeout
  leaves a network thread that blocks interpreter exit, and the chained `judge` never
  starts. `time_eval.py run` now ends with `os._exit(0)`.
- **Before/after across a code change:** relabel the old rows (`agent_noparse`,
  `agent_e2e_before`) before re-running, or the cache resumes with stale results.
- **Small groups (15–20) mean differences under ~0.1 are noise.** Say so in every report.
- **A judge different from the system under test** (`gpt-4.1-mini` judging a
  `gpt-5.4-mini` agent) avoids self-grading.

---

## 7. Local machine and tooling (Windows, 8 GB RAM)

- **Docker Desktop's WSL2 backend uses ~1.6–1.9 GB.** Quit it fully when crawling.
- **Windows consoles garble Vietnamese:** run Python with `-X utf8`.
- **Bash vs PowerShell:** `$_` in a WMI filter gets mangled through the Bash wrapper (use
  the PowerShell tool). A heredoc full of Python triple quotes failed twice ("unexpected EOF
  while looking for matching `''") and a `\n` inside a regex was turned into a real newline,
  breaking a file. Write patch scripts to a file, or use the Edit tool, for anything with
  quotes or escapes.
- **Searching the repo:** a recursive `grep` crawled into `.venv` and timed out. The Grep
  tool respects `.gitignore`, so it silently skips `PROGRESS_LOG*.md` and `CLAUDE.md`. Grep
  those files by name.
- **Git-ignored on purpose:** `PROGRESS_LOG.md`, `PROGRESS_LOG.full-history.md` and
  `CLAUDE.md` (local only). A blanket `*.json` rule means tracked eval results need
  `git add -f`. CRLF warnings on commit are harmless.
- **`uv run --env-file` uses a stricter dotenv parser** than python-dotenv: quote Windows
  paths with backslashes.
- **Background jobs:** foreground `sleep` is blocked; use background runs and their
  notifications. Piping a long run through `grep | tail` hides all progress until it ends,
  so write to a log file instead.
- `install.ps1` at the repo root is the Claude Code installer, not part of the project.

---

## Appendix A: Incident 2026-10-20, a re-dated Guardian article broke `dbt-build` (5 runs)

**Status:** resolved 2026-10-20 15:13 UTC (fix `ab34d4a`; follow-ups `4166f21`).
**Severity:** low. No data was lost, but the vector index went ~19 h without updates
(the last good embed sync before the fix was 2026-10-19 16:04 UTC).

### Symptom

About 10 failure emails on 2026-10-20: each failed run sends one Cloud Run job alert
(`dbt-build`) and one Cloud Workflows alert (`news-pipeline` FAILED).

| `news-pipeline` run (UTC) | guardian | vn | dbt-build | embed-sync |
|---|---|---|---|---|
| 2026-10-19 16:00 | ✅ | ✅ | ✅ | ✅ (last good run) |
| 2026-10-19 20:00 | ✅ | ✅ | ❌ | skipped |
| 2026-10-20 00:00 / 04:00 / 08:00 / 12:00 | ✅ | ✅ | ❌ | skipped |
| 2026-10-20 15:07 (manual, after the fix) | ✅ | ✅ | ✅ | ✅ |

The `dbt-build` log: all 10 source-freshness checks PASS, `PASS=43 WARN=2 ERROR=1 SKIP=2`.
The one error was `unique_obt_articles_article_id` (`Got 1 result`). `news_freshness`
was skipped, and the workflow then skipped `embed-sync` (a failed dbt-build skips it by
design).

### Root cause

On 2026-10-19 18:40 UTC, the Guardian republished an old review with a **new
publication date**:

| | `article_id` | `web_publication_date` | `content_hash` |
|---|---|---|---|
| old copy | `thefilter-us/2026/apr/29/anyday-anydeli-container-review` | 2026-07-07 14:38:10 | `84f0cb2a…` |
| new version | same | 2026-10-19 18:40:47 | `694feef5…` |

What happened in each layer:

1. **`raw.raw_data`** was correct: the MERGE upserts on `article_id`, so it holds one
   row with the October date.
2. **`stg_sf__guardian` and `obt_articles`** are `insert_overwrite` on MONTH partitions of
   `web_publication_date`. They rewrite only the months from today − 40 days (macro
   `recent_publication_months`), so September 2026 onwards on 2026-10-20. The new version
   went into the October partition. The July partition is outside the window and was never
   rewritten, so the stale July row stayed. Result: two rows for one `article_id` in both
   tables (staging had no unique-on-`article_id` test, so only the OBT test caught it).
3. The design assumed that *"every new or re-crawled row is recent by publication date"*
   (stg model header). That holds for VN sites and for Guardian's normal 2-day re-poll,
   but **not when a publisher re-dates an old article**. The Guardian does this for
   evergreen pieces, notably `thefilter` / `thefilter-us` product reviews, which get
   refreshed and republished.

### Fix (`ab34d4a`)

New macro `capstone_dbt_project/macros/delete_moved_articles.sql`, run as a `post_hook`
on both incremental models:

```sql
DELETE FROM {{ this }}
WHERE NOT (<recent window>)
  AND article_id IN (SELECT article_id FROM {{ this }} WHERE <recent window>)
```

- **What it does:** after each run rewrites the recent months, it deletes any row in an
  older month whose `article_id` also exists in the recent months. The recent copy is the
  current one, because raw holds only the latest version.
- **Cost:** it reads `article_id` and `web_publication_date` across all partitions (tens of
  MB). DML rewrites only the partitions that hold a stale row, so a run with nothing to
  delete costs only the scan.
- **Before applying:** a read-only preview matched exactly 1 row per table (the July copy).

Recovery steps:
1. Local `dbt build --select stg_sf__guardian obt_articles`: 27 PASS, 2 WARN (the usual),
   0 ERROR.
2. Rebuilt the `dbt-build` image (`gcloud builds submit` from `capstone_dbt_project/`,
   digest `sha256:aec8348e…`) and ran `gcloud run jobs update dbt-build --image …:latest`.
3. Ran `gcloud workflows run news-pipeline --data='{"transform": true}'`: SUCCEEDED, all
   4 jobs green.
4. Verified by rows:
   - `obt_articles`: 488,410 rows, 0 duplicate `article_id`.
   - `vectors.article_embeddings`: 487,088 → 488,514 (+1,426 rows: the ~19 h backlog).
   - 0 OBT articles without a vector.
   - `vectors.sync_state`: synced through 2026-10-20 15:12 UTC.
   - Fresh raw rows from 9 sources (`vnexpress_en` had no new rows that run, and its
     freshness check passed).

### Not affected

- **Pinecone `news-articles-v2`:** vector id = `article_id`, so the new version's upsert
  replaced the old vector. No stale vector there.
- **Ingestion:** both crawlers succeeded on every run, and raw stayed unique.

### Follow-ups (all handled on 2026-10-20, `4166f21`)

1. **Reverse move (re-dated *into an older* month): now detected, not auto-fixed.** The
   next run would drop the recent copy and never write the old month, so the article would
   silently disappear. The singular test `tests/assert_raw_articles_in_staging.sql` (raw
   `article_id`s missing from staging, ~55 MB/run, severity error) fails the build and so
   sends the alert email. The fix when it fires is `dbt build --full-refresh`. 0 such
   articles on 2026-10-20. Not auto-healed: a full refresh is ~20 GB, too heavy to run
   unattended for a rare case.
2. **Stale embeddings: the real risk was a full Pinecone re-sync, now fixed.**
   `vectors.article_embeddings` keeps one row per `(article_id, content_hash)` (104 old
   versions on 2026-10-20). `sync_vector_index.py sync` with no watermark walks months
   newest first, and the Pinecone id is the `article_id`. So for a re-dated article it
   upserted the current (October) vector, then the stale (July) one *over* it. The sync now
   skips any row that has a newer `embedded_at` for the same article and model
   (`NOT EXISTS`, +45 MB scan). Checked: the filtered set is exactly 488,410 rows, the
   OBT's count. The incremental sync in the job was never affected (old versions are older
   than the watermark). The 104 rows stay: deleting them would rewrite whole partitions of
   a 3.3 GB table, for no benefit now that every reader skips them.
3. **Staging `unique(article_id)` test added** (severity error). It replaces the
   `unique_combination_of_columns(article_id, crawl_timestamp)` warn test, which it
   implies, so a regression now fails one layer earlier.
4. **Alert noise:** decided by the user: leave as is (see section 3).

### How to diagnose this class of failure next time

```bash
# which step failed
gcloud workflows executions list news-pipeline --location asia-southeast1 --limit 10
gcloud run jobs executions list --job dbt-build --region asia-southeast1 --limit 5
# what failed inside dbt (replace the execution name)
gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="dbt-build"
  AND labels."run.googleapis.com/execution_name"="dbt-build-XXXXX"' --order asc \
  --format="value(textPayload)" | grep -E "FAIL|ERROR|Done\."
```

```sql
-- duplicate ids in the OBT and their dates
WITH d AS (SELECT article_id FROM marts.obt_articles GROUP BY 1 HAVING COUNT(*) > 1)
SELECT article_id, web_publication_date, content_hash
FROM marts.obt_articles JOIN d USING (article_id) ORDER BY 1, 2;
```
