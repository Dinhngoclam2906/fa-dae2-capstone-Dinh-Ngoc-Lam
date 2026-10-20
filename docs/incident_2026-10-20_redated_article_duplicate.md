# Incident 2026-10-20: re-dated Guardian article broke `dbt-build` (5 runs)

**Status:** resolved 2026-10-20 15:13 UTC (fix `ab34d4a`). Open follow-ups at the end.
**Severity:** low. No data was lost, but the vector index went ~19 h without updates
(the last good embed sync before the fix was 2026-10-19 16:04 UTC).

## Symptom

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

## Root cause

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
   tables (staging has no unique-on-`article_id` test, so only the OBT test caught it).
3. The design assumed that *"every new or re-crawled row is recent by publication date"*
   (stg model header). That holds for VN sites and for Guardian's normal 2-day re-poll,
   but **not when a publisher re-dates an old article**. The Guardian does this for
   evergreen pieces, notably `thefilter` / `thefilter-us` product reviews, which get
   refreshed and republished.

## Fix (`ab34d4a`)

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

## Not affected

- **Pinecone `news-articles-v2`:** vector id = `article_id`, so the new version's upsert
  replaced the old vector. No stale vector there.
- **Ingestion:** both crawlers succeeded on every run, and raw stayed unique.

## Open follow-ups (revisit later)

1. **The reverse move is not handled.** If a source re-dates an article *into an older*
   month (outside the window), the next run drops the recent copy and never writes the old
   month, so the article silently disappears from staging/OBT. No test catches that. Fix
   only if it's ever seen, either with `dbt build --full-refresh` or a periodic
   reconciliation (`COUNT(DISTINCT article_id)` raw vs OBT, with the OBT filters).
   Consider adding that count to `news_freshness`.
2. **Stale embeddings in BigQuery.** `vectors.article_embeddings` keeps one row per
   `(article_id, content_hash)`, so old versions are never deleted: 104 rows on
   2026-10-20, this article's July version included. They're harmless today (Pinecone is
   the serving index, and `VECTOR_SEARCH` was ruled out), but they'd surface if BigQuery
   vectors were ever queried directly. Optional cleanup: delete embeddings with no matching
   OBT `(article_id, content_hash)`.
3. **Staging has no `unique(article_id)` test.** The OBT test was the only guard. With the
   hook in place, a unique test on staging would catch a regression one layer earlier.
4. **Alert noise.** One root cause gave 2 emails per run, every 4 h, until fixed. That's
   acceptable for now. If it gets noisy, consider a notification rate limit / auto-close
   on the alert policies.

## How to diagnose this class of failure next time

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
