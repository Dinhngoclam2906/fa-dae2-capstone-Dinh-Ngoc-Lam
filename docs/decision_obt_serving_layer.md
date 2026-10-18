# Decision: One Big Table as the serving layer (2026-10-18)

**Status:** adopted. **Replaces:** the star schema (`dim_articles` SCD2, `dim_sections`,
`dim_date`, `fct_articles`) as the agent's source, frozen the same day.

## Context

The marts exist to feed a RAG agent, not analytical reporting. What the agent read:

| Consumer | Read | Used from the star schema |
|---|---|---|
| SQL lexical search (`vw_articles_search_optimized`) | `fct_articles` ⨝ `dim_sections` | article columns + `section_name` |
| Pinecone ingestion | `fct_articles` ⨝ `dim_sections` | same |
| anything | `dim_articles` version history, `dim_date`, crawl-event grain | **nothing** |

The star schema's costs were real:

- **Silent data loss.** `fct_articles` / `dim_articles` inner-joined `dim_sections`, so
  every article with an empty section vanished. That cost 2,708 vietnamnet articles before
  it was caught (2026-10-18), and 371 articles were still missing at the freeze.
- **Not rebuildable.** The marts held crawl events and SCD2 versions that raw no longer
  has (the raw MERGE upserts on `article_id`). Hence the rule "never full-refresh
  `fct_articles` / `dim_articles`", and a two-step deep-backfill procedure.
- **Three incremental models to keep in step.** Each needed the recent-month pruning
  window. `dim_articles` missed it, and the daily build cost ~4.7 GB instead of ~1.6 GB.
- **Wrong grain.** The agent needs one row per article now, and one per chunk later.
  It never used crawl events.

On BigQuery a wide denormalised table is also the idiomatic shape: storage is columnar,
so unused columns cost nothing to keep, while joins are the expensive part.

## Decision

`marts.obt_articles`, built from `stg_sf__guardian`:

- one row per article (current version);
- `section_group` from `section_group_macro`, with an empty section → `Other` (no row is
  ever dropped for its section);
- `source_name`, `language`, `word_count`, `preview`, plus `content_hash` (MD5 of
  title|url|section|MD5(body)), the signal to re-embed a changed article;
- incremental `insert_overwrite` on publication-month partitions (same scheme as
  staging); clustered by `source_name`, `section_group`, `language`;
- filters: valid title, valid content (≥ minimum body length), non-null publication
  date. These are deliberate exclusions, and they are visible in `news_freshness`.

`vw_articles_search_optimized` reads it and keeps its old columns (`section_key`,
`is_current`), so the agent's queries still work. `news_freshness` compares it with staging.

**Frozen, not deleted:** the four star-schema models are `enabled: false`. Their tables
stay in `marts` as they were, and copies live in the **`marts_archive`** dataset. They are
M02 evidence and keep the only copy of the crawl/version history. They receive no new
data. Don't re-enable them: they would merge new rows into stale tables.

## Consequences

- **More complete:** at the freeze the OBT had every raw article except 9 thin-body
  Guardian rows (deliberately filtered out), +371 versus the star schema. Section groups
  were identical for every article both contained.
- **Simpler operations:** every enabled incremental model is derivable from raw, so a deep
  backfill is just `dbt build --full-refresh`.
- **Cost:** first measured incremental build ~2.8 GB (staging 1.18, `obt_articles` 1.31,
  tests 0.30), versus ~4.7 GB before. Both incremental models pay `insert_overwrite`'s
  partition-discovery overhead. Static `partitions` would cut that if needed (Phase 4).
- **Lost:** new re-crawl events and title/body version history are no longer recorded.
  Nothing consumed them. If they're ever needed, add an append-only change log keyed on
  `content_hash`.
- **Still to do (agent phase):** the agent's tools and `data_ingestion_to_pinecone.py`
  are still Snowflake code that queries `FCT_ARTICLES` / `DIM_SECTIONS`. Re-point them at
  `marts.obt_articles` (or the view) when the agent moves to BigQuery. The next serving
  table is `article_chunks` (chunk text + embedding, candidate for BigQuery `VECTOR_SEARCH`).
