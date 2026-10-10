# Phase 0 — Baseline & Guardrails

Reference snapshot taken **2026-09-08**, before the Snowflake → BigQuery migration.
Companion to `PROGRESS_LOG.md` → *Cloud migration roadmap*.

---

## 1. Current warehouse objects (Snowflake, now expired)

| Object | Name | Notes |
|--------|------|-------|
| Database | `DB_T26` | from `SNOWFLAKE_DATABASE` |
| Raw schema | `DB_T26.SC_T26` | source `raw.raw_data` → `DB_T26.SC_T26.RAW_DATA` |
| Internal stage | `DB_T26.SC_T26.CSV_STAGE` | used by batch + Kafka consumer loads |
| File format | `MY_PARQUET_FORMAT` (`TYPE = PARQUET`) | |
| Staging schema | `SC_T26_STAGING` | dbt `01_staging`, `+schema: staging` |
| Marts schema | `SC_T26_MARTS` | dbt `03_mart`, `+schema: marts` — **hardcoded in the agent** |
| Intermediate | none (ephemeral) | dbt `02_intermediate` |

**`RAW_DATA` columns (9):** `article_id`, `crawl_timestamp`, `web_publication_date`,
`web_title`, `body_text`, `web_url`, `section_name`, `data_source` (`batch` | `realtime`),
`loaded_at`.

Local Postgres staging (`sql/init.sql`) is an older 8-column shape (no `data_source`);
`scripts/real_time_data/migrate_add_data_source.py` patches it.

---

## 2. Data inventory

> **Correction (2026-09-09):** the HF archive is **~138k articles, not 2.13M**. The
> "2,133,117" below was a `wc -l` line count — Guardian bodies contain many newlines
> inside quoted CSV fields (~15 physical lines per article). See
> `docs/phase1_load_report.md` for the real numbers and the load result.

| Dataset | Location | Rows | Size | In pipeline today |
|---------|----------|------|------|-------------------|
| **Full HF archive** `Stefan171/TheGuardian-Articles` | `data/batch/TheGuardian-Articles/*.csv` (6 parts) | **~138,236** (`wc -l` says 2,133,117 — wrong); 87,575 `Full` | ~551 MB CSV on disk | **Loaded** Phase 1 → 87,461 rows in `raw.raw_data` |
| Batch sample (current) | `data/batch/guardian_historical_articles.parquet` | 100 | 300 KB | Yes (`batch_data_collector` default) |
| Batch sample CSV | `data/batch/guardian_historical_articles_sample.csv` | ~1,900 | 412 KB | presentation only |
| Real-time pull | `data/real_time/guardian_real_time_articles.parquet` | 500 | 2 MB | Yes |

**Warehouse volume after Phase 1 load: 87,461 rows** in BigQuery `raw.raw_data` (all
`data_source = batch`, 1964 → 2024-05-07), ~480 MB logical.

Article text size: Guardian body ≈ 3–8 KB (measured mean ~5.3 KB). 87k articles ≈
**~0.5 GB** raw text in BigQuery — far below the earlier 10–15 GB estimate.

---

## 3. dbt project inventory

Profile `capstone_dbt_project`, adapter `dbt-snowflake`, targets `dev` (local) + `ci`.

| Model | Layer | Materialization | Grain |
|-------|-------|-----------------|-------|
| `stg_sf__guardian` | staging | table | 1 row / (article_id, crawl_timestamp), dedup via `QUALIFY` |
| `int_article_metrics` | intermediate | ephemeral | per section |
| `dim_date` | mart | table | 1 / date |
| `dim_sections` | mart | table | 1 / section_key |
| `dim_articles` | mart | incremental, SCD2, `unique_key=[article_id, valid_from]` | 1 / article version |
| `fct_articles` | mart | incremental `merge`, `unique_key=article_event_hk` | 1 / crawl event |
| `vw_articles_search_optimized` | mart | view | current articles, lowercased text + 500-char preview |
| `news_freshness` | monitoring | table (`tags=['monitoring']`) | 1 row |

**Macros:** `generate_news_hash` (MD5 + CONCAT_WS + TO_CHAR + `::TIMESTAMP_NTZ` + salt),
`section_group_macro` (`ILIKE ANY`).
**Custom tests:** `assert_data_source_consistency`, `test_scd_content_changes`.
**Schema tests:** `not_null` / `unique` / `relationships` / `accepted_values` /
`dbt_utils.unique_combination_of_columns` / `dbt_utils.expression_is_true` (uses
`ILIKE`, `CURRENT_TIMESTAMP()`, `INTERVAL '24 hours'`).
**Packages:** `dbt_utils` (see `packages.yml`).
`fct_articles` uses `adapter.get_columns_in_relation(this)` — adapter-portable.

---

## 4. Agent warehouse query surface

`ai_agents/capstone_tool_calling_agent.py` — 20 `@tool`s. Warehouse schema is
**hardcoded** `schema = "SC_T26_MARTS"`; database from `SNOWFLAKE_DATABASE`.

| Object hit | Tools |
|------------|-------|
| `…SC_T26_MARTS.VW_ARTICLES_SEARCH_OPTIMIZED` | `search_articles_by_keyword`, `search_articles_by_multiple_keywords`, `search_articles_by_date`, `search_articles_by_date_range`, `get_articles_by_section`, `search_articles_by_data_source`, `get_data_source_breakdown`, `compare_batch_vs_realtime_coverage`, `get_batch_data_info`, `smart_search_articles`, `comprehensive_search` |
| `…SC_T26_MARTS.FCT_ARTICLES` (+ join `DIM_SECTIONS`) | `get_trending_topics`, `get_article_by_id`, `get_news_summary`, `get_latest_publication_date`, `get_data_freshness`, `get_article_full_text_and_summary`, `get_priority_news` |
| Pinecone (not warehouse) | `get_pinecone_index_stats`, `get_approximate_unique_articles`, semantic step of `smart_search_articles` |
| No DB | `get_data_source_info`, `determine_data_source_by_date` |

Connection: `snowflake.connector` + a hand-rolled `SnowflakeConnectionPool` (max 5),
JWT key-pair auth, `%s` paramstyle.

---

## 5. Snowflake-isms to port (consolidated) — for Phase 3 + agent phase

### dbt models / macros

| Snowflake | BigQuery |
|-----------|----------|
| `TIMESTAMP_NTZ`, `x::TYPE` | `TIMESTAMP` (or `DATETIME`), `CAST` / `SAFE_CAST` |
| `TRY_TO_TIMESTAMP_NTZ(x)` | `SAFE.PARSE_TIMESTAMP(fmt, x)` / `SAFE_CAST(x AS TIMESTAMP)` |
| `MD5(s)` (returns hex string) | `TO_HEX(MD5(s))` (MD5 returns BYTES) |
| `CONCAT_WS(sep, a, b)` | `ARRAY_TO_STRING([a, b], sep)` |
| `TO_CHAR(ts, 'YYYY-MM-DD…')` | `FORMAT_TIMESTAMP('%Y-%m-%d…', ts)` |
| `ILIKE` / `ILIKE ANY (…)` | `LOWER(x) LIKE …` or `REGEXP_CONTAINS(x, r'(?i)…')` |
| `DATEADD(day, n, d)` / `DATEDIFF('hour', a, b)` | `DATE_ADD(d, INTERVAL n DAY)` / `TIMESTAMP_DIFF(b, a, HOUR)` |
| `INTERVAL '7 days'` | `INTERVAL 7 DAY` |
| `'9999-12-31'::TIMESTAMP_NTZ` | `TIMESTAMP '9999-12-31'` |
| `CURRENT_TIMESTAMP()` / `CURRENT_DATE()` | same (valid in BQ) |
| `QUALIFY ROW_NUMBER() …` | supported ✓ |
| `config(cluster_by=[…])` | `partition_by={…}` + `cluster_by=[…]` (BQ dbt syntax) |
| `incremental_strategy='merge'` | supported ✓ (also `insert_overwrite`) |

### Agent SQL (also needs the client rewrite: `snowflake.connector` → `google-cloud-bigquery`)

| Snowflake | BigQuery |
|-----------|----------|
| `%s` params | `@name` params + `QueryJobConfig`/`ScalarQueryParameter` |
| `CONTAINS(col, x)` | `CONTAINS_SUBSTR(col, x)` or `col LIKE` |
| `col1 || '...' AS preview` | `CONCAT(col1, '...')` (or `||`, supported) |
| `ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS` / `USE_CACHED_RESULT` | remove — use job `timeout_ms`; result cache is on by default |
| `DATEADD(day, %s, CURRENT_DATE())` | `DATE_ADD(CURRENT_DATE(), INTERVAL @days DAY)` |
| `%%like%%` escaping in f-strings | not needed once parameterised |

---

## 6. Environment variables in use (warehouse)

`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`,
`SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, and **five** overlapping key vars:
`SNOWFLAKE_PRIVATE_KEY_FILE_PATH`, `SNOWFLAKE_PRIVATE_KEY_FILE`,
`SNOWFLAKE_PRIVATE_KEY_PATH`, `SNOWFLAKE_PRIVATE_KEY_PWD`,
`SNOWFLAKE_PRIVATE_KEY_FILE_PWD`, `SNOWFLAKE_PRIVATE_KEY_CONTENT`.

**Migration target:** replace all with `GCP_PROJECT` / `BQ_LOCATION` / `BQ_DATASET_*` +
**Application Default Credentials** for auth (no key file — see §7). `GOOGLE_APPLICATION_CREDENTIALS`
is only needed if a service-account JSON is reintroduced later (e.g. CI via Workload Identity
Federation, Phase 3). Fold into one typed settings module (backlog Tier 2).

---

## 7. GCP provisioning — PROVISIONED 2026-09-09

### Actual values (single region: `asia-southeast1`)

| Thing | Value |
|-------|-------|
| GCP project id | `project-f5433681-7b47-481f-9c7` (reused an existing free-tier project, not a fresh `guardian-capstone-*`) |
| GCP project number | `234134076672` |
| Billing account | `01C677-16322F-1EFAAF` |
| Region / location | `asia-southeast1` (BigQuery + GCS) |
| BigQuery datasets | `raw`, `staging`, `marts`, `monitoring`, `scratch` — all `asia-southeast1`, all `storageBillingModel = PHYSICAL` |
| `scratch` TTL | `defaultTableExpirationMs = 604800000` (7 days) |
| GCS landing bucket | `gs://guardian-capstone-landing-dnl` (STANDARD) |
| GCS raw-HTML bucket | `gs://guardian-capstone-rawhtml-dnl` (COLDLINE) |
| Service account | `dbt-runner@project-f5433681-7b47-481f-9c7.iam.gserviceaccount.com` — roles `bigquery.dataEditor`, `bigquery.jobUser`, `storage.objectAdmin` |
| **SA key** | **none.** `constraints/iam.disableServiceAccountKeyCreation` org policy blocks key creation. Auth is **ADC** instead (see below). |
| APIs enabled | `bigquery`, `storage`, `bigquerydatatransfer`, `billingbudgets`, `cloudbilling` |
| Budget | $10 cap, alerts at 50 / 90 / 100 %, scoped to this project — created in the **Console** (CLI `budgets create` kept returning `INVALID_ARGUMENT`) |
| Daily query quota | BigQuery API "Query usage per day" set to **~40 GiB** in the Console |

### Auth model — Application Default Credentials (no key file)

Local dev (Windows) and any script/dbt run authenticate as the **user**, not the SA:

```powershell
# one-time, on the machine that runs dbt / the ingestion scripts:
gcloud auth login
gcloud config set project project-f5433681-7b47-481f-9c7
gcloud auth application-default login
gcloud auth application-default set-quota-project project-f5433681-7b47-481f-9c7
```

ADC file: `%APPDATA%\gcloud\application_default_credentials.json`. `dbt-bigquery` uses
`method: oauth`; `google-cloud-bigquery` picks ADC up automatically. Verified 2026-09-09:
`bq ls` + `application-default print-access-token` both succeed against the project.

> gcloud CLI on Windows installs to `C:\Users\DELL\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin`
> and was **not** added to PATH by the installer — added manually to the user PATH.

**CI auth (Phase 3, deferred):** the `dbt-runner` SA has the right roles but no key.
Use **Workload Identity Federation** from GitHub Actions rather than reintroducing a key,
or lift the org policy at that point.

### Original checklist (reference — commands as first drafted)

> Fill in real values; keep the service-account JSON out of git.

```bash
# --- project + billing ---
gcloud projects create guardian-capstone-<suffix> --name="Guardian Capstone"
gcloud billing projects link guardian-capstone-<suffix> --billing-account=XXXXXX-XXXXXX-XXXXXX
gcloud config set project guardian-capstone-<suffix>

# --- APIs ---
gcloud services enable bigquery.googleapis.com storage.googleapis.com \
  bigquerydatatransfer.googleapis.com

# --- budget: alerts at $5 and $10 (needs billing account admin) ---
gcloud billing budgets create \
  --billing-account=XXXXXX-XXXXXX-XXXXXX \
  --display-name="guardian-capstone $10 cap" \
  --budget-amount=10USD \
  --threshold-rule=percent=0.5 \
  --threshold-rule=percent=0.9 \
  --threshold-rule=percent=1.0

# --- BigQuery datasets (single region, e.g. US or asia-southeast1) ---
bq --location=asia-southeast1 mk --dataset --description "raw landing"        guardian-capstone-<suffix>:raw
bq --location=asia-southeast1 mk --dataset --description "dbt staging"        guardian-capstone-<suffix>:staging
bq --location=asia-southeast1 mk --dataset --description "dbt marts"          guardian-capstone-<suffix>:marts
bq --location=asia-southeast1 mk --dataset --description "dbt monitoring"     guardian-capstone-<suffix>:monitoring
bq --location=asia-southeast1 mk --dataset --default_table_expiration 604800 --description "scratch / synthetic (7d TTL)" guardian-capstone-<suffix>:scratch

# opt datasets into physical (compressed) storage billing
for d in raw staging marts monitoring scratch; do
  bq update --storage_billing_model=PHYSICAL guardian-capstone-<suffix>:$d
done

# --- GCS buckets ---
gcloud storage buckets create gs://guardian-capstone-landing-<suffix>  --location=asia-southeast1 --uniform-bucket-level-access
gcloud storage buckets create gs://guardian-capstone-rawhtml-<suffix>  --location=asia-southeast1 --uniform-bucket-level-access --default-storage-class=COLDLINE

# --- service account for dbt + scripts ---
gcloud iam service-accounts create dbt-runner --display-name="dbt + pipeline runner"
gcloud projects add-iam-policy-binding guardian-capstone-<suffix> \
  --member="serviceAccount:dbt-runner@guardian-capstone-<suffix>.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding guardian-capstone-<suffix> \
  --member="serviceAccount:dbt-runner@guardian-capstone-<suffix>.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding guardian-capstone-<suffix> \
  --member="serviceAccount:dbt-runner@guardian-capstone-<suffix>.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
gcloud iam service-accounts keys create ~/.gcp/guardian-capstone-dbt.json \
  --iam-account=dbt-runner@guardian-capstone-<suffix>.iam.gserviceaccount.com
```

**Console-only steps** (no clean CLI):

- **IAM & Admin → Quotas → BigQuery API → "Query usage per day"** — set to **~30–50 GiB/day**
  (hard stop; the single most effective guardrail).
- Confirm **"Query usage per day per user"** is also capped if multiple identities will query.

**dbt-side guardrail (Phase 3, noted here):** set `maximum_bytes_billed` in the BigQuery
profile (e.g. `10000000000` = 10 GB) so a single bad query can't blow the daily quota.

### Phase 0 done when

- [x] GCP project created, billing linked — *reused `project-f5433681-7b47-481f-9c7`*
- [x] Budget with $5 / $10 alerts live — *$10 cap, 50/90/100 % thresholds, via Console*
- [x] 5 datasets created (`raw`, `staging`, `marts`, `monitoring`, `scratch` w/ TTL), physical storage billing on
- [x] `landing` (Standard) + `rawhtml` (Coldline) buckets created
- [x] ~~`dbt-runner` service account + key issued~~ — SA created; **key blocked by org policy**, auth via ADC instead
- [x] Daily query-bytes quota set to ~40 GiB in console
- [x] This file updated with the real project id / region / bucket names

**Phase 0 complete — 2026-09-09.** Next: Phase 1 (bulk-load the 2.13M-article HF Guardian archive).

---

## 8. Risks / open items

- **Query-cost blowout risk is the agent phase**, not now: unoptimised `LIKE` scans on
  `fct_articles` / the search view. Mitigate later with a materialised search table +
  clustering on the searched columns + per-query `maximum_bytes_billed`.
- Five overlapping Snowflake key env vars — collapse during the port.
- Hardcoded `SC_T26_MARTS` in the agent — replace with a config value.
- `generate_news_hash` and `section_group_macro` both need dialect rewrites; the SCD2 /
  fact incremental logic is the riskiest port (validate row counts before/after).
- ~~Region choice locks in BigQuery + GCS locality~~ — **decided: `asia-southeast1`** (all
  datasets + buckets created there 2026-09-09).
- **No SA key** — CI (Phase 3) must use Workload Identity Federation or the org policy
  `constraints/iam.disableServiceAccountKeyCreation` must be lifted first.
