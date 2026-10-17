-- models/01_staging/stg_sf__guardian.sql
{{ config(materialized='table') }}

WITH raw_articles AS (
  SELECT
    CAST(crawl_timestamp AS TIMESTAMP) AS crawl_timestamp,
    CAST(article_id AS STRING) AS article_id,
    CAST(web_publication_date AS TIMESTAMP) AS web_publication_date,
    CAST(web_title AS STRING) AS web_title,
    CAST(body_text AS STRING) AS body_text,
    CAST(web_url AS STRING) AS web_url,
    CAST(section_name AS STRING) AS section_name,
    CAST(data_source AS STRING) AS data_source,
    COALESCE(CAST(source_name AS STRING), 'guardian') AS source_name,
    COALESCE(CAST(language AS STRING), 'en') AS language,  -- noqa: RF04
    CAST(loaded_at AS TIMESTAMP) AS loaded_at
  FROM {{ source('raw', 'raw_data') }}
  WHERE article_id IS NOT NULL
),

flagged_articles AS (
  SELECT
    *,
    CASE
      WHEN
        web_title IS NOT NULL
        AND TRIM(UPPER(web_title)) != ''
        AND LENGTH(TRIM(web_title)) <= 500
        THEN TRUE
      ELSE FALSE
    END AS has_valid_title,

    CASE
      WHEN
        web_url IS NOT NULL
        AND TRIM(web_url) != ''
        AND STARTS_WITH(LOWER(web_url), 'http')
        AND LENGTH(TRIM(web_url)) <= 1000
        THEN TRUE
      ELSE FALSE
    END AS has_valid_url,

    CASE
      WHEN
        body_text IS NOT NULL
        AND TRIM(body_text) != ''
        AND LENGTH(TRIM(body_text)) > 50
        THEN TRUE
      ELSE FALSE
    END AS has_valid_content,

    CASE
      WHEN section_name IS NOT NULL AND TRIM(section_name) != ''
        THEN TRUE
      ELSE FALSE
    END AS has_valid_section,

    -- Guardian's batch/realtime split is a fixed cutoff date (HF archive ends
    -- 2024-05-11); the VN crawler's split is relative to crawl time instead (anything
    -- published in the last 7 days when crawled = 'realtime' - see
    -- vn_news_backfill.py's realtime_floor), so the Guardian-specific date rule would
    -- misclassify almost every VN 'batch' row as inconsistent if applied universally.
    -- Non-Guardian sources are considered consistent as long as data_source is one of
    -- the two accepted values; their own crawler already enforced the relative rule.
    CASE
      WHEN source_name = 'guardian' AND data_source = 'batch' AND DATE(web_publication_date) <= DATE('2024-05-11')
        THEN TRUE
      WHEN source_name = 'guardian' AND data_source = 'realtime' AND DATE(web_publication_date) > DATE('2024-05-11')
        THEN TRUE
      WHEN source_name != 'guardian' AND data_source IN ('batch', 'realtime')
        THEN TRUE
      WHEN data_source IS NULL
        THEN FALSE
      ELSE FALSE
    END AS has_consistent_data_source
  FROM raw_articles
),

deduped_articles AS (
  SELECT * FROM flagged_articles
  QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id, crawl_timestamp ORDER BY loaded_at DESC NULLS LAST) = 1
)

SELECT
  crawl_timestamp,
  article_id,
  web_publication_date,
  web_title,
  body_text,
  web_url,
  section_name,
  data_source,
  source_name,
  language,
  loaded_at,
  {{ to_local_ts('crawl_timestamp') }} AS crawl_ts_local,
  {{ to_local_ts('web_publication_date') }} AS web_publication_date_local,
  {{ to_local_ts('loaded_at') }} AS loaded_at_local,
  has_valid_title,
  has_valid_url,
  has_valid_content,
  has_valid_section,
  has_consistent_data_source,
  CURRENT_TIMESTAMP() AS processed_at,
  'dbt_staging' AS processing_source
FROM deduped_articles
