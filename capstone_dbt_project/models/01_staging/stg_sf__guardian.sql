-- CI Demo
-- models/01_staging/stg_sf__guardian.sql
{{ config(materialized='table') }}

WITH raw_articles AS (
  SELECT
    crawl_timestamp::TIMESTAMP_NTZ AS crawl_timestamp,
    article_id::STRING AS article_id,
    TRY_TO_TIMESTAMP_NTZ(web_publication_date::STRING) AS web_publication_date,
    web_title::STRING AS web_title,
    body_text::STRING AS body_text,
    web_url::STRING AS web_url,
    section_name::STRING AS section_name,
    data_source::STRING AS data_source,
    loaded_at::TIMESTAMP_NTZ AS loaded_at
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
        AND web_url ILIKE 'http%'
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
    CASE
      WHEN data_source = 'batch' AND web_publication_date <= '2024-05-11'
        THEN TRUE
      WHEN data_source = 'realtime' AND web_publication_date > '2024-05-11'
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
  loaded_at,
  has_valid_title,
  has_valid_url,
  has_valid_content,
  has_valid_section,
  has_consistent_data_source,
  CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS processed_at,
  'dbt_staging' AS processing_source
FROM deduped_articles
