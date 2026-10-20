-- models/03_mart/obt_articles.sql
--
-- One Big Table the agent reads: one row per article (its current version), with the
-- section group denormalised in. It replaces the star schema (dim_articles,
-- dim_sections, dim_date, fct_articles), which was frozen on 2026-10-18 and copied
-- to the marts_archive dataset.
--
-- Same incremental scheme as stg_sf__guardian: each run replaces only the publication
-- months from (today - incremental_lookback_days) onwards, so BigQuery prunes the read
-- to those partitions. The table is fully derivable from staging, so --full-refresh
-- is always safe (the frozen marts held history that raw no longer has; this doesn't).
--
-- An empty section becomes 'Other' instead of dropping the row (the star schema's
-- inner join on dim_sections silently lost those articles).
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'web_publication_date', 'data_type': 'timestamp', 'granularity': 'month'},
    cluster_by=['source_name', 'section_group', 'language'],
    on_schema_change='append_new_columns',
    post_hook="{{ delete_moved_articles() }}"
) }}

WITH stg AS (
  SELECT * FROM {{ ref('stg_sf__guardian') }}
  WHERE
    has_valid_title = TRUE
    AND has_valid_content = TRUE
    AND web_publication_date IS NOT NULL
    {% if is_incremental() %}
      AND {{ recent_publication_months('web_publication_date') }}
    {% endif %}
  -- raw is unique on article_id; keep the latest crawl in case staging ever holds more.
  QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY crawl_timestamp DESC, loaded_at DESC) = 1
)

SELECT
  article_id,
  web_title,
  body_text,
  LEFT(body_text, 500) AS preview,
  ARRAY_LENGTH(REGEXP_EXTRACT_ALL(body_text, r'\S+')) AS word_count,
  web_url,
  web_publication_date,
  NULLIF(TRIM(section_name), '') AS section_name,
  {{ section_group_macro("COALESCE(TRIM(section_name), '')") }} AS section_group,
  source_name,
  language,  -- noqa: RF04
  data_source,
  -- Changes when the title, URL, section or body changes: the signal to re-embed.
  TO_HEX(MD5(CONCAT(
    COALESCE(web_title, ''), '|', COALESCE(web_url, ''), '|',
    COALESCE(section_name, ''), '|', TO_HEX(MD5(COALESCE(body_text, '')))
  ))) AS content_hash,
  crawl_timestamp,
  loaded_at,
  CURRENT_TIMESTAMP() AS dbt_updated_at,
  web_publication_date_local,
  crawl_ts_local,
  loaded_at_local
FROM stg
