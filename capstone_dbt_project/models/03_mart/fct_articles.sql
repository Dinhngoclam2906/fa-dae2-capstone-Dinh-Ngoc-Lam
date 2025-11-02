{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='article_event_hk',
    cluster_by=['date_id', 'section_key']
) }}

WITH article_events AS (
  SELECT
    article_id,
    crawl_timestamp,
    web_publication_date,
    section_name,
    web_title,
    body_text,
    {{ generate_news_hash(['article_id', 'crawl_timestamp', 'section_name']) }} AS article_event_hk,
    -- New: Hash of key attributes for change detection (e.g., title, body, section changes)
    MD5(CONCAT(
      COALESCE(article_id, ''),
      '|',
      COALESCE(web_title, ''),
      '|',
      COALESCE(section_name, ''),
      '|',
      MD5(COALESCE(body_text, ''))
    )) AS attribute_hash
  FROM {{ ref('stg_sf__guardian') }}
  WHERE
    web_publication_date >= '{{ var("start_date", "2001-01-01") }}'
    AND has_valid_title = TRUE
),

base_facts AS (
  SELECT
    ae.article_event_hk,
    ae.article_id,
    ae.crawl_timestamp,
    ae.web_publication_date,
    a.web_title,
    a.body_text,
    -- Dims
    d.date_id,
    s.section_key,
    art.is_current,
    -- Metrics (lowercase refs from stg)
    CASE WHEN a.has_valid_content THEN 1 ELSE 0 END AS valid_content,
    a.web_url,
    a.loaded_at
  FROM article_events ae
  JOIN {{ ref('stg_sf__guardian') }} a ON ae.article_id = a.article_id AND ae.crawl_timestamp = a.crawl_timestamp
  JOIN {{ ref('dim_date') }} d ON DATE(ae.web_publication_date) = d.date_id
  JOIN {{ ref('dim_sections') }} s ON UPPER(TRIM(ae.section_name)) = UPPER(TRIM(s.section_name))
  JOIN {{ ref('dim_articles') }} art ON ae.article_id = art.article_id AND art.is_current = TRUE  -- Advanced SCD
  {% if is_incremental() %}
    LEFT JOIN {{ this }} f ON ae.article_event_hk = f.article_event_hk
  {% endif %}
  WHERE
    ae.web_publication_date IS NOT NULL
    {% if is_incremental() %}
      AND (
        f.article_event_hk IS NULL
        OR ae.attribute_hash != COALESCE(f.attribute_hash, '00000000000000000000000000000000')
        OR ae.crawl_timestamp > (SELECT MAX(crawl_timestamp) FROM {{ this }})
      )
    {% endif %}
)

SELECT *, CURRENT_TIMESTAMP() AS dbt_updated_at FROM base_facts  -- Added: Metadata for monitoring/freshness
