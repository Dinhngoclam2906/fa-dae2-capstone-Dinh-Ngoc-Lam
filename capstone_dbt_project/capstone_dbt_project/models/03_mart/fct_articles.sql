{{ config(
    materialized='incremental',
    unique_key='article_event_key',
    cluster_by=['date_id', 'section_key']
) }}

WITH article_events AS (
    SELECT
        article_id,
        crawl_timestamp,
        web_publication_date,
        -- Surrogate key for unique incremental (composite article_id + crawl_timestamp)
        MD5(CONCAT(article_id, crawl_timestamp)) as article_event_key
    FROM {{ ref('stg_sf__guardian') }}
    WHERE web_publication_date IS NOT NULL
      AND has_valid_title = TRUE
)

SELECT
    ae.article_id,
    ae.crawl_timestamp,
    ae.web_publication_date,
    ae.article_event_key,
    -- Dimension keys (FKs)
    d.date_id,
    s.section_key,
    art.article_id as article_dim_key,  -- Natural key from SCD dim (use current)
    -- Business metrics (adapt to news analytics)
    LENGTH(a.body_text) as body_length_chars,
    CASE WHEN a.has_valid_content THEN 1 ELSE 0 END as valid_content_flag,
    -- Derived
    LENGTH(a.body_text) / NULLIF(LENGTH(a.web_title), 0) as title_to_body_ratio,
    -- Pass through for aggregation (unique URLs)
    a.web_url,
    -- Metadata
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM article_events ae
JOIN {{ ref('stg_sf__guardian') }} a ON ae.article_id = a.article_id AND ae.crawl_timestamp = a.crawl_timestamp
JOIN {{ ref('dim_date') }} d ON DATE(ae.web_publication_date) = d.date_value
JOIN {{ ref('dim_sections') }} s ON a.section_name = s.section_name
JOIN {{ ref('dim_articles') }} art ON ae.article_id = art.article_id AND art.is_current = TRUE
WHERE ae.web_publication_date IS NOT NULL

{% if is_incremental() %}
  AND ae.web_publication_date > (SELECT MAX(web_publication_date) FROM {{ this }})
{% endif %}