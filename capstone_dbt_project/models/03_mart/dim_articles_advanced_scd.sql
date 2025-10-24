{{ config(
    materialized='incremental',
    unique_key='article_version_key',
    cluster_by=['article_id', 'valid_from']
) }}

WITH base_articles AS (
    SELECT
        article_id,
        web_title,
        body_text,
        web_url,
        section_name,
        crawl_timestamp,
        -- Refined hash: Focus on content drift (title + body for news changes)
        MD5(CONCAT(COALESCE(web_title, ''), '|', COALESCE(body_text, ''))) AS content_change_hash,
        MD5(CONCAT(article_id, crawl_timestamp)) AS article_version_key
    FROM {{ ref('stg_sf__guardian') }}
    WHERE article_id IS NOT NULL AND has_valid_content = TRUE
),

{% if is_incremental() %}
existing_versions AS (
    SELECT article_id, content_change_hash, valid_from, is_current
    FROM {{ this }} WHERE is_current = TRUE
),
change_detection AS (
    SELECT
        b.*,
        e.content_change_hash AS existing_hash,
        CASE
            WHEN e.content_change_hash IS NULL THEN 'new'
            WHEN b.content_change_hash != e.content_change_hash THEN 'content_changed'  -- News: Body edits
            ELSE 'unchanged'
        END AS change_type
    FROM base_articles b
    LEFT JOIN existing_versions e ON b.article_id = e.article_id
),
{% endif %}

scd_logic AS (
    {% if is_incremental() %}
    -- Only new/changed content
    SELECT * FROM change_detection WHERE change_type IN ('new', 'content_changed')
    {% else %}
    SELECT *, 'new' AS change_type FROM base_articles
    {% endif %}
)

SELECT
    sl.article_id,
    sl.content_change_hash,
    sl.web_title,
    sl.body_text,
    sl.web_url,
    sl.section_name,
    sl.article_version_key,
    sl.crawl_timestamp,
    -- SCD Type 2: Time-bound versions
    COALESCE(sl.crawl_timestamp, CURRENT_TIMESTAMP()) AS valid_from,
    CASE
        WHEN sl.change_type = 'content_changed' THEN CURRENT_TIMESTAMP()
        ELSE '9999-12-31'::TIMESTAMP_NTZ
    END AS valid_to,
    CASE WHEN sl.change_type != 'content_changed' THEN TRUE ELSE FALSE END AS is_current,
    sl.change_type,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM scd_logic sl