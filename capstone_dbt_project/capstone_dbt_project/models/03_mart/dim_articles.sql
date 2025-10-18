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
        -- Detect changes using hash (focus on content fields)
        MD5(
            CONCAT(
                COALESCE(web_title, ''),
                COALESCE(body_text, ''),
                COALESCE(section_name, ''),
                COALESCE(web_url, '')
            )
        ) as change_hash,
        -- Surrogate for incremental unique (article_id + crawl_timestamp for versions)
        MD5(CONCAT(article_id, crawl_timestamp)) as article_version_key
    FROM {{ ref('stg_sf__guardian') }}
    WHERE article_id IS NOT NULL
      AND has_valid_content = TRUE  -- From your staging
),

{% if is_incremental() %}
existing_articles AS (
    SELECT
        article_id,
        change_hash,
        article_version_key,
        valid_from,
        valid_to,
        is_current
    FROM {{ this }}
    WHERE is_current = TRUE
),
change_detection AS (
    SELECT
        b.*,
        e.change_hash as existing_change_hash,
        e.valid_from as existing_valid_from,
        e.valid_to as existing_valid_to,
        e.is_current as existing_is_current,
        CASE
            WHEN e.change_hash IS NULL THEN 'new'
            WHEN b.change_hash != e.change_hash THEN 'changed'
            ELSE 'unchanged'
        END as change_type
    FROM base_articles b
    LEFT JOIN existing_articles e ON b.article_id = e.article_id
),
{% endif %}

scd_records AS (
    {% if is_incremental() %}
    -- Incremental: Only new/changed, with SCD logic
    SELECT
        cd.article_id,
        cd.change_hash,
        cd.web_title,
        cd.body_text,
        cd.web_url,
        cd.section_name,
        cd.article_version_key,
        cd.crawl_timestamp,
        -- SCD Type 2: valid_from (crawl for new, now for changed)
        CASE
            WHEN cd.change_type = 'new' THEN cd.crawl_timestamp
            WHEN cd.change_type = 'changed' THEN CURRENT_TIMESTAMP()
        END as valid_from,
        -- valid_to (now for changed, open for new)
        CASE
            WHEN cd.change_type = 'changed' THEN CURRENT_TIMESTAMP()
            WHEN cd.change_type = 'new' THEN '9999-12-31'::TIMESTAMP_NTZ
        END as valid_to,
        -- is_current (FALSE for changed, TRUE for new)
        CASE
            WHEN cd.change_type = 'changed' THEN FALSE
            WHEN cd.change_type = 'new' THEN TRUE
        END as is_current,
        cd.change_type
    FROM change_detection cd
    WHERE cd.change_type IN ('new', 'changed')
    {% else %}
    -- First run: Treat all as new versions (include change_hash for future)
    SELECT
        ba.article_id,
        ba.change_hash,
        ba.web_title,
        ba.body_text,
        ba.web_url,
        ba.section_name,
        ba.article_version_key,
        ba.crawl_timestamp,
        ba.crawl_timestamp as valid_from,
        '9999-12-31'::TIMESTAMP_NTZ as valid_to,
        TRUE as is_current,
        'new' as change_type
    FROM base_articles ba
    {% endif %}
)

SELECT
    *,
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM scd_records