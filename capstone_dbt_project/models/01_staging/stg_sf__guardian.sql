-- models/01_staging/stg_articles.sql
{{ config(materialized='view') }}

WITH raw_articles AS (
    SELECT
        id::INTEGER AS id,
        crawl_timestamp::TIMESTAMP_NTZ AS crawl_timestamp,
        article_id::STRING AS article_id,
        TRY_TO_TIMESTAMP_NTZ(web_publication_date::STRING) AS web_publication_date,  -- Safe cast for potential string dates
        web_title::STRING AS web_title,
        body_text::STRING AS body_text,
        web_url::STRING AS web_url,
        section_name::STRING AS section_name,
        loaded_at::TIMESTAMP_NTZ AS loaded_at
    FROM {{ source('raw', 'raw_data') }}
    WHERE id IS NOT NULL  -- Basic row filter (id is PK)
)

SELECT
    id,
    crawl_timestamp,
    article_id,
    web_publication_date,
    web_title,
    body_text,
    web_url,
    section_name,
    loaded_at,

    -- Enhanced data quality flags
    CASE
        WHEN web_title IS NOT NULL 
             AND TRIM(UPPER(web_title)) != '' 
             AND LENGTH(TRIM(web_title)) <= 500  -- Reasonable title length cap
        THEN TRUE
        ELSE FALSE
    END AS has_valid_title,

    CASE
        WHEN web_url IS NOT NULL 
             AND TRIM(web_url) != '' 
             AND web_url ILIKE 'http%' 
             AND LENGTH(TRIM(web_url)) <= 1000  -- Reasonable URL length
        THEN TRUE
        ELSE FALSE
    END AS has_valid_url,

    CASE
        WHEN body_text IS NOT NULL 
             AND TRIM(body_text) != '' 
             AND LENGTH(TRIM(body_text)) > 50  -- Basic content threshold
        THEN TRUE
        ELSE FALSE
    END AS has_valid_content,

    CASE
        WHEN section_name IS NOT NULL AND TRIM(section_name) != ''
        THEN TRUE
        ELSE FALSE
    END AS has_valid_section,

    -- Processing metadata
    CURRENT_TIMESTAMP() AS processed_at,
    'dbt_staging' AS processing_source

FROM raw_articles
WHERE article_id IS NOT NULL  -- Ensure core identifier is present
  AND has_valid_title  -- Filter out invalid titles early (optional; can be removed if tests handle it)