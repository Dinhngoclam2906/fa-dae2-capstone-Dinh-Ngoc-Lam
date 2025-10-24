-- models/02_intermediate/fct_advanced_articles.sql (updated: uppercase column refs for stg compatibility)
{{ config(
    materialized='incremental',
    unique_key='ARTICLE_EVENT_HK',
    cluster_by=['DATE_ID', 'SECTION_KEY']
) }}

WITH article_events AS (
    SELECT
        ARTICLE_ID,
        CRAWL_TIMESTAMP,
        WEB_PUBLICATION_DATE,
        SECTION_NAME,
        -- Advanced multi-hash: Composite for news events
        {{ dbt_utils.generate_surrogate_key(['ARTICLE_ID', 'CRAWL_TIMESTAMP', 'SECTION_NAME']) }} AS ARTICLE_EVENT_HK
    FROM {{ ref('stg_sf__guardian') }}
    WHERE WEB_PUBLICATION_DATE >= '{{ var("start_date", "2010-01-01") }}'
      AND HAS_VALID_TITLE = TRUE
),

base_facts AS (
    SELECT
        ae.ARTICLE_ID,
        ae.CRAWL_TIMESTAMP,
        ae.WEB_PUBLICATION_DATE,
        ae.ARTICLE_EVENT_HK,
        -- Dims
        d.DATE_ID,
        s.SECTION_KEY,
        art.ARTICLE_ID AS ARTICLE_DIM_KEY,
        art.IS_CURRENT,
        -- Metrics (uppercase refs from stg)
        LENGTH(a.BODY_TEXT) AS BODY_LENGTH_CHARS,
        CASE WHEN a.HAS_VALID_CONTENT THEN 1 ELSE 0 END AS VALID_CONTENT_FLAG,
        LENGTH(a.BODY_TEXT) / NULLIF(LENGTH(a.WEB_TITLE), 0) AS TITLE_TO_BODY_RATIO,
        a.WEB_URL,
        a.LOADED_AT  -- Explicitly include if needed downstream; SELECT * will pull it
    FROM article_events ae
    JOIN {{ ref('stg_sf__guardian') }} a ON ae.ARTICLE_ID = a.ARTICLE_ID AND ae.CRAWL_TIMESTAMP = a.CRAWL_TIMESTAMP
    JOIN {{ ref('dim_date_cross_db') }} d ON DATE(ae.WEB_PUBLICATION_DATE) = d.DATE_VALUE  -- Portable dim (note: uppercase DATE_VALUE if updated in dim_date)
    JOIN {{ ref('dim_sections') }} s ON ae.SECTION_NAME = s.SECTION_NAME
    JOIN {{ ref('dim_articles_advanced_scd') }} art ON ae.ARTICLE_ID = art.ARTICLE_ID AND art.IS_CURRENT = TRUE  -- Advanced SCD
    WHERE ae.WEB_PUBLICATION_DATE IS NOT NULL
)

SELECT *, CURRENT_TIMESTAMP() AS DBT_UPDATED_AT FROM base_facts  -- Added: Metadata for monitoring/freshness

{% if is_incremental() %}
    -- Complex filter: Exclude by hash OR new pubs (news-specific: catch breaking updates)
    WHERE ARTICLE_EVENT_HK NOT IN (SELECT ARTICLE_EVENT_HK FROM {{ this }})
       OR WEB_PUBLICATION_DATE > (SELECT MAX(WEB_PUBLICATION_DATE) FROM {{ this }})
{% endif %}