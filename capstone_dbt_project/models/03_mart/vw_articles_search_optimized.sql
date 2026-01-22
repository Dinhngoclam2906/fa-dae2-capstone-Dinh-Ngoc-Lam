{{ config(
    materialized='view',
    schema='marts'
) }}

SELECT 
    a.ARTICLE_ID,
    LOWER(a.WEB_TITLE) AS web_title_lower,
    LOWER(a.BODY_TEXT) AS body_text_lower,
    a.WEB_TITLE,
    a.WEB_PUBLICATION_DATE,
    a.WEB_URL,
    a.SECTION_KEY,
    s.SECTION_NAME,
    a.IS_CURRENT,
    a.DATA_SOURCE,
    LEFT(a.BODY_TEXT, 500) AS preview
FROM {{ ref('fct_articles') }} a
JOIN {{ ref('dim_sections') }} s ON a.SECTION_KEY = s.SECTION_KEY
WHERE a.IS_CURRENT = TRUE