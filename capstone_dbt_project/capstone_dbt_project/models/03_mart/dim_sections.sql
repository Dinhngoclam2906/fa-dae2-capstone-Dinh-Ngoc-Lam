{{ config(
    materialized='table',
    cluster_by=['section_name']
) }}

SELECT
    section_name as section_key,  -- Natural key
    section_name,  -- For readability
    -- Derived fields (expand with business logic, e.g., hierarchy)
    UPPER(section_name) as section_name_upper,
    CASE
        WHEN section_name ILIKE '%world%' OR section_name ILIKE '%international%' THEN 'Global'
        WHEN section_name ILIKE '%sport%' THEN 'Sports'
        WHEN section_name ILIKE '%business%' THEN 'Economy'
        ELSE 'Other'
    END as section_group,
    -- Metadata
    COUNT(*) OVER () as total_articles_in_section,  -- From full dataset; update via incremental if needed
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM {{ ref('stg_sf__guardian') }}
WHERE section_name IS NOT NULL
  AND has_valid_section = TRUE  -- From your staging flags
GROUP BY section_name
ORDER BY section_name