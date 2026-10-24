-- Test: Ensure all batch articles are published on or before 2024-05-11
-- and all realtime articles are published after 2024-05-11

WITH inconsistent_records AS (
  SELECT
    article_id,
    web_publication_date,
    data_source,
    CASE
      WHEN data_source = 'batch' AND web_publication_date > '2024-05-11'
        THEN 'Batch article published after cutoff date'
      WHEN data_source = 'realtime' AND web_publication_date <= '2024-05-11'
        THEN 'Realtime article published before cutoff date'
      ELSE 'Unknown inconsistency'
    END AS inconsistency_reason
  FROM {{ ref('stg_sf__guardian') }}
  WHERE has_consistent_data_source = FALSE
)

SELECT * FROM inconsistent_records