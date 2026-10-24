-- Every raw article must reach staging. The incremental models rewrite only recent
-- publication months, so an article a source re-dates INTO an older month (outside the
-- window) is dropped from its recent partition and never written to the old one: it
-- silently disappears (follow-up of incident 2026-10-20). The opposite move is handled
-- by the delete_moved_articles post-hook.
-- Fix when this fails: dbt build --full-refresh (staging and the OBT are derivable from raw).
-- Reads only article_id from both tables (~55 MB).
SELECT r.article_id
FROM {{ source('raw', 'raw_data') }} AS r
LEFT JOIN {{ ref('stg_sf__guardian') }} AS s
  ON r.article_id = s.article_id
WHERE r.article_id IS NOT NULL AND s.article_id IS NULL
