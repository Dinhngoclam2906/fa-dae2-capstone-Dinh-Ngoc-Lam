{#
  Post-hook for the two insert_overwrite models. insert_overwrite only replaces the
  recent publication months, so when a source re-dates an old article (Guardian
  republished a July review on 2026-10-19), the new version lands in a recent
  partition while the old one survives in a partition the run never touches: two rows
  for one article_id (raw is unique on article_id, the MERGE upserted it).
  This deletes those stale copies. It scans only article_id + web_publication_date
  (a few tens of MB) and rewrites only the partitions holding a stale row.
  The reverse move (re-dated into an older month) still needs dbt build --full-refresh.
#}
{% macro delete_moved_articles() %}
  DELETE FROM {{ this }}
  WHERE
    NOT ({{ recent_publication_months('web_publication_date') }})
    AND article_id IN (
      SELECT article_id FROM {{ this }}
      WHERE {{ recent_publication_months('web_publication_date') }}
    )
{% endmacro %}
