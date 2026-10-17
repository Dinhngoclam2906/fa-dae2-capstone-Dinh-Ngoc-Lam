{#
  Predicate selecting the publication months an incremental run reprocesses: whole
  months from (today - var incremental_lookback_days) onwards. It is a constant
  expression on the partition column, so BigQuery prunes the read to those months
  (a filter on any other column would still bill every partition).

  Window aligned to month starts so insert_overwrite never replaces a partition with
  only part of its rows. After loading OLD articles (deep backfill), widen it for one
  run instead of full-refreshing the marts:
    dbt build --full-refresh --select stg_sf__guardian
    dbt build --vars '{incremental_lookback_days: 40000}' --exclude stg_sf__guardian
  Exclude staging in step 2: a wide insert_overwrite re-reads and rewrites every
  partition the full refresh just built (measured 2026-10-17: ~20 GB vs ~8 GB).
#}
{% macro recent_publication_months(column_name) %}
  {{ column_name }} >= TIMESTAMP(DATE_TRUNC(
    DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('incremental_lookback_days', 40) }} DAY), MONTH
  ))
{% endmacro %}
