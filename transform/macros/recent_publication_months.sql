{#
  Predicate selecting the publication months an incremental run reprocesses: whole
  months from (today - var incremental_lookback_days) onwards. It is a constant
  expression on the partition column, so BigQuery prunes the read to those months
  (a filter on any other column would still bill every partition).

  Window aligned to month starts so insert_overwrite never replaces a partition with
  only part of its rows. After loading OLD articles (deep backfill), rebuild both
  incremental models from raw - safe, since stg_sf__guardian and obt_articles are
  fully derivable (the frozen star schema, which wasn't, is disabled):
    dbt build --full-refresh
  Don't widen the var instead: a wide insert_overwrite re-reads and rewrites every
  partition (measured 2026-10-17: ~20 GB vs ~8 GB).
#}
{% macro recent_publication_months(column_name) %}
  {{ column_name }} >= TIMESTAMP(DATE_TRUNC(
    DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('incremental_lookback_days', 40) }} DAY), MONTH
  ))
{% endmacro %}
