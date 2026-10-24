{#
  Render a UTC TIMESTAMP as a wall-clock DATETIME in the reporting timezone
  (var `local_timezone`, default Asia/Ho_Chi_Minh). Storage stays UTC; these
  columns are display-only, so never join or dedupe on them.
#}
{% macro to_local_ts(column_name) %}
  DATETIME({{ column_name }}, '{{ var("local_timezone", "Asia/Ho_Chi_Minh") }}')
{% endmacro %}
