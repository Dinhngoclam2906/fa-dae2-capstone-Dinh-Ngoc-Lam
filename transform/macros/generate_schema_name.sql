{#
  Override dbt-core's default generate_schema_name, which concatenates
  "<target.schema>_<custom_schema_name>" for any target not literally named 'prod'
  (e.g. dev -> scratch_marts instead of marts). That's dbt's env-sandboxing default,
  and it's what produced the original Snowflake SC_T26_STAGING / SC_T26_MARTS naming
  too - but this project's custom schemas (staging/marts/monitoring) already map 1:1
  to the 5 datasets provisioned in Phase 0 (docs/phase0_baseline.md §7), so every
  target should write directly to those, with no env-based prefix.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
