{% macro generate_news_hash(fields, separator='|') %}
  {% set salt = var('news_salt', none) %}
  {% set exprs = [] %}
  {% for field in fields %}
    {% if field.upper().endswith('_TIMESTAMP') %}
      {% do exprs.append( "FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E3S', " ~ field ~ ")" ) %}
    {% else %}
      {% do exprs.append( "CAST(" ~ field ~ " AS STRING)" ) %}
    {% endif %}
  {% endfor %}
  {% if salt %}
    {% do exprs.append( "'" ~ salt ~ "'" ) %}
  {% endif %}
  {#- ARRAY_TO_STRING errors on a NULL element unless a null_text replacement is given
     (unlike Snowflake's CONCAT_WS, which just skips NULLs) - '' keeps this a no-op for
     the common case while staying deterministic for hashing. #}
  TO_HEX(MD5( ARRAY_TO_STRING( [{{ exprs | join(', ') }}], '{{ separator }}', '' ) ))
{% endmacro %}
