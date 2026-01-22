{% macro generate_news_hash(fields, separator='|') %}
  {% set salt = var('news_salt', none) %}
  {% set exprs = [] %}
  {% for field in fields %}
    {% if field.upper().endswith('_TIMESTAMP') %}
      {% do exprs.append( "TO_CHAR(" ~ field ~ "::TIMESTAMP_NTZ, 'YYYY-MM-DD HH24:MI:SS.FF3')" ) %}
    {% else %}
      {% do exprs.append( field ~ "::STRING" ) %}
    {% endif %}
  {% endfor %}
  {% if salt %}
    {% do exprs.append( "'" ~ salt ~ "'" ) %}
  {% endif %}
  MD5( CONCAT_WS( '{{ separator }}', {{ exprs | join(', ') }} ) )
{% endmacro %}