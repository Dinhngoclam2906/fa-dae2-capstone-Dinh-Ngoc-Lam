{% macro section_group_macro(section_name) %}
  CASE
    WHEN REGEXP_CONTAINS({{ section_name }}, r'(?i)world|international|uk') THEN 'Global & UK'  -- Broader global incl. domestic
    WHEN REGEXP_CONTAINS({{ section_name }}, r'(?i)sport') THEN 'Sports'
    WHEN REGEXP_CONTAINS({{ section_name }}, r'(?i)business|economy|finance') THEN 'Economy & Business'
    WHEN REGEXP_CONTAINS({{ section_name }}, r'(?i)politics|government') THEN 'Politics'
    WHEN REGEXP_CONTAINS({{ section_name }}, r'(?i)environment|climate|science') THEN 'Science & Environment'
    WHEN REGEXP_CONTAINS({{ section_name }}, r'(?i)culture|art|books|film|music') THEN 'Culture & Arts'
    ELSE 'Other'
  END
{% endmacro %}
