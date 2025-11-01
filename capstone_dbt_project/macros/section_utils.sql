{% macro section_group_macro(section_name) %}
  CASE
    WHEN {{ section_name }} ILIKE ANY ('%world%', '%international%', '%uk%') THEN 'Global & UK'  -- Broader global incl. domestic
    WHEN {{ section_name }} ILIKE '%sport%' THEN 'Sports'
    WHEN {{ section_name }} ILIKE ANY ('%business%', '%economy%', '%finance%') THEN 'Economy & Business'
    WHEN {{ section_name }} ILIKE ANY ('%politics%', '%government%') THEN 'Politics'
    WHEN {{ section_name }} ILIKE ANY ('%environment%', '%climate%', '%science%') THEN 'Science & Environment'
    WHEN {{ section_name }} ILIKE ANY ('%culture%', '%art%', '%books%', '%film%', '%music%') THEN 'Culture & Arts'
    ELSE 'Other'
  END
{% endmacro %}
