{#
  Bucket a section name into a coarse group. English (Guardian) and Vietnamese
  (tuoitre / thanhnien / vneconomy) keywords share the same buckets. The input is
  NFC-normalised first: Vietnamese diacritics can arrive precomposed or combining.
  Order matters - the first match wins.
#}
{% macro section_group_macro(section_name) %}
  {%- set s = "NORMALIZE(" ~ section_name ~ ", NFC)" -%}
  CASE
    -- Sports before Global so 'World Cup 2026' is not caught by 'world'.
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)sport|world cup|thể thao|bóng đá|các môn khác') THEN 'Sports'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)world|international|uk|thế giới|quốc tế') THEN 'Global & UK'  -- Broader global incl. domestic
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)business|economy|finance|kinh tế|kinh doanh|tài chính|ngân hàng|doanh nghiệp|thị trường|chứng khoán|địa ốc|bất động sản|đầu tư') THEN 'Economy & Business'
    -- Before Culture: 'chính sách' (policy) contains 'sách' (books).
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)politics|government|chính trị|thời sự|chính sách|quân sự|quốc phòng|bầu cử|pháp luật') THEN 'Politics'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)environment|climate|science|khoa học|môi trường|khí hậu') THEN 'Science & Environment'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)culture|art|books|film|music|văn hóa|nghệ thuật|sách|phim|âm nhạc|giải trí|nghệ sĩ|truyền hình') THEN 'Culture & Arts'
    ELSE 'Other'
  END
{% endmacro %}
