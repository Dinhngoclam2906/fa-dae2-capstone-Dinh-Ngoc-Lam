{#
  Bucket a section name into a coarse group. English (Guardian, tuoitre_en,
  vneconomy_en) and Vietnamese (tuoitre / thanhnien / vneconomy) keywords share the
  same buckets. The input is NFC-normalised first: Vietnamese diacritics can arrive
  precomposed or combining. Order matters - the first match wins.
#}
{% macro section_group_macro(section_name) %}
  {%- set s = "NORMALIZE(" ~ section_name ~ ", NFC)" -%}
  CASE
    -- Sports before Global so 'World Cup 2026' is not caught by 'world'.
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)sport|world cup|football|soccer|cricket|tennis|golf|rugby|boxing|cycling|olympic|formula one|\bf1\b|thể thao|bóng đá|các môn khác') THEN 'Sports'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)opinion|góc nhìn') THEN 'Opinion'
    -- Vietnam's domestic news desks (the Guardian's domestic desk stays in Global & UK).
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)vietnam news|vietnam today|vietnamnet global|ho chi minh city|hanoi|thời sự') THEN 'National News'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)world|international|uk|\bus news\b|australia news|global|thế giới|quốc tế') THEN 'Global & UK'  -- Broader global incl. UK domestic
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)business|economy|finance|money|investment|banking|property|real estate|\bbiz\b|market|stock|kinh tế|kinh doanh|tài chính|ngân hàng|doanh nghiệp|thị trường|chứng khoán|địa ốc|nhà đất|bất động sản|đầu tư') THEN 'Economy & Business'
    -- Before Culture: 'chính sách' (policy) contains 'sách' (books).
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)politics|government|\blaw\b|chính trị|chính sách|quân sự|quốc phòng|bầu cử|pháp luật') THEN 'Politics'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)technology|\btech\b|\bgames\b|công nghệ|thủ thuật|blockchain|chuyển đổi số|\bgame\b') THEN 'Technology'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)environment|climate|science|khoa học|môi trường|khí hậu|thời tiết') THEN 'Science & Environment'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)culture|art|books|film|music|stage|television|radio|media|văn hóa|nghệ thuật|sách|phim|âm nhạc|giải trí|nghệ sĩ|truyền hình') THEN 'Culture & Arts'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)life and style|lifestyle|vietnam life|travel|food|fashion|wellness|du lịch|ẩm thực|làm đẹp|thời trang|đời sống') THEN 'Lifestyle & Travel'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)society|education|healthcare|cities|careers|giáo dục|sức khỏe|dân sinh|giới trẻ|nhịp sống trẻ|cộng đồng') THEN 'Society & Education'
    ELSE 'Other'
  END
{% endmacro %}
