{#
  Bucket a section name into a coarse group. English (Guardian, tuoitre_en,
  vneconomy_en, dantri_en, vir) and Vietnamese (tuoitre / thanhnien / vneconomy / dantri)
  keywords share the
  same buckets, plus the sub-sections VietNamNet / VnExpress use as their category
  (e.g. 'Tin nóng', 'Tuyển sinh', 'Quốc hội') and Công Thương's trade / energy desks
  ('Thương mại', 'Năng lượng', 'Công nghiệp': anchored, so 'Công nghiệp quốc phòng'
  still reaches Politics) and its English edition VEN's ('News and Events', 'Industry',
  'Trade', 'Tourism'; anchored, being common words). The input is NFC-normalised first: Vietnamese diacritics can arrive
  precomposed or combining. Order matters - the first match wins.
#}
{% macro section_group_macro(section_name) %}
  {%- set s = "NORMALIZE(" ~ section_name ~ ", NFC)" -%}
  CASE
    -- DTiNews (dantri_en) files culture and sport under one 'Culture And Sport' desk with no
    -- sub-section on the page; most of it is culture (festivals, heritage, music).
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)^culture and sport$') THEN 'Culture & Arts'
    -- Sports before Global so 'World Cup 2026' is not caught by 'world'.
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)sport|world cup|football|soccer|cricket|tennis|golf|rugby|boxing|cycling|olympic|formula one|\bf1\b|thể thao|bóng đá|các môn khác|asean cup|chuyển nhượng') THEN 'Sports'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)opinion|perspectives|góc nhìn|ý kiến|bạn đọc|bàn luận|luận bàn|bình luận|nhận định|diễn đàn|blog phóng viên|tuần việt nam|phản hồi') THEN 'Opinion'
    -- Vietnam's domestic news desks (the Guardian's domestic desk stays in Global & UK).
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)vietnam news|vietnam today|vietnamnet global|ho chi minh city|hanoi|thời sự|^tâm điểm$|tin nóng|^tin tức$|^tin tức - sự kiện$|tiêu điểm|sự kiện|giao thông|đô thị|địa phương|phóng sự|điều tra|tỉnh - thành|^news and events$') THEN 'National News'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)world|international|uk|\bus news\b|australia news|global|thế giới|quốc tế') THEN 'Global & UK'  -- Broader global incl. UK domestic
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)business|economy|finance|money|investment|banking|property|real estate|\bbiz\b|market|stock|kinh tế|kinh doanh|tài chính|ngân hàng|doanh nghiệp|thị trường|chứng khoán|địa ốc|nhà đất|bất động sản|đầu tư|dự án|sản phẩm|tiêu dùng|tiêu & dùng|doanh nhân|^hạ tầng$|investing|corporate|^companies$|thương mại|năng lượng|^công nghiệp$|khuyến công|^điện$|giá cả|công thương|^industry$|^trade$') THEN 'Economy & Business'
    -- Before Culture: 'chính sách' (policy) contains 'sách' (books).
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)politics|government|\blaw\b|\bcrime\b|chính trị|chính sách|quân sự|quốc phòng|bầu cử|pháp luật|đối ngoại|xây dựng đảng|quốc hội|đại hội|mặt trận tổ quốc|khung pháp lý|^nội vụ$|tư tưởng của đảng') THEN 'Politics'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)technology|\btech\b|\bgames\b|công nghệ|thủ thuật|blockchain|chuyển đổi số|\bgame\b|\bxe\b|ô tô|xe máy|^ai$|^ai contest$|an ninh mạng|hạ tầng số|nhịp sống số|chống tin giả|ai hôm nay') THEN 'Technology'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)environment|climate|science|khoa học|môi trường|khí hậu|thời tiết|net zero|chuyển động xanh|green growth') THEN 'Science & Environment'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)culture|art|books|film|music|stage|television|radio|media|văn hóa|nghệ thuật|sách|phim|âm nhạc|giải trí|nghệ sĩ|truyền hình|nhạc|mỹ thuật|sân khấu|di sản|hậu trường|xem - nghe') THEN 'Culture & Arts'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)^life$|life and style|lifestyle|vietnam life|travel|food|fashion|wellness|du lịch|ẩm thực|làm đẹp|thời trang|đời sống|gia đình|expat life|lối sống|đi đâu chơi|chơi gì|ăn uống|cà phê|khỏe đẹp|sống đẹp|đẹp \+|tận hưởng|mẹo vặt|mua sắm|nội thất|^nhà$|tâm sự|sống - yêu|ngủ ngủ|hotels & restaurants|tình yêu - giới tính|^tourism$') THEN 'Lifestyle & Travel'
    WHEN REGEXP_CONTAINS({{ s }}, r'(?i)society|education|healthcare|cities|careers|giáo dục|sức khỏe|dân sinh|giới trẻ|nhịp sống trẻ|cộng đồng|xã hội|tuyển sinh|nhà trường|học đường|du học|chọn nghề|phụ huynh|học tiếng anh|dân tộc|tôn giáo|lao động|việc làm|đoàn - hội|y tế|y khoa|biết để khỏe|các loại bệnh|lá lành|tấm lòng vàng|người sống quanh ta|tấm lòng nhân ái|^an sinh$|^community$|an toàn thực phẩm') THEN 'Society & Education'
    ELSE 'Other'
  END
{% endmacro %}
