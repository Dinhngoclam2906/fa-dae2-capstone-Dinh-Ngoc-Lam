-- Advanced test: Enforce >80% valid content rate per section (news quality gate)
SELECT
    section_key,
    valid_rate_percent,
    CASE 
        WHEN valid_rate_percent < 80 THEN 'low_validity_alert' 
        ELSE 'passing' 
    END AS check_result
FROM {{ ref('mart_article_summary_cross_db') }}
WHERE check_result != 'passing'  -- Fail if any low
GROUP BY section_key, valid_rate_percent, check_result  -- Added: Groups for HAVING
HAVING COUNT(*) > 0