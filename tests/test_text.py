"""core.text: the rule-based question parsing (filters, recency, routing, keywords)."""

from datetime import date

import pytest

from core import text

TODAY = date(2026, 10, 24)


@pytest.mark.parametrize("question, sources", [
    ("What did VnExpress International write about rice?", ["vnexpress_en"]),
    ("Tin trên VnExpress hôm nay", ["vnexpress", "vnexpress_en"]),
    ("báo tuổi trẻ viết gì về giá xăng", ["tuoitre", "tuoitre_en"]),
    # lower-case "tuổi trẻ" / "thanh niên" are ordinary words (youth, young people)
    ("tuổi trẻ Việt Nam và thanh niên nông thôn", []),
    ("Compare the Guardian and VnEconomy on inflation", ["guardian", "vneconomy", "vneconomy_en"]),
    ("gold prices", []),
    ("báo Dân Trí viết gì về bão", ["dantri", "dantri_en"]),
    ("nâng cao dân trí cho người dân", []),          # the common phrase, not the site
    ("DTiNews on Hanoi traffic", ["dantri_en"]),
    ("What does VIR say about FDI?", ["vir"]),
    ("the virus outbreak", []),                      # 'vir' inside a word
    ("VietnamFinance viết gì về lãi suất?", ["vietnamfinance"]),
    ("Vietnam finance news this week", []),          # the topic, not the site
])
def test_query_sources(question, sources):
    assert text.query_sources(question) == sources


@pytest.mark.parametrize("question, period", [
    ("giá vàng tháng 8/2026", ("2026-08-01", "2026-08-31")),
    ("tin tháng 2 năm 2025", ("2025-02-01", "2025-02-28")),
    ("floods in August", ("2026-08-01", "2026-08-31")),       # this year: August <= October
    ("elections in December", ("2025-12-01", "2025-12-31")),  # no year: the latest December
    ("Brexit news from 2019", ("2019-01-01", "2019-12-31")),
    ("what happened this year", ("2026-01-01", "2026-10-24")),
    ("tin năm ngoái", ("2025-01-01", "2025-12-31")),
    ("prices may rise", None),                                 # "may" the verb, not the month
    ("latest gold price", None),
])
def test_query_period(question, period):
    got = text.query_period(question, TODAY)
    assert (tuple(d.isoformat() for d in got) if got else None) == period


def test_query_filters_combines_source_and_period():
    assert text.query_filters("Thanh Niên tháng 9/2026 về bão", TODAY) == {
        "source": ["thanhnien"], "start_date": "2026-09-01", "end_date": "2026-09-30"}
    assert text.query_filters("gold prices", TODAY) == {}


@pytest.mark.parametrize("question, days", [
    ("tin hôm nay", 3),
    ("What happened yesterday?", 3),
    ("bóng đá tuần này", 7),
    ("news this month", 31),
    ("latest gold price", 30),
    ("Tin mới nhất về giá vàng", 30),
    ("history of the gold standard", None),
])
def test_recency_days(question, days):
    assert text.recency_days(question) == days


@pytest.mark.parametrize("question, expected", [
    ("hi", False),
    ("thanks!", False),
    ("xin chào", False),
    # ("who are you" never reaches should_retrieve: instant_response answers it first)
    ("What's the latest on fuel prices?", True),
    ("Tin mới nhất về giá vàng", True),
    ("climate policy UK", True),
])
def test_should_retrieve(question, expected):
    assert text.should_retrieve(question) is expected


@pytest.mark.parametrize("question, expected", [
    ("summarise the first one", True),
    ("tell me more about that article", True),
    ("tóm tắt bài đầu tiên", True),
    ("gold prices in Vietnam", False),
])
def test_refers_to_previous(question, expected):
    assert text.refers_to_previous(question) is expected


@pytest.mark.parametrize("question, expected", [
    ("interesting!", True),
    ("yes", True),
    ("is that true?", False),
    ("What about rice exports", False),
])
def test_is_conversational_followup(question, expected):
    assert text.is_conversational_followup(question) is expected


def test_instant_response_only_for_questions_about_the_assistant():
    assert text.instant_response("who are you?") == text.INTRO
    assert text.instant_response("bạn là ai") == text.INTRO
    assert text.instant_response("gold prices") is None


def test_is_vietnamese():
    assert text.is_vietnamese("giá vàng hôm nay")
    assert not text.is_vietnamese("gold price today")


def test_extract_keywords_vietnamese_uses_syllable_pairs():
    kws = text.extract_keywords("Tin mới nhất về giá vàng")
    assert "giá vàng" in kws
    assert len(kws) <= 5


def test_extract_keywords_english():
    kws = text.extract_keywords("What did Keir Starmer say about fuel prices?")
    assert "starmer" in kws and "fuel" in kws
    assert "keir" not in kws  # a full name keeps its surname only
    assert len(kws) <= 5


def test_extract_keywords_skips_meta_questions():
    assert text.extract_keywords("How fresh is the data? Check freshness") == []
