"""ingestion/vn_news_backfill.py: the retry limit for pages that keep failing, and source ids."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

import vn_news_backfill as vn  # noqa: E402


def test_gives_up_after_max_failed_runs():
    failures: dict[str, int] = {}
    for _ in range(5):
        assert vn.record_failures(failures, ["a", "b"], ["ok"], 6) == []
    assert failures == {"a": 5, "b": 5}
    assert vn.record_failures(failures, ["a", "b"], ["ok"], 6) == ["a", "b"]
    assert failures == {}  # given-up ids leave the counter (they go to .seen)


def test_success_resets_the_count():
    failures = {"a": 5}
    assert vn.record_failures(failures, [], ["a"], 6) == []
    assert failures == {}


def test_a_run_where_nothing_parsed_does_not_count():
    # A site-wide HTML change or a block: counting would drop every page.
    failures = {"a": 5}
    assert vn.record_failures(failures, ["a", "b"], [], 6) == []
    assert failures == {"a": 5}


def test_zero_means_retry_forever():
    failures: dict[str, int] = {}
    for _ in range(10):
        assert vn.record_failures(failures, ["a"], ["ok"], 0) == []
    assert failures == {}


def test_vietnamfinance_ids():
    src = vn.SOURCES["vietnamfinance"]
    url = "https://vietnamfinance.vn/gelex-vuot-88-ke-hoach-loi-nhuan-nam-2026-sau-9-thang-d152776.html"
    assert src.article_id(url) == "vietnamfinance-152776"
    assert src.month_sitemap_urls([], 2020, 1) == []  # forward-only: current month only


def test_congthuong_ids_and_sitemaps():
    src = vn.SOURCES["congthuong"]
    url = "https://congthuong.vn/thanh-hoa-siet-chat-an-toan-thuc-pham-478777.html"
    assert src.article_id(url) == "congthuong-478777"
    index = ["https://congthuong.vn/sitemap-article-2026-9-1.xml",
             "https://congthuong.vn/sitemap-article-2026-9-2.xml",
             "https://congthuong.vn/sitemap-article-2026-10-1.xml"]
    assert src.month_sitemap_urls(index, 2026, 9) == index[:2]  # past month: its parts only


def test_congthuong_skips_lottery():
    src = vn.SOURCES["congthuong"]
    html = """<html><head><meta property="article:section" content="Xổ số miền Nam XSMN">
    <meta property="article:published_time" content="2026-10-28T16:30:00+07:00"></head>
    <body><h1 class="article-detail-title">XSMN 28/10</h1>
    <div itemprop="articleBody"><p>""" + "Kết quả xổ số. " * 40 + """</p></div></body></html>"""
    out = src.parse_article(html, "https://congthuong.vn/xsmn-28-10-478900.html", None)
    assert isinstance(out, vn.Skipped) and out.article_id == "congthuong-478900"
