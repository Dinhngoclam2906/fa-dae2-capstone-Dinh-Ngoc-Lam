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


def test_congthuong_emagazine_body():
    src = vn.SOURCES["congthuong"]
    html = """<html><head><meta property="article:section" content="Công Thương 57">
    <meta property="article:published_time" content="2026-10-27T14:09:00+07:00"></head>
    <body><h1>E-magazine | Nghị quyết số 57</h1>
    <article class="mastercms-emag-article edittor-content content-full-width"><p>""" + "Chuyển đổi số. " * 40 + """</p></article>
    </body></html>"""
    out = src.parse_article(html, "https://congthuong.vn/e-magazine-nghi-quyet-so-57-478524.html", None)
    assert isinstance(out, vn.Article) and len(out.body_text) > 120


def test_ven_listing_links():
    src = vn.SOURCES["ven"]
    page = """<a href="/moit-launches-innovation-campaign-59965.html">a</a>
    <a href="/moit-launches-innovation-campaign-59965.html#c">dup</a>
    <a href="https://ven.congthuong.vn/emirates-marks-10-years-of-hanoi-operations-59960.html">b</a>
    <a href="https://congthuong.vn/other-site-478777.html">other host</a>
    <a href="/industry">category</a><a href="tel:0866594498">tel</a>"""
    assert src.listing_links(page) == [
        "https://ven.congthuong.vn/moit-launches-innovation-campaign-59965.html",
        "https://ven.congthuong.vn/emirates-marks-10-years-of-hanoi-operations-59960.html",
    ]
    assert src.article_id("https://ven.congthuong.vn/emirates-marks-10-years-59960.html") == "ven-59960"
    assert src.month_sitemap_urls([], 2020, 1) == []  # forward-only: current month only


def test_ven_skips_backlog():
    src = vn.SOURCES["ven"]
    page = """<html><head><meta property="article:section" content="Trade">
    <meta property="article:published_time" content="{}"></head><body><h1>T</h1>
    <div itemprop="articleBody"><p>""" + "Trade grew. " * 40 + """</p></div></body></html>"""
    url = "https://ven.congthuong.vn/trade-59000.html"
    old = src.parse_article(page.format("2025-11-02T08:00:00+07:00"), url, None)
    assert isinstance(old, vn.Skipped)
    new = vn.datetime.now(vn.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    assert isinstance(src.parse_article(page.format(new), url, None), vn.Article)
