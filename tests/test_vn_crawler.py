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
