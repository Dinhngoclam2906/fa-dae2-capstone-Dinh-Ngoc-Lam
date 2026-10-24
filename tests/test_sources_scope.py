"""The "My sources" scope: the user's chosen sites filter every search unless the
question (or a tool call) names a source."""

from datetime import date

import pytest

from core import articles
from core.agent import scope_note
from core.retrieval import build_filters
from core.tools import _scope

TODAY = date(2026, 10, 24)
MINE = ["tuoitre", "vnexpress"]


def test_scope_applies_when_the_question_names_no_source():
    dense, kw, parsed = build_filters("gold prices", {}, MINE, today=TODAY)
    assert dense == kw == {"source": MINE}
    assert parsed == {}


def test_a_source_named_in_the_question_wins_over_the_scope():
    dense, _, parsed = build_filters("What did the Guardian say about gold?", {}, MINE, today=TODAY)
    assert dense["source"] == ["guardian"]
    assert parsed["source"] == ["guardian"]


def test_scope_keeps_a_period_from_the_question():
    dense, _, _ = build_filters("giá vàng tháng 9/2026", {}, MINE, today=TODAY)
    assert dense == {"source": MINE, "start_date": "2026-09-01", "end_date": "2026-09-30"}


def test_explicit_filters_win_and_scope_fills_the_source():
    dense, _, parsed = build_filters("Guardian gold", {"language": "vi", "source": None}, MINE, today=TODAY)
    assert parsed == {}  # explicit filters: the question isn't parsed
    assert dense == {"language": "vi", "source": MINE}
    dense, _, _ = build_filters("gold", {"source": "thanhnien"}, MINE, today=TODAY)
    assert dense == {"source": "thanhnien"}


def test_no_scope_is_the_old_behaviour():
    assert build_filters("gold prices", {}, None, today=TODAY)[0] == {}
    assert build_filters("gold prices", {}, [], today=TODAY)[0] == {}


def test_source_list_validates():
    assert articles.source_list(None) == []
    assert articles.source_list("guardian") == ["guardian"]
    assert articles.source_list(MINE) == MINE
    with pytest.raises(ValueError):
        articles.source_list(["tuoitre", "nytimes"])


def test_tool_scope_rule():
    cfg = {"configurable": {"sources": MINE}}
    assert _scope(None, cfg) == MINE          # the LLM left source empty: the user's sites
    assert _scope("guardian", cfg) == "guardian"  # the LLM named one (the user asked)
    assert _scope(None, None) is None         # no scope: all sources
    assert _scope(None, {"configurable": {"sources": None}}) is None


def test_scope_note_names_the_sites():
    note = scope_note(MINE)
    assert "Tuổi Trẻ (tuoitre)" in note and "VnExpress (vnexpress)" in note
    assert scope_note(None) == "" and scope_note([]) == ""
