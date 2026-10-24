"""core.retrieval and core.agent helpers that need no network."""

from langchain_core.messages import AIMessage, HumanMessage

from core import retrieval as R
from core.agent import recent_turns


def _c(article_id, text):
    return R.Candidate(text=text, article_id=article_id)


def test_dedupe_by_id_and_by_text():
    out = R._dedupe([_c("a", "Gold rises"), _c("a", "other text"), _c("b", "Gold rises"), _c("c", "Rice")])
    assert [c.article_id for c in out] == ["a", "c"]


def test_interleave_alternates_and_keeps_leftovers():
    a, b = [_c("a1", "1"), _c("a2", "2"), _c("a3", "3")], [_c("b1", "4")]
    assert [c.article_id for c in R._interleave(a, b)] == ["a1", "b1", "a2", "a3"]


def test_pinecone_filter_sources_and_local_dates():
    f = R._pinecone_filter({"source": ["vnexpress", "vnexpress_en"], "language": "vi",
                            "start_date": "2026-08-01", "end_date": "2026-08-31"})
    assert f["source_name"] == {"$in": ["vnexpress", "vnexpress_en"]}
    assert f["language"] == {"$eq": "vi"}
    # Vietnam local days (UTC+7): 2026-08-01 00:00 local = 2026-07-31 17:00 UTC
    assert f["pub_ts"] == {"$gte": 1785517200, "$lt": 1788195600}


def test_pinecone_filter_empty():
    assert R._pinecone_filter({}) is None


def test_format_context_numbers_and_labels_sources():
    c = R.Candidate(text="Body", article_id="x1", title="Gold rises", source_name="tuoitre",
                    language="vi", published_date="2026-10-24", url="https://example.org/a", score=0.5)
    out = R.format_context([c])
    assert out.startswith("[Source 1 — 📰 tuoitre (vi): Gold rises]")
    assert "article_id: x1" in out and "Relevance: 0.500" in out


def test_recent_turns_cuts_at_a_user_message():
    msgs = [HumanMessage("q1"), AIMessage("a1"), HumanMessage("q2"), AIMessage("a2"), HumanMessage("q3")]
    assert [m.content for m in recent_turns(msgs, 2)] == ["q2", "a2", "q3"]
    assert recent_turns(msgs, 5) == msgs
