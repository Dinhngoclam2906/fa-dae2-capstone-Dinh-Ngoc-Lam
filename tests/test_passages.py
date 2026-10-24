"""core.passages: BM25 passage rescue over the dense hits' bodies."""

from core import passages


def test_split_short_body_is_one_passage_with_title():
    assert passages.split("Title", "short body") == ["Title. short body"]
    assert passages.split("Title", "") == ["Title. "]


def test_split_long_body_overlaps():
    body = "x" * 1000
    out = passages.split("T", body)
    # windows start every PASSAGE_STEP (250) chars, each PASSAGE_CHARS (500) long
    assert len(out) == 3
    assert all(p.startswith("T. ") for p in out)
    assert len(out[0]) == len("T. ") + passages.PASSAGE_CHARS


def test_tokens_adds_word_pairs():
    assert passages.tokens("Giá vàng") == ["giá", "vàng", "giá_vàng"]


def test_bm25_scores_only_matching_docs():
    docs = [passages.tokens("rice exports rose"), passages.tokens("football results")]
    scores = passages.bm25("rice exports", docs)
    assert scores[0] > 0 and scores[1] == 0


def test_rescue_keeps_dense_head_then_ranks_by_passage():
    items = [
        ("Weather", "sunny all week"),
        ("Markets", "stocks were flat"),
        ("Farming", "the rice harvest beat forecasts in the Mekong delta"),
    ]
    ranked = passages.rescue("rice harvest Mekong", items, keep=1)
    assert [i for i, _ in ranked] == [0, 2, 1]
    assert "rice harvest" in dict(ranked)[2]


def test_rescue_ignores_time_words():
    # "latest" / "news" would otherwise reward any passage that says "latest news"
    items = [("A", "latest news update"), ("B", "nothing relevant"), ("C", "rice prices fell")]
    ranked = passages.rescue("latest news on rice prices", items, keep=0)
    assert ranked[0][0] == 2
