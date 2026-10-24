"""Passage rescue: find details beyond the ~1,500 characters the article vectors see.

The dense search's top `settings.passage_pool` articles are fetched in full
(`articles.get_bodies`, ~25 MB), split into overlapping passages, and every passage is
scored against the question with BM25 (idf over this pool's passages; time words such as
"this week" / "mới nhất" are dropped from the query, they say when, not what). The dense
top `settings.passage_keep` stay as they are; the remaining slots go to the other articles
in order of their best passage. Every article's best passage is returned for the context.

Why a rescue slot and not a full rerank: fusing the passage rank into the whole list
(reciprocal rank fusion) gave the same known-item gains but pushed older keyword-matching
articles over recent on-topic ones (time_relative strict 0.23 -> 0.15). Why BM25 and not a
cross-encoder or chunk vectors: the questions it rescues hinge on exact names and numbers
deep in the body, which BM25 matches for free; chunk vectors for the archive would be
~$60-70 plus a paid Pinecone tier. Hard eval, 2026-10-21 (docs/hard_eval.md): deep_detail
found@5 0.50 -> 0.70, entity_number 0.85 -> 0.95, topical unchanged.
Vietnamese words are mostly two syllables, so syllable bigrams are indexed too.
"""

from __future__ import annotations

import math
import re
import unicodedata
from collections import Counter

PASSAGE_CHARS = 500
PASSAGE_STEP = 250
_TOKEN = re.compile(r"\w+", re.UNICODE)
_TIME_WORDS = re.compile(
    r"(?i)\b(this week|last week|latest|today|yesterday|recent(ly)?|in the last few days|"
    r"the last few days|past few days|news)\b|tuần này|tuần qua|mới nhất|hôm nay|hôm qua|"
    r"mấy ngày qua|gần đây|vài ngày qua|tin tức")


def tokens(text: str) -> list[str]:
    words = _TOKEN.findall(unicodedata.normalize("NFC", text.lower()))
    return words + [f"{a}_{b}" for a, b in zip(words, words[1:])]


def split(title: str, body: str) -> list[str]:
    """Overlapping windows of the body, each prefixed with the title."""
    body = body or ""
    out = [f"{title}. {body[i:i + PASSAGE_CHARS]}"
           for i in range(0, max(len(body) - PASSAGE_STEP, 1), PASSAGE_STEP)]
    return out or [title]


def bm25(query: str, docs: list[list[str]], k1: float = 1.2, b: float = 0.75) -> list[float]:
    n = len(docs)
    avg = sum(map(len, docs)) / max(n, 1)
    df = Counter(t for d in docs for t in set(d))
    terms = set(tokens(query))
    scores = []
    for d in docs:
        tf = Counter(d)
        s = 0.0
        for t in terms:
            if tf[t]:
                idf = math.log(1 + (n - df[t] + 0.5) / (df[t] + 0.5))
                s += idf * tf[t] * (k1 + 1) / (tf[t] + k1 * (1 - b + b * len(d) / avg))
        scores.append(s)
    return scores


def rescue(query: str, items: list[tuple[str, str]], keep: int) -> list[tuple[int, str]]:
    """items = [(title, body)] in dense order -> [(index into items, best passage)]: the
    first `keep` in dense order, then the rest by best passage score (ties keep dense
    order). An item without a body scores 0 on passages."""
    docs, owner, texts = [], [], []
    for i, (title, body) in enumerate(items):
        for p in split(title, body):
            docs.append(tokens(p))
            owner.append(i)
            texts.append(p)
    best = [(0.0, "")] * len(items)
    for o, s, p in zip(owner, bm25(_TIME_WORDS.sub(" ", query), docs), texts):
        if s > best[o][0]:
            best[o] = (s, p)
    head = list(range(min(keep, len(items))))
    tail = sorted(range(len(head), len(items)), key=lambda i: -best[i][0])
    return [(i, best[i][1]) for i in head + tail]
