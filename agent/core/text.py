"""Query heuristics: keyword extraction, retrieve-or-chat, canned answers.

The English rules are the ones the original app shipped with (tuned on the Guardian eval).
Vietnamese is new: its words are short syllables ("giá xăng dầu"), so the English rule
"keep words of 5+ letters" would drop almost everything; Vietnamese queries use pairs of
adjacent content syllables as keywords instead.
"""

from __future__ import annotations

import re

_VI_CHARS = re.compile(
    r"[ăâđêôơưạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹáàãéèíìóòõúùýĂÂĐÊÔƠƯ]"
)

IMPORTANT_SHORT_TERMS = {
    "ai", "uk", "us", "eu", "un", "ceo", "fbi", "cia", "gdp", "nhs", "cop", "g7", "g20",
    "ufo", "nyc", "la", "qa", "hr", "it", "pr", "q1", "q2", "q3", "q4", "gop", "mlb", "nba",
    "nfl", "ufc", "war", "ban", "law", "tax", "net", "fdi", "vn", "hcm", "asean",
}

_EN_EXTRA_STOPS = {
    "based", "article", "articles", "news", "tell", "according", "say", "compare", "analyze",
    "summarize", "describe", "explain", "show", "find", "give", "share", "discuss", "evaluate",
    "review", "examine", "explore", "investigate", "detail", "check", "whether", "discussing",
    "search", "look", "want", "need", "could", "would", "should", "latest", "recent", "about",
    "today", "yesterday", "tonight", "weekend", "week", "month",
}

# Function words and question words that carry no topic.
_VI_STOPS = {
    "của", "và", "là", "có", "các", "những", "cho", "với", "trong", "được", "này", "đó", "về",
    "như", "thì", "mà", "để", "từ", "khi", "đã", "đang", "sẽ", "không", "gì", "nào", "sao",
    "thế", "nào", "bao", "nhiêu", "hãy", "cho", "tôi", "mình", "bạn", "biết", "xem", "tin",
    "tức", "bài", "báo", "viết", "mới", "nhất", "hôm", "nay", "gần", "đây", "ra", "một",
    "ở", "lại", "rồi", "vậy", "ạ", "nhé", "giúp", "tìm", "kiếm", "những", "nói", "hỏi",
}

_STOPWORDS: set[str] | None = None


def english_stopwords() -> set[str]:
    global _STOPWORDS
    if _STOPWORDS is None:
        try:
            import nltk
            from nltk.corpus import stopwords
            try:
                base = set(stopwords.words("english"))
            except LookupError:
                nltk.download("stopwords", quiet=True)
                base = set(stopwords.words("english"))
        except Exception:  # noqa: BLE001 - keyword search degrades, it doesn't break
            base = set()
        _STOPWORDS = base | _EN_EXTRA_STOPS
    return _STOPWORDS


def is_vietnamese(text: str) -> bool:
    return bool(_VI_CHARS.search(text))


def _vietnamese_keywords(query: str) -> list[str]:
    words = [w for w in re.findall(r"\w+", query.lower()) if w not in _VI_STOPS]
    pairs = [f"{a} {b}" for a, b in zip(words, words[1:])]
    # Pairs first (they're specific), then long single syllables / Latin terms as backup.
    singles = [w for w in words if len(w) >= 5 or w in IMPORTANT_SHORT_TERMS]
    out: list[str] = []
    for k in pairs + singles:
        if k not in out:
            out.append(k)
    return out[:5]


def extract_keywords(query: str) -> list[str]:
    """Up to 5 keywords for the BigQuery keyword search ([] = skip keyword search)."""
    q = query.lower()
    if any(w in q for w in ("data source", "loaded", "freshness", "how many articles")):
        return []
    if is_vietnamese(query):
        return _vietnamese_keywords(query)

    stop = english_stopwords()
    keywords: list[str] = []
    from_names: set[str] = set()
    for name in re.findall(r"\b([A-Z][a-z]+(?: [A-Z][a-z]+)+)\b", query):
        parts = name.split()
        surname = parts[-1].lower()
        if surname not in stop:
            keywords.append(surname)
            from_names.update(p.lower() for p in parts)
    for name in re.findall(r"\b[A-Z][a-z]{3,}\b", query):
        n = name.lower()
        if n not in stop and n not in keywords and n not in from_names:
            keywords.append(n)
    for word in re.findall(r"\b\w+\b", q):
        if word in keywords or word in from_names:
            continue
        # 4+ letters (the original kept 5+, which dropped fuel, gold, rice, flood, bank...)
        if word in IMPORTANT_SHORT_TERMS or (word not in stop and len(word) >= 4):
            keywords.append(word)

    keywords = [k for k in dict.fromkeys(keywords) if len(k) >= 4 or k in IMPORTANT_SHORT_TERMS]
    return keywords[:5]


_FACTUAL = [
    r"\bwhat (do|does|did|are|is|was|were)\b", r"\bhow (do|does|did|can|could|should|would)\b",
    r"\b(why|when|where|which|who)\b", r"\b(medical|experts?|doctors?|scientists?|researchers?)\b",
    r"\b(guardian|article|news|coverage|report|latest)\b", r"\b(show|find|search|get|fetch|retrieve)\b",
    r"\b(compare|analyze|summarize)\b",
]
_GREETINGS = ("hi", "hello", "hey", "good morning", "good afternoon", "good evening", "howdy",
              "greetings", "xin chào", "chào", "alo")
_ACKS = ("thanks", "thank you", "ok", "okay", "got it", "understood", "bye", "goodbye", "see you",
         "cool", "nice", "great", "perfect", "cảm ơn", "cám ơn", "ok nhé", "được rồi", "tạm biệt")
_REACTIONS = [
    r"^(that'?s?|it'?s?|this is) (interesting|fascinating|cool|great|nice|good)\s*[!.]*$",
    r"^i (agree|see|understand)\s*[!.]*$", r"^(makes sense|got it|understood)\s*[!.]*$",
    r"^(yes|yep|yeah|no|nope|vâng|dạ|ừ|không),?\s*[!.]*$",
]


# (pattern, days): the first match sets the window. Dense search has no sense of time,
# so "latest gold price" ranked January articles first (2026-10-20); the old hybrid got
# recency for free from the keyword search's 90-day default window.
_RECENCY = [
    (r"\b(today|tonight|yesterday|this morning|breaking)\b|hôm nay|hôm qua|sáng nay|tối nay|chiều nay", 3),
    (r"\b(this|past|last) week\b|\bthis weekend\b|\b(past|last) few days\b|tuần này|tuần qua|cuối tuần"
     r"|mấy ngày qua|vài ngày qua|những ngày qua|mấy hôm nay", 7),
    (r"\b(this|past|last) month\b|tháng này|tháng qua", 31),
    (r"\b(latest|recent|recently|newest|right now|nowadays)\b"
     r"|mới nhất|gần đây|hiện nay|hiện tại|cập nhật|vừa qua|mới đây", 30),
]


def recency_days(query: str) -> int | None:
    """Days of recent news the question asks for (\"latest ...\", \"tuần này\"), else None."""
    q = query.lower()
    for pattern, days in _RECENCY:
        if re.search(pattern, q):
            return days
    return None


# Explicit sources and periods in a question ("What did VnExpress International write about
# X in August 2026?", "báo Tuổi Trẻ tháng 8/2026"), turned into search filters. Rules, not
# an LLM call: free, instant and predictable; the LLM's tools cover anything else.
# Most specific first. "tuổi trẻ" also means "youth" and "thanh niên" "young people", so
# those two need the capitalised name (case-sensitive) or "báo" (newspaper) in front.
_SOURCE_PATTERNS = [
    (r"(?i)tuoi tre news|tuổi trẻ news", ["tuoitre_en"]),
    (r"(?i)vnexpress international|e\.vnexpress", ["vnexpress_en"]),
    (r"(?i)vietnamnet global", ["vietnamnet_en"]),
    (r"(?i)vneconomy \(?english\)?|en\.vneconomy", ["vneconomy_en"]),
    (r"(?i)dtinews|dân trí english|dan tri english", ["dantri_en"]),
    (r"\bVIR\b|(?i:vietnam investment review|vir\.com\.vn)", ["vir"]),
    # One word only: "Vietnam finance" is also an ordinary topic.
    (r"(?i)vietnamfinance", ["vietnamfinance"]),
    # Before congthuong: VEN lives at ven.congthuong.vn. Upper-case only, like VIR.
    (r"\bVEN\b|(?i:vietnam economic news|ven\.congthuong)", ["ven"]),
    # "Công Thương" is also the ministry (Bộ Công Thương): only the paper or the domain.
    (r"(?i)báo công thương|bao cong thuong|congthuong|cong thuong newspaper|công thương newspaper", ["congthuong"]),
    # "đầu tư" is the ordinary word for investment: only the paper or the domain.
    (r"(?i)báo đầu tư|bao dau tu|baodautu", ["baodautu"]),
    (r"(?i)\bguardian\b", ["guardian"]),
    (r"Tuổi Trẻ|Tuoi Tre|TUỔI TRẺ|(?i:báo tuổi trẻ|báo tuoi tre)", ["tuoitre", "tuoitre_en"]),
    (r"Thanh Niên|Thanh Nien|THANH NIÊN|(?i:báo thanh niên|báo thanh nien)", ["thanhnien"]),
    # "dân trí" is also a common phrase (public awareness, "nâng cao dân trí").
    # Dân Trí itself was dropped (2026-10-27: it refuses Cloud Run), so its name scopes to DTiNews.
    (r"Dân Trí|Dan Tri|DÂN TRÍ|(?i:báo dân trí|báo dan tri|dantri)", ["dantri_en"]),
    (r"(?i)vneconomy", ["vneconomy", "vneconomy_en"]),
    (r"(?i)vietnamnet", ["vietnamnet", "vietnamnet_en"]),
    (r"(?i)vnexpress", ["vnexpress", "vnexpress_en"]),
]
_MONTHS = {name: i for i, names in enumerate(
    [("january", "jan"), ("february", "feb"), ("march", "mar"), ("april", "apr"), ("may",),
     ("june", "jun"), ("july", "jul"), ("august", "aug"), ("september", "sep", "sept"),
     ("october", "oct"), ("november", "nov"), ("december", "dec")], 1) for name in names}
_MONTH_RE = "|".join(sorted(_MONTHS, key=len, reverse=True))


def query_sources(query: str) -> list[str]:
    """Source names the question asks for, in order of mention ([] = any source)."""
    found: list[str] = []
    rest = query
    for pattern, sources in _SOURCE_PATTERNS:
        if re.search(pattern, rest):
            found += [s for s in sources if s not in found]
            rest = re.sub(pattern, " ", rest)  # "VnExpress International" must not match "vnexpress" again
    return found


def _month_span(year: int, month: int):
    from datetime import date, timedelta
    start = date(year, month, 1)
    return start, (start + timedelta(days=32)).replace(day=1) - timedelta(days=1)


def query_period(query: str, today=None):
    """(start, end) dates for an explicit month or year in the question, else None.
    A month without a year is its latest occurrence up to today."""
    from datetime import date
    today = today or date.today()
    q = query.lower()

    def month_without_year(m: int):
        return _month_span(today.year if m <= today.month else today.year - 1, m)

    m = re.search(r"tháng\s*(\d{1,2})(?:\s*(?:năm|/|-|\.)\s*((?:19|20)\d{2}))?", q)
    if m and 1 <= int(m.group(1)) <= 12:
        return _month_span(int(m.group(2)), int(m.group(1))) if m.group(2) else month_without_year(int(m.group(1)))
    # English month names: "August 2026", or "in / during / since August" ("may" alone is a verb).
    m = (re.search(rf"\b({_MONTH_RE})\.?,?\s+((?:19|20)\d{{2}})\b", q)
         or re.search(rf"\b(?:in|during|of|since|from|for)\s+({_MONTH_RE})\b(?!\s+(?:be|have|not)\b)", q))
    if m:
        month = _MONTHS[m.group(1)]
        year = m.group(2) if m.lastindex and m.lastindex >= 2 else None
        return _month_span(int(year), month) if year else month_without_year(month)
    if re.search(r"\bthis year\b|năm nay", q):
        return date(today.year, 1, 1), today
    if re.search(r"\blast year\b|năm ngoái|năm trước", q):
        return date(today.year - 1, 1, 1), date(today.year - 1, 12, 31)
    m = re.search(r"(?:\b(?:in|during|of|since|from|for|năm)\s+|năm\s*)((?:19|20)\d{2})\b", q)
    if m:
        y = int(m.group(1))
        return date(y, 1, 1), date(y, 12, 31)
    return None


def query_filters(query: str, today=None) -> dict:
    """Search filters stated in the question: {"source": [...], "start_date", "end_date"}."""
    out: dict = {}
    sources = query_sources(query)
    if sources:
        out["source"] = sources
    period = query_period(query, today)
    if period:
        out["start_date"], out["end_date"] = period[0].isoformat(), period[1].isoformat()
    return out


def should_retrieve(query: str) -> bool:
    """False for greetings, thanks and small talk; True for anything that needs articles."""
    q = query.lower().strip()
    n = len(q.split())
    if any(re.search(p, q) for p in _FACTUAL) or ("?" in q and n > 3):
        return True
    if n <= 4 and any(q == g or q.startswith(g + " ") or q.startswith(g + ",") for g in _GREETINGS):
        return False
    if re.search(r"^\s*(who|what) are you\b|^\s*what can you do\b|^\s*(tell me about|introduce) yourself\b", q):
        return False
    if n <= 5 and "?" not in q and any(a in " ".join(q.split()[:3]) for a in _ACKS):
        return False
    if n <= 6 and any(re.match(p, q) for p in _REACTIONS):
        return False
    if n <= 2 and "?" not in q and not is_vietnamese(q):
        return False
    return True


def is_conversational_followup(query: str) -> bool:
    """A short reaction to the previous answer ("interesting!", "yes", "I agree")."""
    q = query.lower().strip()
    words = q.split()
    if not words or "?" in q:
        return False
    phrases = ("interesting", "fascinating", "cool", "great", "nice", "i agree", "i see",
               "makes sense", "good point", "i found", "i think", "thú vị", "hay quá", "đồng ý")
    if len(words) <= 7 and any(p in q for p in phrases):
        return True
    return words[0] in {"yes", "yep", "yeah", "ok", "okay", "sure", "no", "nope", "vâng", "dạ", "ừ"} and len(words) <= 4


_REFERS_BACK = re.compile(
    r"\b(the|that|this|those|these) (first|second|third|last|previous|above|same) (one|article|story|stor(y|ies)|result)s?\b"
    r"|\b(first|second|third|last) (one|article|story|result)\b"
    r"|\b(that|this|those|these) (article|story|stories|one|ones|piece)s?\b"
    r"|\b(summari[sz]e|explain|translate|expand on|tell me more about) (it|that|this|them)\b"
    r"|bài (đầu tiên|thứ (hai|ba|nhất)|cuối|đó|này|trên|vừa rồi)|tin (đó|này|trên)|(tóm tắt|dịch|giải thích) (nó|bài đó|bài này)"
)


def refers_to_previous(query: str) -> bool:
    """The message points at the previous answer's articles ("summarise the first one",
    "tóm tắt bài đầu tiên"), so re-searching on its words would lose them."""
    return bool(_REFERS_BACK.search(query.lower()))


INTRO = (
    "I'm a news assistant for The Guardian and fifteen Vietnamese news sites and editions "
    "(Tuổi Trẻ, Thanh Niên, VnEconomy, VietNamNet, VnExpress and their English "
    "editions, plus Vietnam Investment Review, VietnamFinance, Báo Đầu tư and Công Thương with its English edition VEN), "
    "hundreds of thousands of articles, updated every 4 hours. I can find articles by topic, date, "
    "source or section, summarise them, and show what's trending. I answer in English or "
    "Vietnamese.\n\nTry: \"What's the latest on fuel prices in Vietnam?\" or "
    "\"Tin mới nhất về giá vàng?\""
)


def instant_response(query: str) -> str | None:
    """A canned answer for questions about the assistant itself (no LLM call)."""
    q = query.lower().strip()
    if re.search(r"\b(who are you|what can you do|what do you do)\b|bạn là ai|bạn làm được gì", q):
        return INTRO
    return None
