"""
hard_eval.py - a HARD question set for the agent (2026-10-20, after the v2 cut-over).

The 1c set (retrieval_eval.py) paraphrases each article's lead, which favours dense search
and borrows the article's own time words. This set targets the known weak spots:

  known-item (score: is the seed article in the top 5 / cited?)
    keyword_short  2-5 words a user would type ("SJC vàng miếng")
    entity_number  hinges on one exact name + number from the lead
    deep_detail    a detail from body characters 3,000-4,800 (article vectors see ~1,500)
  topical (score: LLM judge, strict = on topic AND inside every stated constraint)
    time_relative  "this week" / "mấy ngày qua" / "latest" about a 0-3-day-old story
    filtered       names a source and/or a month / year ("What did Tuổi Trẻ write about X
                   in August 2026?"); also checked programmatically against source + dates

Systems: agent_default (production automatic retrieval), v2_dense (no recency rule),
keyword (the keyword leg alone) and agent_e2e (the whole agent with tools, scored on the
articles its answer cites). Questions and judging use gpt-4.1-mini, not the agent's model.
Time-relative questions only hold on the day they were written (`asked_on`): run them then.

    uv run --env-file .env python ai_agents/eval/hard_eval.py sample
    uv run --env-file .env python ai_agents/eval/hard_eval.py questions
    uv run --env-file .env python ai_agents/eval/hard_eval.py show      # read them
    uv run --env-file .env python ai_agents/eval/hard_eval.py run
    uv run --env-file .env python ai_agents/eval/hard_eval.py judge
    uv run --env-file .env python ai_agents/eval/hard_eval.py report
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))  # ai_agents/, for core.*

CACHE = Path("data/eval/hard")
SAMPLE = CACHE / "sample.parquet"
QUESTIONS = CACHE / "questions.json"
RUNS = CACHE / "runs.jsonl"
JUDGED = CACHE / "judgements.json"
URL_IDS = CACHE / "url_ids.json"
TRACKED_QUESTIONS = HERE / "hard_eval_questions.json"
RESULTS = HERE / "hard_eval_results.json"

GEN_MODEL = "gpt-4.1-mini"  # writes and judges; not the agent's model
TOP_N = 5
SYSTEMS = ["agent_default", "agent_nopassage", "agent_noparse", "v2_dense", "keyword", "agent_e2e",
           "agent_default_before", "agent_e2e_before"]
# Run by default; the *_before systems hold rows recorded before a change and can't be
# re-run: agent_e2e_before before the query parser (2026-10-20), agent_default_before
# before the passage rerank (2026-10-20 runs; agent_nopassage is the same-day baseline).
RUN_SYSTEMS = ["agent_default", "agent_nopassage", "agent_noparse", "v2_dense", "keyword", "agent_e2e"]
KNOWN = ("keyword_short", "entity_number", "deep_detail")
TOPICAL = ("time_relative", "filtered")
SOURCE_NAMES = {"guardian": "The Guardian", "tuoitre": "Tuổi Trẻ", "thanhnien": "Thanh Niên",
                "vneconomy": "VnEconomy", "vietnamnet": "VietNamNet", "vnexpress": "VnExpress",
                "tuoitre_en": "Tuoi Tre News", "vneconomy_en": "VnEconomy (English)",
                "vietnamnet_en": "VietNamNet Global", "vnexpress_en": "VnExpress International"}
# (phrase, language, window in days back from asked_on). Some are deliberately outside the
# recency_days patterns ("in the last few days", "mấy ngày qua").
TIME_PHRASES = [("this week", "en", 7), ("in the last few days", "en", 4), ("latest", "en", 14),
                ("today or yesterday", "en", 2), ("tuần này", "vi", 7), ("mấy ngày qua", "vi", 4),
                ("mới nhất", "vi", 14), ("hôm nay", "vi", 2)]
_URL = re.compile(r"https?://[^\s)\]>\"'*]+")


def _today_local() -> date:
    return (datetime.now(timezone.utc) + timedelta(hours=7)).date()


# ---------------------------------------------------------------------------- sample
def cmd_sample(_):
    """Per source: fresh (0-3 days), 2026, and older (Guardian 2024-25) pools, long and short."""
    from core import bq
    sql = f"""
    WITH a AS (
      SELECT article_id, source_name, language, web_title, web_url, word_count,
             DATE(web_publication_date_local) AS pub_date,
             LEFT(body_text, 1500) AS lead, SUBSTR(body_text, 3001, 1800) AS deep,
             LENGTH(body_text) AS body_len,
             CASE WHEN web_publication_date >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)) THEN 'fresh'
                  WHEN web_publication_date >= TIMESTAMP('2026-03-01') THEN 'y2026'
                  ELSE 'older' END AS bucket
      FROM `{os.environ['GCP_PROJECT']}.marts.obt_articles`
      WHERE web_publication_date >= TIMESTAMP('2024-01-01') AND word_count >= 120
    )
    SELECT * FROM a
    QUALIFY ROW_NUMBER() OVER (PARTITION BY source_name, bucket, body_len >= 5000
                               ORDER BY FARM_FINGERPRINT(CONCAT(article_id, 'hard-1'))) <= 12
    """
    rows = bq.query(sql, max_bytes=4_000_000_000, timeout=240)
    CACHE.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(rows)
    df.write_parquet(SAMPLE)
    print(df.group_by("language", "bucket", pl.col("body_len") >= 5000).len().sort("language", "bucket"))
    print(f"billed MB: {bq.usage['bytes_billed'] / 1e6:.0f}")


# ------------------------------------------------------------------------- questions
PROMPTS = {
    "keyword_short": """You write test queries for a news search engine.
Write the 2-5 word search query in {lang} that a reader looking for THIS article would type
into a search box (the key name, place or topic words, no full sentence, no question mark).
Make it specific enough that this article is a good result. Return exactly ONE query on
one line, nothing else.

Title: {title}
Article start: {lead}""",
    "entity_number": """You write test questions for a news search assistant.
Write ONE natural question in {lang} whose answer is a specific NUMBER, amount, percentage or
date that appears in this article, and which names the specific person, company, place or
organisation involved. Do not copy the headline. {standalone} Return only the question.

Title: {title}
Article start: {lead}""",
    "deep_detail": """You write test questions for a news search assistant.
Below are the START of a news article and a LATER passage from the same article. Write ONE
natural question in {lang} about a specific detail that appears in the LATER passage and is
NOT mentioned in the start. Mention enough context (who / what) that a reader could ask it
without having seen the article, but do not copy the headline. {standalone} Return only
the question.

Title: {title}
Start: {lead}
Later passage: {deep}""",
    "time_relative": """You write test questions for a news search assistant.
Write ONE natural question in {lang} asking about the general TOPIC of this article and
containing the time phrase "{phrase}" (for example "What has happened with ... {phrase}?").
Do not use details that only this article has. Return only the question.

Title: {title}
Article start: {lead}""",
    "filtered": """You write test questions for a news search assistant.
Write ONE natural question in {lang} about the general TOPIC of this article that includes
this constraint, worded naturally: {constraint}. Do not use details that only this article
has. Return only the question.

Title: {title}
Article start: {lead}""",
}


STANDALONE = ("It must make sense to someone who has NOT seen the article: never refer to "
              "\"the author\", \"the article\", \"the writer\", \"tác giả\", \"bài viết\" or an unnamed "
              "\"he / she / ông / bà\"; name the people and things involved.")


def _pick(df: pl.DataFrame, rng: random.Random, used: set, n: int, **cond) -> list[dict]:
    """n seeds round-robin over sources, matching the column conditions."""
    f = df
    for k, v in cond.items():
        f = f.filter(pl.col(k).is_in(v) if isinstance(v, (list, tuple)) else pl.col(k) == v)
    pool: dict[str, list[dict]] = {}
    for r in f.iter_rows(named=True):
        if r["article_id"] not in used:
            pool.setdefault(r["source_name"], []).append(r)
    for rows in pool.values():
        rng.shuffle(rows)
    out, sources = [], sorted(pool)
    while len(out) < n and any(pool.values()):
        for s in sources:
            if pool[s] and len(out) < n:
                r = pool[s].pop()
                used.add(r["article_id"])
                out.append(r)
    return out


def _month_bounds(d: date) -> tuple[date, date]:
    start = d.replace(day=1)
    nxt = (start + timedelta(days=32)).replace(day=1)
    return start, nxt - timedelta(days=1)


def _filtered_constraint(r: dict, variant: str) -> tuple[str, dict]:
    """The constraint text for the generator and its machine-checkable form."""
    d = date.fromisoformat(str(r["pub_date"])[:10])
    src = SOURCE_NAMES[r["source_name"]]
    m0, m1 = _month_bounds(d)
    month = d.strftime("%B %Y")
    if variant == "source_month":
        return f'only articles from {src} published in {month}', {"source": r["source_name"], "start": m0, "end": m1}
    if variant == "month":
        return f'published in {month}', {"source": None, "start": m0, "end": m1}
    if variant == "source":
        return f'coverage by {src}', {"source": r["source_name"], "start": None, "end": None}
    return f'published in {d.year}', {"source": None, "start": date(d.year, 1, 1), "end": date(d.year, 12, 31)}


def cmd_questions(_):
    from openai import OpenAI
    df = pl.read_parquet(SAMPLE)
    rng, used, seeds = random.Random(7), set(), []
    asked_on = _today_local()
    lang_name = {"en": "English", "vi": "Vietnamese"}
    for lang in ("en", "vi"):
        seeds += [("keyword_short", lang, r, {}) for r in _pick(df, rng, used, 10, language=lang, bucket=["y2026", "older"])]
        seeds += [("entity_number", lang, r, {}) for r in _pick(df, rng, used, 10, language=lang, bucket=["y2026", "older"])]
        seeds += [("deep_detail", lang, r, {}) for r in
                  _pick(df.filter(pl.col("body_len") >= 5000), rng, used, 10, language=lang,
                        bucket=["y2026", "older"])]
        phrases = [p for p in TIME_PHRASES if p[1] == lang]
        for i, r in enumerate(_pick(df, rng, used, 8 if lang == "en" else 7, language=lang, bucket="fresh")):
            phrase, _, days = phrases[i % len(phrases)]
            seeds.append(("time_relative", lang, r, {"phrase": phrase, "start": asked_on - timedelta(days=days - 1),
                                                     "end": asked_on, "source": None}))
        variants = ["source_month", "month", "source", "year"]
        for i, r in enumerate(_pick(df, rng, used, 10, language=lang, bucket=["y2026", "older"])):
            text, c = _filtered_constraint(r, variants[i % len(variants)])
            seeds.append(("filtered", lang, r, {"constraint": text, **c}))
    client = OpenAI()

    def one(seed):
        cat, lang, r, extra = seed
        msg = PROMPTS[cat].format(lang=lang_name[lang], title=r["web_title"], lead=r["lead"][:900],
                                  deep=(r["deep"] or "")[:1800], phrase=extra.get("phrase", ""),
                                  constraint=extra.get("constraint", ""), standalone=STANDALONE)
        q = client.chat.completions.create(model=GEN_MODEL, temperature=0.7,
                                           messages=[{"role": "user", "content": msg}]).choices[0].message.content
        q = q.strip().splitlines()[0].strip().strip('"')  # one line, even if the model lists several
        rec = {"category": cat, "language": lang, "question": q, "asked_on": str(asked_on),
               "article_id": r["article_id"], "source_name": r["source_name"], "pub_date": str(r["pub_date"]),
               "title": r["web_title"], "url": r["web_url"]}
        if cat in TOPICAL:
            rec["constraint"] = {"source": extra.get("source"),
                                 "start": str(extra["start"]) if extra.get("start") else None,
                                 "end": str(extra["end"]) if extra.get("end") else None,
                                 "text": extra.get("constraint") or extra.get("phrase")}
        return rec

    with ThreadPoolExecutor(8) as ex:
        qs = list(ex.map(one, seeds))
    for i, q in enumerate(qs):
        q["qid"] = i
    for path in (QUESTIONS, TRACKED_QUESTIONS):
        path.write_text(json.dumps(qs, ensure_ascii=False, indent=1), encoding="utf-8")
    print(pl.DataFrame(qs).group_by("category", "language").len().sort("category", "language"))


def cmd_show(_):
    for q in json.loads(QUESTIONS.read_text(encoding="utf-8")):
        c = q.get("constraint") or {}
        print(f"[{q['qid']:2d} {q['category']}/{q['language']}] {q['question']}\n"
              f"      <- {q['source_name']} {q['pub_date']} {q['title'][:70]}"
              + (f"  | {c.get('text')}" if c else ""))


# ------------------------------------------------------------------------------- run
def _system(name: str, q: dict) -> dict:
    from core import retrieval as R
    from core.text import extract_keywords
    if name == "agent_default":
        return {"ids": [c.article_id for c in R.hybrid_retrieve(q["question"], top_n=TOP_N,
                                                                include_user_docs=False).candidates]}
    if name == "agent_nopassage":  # production retrieval without the passage rerank
        return {"ids": [c.article_id for c in R.hybrid_retrieve(q["question"], top_n=TOP_N, include_user_docs=False,
                                                                passage_rerank=False).candidates]}
    if name == "agent_noparse":  # production retrieval without text.query_filters
        return {"ids": [c.article_id for c in R.hybrid_retrieve(q["question"], top_n=TOP_N, include_user_docs=False,
                                                                parse_filters=False).candidates]}
    if name == "v2_dense":
        return {"ids": [c.article_id for c in R._dense_articles_v2(q["question"], TOP_N, {})]}
    if name == "keyword":
        kw = extract_keywords(q["question"])
        return {"ids": [c.article_id for c in R._keyword(kw, TOP_N, {})] if kw else []}
    if name in ("agent_e2e", "agent_e2e_before"):
        from core.agent import ask, build_agent
        agent = _agent(build_agent)
        answer, stats = ask(agent, q["question"], thread_id=str(uuid.uuid4()), session_id=None)
        return {"answer": answer, "tools": stats.get("tools_used", []),
                "cited_urls": sorted({u.rstrip(".,;:").rstrip("/") for u in _URL.findall(answer)})}
    raise ValueError(name)


_AGENT = None
_AGENT_LOCK = threading.Lock()


def _agent(build):
    global _AGENT
    with _AGENT_LOCK:
        if _AGENT is None:
            _AGENT = build()
        return _AGENT


def _with_timeout(fn, *args, timeout: int = 150):
    box: dict = {}

    def target():
        try:
            box["result"] = fn(*args)
        except Exception as e:  # noqa: BLE001
            box["error"] = e

    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        raise TimeoutError(f"no answer within {timeout} s")
    if "error" in box:
        raise box["error"]
    return box["result"]


def cmd_run(args):
    from core import bq
    from core.retrieval import warm_up
    qs = json.loads(QUESTIONS.read_text(encoding="utf-8"))
    if any(q["asked_on"] != str(_today_local()) for q in qs if q["category"] == "time_relative"):
        print("WARNING: time_relative questions were written on another day; their windows have moved")
    done = set()
    if RUNS.exists():
        done = {(r["qid"], r["system"]) for r in map(json.loads, RUNS.read_text(encoding="utf-8").splitlines())}
    systems = args.systems.split(",") if args.systems else RUN_SYSTEMS
    warm_up()
    lock = threading.Lock()

    def one(job):
        q, s = job
        b0, t0 = bq.usage["bytes_billed"], time.time()
        try:
            out, err = _with_timeout(_system, s, q), None
        except Exception as e:  # noqa: BLE001
            out, err = {}, repr(e)[:300]
        rec = {"qid": q["qid"], "system": s, **out, "ms": int((time.time() - t0) * 1000),
               "bytes_billed": bq.usage["bytes_billed"] - b0, "error": err}
        with lock, RUNS.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        print(f"q{q['qid']:2d} {s:14s} {rec['ms'] / 1000:5.1f}s {err or ''}", flush=True)

    jobs = [(q, s) for q in qs for s in systems if (q["qid"], s) not in done]
    # Retrieval systems are quick; the agent runs a few questions in parallel.
    with ThreadPoolExecutor(4) as ex:
        list(ex.map(one, jobs))


# ----------------------------------------------------------------------------- judge
JUDGE_PROMPT = """You grade search results for a news assistant. Today is {today}.
Question: {question}

Article: {title}
Source: {source}   Published: {pub_date}
Article start: {snippet}

Grade the article for this question:
2 = about the question's topic AND it satisfies every constraint the question states
    (time period such as "this week" relative to today, a named source, a month or year)
1 = about the topic but breaks a stated constraint, or only partly relevant
0 = not about the topic
Reply with the digit only."""


def _runs() -> list[dict]:
    return [json.loads(x) for x in RUNS.read_text(encoding="utf-8").splitlines()]


def _resolve_urls(urls: list[str]) -> dict[str, str]:
    """Cited URL -> article_id (cached)."""
    from core import bq
    known = json.loads(URL_IDS.read_text(encoding="utf-8")) if URL_IDS.exists() else {}
    todo = sorted(set(urls) - set(known))
    if todo:
        rows = bq.query(f"""SELECT RTRIM(web_url, '/') AS u, article_id
                            FROM `{os.environ['GCP_PROJECT']}.marts.obt_articles`
                            WHERE web_publication_date >= TIMESTAMP('2000-01-01')
                              AND RTRIM(web_url, '/') IN UNNEST(@urls)""",
                        [bq.p_array("urls", todo)], max_bytes=500_000_000, timeout=120)
        found = {r["u"]: r["article_id"] for r in rows}
        known.update({u: found.get(u) for u in todo})
        URL_IDS.write_text(json.dumps(known, ensure_ascii=False, indent=0), encoding="utf-8")
    return known


def _top_ids(r: dict, url_ids: dict) -> list[str]:
    if r["system"].startswith("agent_e2e"):
        ids = [url_ids.get(u) for u in r.get("cited_urls", [])]
        return list(dict.fromkeys(i for i in ids if i))[:TOP_N]
    return list(dict.fromkeys(i for i in r.get("ids", []) if i))[:TOP_N]


def _meta(ids: list[str]) -> dict:
    from core import bq
    rows = bq.query(f"""SELECT article_id, web_title, snippet, source_name,
                               CAST(DATE(web_publication_date, 'Asia/Ho_Chi_Minh') AS STRING) AS pub_date
                        FROM `{os.environ['GCP_PROJECT']}.vectors.article_embeddings`
                        WHERE article_id IN UNNEST(@ids)
                        QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY embedded_at DESC) = 1""",
                    [bq.p_array("ids", ids)], max_bytes=1_500_000_000, timeout=180)
    return {r["article_id"]: r for r in rows}


def cmd_judge(_):
    from openai import OpenAI
    qs = {q["qid"]: q for q in json.loads(QUESTIONS.read_text(encoding="utf-8"))}
    runs = _runs()
    url_ids = _resolve_urls([u for r in runs for u in r.get("cited_urls", [])])
    pairs = sorted({(r["qid"], a) for r in runs if qs[r["qid"]]["category"] in TOPICAL
                    for a in _top_ids(r, url_ids)})
    judged = json.loads(JUDGED.read_text(encoding="utf-8")) if JUDGED.exists() else {}
    todo = [p for p in pairs if f"{p[0]}|{p[1]}" not in judged]
    meta = _meta(sorted({a for _, a in todo})) if todo else {}
    client = OpenAI()

    def one(p):
        qid, aid = p
        m, q = meta.get(aid), qs[qid]
        if not m:
            return p, None
        msg = JUDGE_PROMPT.format(today=q["asked_on"], question=q["question"], title=m["web_title"],
                                  source=SOURCE_NAMES.get(m["source_name"], m["source_name"]),
                                  pub_date=m["pub_date"], snippet=(m["snippet"] or "")[:700])
        out = client.chat.completions.create(model=GEN_MODEL, temperature=0,
                                             messages=[{"role": "user", "content": msg}]).choices[0].message.content
        digit = next((ch for ch in out if ch in "012"), None)
        return p, int(digit) if digit else None

    with ThreadPoolExecutor(8) as ex:
        for (qid, aid), g in ex.map(one, todo):
            if g is not None:
                judged[f"{qid}|{aid}"] = g
    JUDGED.write_text(json.dumps(judged, indent=0), encoding="utf-8")
    # Programmatic constraint check needs source + date of every topical result.
    all_ids = sorted({a for _, a in pairs})
    cmeta = {a: {"source": m["source_name"], "pub_date": m["pub_date"]} for a, m in _meta(all_ids).items()} if all_ids else {}
    (CACHE / "result_meta.json").write_text(json.dumps(cmeta, ensure_ascii=False), encoding="utf-8")
    print(f"{len(todo)} judged now, {len(judged)} total, {sum(1 for p in todo if p[1] not in meta)} without metadata")


# ---------------------------------------------------------------------------- report
def _complies(c: dict, m: dict | None) -> bool:
    if not m:
        return False
    if c.get("source") and m["source"] != c["source"]:
        return False
    if c.get("start") and m["pub_date"] < c["start"]:
        return False
    return not (c.get("end") and m["pub_date"] > c["end"])


def cmd_report(_):
    qs = {q["qid"]: q for q in json.loads(QUESTIONS.read_text(encoding="utf-8"))}
    runs = _runs()
    url_ids = _resolve_urls([u for r in runs for u in r.get("cited_urls", [])])
    judged = json.loads(JUDGED.read_text(encoding="utf-8")) if JUDGED.exists() else {}
    cmeta = json.loads((CACHE / "result_meta.json").read_text(encoding="utf-8")) \
        if (CACHE / "result_meta.json").exists() else {}
    res: dict = {}
    for s in SYSTEMS:
        rows = [r for r in runs if r["system"] == s]
        if not rows:
            continue
        out = {}
        for cat in KNOWN + TOPICAL:
            rs = [r for r in rows if qs[r["qid"]]["category"] == cat]
            if not rs:
                continue
            if cat in KNOWN:
                ranks = []
                for r in rs:
                    ids = _top_ids(r, url_ids)
                    seed = qs[r["qid"]]["article_id"]
                    ranks.append(ids.index(seed) + 1 if seed in ids else None)
                out[cat] = {"n": len(rs), "found@5": sum(1 for x in ranks if x) / len(rs),
                            "hit@1": sum(1 for x in ranks if x == 1) / len(rs)}
            else:
                strict, lenient, comply, n_res = [], [], [], 0
                for r in rs:
                    ids = _top_ids(r, url_ids)
                    grades = [judged.get(f"{r['qid']}|{a}", 0) for a in ids]
                    # P@5 for retrieval; for the agent, precision over what it cited (0 if nothing).
                    denom = TOP_N if not s.startswith("agent_e2e") else max(1, len(ids))
                    strict.append(sum(1 for g in grades if g == 2) / denom)
                    lenient.append(sum(1 for g in grades if g >= 1) / denom)
                    comply.append(sum(1 for a in ids if _complies(qs[r["qid"]]["constraint"], cmeta.get(a))) / denom)
                    n_res += len(ids)
                out[cat] = {"n": len(rs), "strict": sum(strict) / len(rs), "lenient": sum(lenient) / len(rs),
                            "constraint_ok": sum(comply) / len(rs), "results_per_q": n_res / len(rs)}
        ms = sorted(r["ms"] for r in rows)
        out["cost"] = {"p50_s": ms[len(ms) // 2] / 1000, "p90_s": ms[int(len(ms) * 0.9)] / 1000,
                       "mb_billed_per_q": sum(r["bytes_billed"] for r in rows) / len(rows) / 1e6,
                       "errors": sum(1 for r in rows if r["error"])}
        if s.startswith("agent_e2e"):
            out["cost"]["tool_calls_per_q"] = sum(len(r.get("tools", [])) for r in rows) / len(rows)
        res[s] = out
    RESULTS.write_text(json.dumps(res, indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"{'system':14s} " + "  ".join(f"{c:>15s}" for c in KNOWN) + "   (found@5 / hit@1)")
    for s, out in res.items():
        print(f"{s:14s} " + "  ".join(f"{out[c]['found@5']:.2f} / {out[c]['hit@1']:.2f}".rjust(15)
                                      if c in out else " " * 15 for c in KNOWN))
    print(f"\n{'system':14s} " + "  ".join(f"{c:>26s}" for c in TOPICAL) + "   (strict / lenient / constraint ok)")
    for s, out in res.items():
        print(f"{s:14s} " + "  ".join(
            f"{out[c]['strict']:.2f} / {out[c]['lenient']:.2f} / {out[c]['constraint_ok']:.2f}".rjust(26)
            if c in out else " " * 26 for c in TOPICAL))
    print()
    for s, out in res.items():
        print(f"{s:14s} {out['cost']}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("sample", "questions", "show", "judge", "report"):
        sub.add_parser(name)
    r = sub.add_parser("run")
    r.add_argument("--systems", help=f"comma list, default all: {','.join(SYSTEMS)}")
    args = ap.parse_args()
    {"sample": cmd_sample, "questions": cmd_questions, "show": cmd_show, "run": cmd_run,
     "judge": cmd_judge, "report": cmd_report}[args.cmd](args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
