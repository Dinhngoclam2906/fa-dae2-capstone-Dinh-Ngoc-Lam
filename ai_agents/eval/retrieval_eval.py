"""
retrieval_eval.py - eval refresh (agent phase 1c): the agent's retrieval over the FULL corpus.

Two question sets, both written by gpt-4o-mini from seed articles in marts.obt_articles:
  * known-item (100): one paraphrased question per seed article; the system must return
    that article. Types: en_en / vi_vi / en_vi (English question, Vietnamese article),
    recent (inside the keyword search's 90-day default window) and old (2026-03..06,
    outside it).
  * topical (30): a broad question about the seed's topic; many articles can answer it.
    Graded by pooling every system's top 5 and having gpt-4o-mini judge each (0/1/2).

Systems (the real code in core/retrieval.py, not a replay):
  v1_hybrid           production today: v1 dense (Guardian chunks) + keyword + FlashRank
  v2_hybrid           cut-over candidate: v2 dense (gemini-embedding-2, all sources) + keyword + FlashRank
  v2_hybrid_norerank  v2 hybrid, interleaved, no rerank
  v2_dense / v1_dense / keyword   the single legs
BigQuery VECTOR_SEARCH is costed by dry run only (`vsearch_cost`): it reads the same
vectors as v2 at 768 dims, so its quality is v2_dense's (bake-off: 768 vs 512 within 0.01).

    uv run --env-file .env python ai_agents/eval/retrieval_eval.py sample
    uv run --env-file .env python ai_agents/eval/retrieval_eval.py questions
    uv run --env-file .env python ai_agents/eval/retrieval_eval.py run
    uv run --env-file .env python ai_agents/eval/retrieval_eval.py judge
    uv run --env-file .env python ai_agents/eval/retrieval_eval.py vsearch_cost
    uv run --env-file .env python ai_agents/eval/retrieval_eval.py report

Stages cache under data/eval/retrieval/ (git-ignored); `run` resumes per question.
Questions and results are also written next to this file (tracked).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))  # ai_agents/, for core.*

CACHE = Path("data/eval/retrieval")
SAMPLE = CACHE / "sample.parquet"
QUESTIONS = CACHE / "questions.json"
RUNS = CACHE / "runs.jsonl"
JUDGED = CACHE / "judgements.json"
VSEARCH = CACHE / "vsearch_cost.json"
TRACKED_QUESTIONS = HERE / "retrieval_eval_questions.json"
RESULTS = HERE / "retrieval_eval_results.json"

LEAD_CHARS = 1500
TOP_N = 5     # what the agent's prompt receives
LEG_K = 10    # depth of the single-leg searches
# (type, language of the seed, question language, period, count)
KNOWN_ITEM = [("en_en", "en", "English", "recent", 20), ("vi_vi", "vi", "Vietnamese", "recent", 20),
              ("en_vi", "vi", "English", "recent", 20), ("en_en", "en", "English", "old", 20),
              ("vi_vi", "vi", "Vietnamese", "old", 20)]
TOPICAL = [("en_en", "en", "English", "recent", 10), ("vi_vi", "vi", "Vietnamese", "recent", 10),
           ("en_vi", "vi", "English", "recent", 10)]
SYSTEMS = ["v1_hybrid", "v2_hybrid", "v2_hybrid_norerank", "v2_dense", "v1_dense", "keyword"]


# ---------------------------------------------------------------------------- sample
def cmd_sample(_):
    """A pool per (language, period), spread over sources by a fixed hash."""
    from core import bq
    sql = f"""
    WITH a AS (
      SELECT article_id, source_name, language, section_group, web_title,
             LEFT(body_text, {LEAD_CHARS}) AS lead, DATE(web_publication_date_local) AS pub_date,
             IF(web_publication_date >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 80 DAY)),
                'recent', 'old') AS period
      FROM `{os.environ['GCP_PROJECT']}.marts.obt_articles`
      WHERE ((web_publication_date >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 80 DAY))
              AND web_publication_date < TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)))
          OR (web_publication_date >= TIMESTAMP('2026-03-01') AND web_publication_date < TIMESTAMP('2026-07-01')))
        AND word_count >= 80
    )
    SELECT * FROM a
    QUALIFY ROW_NUMBER() OVER (PARTITION BY source_name, period
                               ORDER BY FARM_FINGERPRINT(CONCAT(article_id, 'eval-1c'))) <= 40
    """
    rows = bq.query(sql, max_bytes=3_000_000_000, timeout=180)
    CACHE.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(rows)
    df.write_parquet(SAMPLE)
    print(df.group_by("language", "period").len().sort("language", "period"))
    print(f"billed MB: {bq.usage['bytes_billed'] / 1e6:.0f}")


# ------------------------------------------------------------------------- questions
KNOWN_PROMPT = """You write test questions for a news search assistant.
Write ONE natural question in {lang} that a reader might type, which this article answers.
Rules: do not copy the headline; paraphrase and avoid more than two consecutive words from
the title; include one specific detail (who, what or where) so this article is a good answer.
Return only the question.

Title: {title}
Article start: {lead}"""

TOPICAL_PROMPT = """You write test questions for a news search assistant.
Write ONE broad question in {lang} that a reader might ask about the TOPIC of this article,
the kind of question several news articles could answer (for example "What is happening
with ...?" or "What are the latest developments in ...?"). Do not use details that only
this article has. Return only the question.

Title: {title}
Article start: {lead}"""


def _pick(df: pl.DataFrame, rng: random.Random, used: set, language: str, period: str, n: int) -> list[dict]:
    """n seeds round-robin over sources, so no source dominates a type."""
    pool: dict[str, list[dict]] = {}
    for r in df.filter((pl.col("language") == language) & (pl.col("period") == period)).iter_rows(named=True):
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


def cmd_questions(_):
    from openai import OpenAI
    df = pl.read_parquet(SAMPLE)
    rng, used, seeds = random.Random(1), set(), []
    for kind, prompt in (("known", KNOWN_PROMPT), ("topical", TOPICAL_PROMPT)):
        for qtype, seed_lang, q_lang, period, n in (KNOWN_ITEM if kind == "known" else TOPICAL):
            seeds += [(kind, qtype, q_lang, period, prompt, r) for r in _pick(df, rng, used, seed_lang, period, n)]
    client = OpenAI()

    def one(seed):
        kind, qtype, lang, period, prompt, r = seed
        msg = prompt.format(lang=lang, title=r["web_title"], lead=r["lead"][:800])
        q = client.chat.completions.create(model="gpt-4o-mini", temperature=0.7,
                                           messages=[{"role": "user", "content": msg}]).choices[0].message.content
        return {"kind": kind, "type": qtype, "period": period, "question": q.strip().strip('"'),
                "article_id": r["article_id"], "source_name": r["source_name"],
                "pub_date": str(r["pub_date"]), "title": r["web_title"]}

    with ThreadPoolExecutor(8) as ex:
        qs = list(ex.map(one, seeds))
    for i, q in enumerate(qs):
        q["qid"] = i
    for path in (QUESTIONS, TRACKED_QUESTIONS):
        path.write_text(json.dumps(qs, ensure_ascii=False, indent=1), encoding="utf-8")
    for q in qs[::13]:
        print(f"[{q['kind']}/{q['type']}/{q['period']}] {q['question']}  <- {q['title'][:60]}")
    print(len(qs), "questions")


# ------------------------------------------------------------------------------- run
def _system(name: str, question: str) -> list:
    from core import retrieval as R
    from core.text import extract_keywords
    if name == "v1_hybrid":
        return R.hybrid_retrieve(question, top_n=TOP_N, include_user_docs=False, article_vectors="v1").candidates
    if name == "v2_hybrid":
        return R.hybrid_retrieve(question, top_n=TOP_N, include_user_docs=False, article_vectors="v2").candidates
    if name == "v2_hybrid_norerank":
        return R.hybrid_retrieve(question, top_n=TOP_N, include_user_docs=False, article_vectors="v2",
                                 rerank=False).candidates
    if name == "v2_dense":
        return R._dense_articles_v2(question, LEG_K, {})
    if name == "v1_dense":
        return R._dense_articles(question, LEG_K)
    if name == "keyword":
        kw = extract_keywords(question)
        return R._keyword(kw, LEG_K, {}) if kw else []
    raise ValueError(name)


CALL_TIMEOUT = 60  # s; the first run hung for 12+ minutes on one v1 Pinecone call


def _system_with_timeout(name: str, question: str) -> list:
    """_system in a daemon thread, so a call that never returns is recorded as an error
    (and can't keep the process alive) instead of stalling the whole run."""
    import threading
    box: dict = {}

    def target():
        try:
            box["result"] = _system(name, question)
        except Exception as e:  # noqa: BLE001
            box["error"] = e

    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(CALL_TIMEOUT)
    if t.is_alive():
        raise TimeoutError(f"{name} did not return within {CALL_TIMEOUT} s")
    if "error" in box:
        raise box["error"]
    return box["result"]


def _ids(cands: list) -> list[str]:
    """Article ids in rank order, deduplicated (v1 returns several chunks per article)."""
    out = []
    for c in cands:
        if c.article_id and c.article_id not in out:
            out.append(c.article_id)
    return out


def cmd_run(args):
    from core import bq
    qs = json.loads(QUESTIONS.read_text(encoding="utf-8"))
    done = set()
    if RUNS.exists():
        done = {(r["qid"], r["system"]) for r in map(json.loads, RUNS.read_text(encoding="utf-8").splitlines())}
    systems = args.systems.split(",") if args.systems else SYSTEMS
    _system("v2_hybrid", "warm up")  # model loads / connections out of the latency numbers
    _system("v1_hybrid", "warm up")
    with RUNS.open("a", encoding="utf-8") as f:
        for q in qs:
            for s in systems:
                if (q["qid"], s) in done:
                    continue
                b0, t0 = bq.usage["bytes_billed"], time.time()
                try:
                    ids, err = _ids(_system_with_timeout(s, q["question"])), None
                except Exception as e:  # noqa: BLE001 - record and carry on
                    ids, err = [], repr(e)[:300]
                    print(f"  q{q['qid']} {s}: {err}", flush=True)
                rec = {"qid": q["qid"], "system": s, "ids": ids, "ms": int((time.time() - t0) * 1000),
                       "bytes_billed": bq.usage["bytes_billed"] - b0, "error": err}
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                f.flush()
            print(f"q{q['qid']:3d} [{q['kind']}/{q['type']}/{q['period']}] done", flush=True)


# ----------------------------------------------------------------------------- judge
JUDGE_PROMPT = """You grade search results for a news assistant.
Question: {question}

Article title: {title}
Article start: {snippet}

How well does this article help answer the question?
2 = directly about the question's topic, 1 = related / partly useful, 0 = not relevant.
Reply with the digit only."""


def _runs() -> list[dict]:
    return [json.loads(x) for x in RUNS.read_text(encoding="utf-8").splitlines()]


def cmd_judge(_):
    from openai import OpenAI

    from core import bq
    qs = {q["qid"]: q for q in json.loads(QUESTIONS.read_text(encoding="utf-8")) if q["kind"] == "topical"}
    pairs = sorted({(r["qid"], a) for r in _runs() if r["qid"] in qs for a in r["ids"][:TOP_N]})
    judged = json.loads(JUDGED.read_text(encoding="utf-8")) if JUDGED.exists() else {}
    todo = [p for p in pairs if f"{p[0]}|{p[1]}" not in judged]
    ids = sorted({a for _, a in todo})
    meta = {}
    if ids:
        rows = bq.query(f"""SELECT article_id, web_title, snippet
                            FROM `{os.environ['GCP_PROJECT']}.vectors.article_embeddings`
                            WHERE article_id IN UNNEST(@ids)
                            QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY embedded_at DESC) = 1""",
                        [bq.p_array("ids", ids)], max_bytes=1_000_000_000, timeout=120)
        meta = {r["article_id"]: r for r in rows}
    client = OpenAI()

    def one(p):
        qid, aid = p
        m = meta.get(aid)
        if not m:
            return p, None
        msg = JUDGE_PROMPT.format(question=qs[qid]["question"], title=m["web_title"], snippet=(m["snippet"] or "")[:700])
        out = client.chat.completions.create(model="gpt-4o-mini", temperature=0,
                                             messages=[{"role": "user", "content": msg}]).choices[0].message.content
        digit = next((ch for ch in out if ch in "012"), None)
        return p, int(digit) if digit else None

    with ThreadPoolExecutor(8) as ex:
        for (qid, aid), g in ex.map(one, todo):
            if g is not None:
                judged[f"{qid}|{aid}"] = g
    JUDGED.write_text(json.dumps(judged, indent=0), encoding="utf-8")
    print(f"{len(todo)} judged now, {len(judged)} total, {sum(1 for p in todo if p[1] not in meta)} without metadata")


# ---------------------------------------------------------------------- vsearch cost
def cmd_vsearch_cost(_):
    """Bytes BigQuery VECTOR_SEARCH would bill per question (dry run: free)."""
    from google.cloud import bigquery

    from core import bq
    from core.retrieval import embed_query
    vec = embed_query("What is the latest on Vietnam's economy?", dims=768)
    out = {}
    for label, days in (("all", None), ("90d", 90), ("30d", 30), ("7d", 7)):
        where = (f"WHERE web_publication_date >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY))"
                 if days else "")
        sql = f"""SELECT base.article_id, distance FROM VECTOR_SEARCH(
                    (SELECT article_id, embedding FROM `{os.environ['GCP_PROJECT']}.vectors.article_embeddings` {where}),
                    'embedding', (SELECT @v AS embedding), top_k => 10, distance_type => 'COSINE')"""
        cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False,
                                      query_parameters=[bigquery.ArrayQueryParameter("v", "FLOAT64", vec)])
        job = bq.client().query(sql, job_config=cfg)
        out[label] = job.total_bytes_processed
        print(f"VECTOR_SEARCH {label:4s}: {job.total_bytes_processed / 1e9:6.2f} GB per question")
    VSEARCH.write_text(json.dumps(out), encoding="utf-8")


# ---------------------------------------------------------------------------- report
def _ndcg(gains: list[int], ideal: list[int], k: int) -> float:
    dcg = sum(g / math.log2(i + 2) for i, g in enumerate(gains[:k]))
    idcg = sum(g / math.log2(i + 2) for i, g in enumerate(sorted(ideal, reverse=True)[:k]))
    return dcg / idcg if idcg else 0.0


def cmd_report(_):
    qs = {q["qid"]: q for q in json.loads(QUESTIONS.read_text(encoding="utf-8"))}
    runs = _runs()
    judged = json.loads(JUDGED.read_text(encoding="utf-8")) if JUDGED.exists() else {}
    res: dict = {"known_item": {}, "topical": {}, "cost": {}}

    def known(rows):
        ranks = []
        for r in rows:
            ids = r["ids"][:TOP_N]
            seed = qs[r["qid"]]["article_id"]
            ranks.append(ids.index(seed) + 1 if seed in ids else None)
        n = len(ranks) or 1
        return {"n": len(ranks), "hit@1": sum(1 for x in ranks if x == 1) / n,
                "recall@5": sum(1 for x in ranks if x) / n,
                "mrr@5": sum(1 / x for x in ranks if x) / n}

    for s in SYSTEMS:
        rows = [r for r in runs if r["system"] == s]
        if not rows:
            continue
        k_rows = [r for r in rows if qs[r["qid"]]["kind"] == "known"]
        groups = {"all": k_rows}
        for r in k_rows:
            q = qs[r["qid"]]
            groups.setdefault(f"{q['type']}/{q['period']}", []).append(r)
        res["known_item"][s] = {g: known(v) for g, v in sorted(groups.items())}

        t_rows = [r for r in rows if qs[r["qid"]]["kind"] == "topical"]
        tg: dict = {}
        for r in t_rows:
            q = qs[r["qid"]]
            pool = [g for k, g in judged.items() if k.startswith(f"{r['qid']}|")]
            gains = [judged.get(f"{r['qid']}|{a}", 0) for a in r["ids"][:TOP_N]]
            for g in ("all", q["type"]):
                d = tg.setdefault(g, {"p@5": [], "ndcg@5": []})
                d["p@5"].append(sum(1 for x in gains if x >= 1) / TOP_N)
                d["ndcg@5"].append(_ndcg(gains, pool, TOP_N))
        res["topical"][s] = {g: {"n": len(v["p@5"]), **{m: sum(x) / len(x) for m, x in v.items()}}
                             for g, v in sorted(tg.items())}
        ms = sorted(r["ms"] for r in rows)
        res["cost"][s] = {"p50_ms": ms[len(ms) // 2], "p90_ms": ms[int(len(ms) * 0.9)],
                          "mb_billed_per_q": sum(r["bytes_billed"] for r in rows) / len(rows) / 1e6,
                          "errors": sum(1 for r in rows if r["error"])}
    if VSEARCH.exists():
        res["vector_search_gb_per_q"] = {k: v / 1e9 for k, v in json.loads(VSEARCH.read_text()).items()}
    RESULTS.write_text(json.dumps(res, indent=1), encoding="utf-8")

    print("\nKNOWN-ITEM  recall@5 / hit@1 (top 5 the agent sees)")
    groups = sorted({g for v in res["known_item"].values() for g in v})
    print(f"{'system':20s}" + "".join(f"{g:>16s}" for g in groups))
    for s, v in res["known_item"].items():
        print(f"{s:20s}" + "".join(f"{v[g]['recall@5']:>9.2f} /{v[g]['hit@1']:4.2f}" if g in v else f"{'':>16s}"
                                   for g in groups))
    print("\nTOPICAL  P@5 / NDCG@5")
    for s, v in res["topical"].items():
        print(f"{s:20s}" + "".join(f"{g:>8s} {x['p@5']:.2f}/{x['ndcg@5']:.2f}" for g, x in v.items()))
    print("\nCOST / LATENCY")
    for s, v in res["cost"].items():
        print(f"{s:20s} p50 {v['p50_ms']:6d} ms  p90 {v['p90_ms']:6d} ms  {v['mb_billed_per_q']:6.1f} MB/q  errors {v['errors']}")
    if "vector_search_gb_per_q" in res:
        print("VECTOR_SEARCH GB/question:", {k: round(v, 2) for k, v in res["vector_search_gb_per_q"].items()})


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("sample", "questions", "judge", "vsearch_cost", "report"):
        sub.add_parser(name)
    r = sub.add_parser("run")
    r.add_argument("--systems", help=f"comma list, default all: {','.join(SYSTEMS)}")
    args = ap.parse_args()
    {"sample": cmd_sample, "questions": cmd_questions, "run": cmd_run, "judge": cmd_judge,
     "vsearch_cost": cmd_vsearch_cost, "report": cmd_report}[args.cmd](args)


if __name__ == "__main__":
    main()
