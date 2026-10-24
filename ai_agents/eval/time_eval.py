"""
time_eval.py - time-relative questions, written fresh on the day they are asked.

The hard set's 15 time_relative questions ("this week", "mới nhất"...) expire: asked two
days later, the right answers move outside their windows (docs/hard_eval.md, 2026-10-22).
This set is regenerated per day under data/eval/time/<asked_on>/, from articles published in
the last 2 days, 5 questions per phrase (hard_eval.TIME_PHRASES: 4 English + 4 Vietnamese,
40 questions), with hard_eval's generator and judge prompts.

    uv run --env-file .env python ai_agents/eval/time_eval.py questions
    uv run --env-file .env python ai_agents/eval/time_eval.py run [--systems a,b]
    uv run --env-file .env python ai_agents/eval/time_eval.py judge
    uv run --env-file .env python ai_agents/eval/time_eval.py report

Metrics over the top 5 (LLM judge 2 = on topic AND inside the phrase's window):
  strict / lenient   precision@5 (strict is capped by how many recent on-topic articles exist)
  any_strict         at least one strict result in the top 5 (did the user get a good answer?)
  in_window          share of results published inside the window (programmatic)
  seed@5             the seed article is in the top 5

Systems (retrieval variants, swapped in by patching core.retrieval for the run):
  current            production (2026-10-22): recent-first 5 for windows <= 7 days ("today",
                     "this week"), 3 for longer ones ("latest")
  first3             the rule before 2026-10-22: recent-first 3 for every window
  first5             recent-first 5 for every window
  no_recency         recency rule off (what the dense ranking alone gives)
  agent_e2e          the full agent (OpenAI cost, ~$0.01 per question); scored on cited URLs
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))  # ai_agents/, for core.*
sys.path.insert(0, str(HERE))
import hard_eval as H  # noqa: E402

ASKED_ON = H._today_local()
CACHE = Path("data/eval/time") / str(ASKED_ON)
QUESTIONS = CACHE / "questions.json"
RUNS = CACHE / "runs.jsonl"
JUDGED = CACHE / "judgements.json"
META = CACHE / "meta.json"
PER_PHRASE = 5
SYSTEMS = ["current", "first3", "first5", "no_recency", "agent_e2e"]
RETRIEVAL = SYSTEMS[:-1]


def cmd_questions(_):
    from openai import OpenAI

    from core import bq
    sql = f"""
    SELECT article_id, source_name, language, web_title, web_url, LEFT(body_text, 900) AS lead,
           CAST(DATE(web_publication_date_local) AS STRING) AS pub_date
    FROM `{os.environ['GCP_PROJECT']}.marts.obt_articles`
    WHERE web_publication_date >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY))
      AND DATE(web_publication_date_local) >= @start AND word_count >= 120
    """
    df = pl.DataFrame(bq.query(sql, [bq.p("start", ASKED_ON - timedelta(days=1), "DATE")], max_bytes=1_500_000_000))
    rng, used, seeds = random.Random(int(ASKED_ON.strftime("%Y%m%d"))), set(), []
    for phrase, lang, days in H.TIME_PHRASES:
        for r in H._pick(df, rng, used, PER_PHRASE, language=lang):
            seeds.append((phrase, lang, days, r))
    client = OpenAI()
    lang_name = {"en": "English", "vi": "Vietnamese"}

    def one(seed):
        phrase, lang, days, r = seed
        msg = H.PROMPTS["time_relative"].format(lang=lang_name[lang], title=r["web_title"], lead=r["lead"], phrase=phrase)
        q = client.chat.completions.create(model=H.GEN_MODEL, temperature=0.7,
                                           messages=[{"role": "user", "content": msg}]).choices[0].message.content
        return {"question": q.strip().splitlines()[0].strip().strip('"'), "language": lang, "phrase": phrase,
                "asked_on": str(ASKED_ON), "start": str(ASKED_ON - timedelta(days=days - 1)), "end": str(ASKED_ON),
                "article_id": r["article_id"], "source_name": r["source_name"], "pub_date": r["pub_date"],
                "title": r["web_title"], "url": r["web_url"]}

    with ThreadPoolExecutor(8) as ex:
        qs = list(ex.map(one, seeds))
    for i, q in enumerate(qs):
        q["qid"] = i
    CACHE.mkdir(parents=True, exist_ok=True)
    QUESTIONS.write_text(json.dumps(qs, ensure_ascii=False, indent=1), encoding="utf-8")
    print(pl.DataFrame(qs).group_by("language", "phrase").len().sort("language", "phrase"))


@contextmanager
def _variant(name: str, question: str):
    """Patch core.retrieval for one question; runs are sequential, so this is safe."""
    from core import retrieval as R
    saved = (R.RECENT_FIRST, R.RECENT_FIRST_SHORT, R.recency_days)
    try:
        if name == "no_recency":
            R.recency_days = lambda q: None
        elif name == "first3":
            R.RECENT_FIRST_SHORT = 3
        elif name == "first5":
            R.RECENT_FIRST = 5
        yield R
    finally:
        R.RECENT_FIRST, R.RECENT_FIRST_SHORT, R.recency_days = saved


def _one(name: str, q: dict) -> dict:
    if name == "agent_e2e":
        from core.agent import ask, build_agent
        agent = H._agent(build_agent)
        answer, stats = ask(agent, q["question"], thread_id=str(uuid.uuid4()))
        return {"answer": answer, "tools": stats.get("tools_used", []),
                "cited_urls": sorted({u.rstrip(".,;:").rstrip("/") for u in H._URL.findall(answer)})}
    with _variant(name, q["question"]) as R:
        res = R.hybrid_retrieve(q["question"], top_n=H.TOP_N)
    return {"ids": [c.article_id for c in res.candidates], "recency_days": res.stats.get("recency_days")}


def cmd_run(args):
    from core.retrieval import warm_up
    qs = json.loads(QUESTIONS.read_text(encoding="utf-8"))
    done = set()
    if RUNS.exists():
        done = {(r["qid"], r["system"]) for r in map(json.loads, RUNS.read_text(encoding="utf-8").splitlines())}
    systems = args.systems.split(",") if args.systems else RETRIEVAL
    warm_up()
    with RUNS.open("a", encoding="utf-8") as f:
        for s in systems:
            for q in qs:
                if (q["qid"], s) in done:
                    continue
                t0 = time.time()
                try:
                    out, err = H._with_timeout(_one, s, q), None
                except Exception as e:  # noqa: BLE001
                    out, err = {}, repr(e)[:300]
                f.write(json.dumps({"qid": q["qid"], "system": s, **out, "ms": int((time.time() - t0) * 1000),
                                    "error": err}, ensure_ascii=False) + "\n")
                f.flush()
            print(f"{s}: done", flush=True)
    # A retrieval leg that timed out leaves a hung network thread behind, which blocked
    # the normal interpreter exit (2026-10-22). Everything is written; exit hard.
    os._exit(0)


def _runs() -> list[dict]:
    return [json.loads(x) for x in RUNS.read_text(encoding="utf-8").splitlines()]


def _url_ids(runs: list[dict]) -> dict:
    """Cited URL -> article_id, via hard_eval's resolver (its cache file is shared)."""
    urls = [u for r in runs for u in r.get("cited_urls", [])]
    return H._resolve_urls(urls) if urls else {}


def cmd_judge(_):
    from openai import OpenAI
    qs = {q["qid"]: q for q in json.loads(QUESTIONS.read_text(encoding="utf-8"))}
    runs = _runs()
    url_ids = _url_ids(runs)
    pairs = sorted({(r["qid"], a) for r in runs for a in H._top_ids(r, url_ids)})
    judged = json.loads(JUDGED.read_text(encoding="utf-8")) if JUDGED.exists() else {}
    meta = json.loads(META.read_text(encoding="utf-8")) if META.exists() else {}
    new_ids = sorted({a for _, a in pairs} - set(meta))
    if new_ids:
        meta.update({a: {"title": m["web_title"], "snippet": m["snippet"], "source": m["source_name"],
                         "pub_date": m["pub_date"]} for a, m in H._meta(new_ids).items()})
        META.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    todo = [p for p in pairs if f"{p[0]}|{p[1]}" not in judged and p[1] in meta]
    client = OpenAI()

    def one(p):
        q, m = qs[p[0]], meta[p[1]]
        msg = H.JUDGE_PROMPT.format(today=q["asked_on"], question=q["question"], title=m["title"],
                                    source=H.SOURCE_NAMES.get(m["source"], m["source"]), pub_date=m["pub_date"],
                                    snippet=(m["snippet"] or "")[:700])
        out = client.chat.completions.create(model=H.GEN_MODEL, temperature=0,
                                             messages=[{"role": "user", "content": msg}]).choices[0].message.content
        d = next((ch for ch in out if ch in "012"), None)
        return p, int(d) if d else None

    with ThreadPoolExecutor(8) as ex:
        for (qid, aid), g in ex.map(one, todo):
            if g is not None:
                judged[f"{qid}|{aid}"] = g
    JUDGED.write_text(json.dumps(judged, indent=0), encoding="utf-8")
    print(f"{len(todo)} judged now, {len(judged)} total")


def cmd_report(_):
    qs = {q["qid"]: q for q in json.loads(QUESTIONS.read_text(encoding="utf-8"))}
    runs = _runs()
    url_ids = _url_ids(runs)
    judged = json.loads(JUDGED.read_text(encoding="utf-8"))
    meta = json.loads(META.read_text(encoding="utf-8"))
    groups = {"all": lambda q: True, "en": lambda q: q["language"] == "en", "vi": lambda q: q["language"] == "vi"}
    groups.update({p: (lambda q, p=p: q["phrase"] == p) for p, _, _ in H.TIME_PHRASES})
    res: dict = {}
    print(f"asked_on {ASKED_ON}, {len(qs)} questions\n")
    print(f"{'system':14s} {'group':22s} {'n':>3s} {'strict':>7s} {'lenient':>8s} {'any_strict':>11s} "
          f"{'in_window':>10s} {'seed@5':>7s} {'p50_s':>6s}")
    for s in SYSTEMS:
        rows = [r for r in runs if r["system"] == s]
        if not rows:
            continue
        res[s] = {}
        for g, f in groups.items():
            rs = [r for r in rows if f(qs[r["qid"]])]
            if not rs:
                continue
            m = {"strict": [], "lenient": [], "any": [], "win": [], "seed": []}
            for r in rs:
                q, ids = qs[r["qid"]], H._top_ids(r, url_ids)
                grades = [judged.get(f"{r['qid']}|{a}", 0) for a in ids]
                denom = H.TOP_N if s != "agent_e2e" else max(1, len(ids))
                m["strict"].append(sum(g == 2 for g in grades) / denom)
                m["lenient"].append(sum(g >= 1 for g in grades) / denom)
                m["any"].append(any(g == 2 for g in grades))
                m["win"].append(sum(q["start"] <= (meta.get(a) or {}).get("pub_date", "") <= q["end"] for a in ids) / denom)
                m["seed"].append(q["article_id"] in ids)
            out = {k: sum(v) / len(v) for k, v in m.items()}
            ms = sorted(r["ms"] for r in rs)
            out.update(n=len(rs), p50_s=ms[len(ms) // 2] / 1000)
            res[s][g] = out
            if g in ("all", "en", "vi") or s == "current":
                print(f"{s:14s} {g:22s} {len(rs):3d} {out['strict']:7.2f} {out['lenient']:8.2f} {out['any']:11.2f} "
                      f"{out['win']:10.2f} {out['seed']:7.2f} {out['p50_s']:6.1f}")
    (CACHE / "results.json").write_text(json.dumps(res, indent=1, ensure_ascii=False), encoding="utf-8")


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")  # Vietnamese phrases on Windows consoles
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("questions", "judge", "report"):
        sub.add_parser(name)
    r = sub.add_parser("run")
    r.add_argument("--systems", help=f"comma list, default the retrieval ones: {','.join(RETRIEVAL)}")
    args = ap.parse_args()
    {"questions": cmd_questions, "run": cmd_run, "judge": cmd_judge, "report": cmd_report}[args.cmd](args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
