"""
embedding_bakeoff.py - choose the embedding model for article-level vectors (agent phase 2a).

Known-item retrieval over a fixed sample: an LLM writes one paraphrased question per
seed article, and each model must rank that article in its top 10 among ~5,000.
Three question types: English->English, Vietnamese->Vietnamese, English->Vietnamese
(cross-lingual). Plain cosine search in memory (no vector DB involved).

    uv run --env-file .env python ai_agents/eval/embedding_bakeoff.py sample
    uv run --env-file .env python ai_agents/eval/embedding_bakeoff.py questions
    uv run --env-file .env python ai_agents/eval/embedding_bakeoff.py embed --model openai|gemini|e5
    uv run --env-file .env python ai_agents/eval/embedding_bakeoff.py evaluate

Every stage caches under data/eval/bakeoff/ (git-ignored), so reruns cost nothing.
The questions are also saved to ai_agents/eval/bakeoff_questions.json (tracked).

Document text = title + first 1,500 characters of the body (the article-level design).
Shortened variants (the 2 GB Pinecone constraint) are scored by truncating the full
vector and re-normalising, which is how both API models define shorter dimensions.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import polars as pl

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))  # ai_agents/, for core.*

CACHE = Path("data/eval/bakeoff")
SAMPLE = CACHE / "sample.parquet"
QUESTIONS = CACHE / "questions.json"
TRACKED_QUESTIONS = HERE / "bakeoff_questions.json"
RESULTS = HERE / "bakeoff_results.json"

QUOTAS = {"guardian": 1200, "tuoitre": 500, "thanhnien": 500, "vietnamnet": 500, "vneconomy": 500,
          "vnexpress": 500, "tuoitre_en": 500, "vneconomy_en": 400, "vnexpress_en": 400, "vietnamnet_en": 100}
PER_TYPE = 25
LEAD_CHARS = 1500


def doc_text(row: dict) -> str:
    return f"{row['web_title']}\n\n{row['lead']}"


# ---------------------------------------------------------------------------- sample
def cmd_sample(_):
    from core import bq
    quota = " ".join(f"WHEN '{s}' THEN {n}" for s, n in QUOTAS.items())
    sql = f"""
    SELECT article_id, source_name, language, section_group, web_title,
           LEFT(body_text, {LEAD_CHARS}) AS lead, DATE(web_publication_date_local) AS pub_date
    FROM `{os.environ['GCP_PROJECT']}.marts.obt_articles`
    WHERE web_publication_date >= TIMESTAMP('2026-08-18') AND web_publication_date < TIMESTAMP('2026-10-18')
      AND word_count >= 80
    QUALIFY ROW_NUMBER() OVER (PARTITION BY source_name ORDER BY FARM_FINGERPRINT(article_id))
            <= CASE source_name {quota} ELSE 0 END
    """
    rows = bq.query(sql, max_bytes=2_000_000_000, timeout=120)
    CACHE.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(rows)
    df.write_parquet(SAMPLE)
    print(df.group_by("source_name").len().sort("source_name"), f"\nbilled MB: {bq.usage['bytes_billed']/1e6:.0f}")


# ------------------------------------------------------------------------- questions
PROMPT = """You write test questions for a news search assistant.
Write ONE natural question in {lang} that a reader might type, which this article answers.
Rules: do not copy the headline; paraphrase and avoid more than two consecutive words from
the title; include one specific detail (who, what or where) so this article is a good answer.
Return only the question.

Title: {title}
Article start: {lead}"""


def cmd_questions(_):
    from openai import OpenAI
    df = pl.read_parquet(SAMPLE)
    rng = random.Random(42)
    en = [r for r in df.iter_rows(named=True) if r["language"] == "en"]
    vi = [r for r in df.iter_rows(named=True) if r["language"] == "vi"]
    rng.shuffle(en)
    rng.shuffle(vi)
    seeds = ([(r, "en_en", "English") for r in en[:PER_TYPE]]
             + [(r, "vi_vi", "Vietnamese") for r in vi[:PER_TYPE]]
             + [(r, "en_vi", "English") for r in vi[PER_TYPE:2 * PER_TYPE]])
    client = OpenAI()

    def one(seed):
        r, kind, lang = seed
        msg = PROMPT.format(lang=lang, title=r["web_title"], lead=r["lead"][:800])
        q = client.chat.completions.create(model="gpt-4o-mini", temperature=0.7,
                                           messages=[{"role": "user", "content": msg}]).choices[0].message.content
        return {"type": kind, "question": q.strip().strip('"'), "article_id": r["article_id"],
                "source_name": r["source_name"], "title": r["web_title"]}

    with ThreadPoolExecutor(8) as ex:
        qs = list(ex.map(one, seeds))
    for path in (QUESTIONS, TRACKED_QUESTIONS):
        path.write_text(json.dumps(qs, ensure_ascii=False, indent=1), encoding="utf-8")
    for q in qs[::10]:
        print(f"[{q['type']}] {q['question']}  <- {q['title'][:70]}")
    print(len(qs), "questions")


# ----------------------------------------------------------------------------- embed
def _normalise(m: np.ndarray) -> np.ndarray:
    return m / np.linalg.norm(m, axis=1, keepdims=True)


_CLIENTS: dict = {}  # one client per model, so query latency excludes connection/auth setup


def _openai(texts: list[str], kind: str) -> np.ndarray:
    from openai import OpenAI
    c = _CLIENTS.setdefault("openai", OpenAI())
    out = []
    for i in range(0, len(texts), 200):
        out += [d.embedding for d in c.embeddings.create(model="text-embedding-3-small", input=texts[i:i + 200]).data]
    return np.array(out, dtype=np.float32)


def _gemini(texts: list[str], kind: str) -> np.ndarray:
    from google import genai
    from google.genai import types
    # A per-request timeout: the first run hung for 20+ minutes on one request that never
    # returned (all 5,064 others had succeeded), because the client waits forever by default.
    if "gemini" not in _CLIENTS:
        _CLIENTS["gemini"] = genai.Client(vertexai=True, project=os.environ["GCP_PROJECT"], location="asia-southeast1",
                                          http_options=types.HttpOptions(timeout=30_000))
    c = _CLIENTS["gemini"]
    task = "RETRIEVAL_DOCUMENT" if kind == "doc" else "RETRIEVAL_QUERY"
    partial = CACHE / f"partial_gemini_{kind}.json"
    done: dict[str, list[float]] = json.loads(partial.read_text()) if kind == "doc" and partial.exists() else {}

    def one(i_t):
        i, t = i_t
        for attempt in range(6):
            try:
                r = c.models.embed_content(model="gemini-embedding-001", contents=[t],
                                           config=types.EmbedContentConfig(task_type=task, output_dimensionality=1536))
                return i, r.embeddings[0].values
            except Exception as e:  # noqa: BLE001 - timeouts / 429s; back off and retry
                if attempt == 5:
                    raise
                print(f"  retry {i} ({type(e).__name__})", flush=True)
                time.sleep(2 ** attempt + random.random())

    todo = [(i, t) for i, t in enumerate(texts) if str(i) not in done]
    with ThreadPoolExecutor(8) as ex:
        for n, (i, v) in enumerate(ex.map(one, todo), 1):
            done[str(i)] = v
            if kind == "doc" and n % 500 == 0:
                partial.write_text(json.dumps(done))
                print(f"  gemini docs: {len(done)}/{len(texts)}", flush=True)
    if kind == "doc":
        partial.unlink(missing_ok=True)
    return np.array([done[str(i)] for i in range(len(texts))], dtype=np.float32)


_E5 = None


def _e5(texts: list[str], kind: str) -> np.ndarray:
    global _E5
    from sentence_transformers import SentenceTransformer
    if _E5 is None:
        _E5 = SentenceTransformer("intfloat/multilingual-e5-base")
        _E5.max_seq_length = 512
    prefix = "passage: " if kind == "doc" else "query: "
    return _E5.encode([prefix + t for t in texts], batch_size=32, normalize_embeddings=True,
                      show_progress_bar=kind == "doc").astype(np.float32)


MODELS = {"openai": _openai, "gemini": _gemini, "e5": _e5}
# Scored variants: (label, model, dims) - dims=None keeps the full vector.
VARIANTS = [
    ("openai-3-small@1536", "openai", None), ("openai-3-small@512", "openai", 512),
    ("openai-3-small@256", "openai", 256),
    ("gemini-001@1536", "gemini", None), ("gemini-001@768", "gemini", 768),
    ("gemini-001@512", "gemini", 512), ("gemini-001@256", "gemini", 256),
    ("multilingual-e5-base@768", "e5", None),
]


def cmd_embed(args):
    df = pl.read_parquet(SAMPLE)
    qs = json.loads(QUESTIONS.read_text(encoding="utf-8"))
    fn = MODELS[args.model]
    docs_path = CACHE / f"docs_{args.model}.npy"
    if not docs_path.exists():
        t = time.time()
        np.save(docs_path, fn([doc_text(r) for r in df.iter_rows(named=True)], "doc"))
        print(f"{args.model}: {df.height} docs in {time.time() - t:.0f}s")
    t = time.time()
    np.save(CACHE / f"queries_{args.model}.npy", fn([q["question"] for q in qs], "query"))
    # Single-query latency, as the agent sees it (median of 15 calls on a warm client).
    fn([qs[0]["question"]], "query")
    lat = []
    for q in qs[:15]:
        t1 = time.time()
        fn([q["question"]], "query")
        lat.append(time.time() - t1)
    (CACHE / f"latency_{args.model}.json").write_text(json.dumps({"median_s": float(np.median(lat))}))
    print(f"{args.model}: {len(qs)} queries; single-query median {np.median(lat):.2f}s")


# -------------------------------------------------------------------------- evaluate
def _keyword_ranks(df: pl.DataFrame, qs: list[dict]) -> list[int | None]:
    """The agent's current keyword search, replayed in memory (title hit 2, lead hit 1)."""
    from core.text import extract_keywords
    titles = [t.lower() for t in df["web_title"]]
    leads = [(l or "")[:500].lower() for l in df["lead"]]
    ids = df["article_id"].to_list()
    ranks = []
    for q in qs:
        kws = extract_keywords(q["question"])
        if not kws:
            ranks.append(None)
            continue
        scores = np.array([sum(2 if k in t else 1 if k in l else 0 for k in kws) for t, l in zip(titles, leads)])
        need = min(2, len(kws))
        order = [i for i in np.argsort(-scores, kind="stable") if scores[i] >= need]
        ranks.append(next((r for r, i in enumerate(order[:10], 1) if ids[i] == q["article_id"]), None))
    return ranks


def _metrics(ranks: list[int | None]) -> dict:
    n = len(ranks)
    return {"hit@1": sum(1 for r in ranks if r == 1) / n,
            "recall@10": sum(1 for r in ranks if r and r <= 10) / n,
            "mrr@10": sum(1 / r for r in ranks if r and r <= 10) / n}


def cmd_evaluate(_):
    df = pl.read_parquet(SAMPLE)
    qs = json.loads(QUESTIONS.read_text(encoding="utf-8"))
    ids = df["article_id"].to_list()
    target = [ids.index(q["article_id"]) for q in qs]
    types_ = sorted({q["type"] for q in qs})
    results = {}

    def score(label: str, ranks: list[int | None], extra: dict | None = None):
        row = {"all": _metrics(ranks)}
        for t in types_:
            row[t] = _metrics([r for r, q in zip(ranks, qs) if q["type"] == t])
        results[label] = row | (extra or {})

    for label, model, dims in VARIANTS:
        dp, qp = CACHE / f"docs_{model}.npy", CACHE / f"queries_{model}.npy"
        if not (dp.exists() and qp.exists()):
            print(f"skip {label}: not embedded yet")
            continue
        D, Q = np.load(dp), np.load(qp)
        if dims:
            D, Q = D[:, :dims], Q[:, :dims]
        D, Q = _normalise(D), _normalise(Q)
        sims = Q @ D.T
        ranks = []
        for qi, ti in enumerate(target):
            r = int((sims[qi] > sims[qi, ti]).sum()) + 1
            ranks.append(r if r <= 10 else None)
        lat = CACHE / f"latency_{model}.json"
        score(label, ranks, {"dims": dims or D.shape[1],
                             "query_latency_s": json.loads(lat.read_text())["median_s"] if lat.exists() else None})
    score("keyword (current agent)", _keyword_ranks(df, qs), {"dims": None, "query_latency_s": None})

    RESULTS.write_text(json.dumps(results, indent=1), encoding="utf-8")
    head = f"{'variant':28s} {'R@10 all':>9s} " + " ".join(f"{t + ' R@10':>11s}" for t in types_) + f" {'MRR':>6s} {'hit@1':>6s} {'q-lat':>6s}"
    print(head)
    for label, r in results.items():
        lat = f"{r['query_latency_s']:.2f}s" if r.get("query_latency_s") else "-"
        print(f"{label:28s} {r['all']['recall@10']:9.2f} " + " ".join(f"{r[t]['recall@10']:11.2f}" for t in types_)
              + f" {r['all']['mrr@10']:6.2f} {r['all']['hit@1']:6.2f} {lat:>6s}")


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("sample")
    sub.add_parser("questions")
    e = sub.add_parser("embed")
    e.add_argument("--model", choices=sorted(MODELS), required=True)
    sub.add_parser("evaluate")
    args = ap.parse_args()
    {"sample": cmd_sample, "questions": cmd_questions, "embed": cmd_embed, "evaluate": cmd_evaluate}[args.cmd](args)


if __name__ == "__main__":
    main()
