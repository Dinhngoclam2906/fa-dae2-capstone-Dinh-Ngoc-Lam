"""
chunk_pilot.py - would chunk vectors lift the deep-detail ceiling? A ~$1 pilot on 7 days.

Article vectors see the title + the first 1,500 body characters. The passage rescue
(core/passages.py) re-ranks the dense top 30 by BM25 over full bodies, so it can only find a
deep detail whose article is already in that pool: 0.80 of deep-detail seeds on the hard set
(docs/hard_eval.md). This pilot embeds the REST of each body as chunks for the last 7 days,
writes fresh questions on those articles, and compares, with every system limited to the
same 7-day window (a smaller corpus would flatter the chunks otherwise):

  dense          production v2 dense (Pinecone, 512 dims, date filter), top 5
  dense_rescue   production: dense top 30 + passage rescue (keep 4)
  local          control: exact 768-dim search over the article vectors only (no chunks), so
                 the chunk gain is not confused with 512 dims / approximate search
  chunks         local: article score = max cosine over its article vector and chunk vectors
  chunks_rescue  chunks top 30 + passage rescue
  union_rescue   dense top 30 and chunk top 30 fused (reciprocal rank), then the rescue

Nothing is written to BigQuery or Pinecone: vectors live under data/eval/chunks/.

    uv run --env-file .env python ai_agents/eval/chunk_pilot.py corpus      # ~0.5 GB read
    uv run --env-file .env python ai_agents/eval/chunk_pilot.py questions   # gpt-4.1-mini
    uv run --env-file .env python ai_agents/eval/chunk_pilot.py embed [--max-tokens 9e6]
    uv run --env-file .env python ai_agents/eval/chunk_pilot.py run
    uv run --env-file .env python ai_agents/eval/chunk_pilot.py report

Metrics: found@5 (seed in the top 5), hit@1, pool@30 (seed in the 30 the rescue reads).
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import polars as pl

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))  # ai_agents/, for core.*
sys.path.insert(0, str(HERE.parent / "jobs"))  # for embed_articles
sys.path.insert(0, str(HERE))
import hard_eval as H  # noqa: E402

CACHE = Path("data/eval/chunks")
CORPUS = CACHE / "corpus.parquet"
ARTICLE_VECS = CACHE / "article_vectors.parquet"
META = CACHE / "meta.json"
QUESTIONS = CACHE / "questions.json"
CHUNK_DIR = CACHE / "chunk_vectors"
RUNS = CACHE / "runs.jsonl"
DAYS = 7
LEAD_CHARS = 1300   # chunks start here: 200 chars of overlap with the article vector's 1,500
CHUNK_CHARS = 1500
CHUNK_STEP = 1300
POOL = 30
SYSTEMS = ["dense", "dense_rescue", "local", "local_rescue", "chunks", "chunks_rescue", "union_rescue"]


def _meta() -> dict:
    return json.loads(META.read_text(encoding="utf-8"))


def chunks_of(title: str, body: str) -> list[str]:
    body = body or ""
    return [f"title: {title} | text: {body[i:i + CHUNK_CHARS]}"
            for i in range(LEAD_CHARS, max(len(body) - 200, LEAD_CHARS), CHUNK_STEP)]


def cmd_corpus(_):
    from core import bq
    end = H._today_local()
    start = end - timedelta(days=DAYS - 1)
    p = os.environ["GCP_PROJECT"]
    window = [bq.p("start", start, "DATE"), bq.p("end", end, "DATE"),
              bq.p("lo", datetime.combine(start - timedelta(days=1), datetime.min.time(), timezone.utc), "TIMESTAMP"),
              bq.p("hi", datetime.combine(end + timedelta(days=2), datetime.min.time(), timezone.utc), "TIMESTAMP")]
    rows = bq.query(f"""
        SELECT article_id, source_name, language, web_title, web_url, body_text,
               CAST(DATE(web_publication_date_local) AS STRING) AS pub_date, LENGTH(body_text) AS body_len
        FROM `{p}.marts.obt_articles`
        WHERE web_publication_date >= @lo AND web_publication_date < @hi
          AND DATE(web_publication_date_local) BETWEEN @start AND @end""", window, max_bytes=2_000_000_000)
    corpus = pl.DataFrame(rows)
    vecs = bq.query(f"""
        SELECT article_id, embedding FROM `{p}.vectors.article_embeddings`
        WHERE web_publication_date >= @lo AND web_publication_date < @hi AND model = 'gemini-embedding-2'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY embedded_at DESC) = 1""",
                    window, max_bytes=1_000_000_000)
    av = pl.DataFrame(vecs).filter(pl.col("article_id").is_in(corpus["article_id"].implode()))
    corpus = corpus.filter(pl.col("article_id").is_in(av["article_id"].implode()))  # embedded = in Pinecone
    CACHE.mkdir(parents=True, exist_ok=True)
    corpus.write_parquet(CORPUS)
    av.write_parquet(ARTICLE_VECS)
    n_chunks = {lang: sum(len(chunks_of(t, b)) for t, b in zip(g["web_title"], g["body_text"]))
                for (lang,), g in corpus.group_by("language")}
    chars = {lang: sum(len(c) for t, b in zip(g["web_title"], g["body_text"]) for c in chunks_of(t, b))
             for (lang,), g in corpus.group_by("language")}
    est = chars.get("vi", 0) / 3.6 + chars.get("en", 0) / 4.9  # chars per token measured on the article vectors
    META.write_text(json.dumps({"start": str(start), "end": str(end), "articles": corpus.height,
                                "chunks": n_chunks, "est_tokens": est}), encoding="utf-8")
    print(f"window {start}..{end}: {corpus.height} articles with vectors, chunks {n_chunks}, "
          f"≈ {est / 1e6:.1f}M tokens ≈ ${est / 1e6 * 0.2:.2f}; BigQuery {bq.usage['bytes_billed'] / 1e9:.2f} GB")


def cmd_questions(_):
    from openai import OpenAI
    df = pl.read_parquet(CORPUS).with_columns(
        pl.col("body_text").str.slice(0, 1500).alias("lead"), pl.col("body_text").str.slice(3000, 1800).alias("deep"))
    rng, used, seeds = random.Random(1023), set(), []
    for lang in ("en", "vi"):
        seeds += [("deep_detail", lang, r) for r in H._pick(df.filter(pl.col("body_len") >= 5000), rng, used, 20, language=lang)]
        seeds += [("keyword_short", lang, r) for r in H._pick(df, rng, used, 10, language=lang)]
    client, lang_name = OpenAI(), {"en": "English", "vi": "Vietnamese"}

    def one(seed):
        cat, lang, r = seed
        msg = H.PROMPTS[cat].format(lang=lang_name[lang], title=r["web_title"], lead=r["lead"][:900],
                                    deep=(r["deep"] or "")[:1800], phrase="", constraint="", standalone=H.STANDALONE)
        q = client.chat.completions.create(model=H.GEN_MODEL, temperature=0.7,
                                           messages=[{"role": "user", "content": msg}]).choices[0].message.content
        return {"category": cat, "language": lang, "question": q.strip().splitlines()[0].strip().strip('"'),
                "article_id": r["article_id"], "source_name": r["source_name"], "pub_date": r["pub_date"],
                "title": r["web_title"]}

    with ThreadPoolExecutor(8) as ex:
        qs = list(ex.map(one, seeds))
    for i, q in enumerate(qs):
        q["qid"] = i
    QUESTIONS.write_text(json.dumps(qs, ensure_ascii=False, indent=1), encoding="utf-8")
    print(pl.DataFrame(qs).group_by("category", "language").len().sort("category", "language"))


def _done_chunks() -> set[str]:
    if not CHUNK_DIR.exists() or not any(CHUNK_DIR.glob("*.parquet")):
        return set()
    return set(pl.read_parquet(CHUNK_DIR / "*.parquet", columns=["chunk_id"])["chunk_id"])


def cmd_embed(args):
    import embed_articles as E
    corpus = pl.read_parquet(CORPUS)
    todo = [(f"{r['article_id']}#{i}", r["article_id"], text)
            for r in corpus.iter_rows(named=True) for i, text in enumerate(chunks_of(r["web_title"], r["body_text"]))]
    done = _done_chunks()
    todo = [t for t in todo if t[0] not in done]
    print(f"{len(todo)} chunks to embed ({len(done)} already done)", flush=True)
    CHUNK_DIR.mkdir(parents=True, exist_ok=True)
    tokens, n, t0 = 0, 0, time.time()
    schema = {"chunk_id": pl.String, "article_id": pl.String, "embedding": pl.List(pl.Float32), "tokens": pl.Int64}

    def one(t):
        try:
            return t, *E.embed_one(t[2])
        except Exception as e:  # noqa: BLE001 - left for the next run
            print(f"failed {t[0]}: {e!r}"[:200], flush=True)
            return t, None, 0

    with ThreadPoolExecutor(16) as ex:  # 16 parallel requests measured best (embed_articles.py)
        for b in range(0, len(todo), 512):
            rows = [{"chunk_id": t[0], "article_id": t[1], "embedding": v, "tokens": k}
                    for t, v, k in ex.map(one, todo[b:b + 512]) if v is not None]
            if rows:
                pl.DataFrame(rows, schema=schema).write_parquet(CHUNK_DIR / f"part-{b:06d}-{time.time_ns()}.parquet")
            tokens, n = tokens + sum(r["tokens"] for r in rows), n + len(rows)
            print(f"{n}/{len(todo)} ({n / (time.time() - t0):.0f}/s) | {tokens / 1e6:.2f}M tokens "
                  f"≈ ${tokens / 1e6 * 0.2:.2f}", flush=True)
            if tokens > args.max_tokens:
                print(f"token cap {args.max_tokens / 1e6:.1f}M reached, stopping (rerun to continue)", flush=True)
                break
    print(f"DONE: {n} embedded, {tokens / 1e6:.2f}M tokens ≈ ${tokens / 1e6 * 0.2:.2f}, "
          f"{(time.time() - t0) / 60:.0f} min", flush=True)


def _unit(m: np.ndarray) -> np.ndarray:
    return m / np.linalg.norm(m, axis=1, keepdims=True)


def cmd_run(_):
    from core import passages
    from core import retrieval as R
    from core.config import settings
    meta, qs = _meta(), json.loads(QUESTIONS.read_text(encoding="utf-8"))
    corpus = pl.read_parquet(CORPUS)
    titles = dict(zip(corpus["article_id"], corpus["web_title"]))
    bodies = dict(zip(corpus["article_id"], corpus["body_text"]))
    av = pl.read_parquet(ARTICLE_VECS)
    cv = pl.read_parquet(CHUNK_DIR / "*.parquet")
    ids = av["article_id"].to_list() + cv["article_id"].to_list()  # one row per vector -> its article
    owner_idx = {a: i for i, a in enumerate(av["article_id"])}
    owner = np.array([owner_idx[a] for a in ids])
    mat = _unit(np.vstack([np.asarray(av["embedding"].to_list(), dtype=np.float32),
                           np.asarray(cv["embedding"].to_list(), dtype=np.float32)]))
    art_ids = av["article_id"].to_list()
    print(f"{len(art_ids)} articles, {cv.height} chunks, {mat.shape[0]} vectors", flush=True)
    filters = {"start_date": meta["start"], "end_date": meta["end"]}

    def rescue(q, pool):
        ranked = passages.rescue(q, [(titles.get(a, ""), bodies.get(a, "")) for a in pool], settings.passage_keep)
        return [pool[i] for i, _ in ranked]

    out = []
    for q in qs:
        v768 = np.asarray(R.embed_query(q["question"], dims=768), dtype=np.float32)
        v512 = v768[:512] / np.linalg.norm(v768[:512])
        dense = [c.article_id for c in R._dense_articles_v2(q["question"], POOL, filters, vec=v512.tolist())]
        best = np.full(len(art_ids), -1.0, dtype=np.float32)
        np.maximum.at(best, owner, mat @ v768)
        chunks = [art_ids[i] for i in np.argsort(-best)[:POOL]]
        local = [art_ids[i] for i in np.argsort(-(mat[:len(art_ids)] @ v768))[:POOL]]
        fused: dict[str, float] = {}
        for lst in (dense, chunks):
            for rank, a in enumerate(lst):
                fused[a] = fused.get(a, 0) + 1 / (60 + rank)
        union = sorted(fused, key=lambda a: -fused[a])[:POOL]
        res = {"dense": (dense, dense[:5]), "dense_rescue": (dense, rescue(q["question"], dense)[:5]),
               "local": (local, local[:5]), "local_rescue": (local, rescue(q["question"], local)[:5]),
               "chunks": (chunks, chunks[:5]), "chunks_rescue": (chunks, rescue(q["question"], chunks)[:5]),
               "union_rescue": (union, rescue(q["question"], union)[:5])}
        for s, (pool, top) in res.items():
            out.append({"qid": q["qid"], "system": s, "top": top, "pool": pool})
    RUNS.write_text("\n".join(json.dumps(r) for r in out) + "\n", encoding="utf-8")
    print(f"{len(out)} rows", flush=True)


def cmd_report(_):
    qs = {q["qid"]: q for q in json.loads(QUESTIONS.read_text(encoding="utf-8"))}
    rows = []
    for r in map(json.loads, RUNS.read_text(encoding="utf-8").splitlines()):
        q = qs[r["qid"]]
        rows.append({"system": r["system"], "category": q["category"], "language": q["language"],
                     "found@5": q["article_id"] in r["top"], "hit@1": r["top"][:1] == [q["article_id"]],
                     "pool@30": q["article_id"] in r["pool"]})
    df = pl.DataFrame(rows)
    order = {s: i for i, s in enumerate(SYSTEMS)}
    for keys in (["category"], ["category", "language"]):
        t = (df.group_by(["system", *keys]).agg(pl.len().alias("n"), *[pl.col(c).mean().round(2) for c in
                                                                       ("found@5", "hit@1", "pool@30")])
             .with_columns(pl.col("system").replace_strict(order).alias("_o")).sort([*keys, "_o"]).drop("_o"))
        with pl.Config(tbl_rows=100, tbl_hide_dataframe_shape=True):
            print(t)
    m = _meta()
    tok = pl.read_parquet(CHUNK_DIR / "*.parquet", columns=["tokens"])["tokens"].sum()
    print(f"window {m['start']}..{m['end']}, {m['articles']} articles; chunk tokens {tok / 1e6:.2f}M ≈ ${tok / 1e6 * 0.2:.2f}")


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("corpus", "questions", "run", "report"):
        sub.add_parser(name)
    e = sub.add_parser("embed")
    e.add_argument("--max-tokens", type=float, default=9e6, help="stop once this many tokens are used (cost guard)")
    args = ap.parse_args()
    return {"corpus": cmd_corpus, "questions": cmd_questions, "embed": cmd_embed,
            "run": cmd_run, "report": cmd_report}[args.cmd](args) or 0


if __name__ == "__main__":
    sys.exit(main())
