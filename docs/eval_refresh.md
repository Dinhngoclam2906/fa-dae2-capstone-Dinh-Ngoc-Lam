# Retrieval eval refresh (agent phase 1c, 2026-10-19)

**Question:** should the agent's automatic retrieval move from the v1 index (Guardian-only
OpenAI chunks, built 2026-01) to v2 (`news-articles-v2`, gemini-embedding-2, all 10 sources),
and is the hybrid recipe (dense + keyword + FlashRank rerank) still the right one?

**Answer:** move to v2, and **drop the reranker and the keyword interleaving from the
automatic retrieval**: v2 dense alone wins every question group, at ~0.7 s and 0 BigQuery
bytes. BigQuery `VECTOR_SEARCH` is ruled out on cost. **Status: applied 2026-10-20**, with a
soft recency rule added. Uploads stayed on v1 until both were removed on 2026-10-24. See "Cut-over".

## Setup

- **Code:** `ai_agents/eval/retrieval_eval.py` (stages `sample → questions → run → judge →
  vsearch_cost → report`, cached under `data/eval/retrieval/`). Questions:
  `ai_agents/eval/retrieval_eval_questions.json`; results:
  `ai_agents/eval/retrieval_eval_results.json`. Commit `fe3ff4f`.
- **Corpus:** the full production indexes (~486k articles), not a sample. Every setup runs
  through the real `core/retrieval.py` code (`hybrid_retrieve` gained `article_vectors` /
  `rerank` overrides for this).
- **Seeds:** from `marts.obt_articles` (word_count ≥ 80), 40 per source per period by a fixed
  hash, then picked round-robin over sources. "Recent" = last 80 days (inside the keyword
  search's 90-day default window); "old" = 2026-03..06 (outside it).
- **Known-item questions (100):** gpt-4o-mini paraphrases one seed article (no headline copying,
  one specific detail). 20 each: EN→EN recent, VI→VI recent, EN→VI recent (English question,
  Vietnamese article), EN→EN old, VI→VI old. Scored on the **top 5 the agent's prompt
  receives**: found (recall@5) and ranked first (hit@1).
- **Topical questions (30):** broad questions about a seed's topic (10 EN, 10 VI, 10 EN about
  a Vietnamese article). All setups' top 5 pooled (490 pairs) and graded 0/1/2 by gpt-4o-mini
  on title + snippet. Scored P@5 (grade ≥ 1) and NDCG@5.
- **Setups:** `v1_hybrid` (production today), `v2_hybrid`, `v2_hybrid_norerank`, and the single
  legs `v2_dense`, `v1_dense`, `keyword` (top 10).
- **Cost:** < $1 of gpt-4o-mini; ~100 MB BigQuery per keyword query.

## Results

Known-item, recall@5 / hit@1:

| Setup | All | EN recent | VI recent | EN→VI | EN old | VI old |
|---|---|---|---|---|---|---|
| v1_hybrid (production) | 0.22 / 0.18 | 0.80 / 0.75 | 0.30 / 0.15 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| v2_hybrid | 0.61 / 0.26 | 0.90 / 0.60 | 0.60 / 0.15 | 0.15 / 0.00 | 0.80 / 0.45 | 0.60 / 0.10 |
| v2_hybrid_norerank | 0.90 / 0.75 | 1.00 / 0.95 | 0.95 / 0.65 | 0.80 / 0.55 | 0.95 / 0.85 | 0.80 / 0.75 |
| **v2_dense** | **0.95 / 0.75** | 1.00 / 0.95 | 0.95 / 0.65 | **0.90** / 0.55 | 0.95 / 0.85 | **0.95** / 0.75 |
| v1_dense | 0.00 / 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| keyword | 0.22 / 0.15 | 0.70 / 0.45 | 0.40 / 0.30 | 0.00 | 0.00 | 0.00 |

Topical, P@5 / NDCG@5:

| Setup | All | EN | VI | EN→VI |
|---|---|---|---|---|
| v1_hybrid (production) | 0.31 / 0.27 | 0.36 / 0.30 | 0.22 / 0.21 | 0.36 / 0.31 |
| v2_hybrid | 0.57 / 0.54 | 0.60 / 0.61 | 0.60 / 0.56 | 0.52 / 0.44 |
| v2_hybrid_norerank | 0.64 / 0.67 | 0.66 / 0.67 | 0.68 / 0.73 | 0.58 / 0.60 |
| **v2_dense** | **0.86 / 0.86** | 0.84 / 0.82 | **0.96 / 0.95** | 0.78 / 0.81 |
| v1_dense | 0.12 / 0.10 | 0.08 / 0.05 | 0.08 / 0.08 | 0.20 / 0.16 |
| keyword | 0.28 / 0.25 | 0.30 / 0.28 | 0.30 / 0.28 | 0.24 / 0.18 |

Latency and cost per question:

| Setup | p50 | p90 | BigQuery |
|---|---|---|---|
| v1_hybrid | 3.2 s | 4.5 s | ~100 MB |
| v2_hybrid | 2.8 s | 3.8 s | ~100 MB (shown as 0: BigQuery result cache, same query as v1_hybrid) |
| v2_hybrid_norerank | 0.7 s | 1.1 s | ~100 MB (cached in the run) |
| v2_dense | 0.7 s | 1.0 s | 0 |
| keyword | 0.4 s | 0.6 s | ~100 MB (cached in the run) |
| `VECTOR_SEARCH` (dry run) | – | – | 3.02 GB all-time, 0.69 GB 90 d, 0.30 GB 30 d, 0.13 GB 7 d (month partitions) |

## Findings

1. **Production retrieval is degraded today.** v1 dense found none of the 2026-03+ articles
   (its index ends 2026-01), so `v1_hybrid` is effectively keyword + rerank: nothing
   cross-lingual, nothing older than 90 days.
2. **The FlashRank reranker hurts** (v2: recall@5 0.90 → 0.61, EN→VI 0.80 → 0.15, topical P@5
   0.64 → 0.57) and costs ~2 s. It reorders by a 400–500-char passage with
   `ms-marco-MultiBERT-L-12` (multilingual, weak) and pushes good dense hits out of the top 5.
   This reverses the 2026-01 eval (English Guardian only, P@5 0.87 vs 0.61 dense-only), where
   the dense leg was OpenAI chunks.
3. **Interleaving keyword hits also hurts** (recall@5 0.95 → 0.90, topical P@5 0.86 → 0.64):
   the 50/50 interleave puts keyword results at ranks 2 and 4, and keyword never beat dense
   in any group.
4. **`VECTOR_SEARCH` is ruled out**: 3 GB per question all-time is 5× the agent's 600 MB cap
   (≈ 330 questions would use the monthly free tier); Pinecone bills 0 BigQuery bytes. Its
   quality would equal `v2_dense` (same vectors, 768 vs 512 dims within 0.01 in the bake-off).

**Caveats:** ~20 questions per group (differences < ~0.1 are noise; the gaps above are
0.3–0.7). Questions were written by an LLM from each article's lead, which favours dense
retrieval on known-item; topical grades are an LLM's, from snippets. Direction is clear,
magnitudes approximate.

## Cut-over (step 1b, applied 2026-10-20)

What changed (`ai_agents/core/`):
1. `AGENT_ARTICLE_VECTORS` now defaults to `v2`.
2. New setting `AGENT_RETRIEVAL_MODE`, default `dense`: automatic retrieval is **v2 dense
   top 5**, with no FlashRank and no keyword interleave. `hybrid` is the old pipeline; demo
   mode runs it, for comparison.
3. Keyword search stays an LLM tool (`search_articles_by_keywords`). `search_news` now runs
   the dense search with the filters applied inside Pinecone.
4. **Soft recency (new, not in the original proposal).** Dense search has no sense of
   time: "Tin mới nhất về giá vàng" ranked January articles first. The old hybrid hid this,
   because the keyword leg's 90-day default window supplied recency.
   - When a question has a time phrase (`core.text.recency_days`: latest / today / this
     week / mới nhất / hôm nay / tuần này … → 3, 7, 30 or 31 days) and no explicit dates,
     the top 3 hits from that window come first, then the best hits of all dates.
   - A *hard* window was tried first. It lost 8 of 100 known-item questions, whose "recent"
     or "last week" referred to the article's own time, not today's.
5. **Warm-up.** The Vertex and Pinecone clients load in the background when the app or CLI
   starts. The first question used to spend ~14 s initialising, close to the 20 s retrieval
   deadline. Client creation is now locked: two cold threads each built a client, and the
   garbage-collected one closed its HTTP connection mid-request.
6. **Not done then:** user uploads stayed on the v1 index `guardian-capstone`, because the
   user planned to remove the upload feature. Done 2026-10-24: uploads removed and the v1
   index deleted, so `v1_hybrid` / `v1_dense` below can no longer be re-run.

Verification. Both systems were re-run on the same day's index (+1,426 articles since
2026-10-19, which moved a few `v2_dense` numbers):

| Setup | Known recall@5 / hit@1 | EN recent | VI recent | EN→VI | EN old | VI old | Topical P@5 / NDCG@5 | p50 |
|---|---|---|---|---|---|---|---|---|
| v2_dense (re-run) | 0.95 / 0.75 | 1.00 / 0.95 | 0.95 / 0.65 | 0.90 / 0.55 | 0.95 / 0.85 | 0.95 / 0.75 | 0.83 / 0.81 | 0.77 s |
| **agent_default** (new production path) | **0.96 / 0.71** | 1.00 / 0.90 | 1.00 / 0.70 | 0.90 / 0.55 | 0.95 / 0.70 | 0.95 / 0.70 | **0.83 / 0.80** | 0.83 s |

The two are equal within noise (~20 questions per group), against 0.22 / 0.31 for the old
production path. The eval can't measure the recency gain itself: its questions take their
time words from the article. A 5-turn agent conversation (`ask()`: VI → VI follow-up →
"summarize the first article" → EN with a year → thanks) behaved correctly: follow-up,
back-reference to the previous sources, small-talk skip, cited recent sources.

Observed, not caused by the cut-over: Vietnamese questions get English answers in both
modes (gpt-4o-mini ignores prompt rule 3). Turn 1 also calls `browse_articles` twice even
when the context already answers it (~18 s for that turn).

## Incident during the run

The first run hung 12 min on one v1 Pinecone call that never returned. That exposed a
production bug: `hybrid_retrieve`'s 20 s per-leg timeout never fired, because leaving the
executor's `with` block waits for every thread. Fixed in `fe3ff4f` (one 20 s deadline,
`shutdown(wait=False)`); the eval runner also got a 60 s per-call limit. 2 calls timed out
in the rerun (network) and were retried; no hybrid call lost a leg (none ≥ 20 s).
