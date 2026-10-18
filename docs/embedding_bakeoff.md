# Embedding-model bake-off (2026-10-18)

**Question:** which embedding model to use for article-level vectors (agent phase, step 2).
**Answer:** `gemini-embedding-001` (Vertex AI). Store it at 768 dimensions in BigQuery and
serve 512 (or 256) dimensions from Pinecone. Final go-ahead pending the user's choice of
backfill scope and a check of the official price.

## Setup

- **Sample:** 5,064 articles published 2026-08-18 → 2026-10-17, from all 10 sources
  (Guardian 1,200; each Vietnamese site 500; English editions 400-500; vietnamnet_en 64),
  picked deterministically (`FARM_FINGERPRINT`).
- **Document text:** title + the first 1,500 characters of the body, the article-level design.
- **Questions:** 75 known-item questions. `gpt-4o-mini` wrote one paraphrased question per
  seed article (not copying the headline, with one specific detail): 25 English→English,
  25 Vietnamese→Vietnamese, 25 English→Vietnamese (cross-lingual). Each model must rank the
  seed article in its top 10 of all 5,064 (cosine similarity in memory).
- **Shorter variants:** the full vector truncated and re-normalised. That is how both API
  models define shorter dimensions, so one embedding run scores every size.
- **Baseline:** the agent's current keyword search, replayed in memory.
- **Reproduce:** `ai_agents/eval/embedding_bakeoff.py` (stages cached under
  `data/eval/bakeoff/`). Questions are in `ai_agents/eval/bakeoff_questions.json`, results in
  `ai_agents/eval/bakeoff_results.json`. The e5 document vectors were computed on a free
  Colab T4 (`ai_agents/eval/e5_gpu_embed.py`) and matched a local CPU check (cosine 1.000).

## Results

| Variant | R@10 all | EN→EN | EN→VI | VI→VI | MRR@10 | hit@1 | query latency |
|---|---|---|---|---|---|---|---|
| gemini-embedding-001 @1536 | 1.00 | 1.00 | 1.00 | 1.00 | 0.98 | 0.97 | 0.17 s |
| gemini-embedding-001 @768 | 1.00 | 1.00 | 1.00 | 1.00 | 0.98 | 0.97 | 0.17 s |
| gemini-embedding-001 @512 | 1.00 | 1.00 | 1.00 | 1.00 | 0.97 | 0.96 | 0.17 s |
| gemini-embedding-001 @256 | 1.00 | 1.00 | 1.00 | 1.00 | 0.96 | 0.95 | 0.17 s |
| text-embedding-3-small @1536 | 0.97 | 1.00 | 0.96 | 0.96 | 0.86 | 0.79 | 0.98 s |
| text-embedding-3-small @512 | 0.95 | 1.00 | 0.88 | 0.96 | 0.80 | 0.73 | 0.98 s |
| text-embedding-3-small @256 | 0.92 | 1.00 | 0.80 | 0.96 | 0.80 | 0.75 | 0.98 s |
| multilingual-e5-base @768 | 0.93 | 1.00 | 0.80 | 1.00 | 0.83 | 0.77 | 0.12 s (CPU) |
| keyword search (current agent) | 0.67 | 0.88 | 0.28 | 0.84 | 0.55 | 0.49 | - |

Query latency is the median of 15 single-question calls on a warm client, from Vietnam.
Gemini is served from asia-southeast1 (our region), OpenAI from the US. e5 ran locally on
CPU.

## Cost and size (full corpus, ~485k articles)

Tokens were measured on 40 sample documents per language. English is ~equal for both API
models (Gemini 323 tokens, OpenAI 314). Vietnamese is not: Gemini 418, OpenAI 721.

| | Gemini | OpenAI 3-small | e5-base |
|---|---|---|---|
| One-off backfill | ~177M tokens × $0.15/1M ≈ **$26** | ~238M × $0.02/1M ≈ **$5** | $0 API, but a GPU is required: 55+ CPU-minutes did not finish 5k docs here, so ~485k would take about a week on this laptop |
| New articles (~1,050/day) | ≈ $2/month | ≈ $0.40/month | $0 (CPU, minutes/day) |
| Pinecone size | 256 dims ≈ 0.6 GB · 512 ≈ 1.1 GB · 768 ≈ 1.6 GB | 512 ≈ 1.1 GB | 768 fixed ≈ 1.6 GB |
| App at question time | API call | API call | must load a ~1 GB model (RAM, cold starts) |

The Gemini price comes from third-party price trackers. Verify it on Google's pricing page
before the backfill. A Vertex batch mode may be cheaper and is also worth checking.
Pay-as-you-go: nothing is paid upfront; the backfill bills in the month it runs (it
would trip the $5 / $10 budget alerts once). Narrower scopes: 2024 → today ≈ $23, the
last 12 months ≈ $17.

## Reading the results

- **Gemini wins on every measure except price.** It is perfect on recall in all three
  question types, puts the right article first 95-97% of the time (OpenAI 73-79%, e5 77%),
  and is the fastest API. Truncation barely costs anything (256 ≈ 768), so it fits
  Pinecone's free 2 GB with years of headroom.
- **Cross-lingual is where models differ.** English questions about Vietnamese articles:
  Gemini 1.00, OpenAI 0.88-0.96, e5 0.80. Keyword search finds only 0.28 here, which is the
  gap dense search exists to fill.
- **e5 is the best free option for Vietnamese→Vietnamese (1.00)** but the weakest
  cross-lingually, and it needs a GPU plus a model inside the app.
- **Caveat: the test is near its ceiling.** Generated questions contain specific details,
  which makes known-item retrieval easier than real questions. hit@1 and MRR separate the
  models better than recall@10. The step-2c eval (realistic questions, graded relevance)
  will re-check the choice on harder queries.

## Decision

1. **Model: `gemini-embedding-001`** with task types `RETRIEVAL_DOCUMENT` (articles) and
   `RETRIEVAL_QUERY` (questions).
2. **Store 768 dimensions in BigQuery** (`article_embeddings`, the source of truth). Serve a
   truncated, re-normalised **512** from Pinecone (~1.1 GB). If space runs short, drop
   Pinecone to 256 **without re-embedding** (truncation of the stored vector).
3. **Fallback:** OpenAI `text-embedding-3-small` if cost matters more than cross-lingual
   quality ($5 vs $26 one-off).
