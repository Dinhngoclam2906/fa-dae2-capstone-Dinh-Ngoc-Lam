# Chat-model comparison for the agent (2026-10-20)

**Question:** after the answer-language fix, should the agent move off `gpt-4o-mini`?
**Answer:** `gpt-5.4-mini` gives the best answers (grounded, every fact linked to its
article, fewer wasted tool calls, faster median) at ~3.5× the cost per turn. `gpt-4o-mini`
is acceptable now that the language fix works. The other three each have a disqualifying
flaw. **Status: the user chose `gpt-5.4-mini`; it is the default since 2026-10-20** (see
"Switch" at the end).

## Setup

- **Code:** `ai_agents/eval/llm_compare.py` (results in `llm_compare_results.json`,
  transcripts in `data/eval/llm_compare/runs.jsonl`). Each model drives the real agent
  (`core.agent.build_agent(llm=...)`): same retrieval (v2 dense + soft recency), tools,
  prompt and memory.
- **Conversations:** 7 scripted conversations, 16 turns. VI news + follow-ups + "summarize
  the first article"; EN about Vietnam, then "summarize in Vietnamese"; EN Guardian with a
  year, then "this month"; VI football; EN about VI real estate + "the second one"; a VI
  question asking for an English answer; small talk.
- **Metrics:**
  - language: share of words with Vietnamese letters, after removing titles and links
  - broken links: every flagged URL checked against `obt_articles.web_url`
  - tool calls per turn
  - whole-turn latency (retrieval included, the same for every model)
  - tokens → cost
- **Prices:** list price per 1M tokens, input / output, checked 2026-10-20 on OpenAI's
  and Google's pricing pages:
  - gpt-4o-mini 0.15 / 0.60
  - gpt-4.1-mini 0.40 / 1.60
  - gpt-5.4-mini 0.75 / 4.50
  - gemini-3.8-flash 0.75 / 3.75 (1.50 / 7.50 from 2027-01-01)
  - gemini-3.5-flash-lite 0.30 / 2.50
- **Settings:** `gpt-5.4-mini` uses reasoning effort `low`. It needs the Responses API
  (`use_responses_api=True`): with tools plus a reasoning effort, chat/completions returns
  400. The Gemini models ran on Vertex AI (`global`) via `langchain-google-genai`, used
  only through `uv run --with`; it is not a project dependency.

## Results

| Model | Language | Broken links | URLs cited | Tools / turn | p50 | p90 | $ / 1k turns |
|---|---|---|---|---|---|---|---|
| gpt-4o-mini (current) | 16/16 | 0 | 34 | 1.38 | 9.1 s | 12.1 s | 2.08 |
| gpt-4.1-mini | 16/16 | 0 | **6** | 0.50 | 5.0 s | 13.7 s | 2.35 |
| **gpt-5.4-mini** | 16/16 | 0 (6 homepage links in one turn) | 36 | 0.75 | 6.8 s | 13.5 s | 7.29 |
| gemini-3.8-flash | 16/16 (1 error) | 0 | 59 | 0.93 | **22.8 s** | **125 s** | 16.11 |
| gemini-3.5-flash-lite | 16/16 | **3 mangled** | 23 | 0.50 | 6.6 s | 10.8 s | 3.10 |

**Before the language fix**, `gpt-4o-mini` answered 0 of 3 Vietnamese questions in
Vietnamese. The fix (`core/agent.py` `_language_note`: "Write your reply in {language}…",
sent as the last message on every LLM call and never stored in memory) brought it to 16/16.

## Reading the transcripts

- **gpt-4o-mini:** correct and cited, but it wastes tools. On "Tin mới nhất về giá vàng"
  it called `get_news_summary` 3 times (50 s) and padded the answer with a loosely related
  Brent-oil article. It has the highest input tokens per turn of the OpenAI models.
- **gpt-4.1-mini:** fast and cheap, but it **rarely gives URLs**: 6 in 16 turns, against
  prompt rule 2. It cites by title only. It also hedged ("the search did not return
  specific articles"), then listed relevant ones anyway.
- **gpt-5.4-mini:** the most precise answers. Each fact sits next to its article and URL,
  and it answered from the context without tools where the context sufficed. Its weak
  spot: on "which outlets covered it" it linked outlet homepages instead of the articles.
- **gemini-3.8-flash:** good, thorough answers, but too slow for chat. Turns took
  3–412 s, one errored, and its heavy thinking made it the most expensive.
- **gemini-3.5-flash-lite:** good prose, but **3 of 23 URLs were mangled**. It dropped "19"
  from a date, misspelled `chua-bao-gio` as `chua-bao-jay`, and wrote Vietnamese letters
  into a slug (`cựu-binh`). For a news assistant, a broken source link disqualifies it.

## Cost at our scale

Rough guide: 2–3 users ≈ 1,000 turns/month.
- gpt-4o-mini ≈ $2/month
- gpt-4.1-mini ≈ $2.4/month
- **gpt-5.4-mini ≈ $7.3/month**
- flash-lite ≈ $3/month
- 3.8-flash ≈ $16/month

The OpenAI bill is separate from the GCP budget ($5–10/month). A demo with a few hundred
turns/month costs a fraction of these.

## Caveats

One run, 16 turns per model, so small differences (latency, tools per turn) are noise. The
clear signals: missing URLs (4.1-mini), mangled URLs (flash-lite) and latency
(3.8-flash). No LLM judge: answer quality above is from reading the transcripts side by side.

Reproduce: `uv run --with langchain-google-genai --env-file .env python ai_agents/eval/llm_compare.py run`,
then `... llm_compare.py report --prices '{"gpt-4o-mini":[0.15,0.6], ...}'`.

## Switch (applied 2026-10-20)

- `core/llm.py` `chat_model()` builds the model for both the agent and the
  `summarize_article` tool. Reasoning models (gpt-5.x, o-series) get the Responses API,
  `reasoning={"effort": AGENT_REASONING_EFFORT}` (default `low`), no temperature, and
  2,500 extra tokens of headroom for reasoning. Other models keep temperature + `max_tokens`.
- `OPENAI_MODEL` now defaults to `gpt-5.4-mini` (`.env` updated too).
- Responses API replies are content blocks, so `ask()` and the summary tool read
  `message.text`; `str(message.content)` would have shown raw block lists.
- **Gotcha:** `uv run --env-file .env` does not override variables already set in the
  shell. This session's terminal had inherited `OPENAI_MODEL=gpt-4o-mini` from its parent
  process, so the first check still ran 4o-mini. Open a new terminal, or `unset OPENAI_MODEL`.
- Same 5-turn conversation as the cut-over check, gpt-4o-mini → gpt-5.4-mini:
  - turn times 19.5 / 9.7 / 15.6 / 10.6 / 1.5 s → 9.0 / 4.5 / 11.0 / 3.8 / 1.8 s
  - tool calls 7 → 1: before, two searches in each of turns 1, 2 and 4, plus the summary
    tool; after, only `summarize_article`, which the request needs
  - answer languages correct; every fact links its article
- Roll back with `OPENAI_MODEL=gpt-4o-mini`; no code change needed.
