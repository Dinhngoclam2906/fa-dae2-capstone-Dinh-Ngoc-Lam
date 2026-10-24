"""
llm_compare.py - which chat model should the agent use? (2026-10-20)

Scripted multi-turn conversations run through the real agent (core.agent.build_agent: the
same retrieval, tools, prompt and memory), once per model. Measured per turn, objectively:
  * language   - the answer is in the expected language (share of words with Vietnamese
                 letters, after removing bold titles, links and URLs)
  * citations  - every URL in the answer appears in that turn's retrieved context or tool
                 output (an invented or mangled link counts as invalid)
  * tools      - tool calls per turn (the context usually answers already)
  * latency    - whole turn, retrieval included (identical for every model)
  * tokens     - input / output (reasoning included) per turn, for cost
Transcripts are saved so answer quality can be read side by side.

    uv run --with langchain-google-genai --env-file .env python ai_agents/eval/llm_compare.py run
    uv run --env-file .env python ai_agents/eval/llm_compare.py report

`run` resumes per (model, conversation); delete data/eval/llm_compare/runs.jsonl to redo.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))  # ai_agents/, for core.*

CACHE = Path("data/eval/llm_compare")
RUNS = CACHE / "runs.jsonl"
RESULTS = HERE / "llm_compare_results.json"

MODELS = ["gpt-4o-mini", "gpt-4.1-mini", "gpt-5.4-mini", "gemini-3.8-flash", "gemini-3.5-flash-lite"]

# (question, expected answer language). Turns share memory within a conversation.
CONVERSATIONS = {
    "vi_gold": [("Tin mới nhất về giá vàng?", "vi"), ("Còn giá xăng thì sao?", "vi"),
                ("Tóm tắt bài báo đầu tiên bạn nhắc đến", "vi")],
    "en_vn_fuel": [("What's the latest on fuel prices in Vietnam?", "en"),
                   ("Which outlets covered it, and when?", "en"),
                   ("Summarize the first article in Vietnamese, please", "vi")],
    "en_guardian": [("What did The Guardian write about UK climate policy in 2024?", "en"),
                    ("Anything on that this month?", "en")],
    "vi_football": [("Tình hình bóng đá Việt Nam tuần này", "vi"), ("Còn đội tuyển nữ thì sao?", "vi")],
    "en_about_vi": [("What is happening with Vietnam's real estate market recently?", "en"),
                    ("Tell me more about the second one", "en")],
    "vi_asks_en": [("Giá vàng hôm nay thế nào? Trả lời bằng tiếng Anh nhé", "en"), ("cảm ơn nhé", "vi")],
    "smalltalk": [("hi there", "en"), ("what kinds of news can you help me with?", "en")],
}

_VI_CHARS = re.compile(r"[ăâđêôơưạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹáàãéèíìóòõúùýĂÂĐÊÔƠƯ]")
_URL = re.compile(r"https?://[^\s)\]>\"'*]+")


def make_llm(name: str):
    from core.config import settings
    if name.startswith("gemini"):
        from langchain_google_genai import ChatGoogleGenerativeAI
        return ChatGoogleGenerativeAI(model=name, vertexai=True, project=settings.gcp_project,
                                      location="global", temperature=0.3, max_output_tokens=4000,
                                      timeout=60)
    from langchain_openai import ChatOpenAI
    if name.startswith("gpt-5"):
        # Reasoning model: no temperature, the token cap includes reasoning, and tools with a
        # reasoning effort need the Responses API (chat/completions returns 400).
        return ChatOpenAI(model=name, use_responses_api=True, reasoning={"effort": "low"},
                          max_tokens=4000, timeout=60)
    return ChatOpenAI(model=name, temperature=0.3, max_tokens=1500, timeout=60)


def answer_language(text: str) -> str:
    t = re.sub(r"\*\*[^*]+\*\*", " ", text)          # bold / italic titles may stay in their language
    t = re.sub(r"\*[^*\n]+\*", " ", t)
    t = re.sub(r"\[[^\]]*\]\([^)]*\)", " ", t)       # markdown links
    t = _URL.sub(" ", t)
    words = re.findall(r"\w+", t)
    if not words:
        return "?"
    share = sum(1 for w in words if _VI_CHARS.search(w)) / len(words)
    return "vi" if share > 0.2 else "en"


def _norm_url(u: str) -> str:
    return u.rstrip(".,;:").rstrip("/")


def run_conversation(model: str, conv: str) -> list[dict]:
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

    from core.agent import build_agent
    agent = build_agent(llm=make_llm(model))
    tid = str(uuid.uuid4())
    out = []
    for turn, (q, lang) in enumerate(CONVERSATIONS[conv]):
        cfg = {"configurable": {"thread_id": tid}, "recursion_limit": 12}
        t0, err = time.time(), None
        try:
            final = agent.invoke({"messages": [HumanMessage(content=q)]}, cfg)
        except Exception as e:  # noqa: BLE001 - record and carry on
            err, final = repr(e)[:300], None
        ms = int((time.time() - t0) * 1000)
        rec = {"model": model, "conv": conv, "turn": turn, "question": q, "expected_lang": lang,
               "ms": ms, "error": err}
        if final:
            msgs = final["messages"]
            start = max(i for i, m in enumerate(msgs) if isinstance(m, HumanMessage))
            turn_msgs = msgs[start:]
            answer = msgs[-1].content if isinstance(msgs[-1].content, str) else str(msgs[-1].content)
            if isinstance(msgs[-1].content, list):  # Gemini content blocks
                answer = "".join(b.get("text", "") if isinstance(b, dict) else str(b) for b in msgs[-1].content)
            available = (final.get("context") or "") + " ".join(
                str(m.content) for m in turn_msgs if isinstance(m, ToolMessage))
            avail_urls = {_norm_url(u) for u in _URL.findall(available)}
            cited = sorted({_norm_url(u) for u in _URL.findall(answer)})
            usage = [m.usage_metadata or {} for m in turn_msgs if isinstance(m, AIMessage)]
            rec.update(
                answer=answer, lang=answer_language(answer),
                tools=[tc["name"] for m in turn_msgs for tc in (getattr(m, "tool_calls", None) or [])],
                cited=cited, invalid=[u for u in cited if u not in avail_urls],
                input_tokens=sum(u.get("input_tokens", 0) for u in usage),
                output_tokens=sum(u.get("output_tokens", 0) for u in usage),
                retrieval_skipped=(final.get("retrieval_stats") or {}).get("skipped"))
        out.append(rec)
        print(f"{model:22s} {conv}/{turn} {ms / 1000:5.1f}s lang={rec.get('lang')}/{lang} "
              f"tools={len(rec.get('tools', []))} invalid={len(rec.get('invalid', []))} {err or ''}",
              flush=True)
    return out


def cmd_run(args):
    from core.retrieval import warm_up
    CACHE.mkdir(parents=True, exist_ok=True)
    done = set()
    if RUNS.exists():
        done = {(r["model"], r["conv"]) for r in map(json.loads, RUNS.read_text(encoding="utf-8").splitlines())}
    models = args.models.split(",") if args.models else MODELS
    warm_up()
    todo = [(m, c) for m in models for c in CONVERSATIONS if (m, c) not in done]
    # One worker per model: models run side by side, each model's conversations in order.
    by_model = {m: [c for mm, c in todo if mm == m] for m in models}

    def worker(m):
        recs = []
        for c in by_model[m]:
            recs.extend(run_conversation(m, c))
            with RUNS.open("a", encoding="utf-8") as f:
                f.writelines(json.dumps(r, ensure_ascii=False) + "\n" for r in recs if r["conv"] == c)
        return recs

    with ThreadPoolExecutor(max_workers=len(models)) as ex:
        list(ex.map(worker, models))


def cmd_report(args):
    rows = [json.loads(x) for x in RUNS.read_text(encoding="utf-8").splitlines()]
    prices = json.loads(args.prices) if args.prices else {}
    res = {}
    for m in MODELS:
        rs = [r for r in rows if r["model"] == m]
        if not rs:
            continue
        ok = [r for r in rs if not r["error"]]
        for r in ok:
            r["lang"] = answer_language(r["answer"])
        lat = sorted(r["ms"] for r in rs)
        tin = sum(r.get("input_tokens", 0) for r in ok)
        tout = sum(r.get("output_tokens", 0) for r in ok)
        p = prices.get(m)
        res[m] = {
            "turns": len(rs), "errors": len(rs) - len(ok),
            "language_ok": sum(1 for r in ok if r["lang"] == r["expected_lang"]) / max(1, len(ok)),
            "language_misses": [f"{r['conv']}/{r['turn']}" for r in ok if r["lang"] != r["expected_lang"]],
            "citations": sum(len(r["cited"]) for r in ok),
            "invalid_citations": sum(len(r["invalid"]) for r in ok),
            "tool_calls_per_turn": sum(len(r["tools"]) for r in ok) / max(1, len(ok)),
            "p50_s": lat[len(lat) // 2] / 1000, "p90_s": lat[int(len(lat) * 0.9)] / 1000,
            "input_tokens_per_turn": tin / max(1, len(ok)), "output_tokens_per_turn": tout / max(1, len(ok)),
            "usd_per_1k_turns": (tin * p[0] + tout * p[1]) / 1e6 / max(1, len(ok)) * 1000 if p else None,
        }
    RESULTS.write_text(json.dumps(res, indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"{'model':22s} lang  invalid/cited  tools/turn  p50   p90   in/turn  out/turn  $/1k turns")
    for m, r in res.items():
        cost = f"{r['usd_per_1k_turns']:.2f}" if r["usd_per_1k_turns"] is not None else "-"
        print(f"{m:22s} {r['language_ok']:.2f}  {r['invalid_citations']:3d}/{r['citations']:<4d}     "
              f"{r['tool_calls_per_turn']:.2f}     {r['p50_s']:4.1f}  {r['p90_s']:4.1f}  "
              f"{r['input_tokens_per_turn']:7.0f}  {r['output_tokens_per_turn']:7.0f}   {cost}"
              f"  errors={r['errors']} misses={r['language_misses']}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--models", help=f"comma list, default all: {','.join(MODELS)}")
    rp = sub.add_parser("report")
    rp.add_argument("--prices", help='JSON {"model": [usd_per_1M_input, usd_per_1M_output]}')
    args = ap.parse_args()
    {"run": cmd_run, "report": cmd_report}[args.cmd](args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
