# cli_agent.py - the Daily News AI Assistant in the terminal (same core as app.py).
#
#   uv run python agent/cli_agent.py            # chat
#   uv run python agent/cli_agent.py --stats    # also print retrieval stats per answer
#
# Commands: /new (new conversation), /stats (toggle stats), /quit.
import argparse
import logging
import sys
import threading
import time
import uuid

from core import bq
from core.agent import ask, build_agent
from core.config import SOURCES
from core.memory import create_checkpointer


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily News AI Assistant (terminal)")
    ap.add_argument("--stats", action="store_true", help="print retrieval stats after each answer")
    ap.add_argument("--demo", action="store_true", help="demo mode (the old hybrid pipeline, for comparison)")
    ap.add_argument("--sources", help="comma-separated source names to search, e.g. tuoitre,vnexpress "
                                      f"(default: all of {', '.join(SOURCES)})")
    args = ap.parse_args()
    sources = [s.strip() for s in args.sources.split(",") if s.strip()] if args.sources else None
    unknown = [s for s in sources or [] if s not in SOURCES]
    if unknown:
        ap.error(f"unknown source(s) {', '.join(unknown)}; valid: {', '.join(SOURCES)}")
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")  # Vietnamese output on Windows consoles

    from core.retrieval import warm_up
    threading.Thread(target=warm_up, daemon=True).start()
    checkpointer, kind = create_checkpointer()
    agent = build_agent(checkpointer)
    thread_id = str(uuid.uuid4())
    show_stats = args.stats
    print(f"Daily News AI Assistant - memory: {kind}. /new, /stats, /quit")
    print("=" * 64)
    while True:
        try:
            q = input("\nYou: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            return 0
        if not q:
            continue
        if q.lower() in {"/quit", "quit", "exit", "bye"}:
            print("Goodbye!")
            return 0
        if q == "/new":
            thread_id = str(uuid.uuid4())
            print("(new conversation)")
            continue
        if q == "/stats":
            show_stats = not show_stats
            print(f"(stats {'on' if show_stats else 'off'})")
            continue
        t0, b0 = time.time(), bq.usage["bytes_billed"]
        try:
            text, stats = ask(agent, q, thread_id=thread_id, demo_mode=args.demo, sources=sources)
        except Exception as e:  # noqa: BLE001
            print(f"Error: {type(e).__name__}: {e}")
            continue
        print(f"\nAssistant: {text}")
        if show_stats:
            keep = ("skipped", "mode", "keywords", "dense", "keyword", "unique", "returned", "scope",
                    "rerank_model", "retrieve_ms", "rerank_ms", "tools_used")
            print(f"\n  [stats] { {k: stats[k] for k in keep if k in stats} }")
            print(f"  [cost]  {time.time() - t0:.1f}s, BigQuery {(bq.usage['bytes_billed'] - b0) / 1e6:.0f} MB")


if __name__ == "__main__":
    sys.exit(main())
