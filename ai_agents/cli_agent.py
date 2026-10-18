# cli_agent.py - the Daily News AI Assistant in the terminal (same core as app.py).
#
#   uv run python ai_agents/cli_agent.py            # chat
#   uv run python ai_agents/cli_agent.py --stats    # also print retrieval stats per answer
#
# Commands: /new (new conversation), /stats (toggle stats), /quit.
import argparse
import logging
import sys
import time
import uuid

from core import bq
from core.agent import ask, build_agent
from core.memory import create_checkpointer


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily News AI Assistant (terminal)")
    ap.add_argument("--stats", action="store_true", help="print retrieval stats after each answer")
    ap.add_argument("--demo", action="store_true", help="demo mode (fewer semantic, more keyword results)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")  # Vietnamese output on Windows consoles

    checkpointer, kind = create_checkpointer()
    agent = build_agent(checkpointer)
    session_id, thread_id = str(uuid.uuid4()), str(uuid.uuid4())
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
            text, stats = ask(agent, q, thread_id=thread_id, session_id=session_id, demo_mode=args.demo)
        except Exception as e:  # noqa: BLE001
            print(f"Error: {type(e).__name__}: {e}")
            continue
        print(f"\nAssistant: {text}")
        if show_stats:
            keep = ("skipped", "keywords", "dense", "user_docs", "keyword", "unique", "returned",
                    "rerank_model", "retrieve_ms", "rerank_ms", "tools_used")
            print(f"\n  [stats] { {k: stats[k] for k in keep if k in stats} }")
            print(f"  [cost]  {time.time() - t0:.1f}s, BigQuery {(bq.usage['bytes_billed'] - b0) / 1e6:.0f} MB")


if __name__ == "__main__":
    sys.exit(main())
