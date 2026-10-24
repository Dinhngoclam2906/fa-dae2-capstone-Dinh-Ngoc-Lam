# app.py - Daily News AI Assistant (Streamlit). The agent itself lives in core/.
#
#   uv run streamlit run agent/app.py
#
# Replaces the old app.py + app_demo.py pair (merged; the demo variant's features are
# here: demo mode, hybrid-search stats; its PDF upload was removed 2026-10-24).
# Public-demo safety:
#   * conversations are listed only within the browser session (no "switch to any
#     thread ID" box: thread ids are random UUIDs);
#   * a per-session question limit (AGENT_RATE_LIMIT_PER_HOUR) and per-query BigQuery
#     byte caps (core/bq.py).
import re
import threading
import time
import uuid

import streamlit as st

from core import bq
from core.agent import ask, build_agent
from core.config import settings
from core.memory import create_checkpointer

st.set_page_config(page_title="Daily News AI Assistant", page_icon="📰", layout="centered",
                   initial_sidebar_state="expanded")

st.markdown("""
<style>
    [data-testid="stChatMessageUser"] { background: #2d5a8c; color: white; border-radius: 16px; padding: 12px; }
    [data-testid="stChatMessageAssistant"] { background: #16213e; border: 1px solid #4e9af1; border-radius: 16px; padding: 12px; }
    h1 { color: #4e9af1; text-align: center; }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_agent():
    """One compiled graph per server process; conversations are separated by thread_id."""
    from core.retrieval import warm_up
    threading.Thread(target=warm_up, daemon=True).start()
    checkpointer, kind = create_checkpointer()
    return build_agent(checkpointer), kind


agent, memory_kind = get_agent()


# ==================== SESSION STATE ====================
def init_session():
    ss = st.session_state
    if "threads" not in ss:
        tid = str(uuid.uuid4())
        ss.threads = {tid: {"name": "Conversation 1", "messages": [], "stats": []}}
        ss.thread_id = tid
    ss.setdefault("question_times", [])
    ss.setdefault("demo_mode", False)
    ss.setdefault("processing", False)


def current():
    return st.session_state.threads[st.session_state.thread_id]


def new_conversation():
    tid = str(uuid.uuid4())
    st.session_state.threads[tid] = {"name": f"Conversation {len(st.session_state.threads) + 1}",
                                     "messages": [], "stats": []}
    st.session_state.thread_id = tid


def rate_limited() -> bool:
    now = time.time()
    recent = [t for t in st.session_state.question_times if now - t < 3600]
    st.session_state.question_times = recent
    return len(recent) >= settings.rate_limit_per_hour


def answer(question: str) -> str:
    if rate_limited():
        return (f"You've reached the limit of {settings.rate_limit_per_hour} questions per hour "
                "for this demo. Please try again a bit later.")
    st.session_state.question_times.append(time.time())
    t0 = time.time()
    try:
        text, stats = ask(agent, question, thread_id=st.session_state.thread_id,
                          demo_mode=st.session_state.demo_mode)
    except Exception as e:  # noqa: BLE001
        return f"❌ Something went wrong: {type(e).__name__}: {str(e)[:300]}"
    stats["answer_ms"] = int((time.time() - t0) * 1000)
    current()["stats"].append(stats)
    return text.replace("$", "\\$")  # Streamlit markdown treats $...$ as LaTeX


def run_question(question: str):
    msgs = current()["messages"]
    msgs.append({"role": "user", "content": question})
    with st.spinner("🔍 Searching → writing..."):
        msgs.append({"role": "assistant", "content": answer(question)})
    st.rerun()


init_session()

# ==================== HEADER ====================
st.markdown("<h1>Daily News AI Assistant</h1>", unsafe_allow_html=True)
st.markdown("**📰 The Guardian + 9 Vietnamese news sources • Multilingual semantic search • "
            "keyword search as a tool**")
cols = st.columns(4)
cols[0].caption("🟢 Semantic: Pinecone + Gemini embeddings")
cols[1].caption("🟢 Keyword: BigQuery (tool)")
cols[2].caption(f"🟢 Retrieval: {settings.retrieval_mode}")
cols[3].caption(f"🟢 Model: {settings.llm_model}")

# ==================== SIDEBAR ====================
with st.sidebar:
    st.markdown("### 🎓 Demo mode")
    st.session_state.demo_mode = st.checkbox(
        "Enable demo mode", value=st.session_state.demo_mode,
        help="Runs the old hybrid pipeline (semantic + keyword + FlashRank re-rank) to compare "
             "with the default semantic search, which beat it in the 2026-10-19 eval")
    if st.session_state.demo_mode:
        st.caption("_Click a query to run it through the old hybrid pipeline_")
        for q, emoji in [("Latest news on fuel prices in Vietnam", "⛽"), ("Tin mới nhất về giá vàng", "🪙"),
                         ("Climate policy in the UK", "🌍"), ("AI regulation", "🤖"),
                         ("Tình hình bóng đá Việt Nam tuần này", "⚽")]:
            if st.button(f"{emoji} {q}", key=f"showcase_{q}", use_container_width=True):
                run_question(q)

    st.divider()
    st.markdown("### 🔍 Search stats")
    stats_list = current()["stats"]
    if stats_list:
        s = stats_list[-1]
        if s.get("skipped"):
            st.info(f"No search for the last message ({s['skipped'].replace('_', ' ')}).")
        else:
            hybrid = s.get("mode") == "hybrid"
            c1, c2 = st.columns(2)
            c1.metric("🧠 Semantic", s.get("dense", 0))
            if hybrid:
                c2.metric("🔤 Keyword", s.get("keyword", 0))
            if hybrid:
                st.caption(f"🔍 _Keywords: {', '.join(s.get('keywords') or []) or 'none extracted'}_")
            st.metric("Unique after dedupe", s.get("unique", 0))
            if s.get("rerank_skipped") == "off":
                st.caption(f"🎯 Top {s.get('returned', 0)} by semantic similarity (no re-ranker)")
            elif s.get("rerank_skipped"):
                st.warning("Re-ranking skipped (fewer than 3 results)")
            else:
                st.caption(f"🎯 Top {s.get('returned', 0)} re-ranked by `{s.get('rerank_model', '?')}`")
            with st.expander("⏱️ Timing"):
                st.caption(f"Searches (parallel): {s.get('retrieve_ms', 0)} ms")
                st.caption(f"Re-ranking: {s.get('rerank_ms', 0)} ms")
                st.caption(f"Whole answer: {s.get('answer_ms', 0)} ms")
        if s.get("tools_used"):
            st.caption(f"🛠️ Tools called: {', '.join(s['tools_used'])}")
    else:
        st.info("👋 Ask a question to see how the search works.")
    st.caption(f"BigQuery this server: {bq.usage['queries']} queries, "
               f"{bq.usage['bytes_billed'] / 1e9:.2f} GB billed")

    st.divider()
    st.markdown("### 💬 Conversations")
    if st.button("New conversation", type="primary", use_container_width=True):
        new_conversation()
        st.rerun()
    for tid, t in st.session_state.threads.items():
        is_current = tid == st.session_state.thread_id
        label = f"{'🟢' if is_current else '⚪'} {t['name']} ({len(t['messages']) // 2} questions)"
        if st.button(label, key=f"thread_{tid}", use_container_width=True, disabled=is_current):
            st.session_state.thread_id = tid
            st.rerun()
    st.caption(f"Memory: {memory_kind}")

# ==================== CHAT ====================
for m in current()["messages"]:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

prompt = st.chat_input("💬 Ask about the news (English or Vietnamese)...")
if prompt:
    if re.fullmatch(r"\s*(bye|goodbye|see you|tạm biệt|hẹn gặp lại)[\s!.]*", prompt, re.IGNORECASE):
        current()["messages"] += [{"role": "user", "content": prompt},
                                  {"role": "assistant", "content": "**Take care! See you next time! 👋**"}]
        st.rerun()
    run_question(prompt)

st.divider()
st.markdown(
    "<p style='text-align: center; color: #666;'>Built by Dinh Ngoc Lam • Foundry AI Academy Capstone<br>"
    "RAG: Pinecone (multilingual semantic search) + BigQuery (keyword tool)</p>",
    unsafe_allow_html=True,
)
