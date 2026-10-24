"""The chat model, built one way for the agent and the summary tool.

gpt-5.x (the default since 2026-10-20, docs/llm_compare.md) is a reasoning model: it
takes no temperature, its token cap also covers reasoning tokens, and tools combined with
a reasoning effort need the Responses API (chat/completions returns 400). Its replies are
content blocks, so callers read `message.text`, never `str(message.content)`.
"""

from __future__ import annotations

from core.config import settings

_REASONING_PREFIXES = ("gpt-5", "o1", "o3", "o4")
_REASONING_HEADROOM = 2500  # tokens for reasoning on top of the visible answer


def chat_model(temperature: float = 0.3, max_tokens: int | None = None):
    from langchain_openai import ChatOpenAI
    name = settings.llm_model
    max_tokens = max_tokens or settings.llm_max_tokens
    if name.startswith(_REASONING_PREFIXES):
        return ChatOpenAI(model=name, use_responses_api=True,
                          reasoning={"effort": settings.llm_reasoning_effort},
                          max_tokens=max_tokens + _REASONING_HEADROOM, timeout=60)
    return ChatOpenAI(model=name, temperature=temperature, max_tokens=max_tokens, timeout=60)
