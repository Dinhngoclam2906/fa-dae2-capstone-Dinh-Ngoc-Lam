# capstone_tool_calling_agent.py - compatibility shim.
#
# The agent moved to ai_agents/core/ (BigQuery, 10 sources) on 2026-10-18; the original
# Snowflake version of this file is in git history. Kept so the evaluation scripts
# (baseline_systems.py, label_ground_truth*.py) still import their keyword search; they
# get refreshed in the eval step of the agent phase.
from typing import Any, Dict, List

from langchain_core.tools import tool

from core import articles
from core.tools import TOOLS as tools  # noqa: F401 - the old module exported `tools`


@tool
def search_articles_by_multiple_keywords(keywords: List[str], limit: int = 15) -> Dict[str, Any]:
    """Keyword search over all sources (title + lead, last 90 days). Same result keys as
    the original Snowflake tool: article_id, web_title, section_name,
    web_publication_date, web_url, preview (+ source_name, language)."""
    try:
        rows = articles.keyword_search(keywords, limit=limit)
    except Exception as e:  # noqa: BLE001
        return {"error": str(e), "results": []}
    return {"keywords": keywords, "count": len(rows), "results": rows}
