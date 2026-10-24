"""BigQuery access for the agent.

Every query is parameterised (never string-built from user or LLM input) and runs with
`maximum_bytes_billed`, so one question can never scan the whole archive by accident.
BigQuery bills by bytes scanned, and the agent shares the project's free 1 TB/month
(and daily quota) with the nightly dbt build.
"""

from __future__ import annotations

import logging
import threading
from datetime import date, datetime
from typing import Any

from google.api_core import exceptions as gexc
from google.cloud import bigquery

from core.config import settings

logger = logging.getLogger(__name__)

_client: bigquery.Client | None = None
_lock = threading.Lock()

# Process-wide usage counters (shown in the UI's stats panel).
usage = {"queries": 0, "bytes_billed": 0}


class QueryTooExpensive(Exception):
    """The query would bill more than the per-query cap."""


def client() -> bigquery.Client:
    global _client
    with _lock:
        if _client is None:
            if not settings.gcp_project:
                raise RuntimeError("GCP_PROJECT is not set")
            _client = bigquery.Client(project=settings.gcp_project, location=settings.bq_location)
        return _client


def p(name: str, value: Any, type_: str = "STRING") -> bigquery.ScalarQueryParameter:
    """A typed scalar parameter (the type is needed when the value is None)."""
    return bigquery.ScalarQueryParameter(name, type_, value)


def p_array(name: str, values: list[str]) -> bigquery.ArrayQueryParameter:
    return bigquery.ArrayQueryParameter(name, "STRING", values)


def _plain(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.isoformat(timespec="minutes")
    if isinstance(v, date):
        return v.isoformat()
    return v


def query(sql: str, params: list | None = None, *, max_bytes: int | None = None,
          timeout: float = 30) -> list[dict]:
    """Run a parameterised query; returns rows as plain dicts (dates as ISO strings).
    Raises QueryTooExpensive if it would bill more than the cap."""
    cfg = bigquery.QueryJobConfig(
        query_parameters=params or [],
        maximum_bytes_billed=max_bytes or settings.max_bytes_per_query,
        labels={"app": "news-agent"},
    )
    try:
        job = client().query(sql, job_config=cfg)
        rows = job.result(timeout=timeout)
    except (gexc.BadRequest, gexc.Forbidden) as e:
        if "bytes billed" in str(e).lower():
            raise QueryTooExpensive(
                "That search would scan too much data. Narrow it with a date range, "
                "a source or a section."
            ) from e
        raise
    billed = job.total_bytes_billed or 0
    with _lock:
        usage["queries"] += 1
        usage["bytes_billed"] += billed
    logger.info("bq_query bytes_billed=%d cache_hit=%s rows=%s", billed, job.cache_hit, rows.total_rows)
    return [{k: _plain(v) for k, v in row.items()} for row in rows]
