"""Published-app guards: the password gate and the app-wide daily question limit.

Both are off by default (local use). They hold per server process, which is the whole app
when Cloud Run runs one instance; a restart resets the daily count, so the hard cost caps
stay outside the app (OpenAI project limit, the app SA's BigQuery quota).
"""

from __future__ import annotations

import hmac
import threading
from datetime import datetime, timezone


def password_ok(given: str, expected: str) -> bool:
    """True if no password is set, or `given` matches it (constant-time compare)."""
    if not expected:
        return True
    return hmac.compare_digest(given.encode(), expected.encode())


def _utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


class DailyLimit:
    """Counts questions per UTC day across all sessions. limit <= 0 means no limit."""

    def __init__(self, limit: int, today=_utc_today):
        self.limit = limit
        self._today = today
        self._lock = threading.Lock()
        self._date = ""
        self._used = 0

    def _roll(self) -> None:
        today = self._today()
        if today != self._date:
            self._date, self._used = today, 0

    def take(self) -> bool:
        """Reserve one question; False if today's limit is used up."""
        with self._lock:
            self._roll()
            if 0 < self.limit <= self._used:
                return False
            self._used += 1
            return True

    @property
    def used(self) -> int:
        with self._lock:
            self._roll()
            return self._used
