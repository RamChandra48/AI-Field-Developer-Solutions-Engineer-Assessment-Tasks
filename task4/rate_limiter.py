"""Token-aware sliding-window rate limiter backed by an on-disk SQLite database.

Model: a sliding *log*. Every admitted request writes one row
(key, timestamp, tokens). A key's usage is the sum of its rows whose
timestamp is inside the last ``window`` seconds. Unlike fixed windows, there is
no boundary burst where a tenant gets 2x the limit across a minute boundary.

Lifecycle of a request's tokens:

    acquire(key, estimate)  -> reserves the *estimated* tokens up front, so
                               concurrent requests cannot jointly overshoot
    reconcile(id, actual)   -> replaces the estimate with real usage afterwards
    release(id)             -> refunds the reservation (request produced nothing)

Usage above the reservation is charged in full, even if it takes the key past
its limit. The tokens were spent, so the overrun is debt: the key is refused
(with an exact Retry-After) until enough of its window expires to pay it back.
Overruns come only from the prompt estimate, because the completion is capped
by max_tokens, which is part of the estimate. Concurrent requests can therefore
overshoot the limit by at most the sum of their prompt underestimates, never by
completion length. One row is capped at ``limit``: a single request can't block
a key for more than one window, and a nonsensical figure reported by a provider
can't overflow SQLite's 64-bit INTEGER.

Concurrency: check-and-insert runs inside ``BEGIN IMMEDIATE``, which takes
SQLite's write lock before reading. Two requests (threads, or separate gateway
processes sharing the file) can never both read "40k used" and both insert
10k. Expired rows are deleted on every acquire, so the table holds roughly
one minute of traffic.
"""
from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Union

SCHEMA = """
CREATE TABLE IF NOT EXISTS token_usage (
    id     INTEGER PRIMARY KEY,
    key    TEXT    NOT NULL,
    ts_ms  INTEGER NOT NULL,
    tokens INTEGER NOT NULL CHECK (tokens >= 0)
);
CREATE INDEX IF NOT EXISTS token_usage_key_ts ON token_usage (key, ts_ms);
CREATE INDEX IF NOT EXISTS token_usage_ts ON token_usage (ts_ms);
"""


@dataclass(frozen=True)
class Decision:
    allowed: bool
    limit: int
    used: int  # tokens counted in the window, including this request if admitted
    reservation_id: Optional[int] = None
    retry_after: Optional[float] = None  # seconds until it would fit; None = never

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.used)


class SlidingWindowTokenLimiter:
    def __init__(
        self,
        db_path: Union[str, Path],
        limit: int = 50_000,
        window_seconds: float = 60.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if limit <= 0 or window_seconds <= 0:
            raise ValueError("limit and window must be positive")
        self.db_path = str(db_path)
        self.limit = limit
        self.window_ms = int(window_seconds * 1000)
        self._clock = clock
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.Lock()  # one connection, used from worker threads

    # -- public async API (SQLite is blocking, so it runs in a worker thread)
    async def acquire(self, key: str, tokens: int) -> Decision:
        return await asyncio.to_thread(self._acquire, key, tokens)

    async def reconcile(self, reservation_id: int, actual_tokens: int) -> int:
        """Charge real usage instead of the estimate; returns what was charged."""
        charged = min(max(0, int(actual_tokens)), self.limit)
        await asyncio.to_thread(self._execute, "UPDATE token_usage SET tokens = ? WHERE id = ?",
                                (charged, reservation_id))
        return charged

    async def release(self, reservation_id: int) -> None:
        await asyncio.to_thread(self._execute, "DELETE FROM token_usage WHERE id = ?", (reservation_id,))

    async def usage(self, key: str) -> int:
        return await asyncio.to_thread(self._usage, key)

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    # -- implementation
    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            # Opened lazily, so importing the app has no side effects on disk.
            # isolation_level=None: transactions are explicit (BEGIN IMMEDIATE).
            conn = sqlite3.connect(self.db_path, timeout=10, isolation_level=None, check_same_thread=False)
            self._enable_wal(conn)  # readers don't block the writer
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.executescript(SCHEMA)
            self._conn = conn
        return self._conn

    @staticmethod
    def _enable_wal(conn: sqlite3.Connection, attempts: int = 100) -> None:
        """Switch to WAL, retrying on lock contention.

        Changing journal_mode needs an exclusive lock and does *not* wait on
        the busy timeout, so two processes opening a fresh database at once
        can get an immediate "database is locked". WAL is persistent, so once
        any connection has set it the others skip the switch.
        """
        for attempt in range(attempts):
            try:
                if conn.execute("PRAGMA journal_mode").fetchone()[0].lower() != "wal":
                    conn.execute("PRAGMA journal_mode=WAL")
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc) or attempt == attempts - 1:
                    raise
                time.sleep(0.005 * (attempt + 1))

    def _now_ms(self) -> int:
        return int(self._clock() * 1000)

    def _acquire(self, key: str, tokens: int) -> Decision:
        if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens <= 0:
            raise ValueError("tokens must be a positive integer")
        now = self._now_ms()
        cutoff = now - self.window_ms  # rows with ts_ms > cutoff are in the window
        with self._lock:
            conn = self._connection()
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute("DELETE FROM token_usage WHERE ts_ms <= ?", (cutoff,))  # evict, all keys
                used = conn.execute(
                    "SELECT COALESCE(SUM(tokens), 0) FROM token_usage WHERE key = ? AND ts_ms > ?", (key, cutoff)
                ).fetchone()[0]
                if used + tokens > self.limit:
                    retry = None if tokens > self.limit else self._retry_after(conn, key, cutoff, used + tokens - self.limit, now)
                    conn.execute("COMMIT")
                    return Decision(False, self.limit, used, retry_after=retry)
                reservation_id = conn.execute(
                    "INSERT INTO token_usage (key, ts_ms, tokens) VALUES (?, ?, ?)", (key, now, tokens)
                ).lastrowid
                conn.execute("COMMIT")
                return Decision(True, self.limit, used + tokens, reservation_id=reservation_id)
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    def _retry_after(self, conn: sqlite3.Connection, key: str, cutoff: int, excess: int, now: int) -> float:
        """Seconds until enough of the oldest rows expire to free ``excess`` tokens."""
        freed = 0
        rows = conn.execute(
            "SELECT ts_ms, tokens FROM token_usage WHERE key = ? AND ts_ms > ? ORDER BY ts_ms, id", (key, cutoff)
        )
        for ts_ms, row_tokens in rows:
            freed += row_tokens
            if freed >= excess:
                return max(0.001, (ts_ms + self.window_ms - now) / 1000)  # row leaves when now >= ts + window
        return self.window_ms / 1000  # unreachable when tokens <= limit; safe upper bound

    def _usage(self, key: str) -> int:
        with self._lock:
            return self._connection().execute(
                "SELECT COALESCE(SUM(tokens), 0) FROM token_usage WHERE key = ? AND ts_ms > ?",
                (key, self._now_ms() - self.window_ms),
            ).fetchone()[0]

    def _execute(self, sql: str, params: tuple) -> None:
        with self._lock:
            self._connection().execute(sql, params)
