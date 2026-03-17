from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class StoredAccount:
    phone: str
    session_file: str
    last_used_at: float
    enabled: bool


class RuntimeStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    phone TEXT PRIMARY KEY,
                    session_file TEXT NOT NULL,
                    last_used_at REAL NOT NULL DEFAULT 0,
                    enabled INTEGER NOT NULL DEFAULT 1
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

    def upsert_account(self, phone: str, session_file: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO accounts (phone, session_file, last_used_at, enabled)
                VALUES (?, ?, 0, 1)
                ON CONFLICT(phone) DO UPDATE SET
                    session_file=excluded.session_file,
                    enabled=1
                """,
                (phone, session_file),
            )

    def mark_account_used(self, phone: str, used_at: float | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE accounts SET last_used_at=? WHERE phone=?",
                (used_at or time.time(), phone),
            )

    def list_accounts(self) -> list[StoredAccount]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT phone, session_file, last_used_at, enabled FROM accounts ORDER BY phone"
            ).fetchall()
        return [StoredAccount(r[0], r[1], float(r[2]), bool(r[3])) for r in rows]

    def get_available_accounts(self, limit: int, cooldown_seconds: int) -> list[StoredAccount]:
        now = time.time()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT phone, session_file, last_used_at, enabled
                FROM accounts
                WHERE enabled=1 AND (? - last_used_at) >= ?
                ORDER BY last_used_at ASC
                LIMIT ?
                """,
                (now, cooldown_seconds, limit),
            ).fetchall()
        return [StoredAccount(r[0], r[1], float(r[2]), bool(r[3])) for r in rows]

    def get_next_ready_in_seconds(self, cooldown_seconds: int) -> int:
        now = time.time()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT MIN(last_used_at) FROM accounts WHERE enabled=1"
            ).fetchone()
        if not row or row[0] is None:
            return cooldown_seconds
        wait_left = int(cooldown_seconds - (now - float(row[0])))
        return max(0, wait_left)

    def get_index(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM state WHERE key='excel_index'").fetchone()
        return int(row[0]) if row else 0

    def set_index(self, index: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO state (key, value) VALUES ('excel_index', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(index),),
            )


    def advance_index(self, delta: int, total: int) -> int:
        current = self.get_index()
        if total <= 0:
            return current
        new_index = (current + delta) % total
        self.set_index(new_index)
        return new_index
