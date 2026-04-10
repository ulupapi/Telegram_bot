from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


VALID_STATUSES = {"done", "in_progress", "blocked"}


@dataclass(frozen=True)
class StoredMessage:
    created_at: str
    user_name: str
    text: str


@dataclass(frozen=True)
class TaskRecord:
    external_id: str
    title: str
    assignee: str
    status: str


class Database:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def init_schema(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    user_name TEXT NOT NULL,
                    text TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(chat_id, message_id)
                )
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_messages_thread
                ON messages(chat_id, thread_id, message_id)
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    external_id TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    assignee TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scope_aliases (
                    alias TEXT PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER NOT NULL,
                    is_manual INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

    def save_message(
        self,
        *,
        chat_id: int,
        thread_id: int,
        message_id: int,
        user_name: str,
        text: str,
        created_at: str,
    ) -> None:
        clean_text = text.strip()
        if not clean_text:
            return

        with self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO messages
                (chat_id, thread_id, message_id, user_name, text, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (chat_id, thread_id, message_id, user_name, clean_text, created_at),
            )

    def get_recent_thread_messages(
        self,
        *,
        chat_id: int,
        thread_id: int,
        limit: int,
    ) -> list[StoredMessage]:
        rows = self.conn.execute(
            """
            SELECT created_at, user_name, text
            FROM messages
            WHERE chat_id = ? AND thread_id = ?
            ORDER BY message_id DESC
            LIMIT ?
            """,
            (chat_id, thread_id, limit),
        ).fetchall()

        return [
            StoredMessage(
                created_at=row["created_at"],
                user_name=row["user_name"],
                text=row["text"],
            )
            for row in reversed(rows)
        ]

    def replace_tasks(self, tasks: Sequence[TaskRecord]) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM tasks")
            for task in tasks:
                if task.status not in VALID_STATUSES:
                    continue
                self.conn.execute(
                    """
                    INSERT INTO tasks (external_id, title, assignee, status)
                    VALUES (?, ?, ?, ?)
                    """,
                    (task.external_id, task.title, task.assignee, task.status),
                )

    def learn_scope_alias(self, *, alias: str, chat_id: int, thread_id: int) -> None:
        clean_alias = alias.strip().lower()
        if not clean_alias:
            return

        with self.conn:
            self.conn.execute(
                """
                INSERT INTO scope_aliases (alias, chat_id, thread_id, is_manual)
                VALUES (?, ?, ?, 0)
                ON CONFLICT(alias) DO UPDATE SET
                    chat_id = CASE
                        WHEN scope_aliases.is_manual = 1 THEN scope_aliases.chat_id
                        ELSE excluded.chat_id
                    END,
                    thread_id = CASE
                        WHEN scope_aliases.is_manual = 1 THEN scope_aliases.thread_id
                        ELSE excluded.thread_id
                    END,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (clean_alias, chat_id, thread_id),
            )

    def set_manual_scope_alias(self, *, alias: str, chat_id: int, thread_id: int) -> None:
        clean_alias = alias.strip().lower()
        if not clean_alias:
            return

        with self.conn:
            self.conn.execute(
                """
                INSERT INTO scope_aliases (alias, chat_id, thread_id, is_manual)
                VALUES (?, ?, ?, 1)
                ON CONFLICT(alias) DO UPDATE SET
                    chat_id = excluded.chat_id,
                    thread_id = excluded.thread_id,
                    is_manual = 1,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (clean_alias, chat_id, thread_id),
            )

    def resolve_scope_alias(self, *, alias: str) -> tuple[int, int] | None:
        clean_alias = alias.strip().lower()
        if not clean_alias:
            return None

        row = self.conn.execute(
            """
            SELECT chat_id, thread_id
            FROM scope_aliases
            WHERE alias = ?
            """,
            (clean_alias,),
        ).fetchone()
        if row is None:
            return None
        return int(row["chat_id"]), int(row["thread_id"])

    def close(self) -> None:
        self.conn.close()
