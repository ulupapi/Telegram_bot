from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional import at runtime
    psycopg = None
    dict_row = None


VALID_STATUSES = {
    "В ожидании",
    "В работе",
    "Завершена",
    "Отклонена",
    "Отозвана",
}


@dataclass(frozen=True)
class StoredMessage:
    created_at: str
    user_name: str
    text: str


@dataclass(frozen=True)
class TaskRecord:
    external_id: str
    title: str
    description: str
    deadline_date: str
    author_name: str
    assignee: str
    status: str


class _DatabaseBackend(Protocol):
    def init_schema(self) -> None: ...
    def save_message(
        self,
        *,
        chat_id: int,
        thread_id: int,
        message_id: int,
        user_name: str,
        text: str,
        created_at: str,
    ) -> None: ...
    def get_recent_thread_messages(
        self,
        *,
        chat_id: int,
        thread_id: int,
        limit: int,
    ) -> list[StoredMessage]: ...
    def replace_tasks(self, tasks: Sequence[TaskRecord]) -> None: ...
    def upsert_task(self, task: TaskRecord, *, source: str) -> None: ...
    def list_tasks(self) -> list[TaskRecord]: ...
    def get_task(self, *, external_id: str) -> TaskRecord | None: ...
    def set_task_post_message_id(
        self,
        *,
        chat_id: int,
        thread_id: int,
        external_id: str,
        bot_message_id: int,
    ) -> None: ...
    def get_task_post_message_id(
        self,
        *,
        chat_id: int,
        thread_id: int,
        external_id: str,
    ) -> int | None: ...
    def find_task_external_id_by_post_message(
        self,
        *,
        chat_id: int,
        thread_id: int,
        bot_message_id: int,
    ) -> str | None: ...
    def clear_scope(self, *, chat_id: int, thread_id: int) -> tuple[int, int]: ...
    def clear_all(self) -> tuple[int, int, int]: ...
    def learn_scope_alias(self, *, alias: str, chat_id: int, thread_id: int) -> None: ...
    def set_manual_scope_alias(self, *, alias: str, chat_id: int, thread_id: int) -> None: ...
    def resolve_scope_alias(self, *, alias: str) -> tuple[int, int] | None: ...
    def close(self) -> None: ...


class _SQLiteBackend:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def init_schema(self) -> None:
        with self._lock:
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
                        description TEXT NOT NULL DEFAULT '',
                        deadline_date TEXT NOT NULL DEFAULT '',
                        author_name TEXT NOT NULL DEFAULT '',
                        assignee TEXT NOT NULL DEFAULT '',
                        status TEXT NOT NULL,
                        source TEXT NOT NULL DEFAULT 'llm',
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                self._ensure_column("tasks", "description", "TEXT NOT NULL DEFAULT ''")
                self._ensure_column("tasks", "deadline_date", "TEXT NOT NULL DEFAULT ''")
                self._ensure_column("tasks", "author_name", "TEXT NOT NULL DEFAULT ''")
                self._ensure_column("tasks", "assignee", "TEXT NOT NULL DEFAULT ''")
                self._ensure_column("tasks", "source", "TEXT NOT NULL DEFAULT 'llm'")
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
                self.conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS task_posts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        chat_id INTEGER NOT NULL,
                        thread_id INTEGER NOT NULL,
                        external_id TEXT NOT NULL,
                        bot_message_id INTEGER NOT NULL,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(chat_id, thread_id, external_id),
                        UNIQUE(chat_id, thread_id, bot_message_id)
                    )
                    """
                )

    def _ensure_column(self, table_name: str, column_name: str, ddl: str) -> None:
        rows = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        columns = {row["name"] for row in rows}
        if column_name not in columns:
            self.conn.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}"
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

        with self._lock:
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
        with self._lock:
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
        with self._lock:
            with self.conn:
                self.conn.execute(
                    "DELETE FROM tasks WHERE source = 'llm' OR source IS NULL"
                )
                for task in tasks:
                    if task.status not in VALID_STATUSES:
                        continue
                    self.conn.execute(
                        """
                        INSERT INTO tasks (
                            external_id,
                            title,
                            description,
                            deadline_date,
                            author_name,
                            assignee,
                            status,
                            source
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, 'llm')
                        """,
                        (
                            task.external_id,
                            task.title,
                            task.description,
                            task.deadline_date,
                            task.author_name,
                            task.assignee,
                            task.status,
                        ),
                    )

    def upsert_task(self, task: TaskRecord, *, source: str) -> None:
        if task.status not in VALID_STATUSES:
            return
        clean_source = "manual" if source.strip().lower() == "manual" else "llm"
        with self._lock:
            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO tasks (
                        external_id,
                        title,
                        description,
                        deadline_date,
                        author_name,
                        assignee,
                        status,
                        source
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(external_id) DO UPDATE SET
                        title = excluded.title,
                        description = excluded.description,
                        deadline_date = excluded.deadline_date,
                        author_name = excluded.author_name,
                        assignee = excluded.assignee,
                        status = excluded.status,
                        source = excluded.source,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        task.external_id,
                        task.title,
                        task.description,
                        task.deadline_date,
                        task.author_name,
                        task.assignee,
                        task.status,
                        clean_source,
                    ),
                )

    def list_tasks(self) -> list[TaskRecord]:
        status_sort = (
            "CASE status "
            "WHEN 'В ожидании' THEN 1 "
            "WHEN 'В работе' THEN 2 "
            "WHEN 'Завершена' THEN 3 "
            "WHEN 'Отклонена' THEN 4 "
            "WHEN 'Отозвана' THEN 5 "
            "ELSE 99 END"
        )
        with self._lock:
            rows = self.conn.execute(
                f"""
                SELECT external_id, title, description, deadline_date, author_name, assignee, status
                FROM tasks
                ORDER BY {status_sort}, updated_at DESC, id DESC
                """
            ).fetchall()
        return [
            TaskRecord(
                external_id=row["external_id"],
                title=row["title"],
                description=row["description"],
                deadline_date=row["deadline_date"],
                author_name=row["author_name"],
                assignee=row["assignee"],
                status=row["status"],
            )
            for row in rows
        ]

    def get_task(self, *, external_id: str) -> TaskRecord | None:
        with self._lock:
            row = self.conn.execute(
                """
                SELECT external_id, title, description, deadline_date, author_name, assignee, status
                FROM tasks
                WHERE external_id = ?
                """,
                (external_id,),
            ).fetchone()
        if row is None:
            return None
        return TaskRecord(
            external_id=row["external_id"],
            title=row["title"],
            description=row["description"],
            deadline_date=row["deadline_date"],
            author_name=row["author_name"],
            assignee=row["assignee"],
            status=row["status"],
        )

    def set_task_post_message_id(
        self,
        *,
        chat_id: int,
        thread_id: int,
        external_id: str,
        bot_message_id: int,
    ) -> None:
        with self._lock:
            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO task_posts (chat_id, thread_id, external_id, bot_message_id)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(chat_id, thread_id, external_id) DO UPDATE SET
                        bot_message_id = excluded.bot_message_id,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (chat_id, thread_id, external_id, bot_message_id),
                )

    def get_task_post_message_id(
        self,
        *,
        chat_id: int,
        thread_id: int,
        external_id: str,
    ) -> int | None:
        with self._lock:
            row = self.conn.execute(
                """
                SELECT bot_message_id
                FROM task_posts
                WHERE chat_id = ? AND thread_id = ? AND external_id = ?
                """,
                (chat_id, thread_id, external_id),
            ).fetchone()
        if row is None:
            return None
        return int(row["bot_message_id"])

    def find_task_external_id_by_post_message(
        self,
        *,
        chat_id: int,
        thread_id: int,
        bot_message_id: int,
    ) -> str | None:
        with self._lock:
            row = self.conn.execute(
                """
                SELECT external_id
                FROM task_posts
                WHERE chat_id = ? AND thread_id = ? AND bot_message_id = ?
                """,
                (chat_id, thread_id, bot_message_id),
            ).fetchone()
        if row is None:
            return None
        return str(row["external_id"])

    def clear_scope(self, *, chat_id: int, thread_id: int) -> tuple[int, int]:
        with self._lock:
            with self.conn:
                cur_messages = self.conn.execute(
                    """
                    DELETE FROM messages
                    WHERE chat_id = ? AND thread_id = ?
                    """,
                    (chat_id, thread_id),
                )
                deleted_messages = max(0, cur_messages.rowcount)
                cur_tasks = self.conn.execute("DELETE FROM tasks")
                deleted_tasks = max(0, cur_tasks.rowcount)
                self.conn.execute(
                    """
                    DELETE FROM task_posts
                    WHERE chat_id = ? AND thread_id = ?
                    """,
                    (chat_id, thread_id),
                )
        return deleted_messages, deleted_tasks

    def clear_all(self) -> tuple[int, int, int]:
        with self._lock:
            with self.conn:
                cur_messages = self.conn.execute("DELETE FROM messages")
                deleted_messages = max(0, cur_messages.rowcount)
                cur_tasks = self.conn.execute("DELETE FROM tasks")
                deleted_tasks = max(0, cur_tasks.rowcount)
                cur_aliases = self.conn.execute("DELETE FROM scope_aliases")
                deleted_aliases = max(0, cur_aliases.rowcount)
                self.conn.execute("DELETE FROM task_posts")
        return deleted_messages, deleted_tasks, deleted_aliases

    def learn_scope_alias(self, *, alias: str, chat_id: int, thread_id: int) -> None:
        clean_alias = alias.strip().lower()
        if not clean_alias:
            return

        with self._lock:
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

        with self._lock:
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

        with self._lock:
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
        with self._lock:
            self.conn.close()


class _PostgresBackend:
    def __init__(self, dsn: str) -> None:
        if psycopg is None:
            raise RuntimeError(
                "PostgreSQL backend selected, but psycopg is not installed."
            )
        self._lock = threading.RLock()
        self.conn = psycopg.connect(dsn, autocommit=True)

    def init_schema(self) -> None:
        with self._lock:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS messages (
                        id BIGSERIAL PRIMARY KEY,
                        chat_id BIGINT NOT NULL,
                        thread_id BIGINT NOT NULL,
                        message_id BIGINT NOT NULL,
                        user_name TEXT NOT NULL,
                        text TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        UNIQUE(chat_id, message_id)
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_messages_thread
                    ON messages(chat_id, thread_id, message_id)
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS tasks (
                        id BIGSERIAL PRIMARY KEY,
                        external_id TEXT NOT NULL UNIQUE,
                        title TEXT NOT NULL,
                        description TEXT NOT NULL DEFAULT '',
                        deadline_date TEXT NOT NULL DEFAULT '',
                        author_name TEXT NOT NULL DEFAULT '',
                        assignee TEXT NOT NULL DEFAULT '',
                        status TEXT NOT NULL,
                        source TEXT NOT NULL DEFAULT 'llm',
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE tasks
                    ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT ''
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE tasks
                    ADD COLUMN IF NOT EXISTS deadline_date TEXT NOT NULL DEFAULT ''
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE tasks
                    ADD COLUMN IF NOT EXISTS author_name TEXT NOT NULL DEFAULT ''
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE tasks
                    ADD COLUMN IF NOT EXISTS assignee TEXT NOT NULL DEFAULT ''
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE tasks
                    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'llm'
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS scope_aliases (
                        alias TEXT PRIMARY KEY,
                        chat_id BIGINT NOT NULL,
                        thread_id BIGINT NOT NULL,
                        is_manual BOOLEAN NOT NULL DEFAULT FALSE,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS task_posts (
                        id BIGSERIAL PRIMARY KEY,
                        chat_id BIGINT NOT NULL,
                        thread_id BIGINT NOT NULL,
                        external_id TEXT NOT NULL,
                        bot_message_id BIGINT NOT NULL,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(chat_id, thread_id, external_id),
                        UNIQUE(chat_id, thread_id, bot_message_id)
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

        with self._lock:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO messages
                    (chat_id, thread_id, message_id, user_name, text, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (chat_id, message_id) DO NOTHING
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
        with self._lock:
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT created_at, user_name, text
                    FROM messages
                    WHERE chat_id = %s AND thread_id = %s
                    ORDER BY message_id DESC
                    LIMIT %s
                    """,
                    (chat_id, thread_id, limit),
                )
                rows = cur.fetchall()

        return [
            StoredMessage(
                created_at=row["created_at"],
                user_name=row["user_name"],
                text=row["text"],
            )
            for row in reversed(rows)
        ]

    def replace_tasks(self, tasks: Sequence[TaskRecord]) -> None:
        with self._lock:
            with self.conn.transaction():
                with self.conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM tasks WHERE source = 'llm' OR source IS NULL"
                    )
                    for task in tasks:
                        if task.status not in VALID_STATUSES:
                            continue
                        cur.execute(
                            """
                            INSERT INTO tasks (
                                external_id,
                                title,
                                description,
                                deadline_date,
                                author_name,
                                assignee,
                                status,
                                source
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, 'llm')
                            """,
                            (
                                task.external_id,
                                task.title,
                                task.description,
                                task.deadline_date,
                                task.author_name,
                                task.assignee,
                                task.status,
                            ),
                        )

    def upsert_task(self, task: TaskRecord, *, source: str) -> None:
        if task.status not in VALID_STATUSES:
            return
        clean_source = "manual" if source.strip().lower() == "manual" else "llm"
        with self._lock:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tasks (
                        external_id,
                        title,
                        description,
                        deadline_date,
                        author_name,
                        assignee,
                        status,
                        source
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (external_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        deadline_date = EXCLUDED.deadline_date,
                        author_name = EXCLUDED.author_name,
                        assignee = EXCLUDED.assignee,
                        status = EXCLUDED.status,
                        source = EXCLUDED.source,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        task.external_id,
                        task.title,
                        task.description,
                        task.deadline_date,
                        task.author_name,
                        task.assignee,
                        task.status,
                        clean_source,
                    ),
                )

    def list_tasks(self) -> list[TaskRecord]:
        status_sort = (
            "CASE status "
            "WHEN 'В ожидании' THEN 1 "
            "WHEN 'В работе' THEN 2 "
            "WHEN 'Завершена' THEN 3 "
            "WHEN 'Отклонена' THEN 4 "
            "WHEN 'Отозвана' THEN 5 "
            "ELSE 99 END"
        )
        with self._lock:
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT external_id, title, description, deadline_date, author_name, assignee, status
                    FROM tasks
                    ORDER BY {status_sort}, updated_at DESC, id DESC
                    """
                )
                rows = cur.fetchall()
        return [
            TaskRecord(
                external_id=row["external_id"],
                title=row["title"],
                description=row["description"],
                deadline_date=row["deadline_date"],
                author_name=row["author_name"],
                assignee=row["assignee"],
                status=row["status"],
            )
            for row in rows
        ]

    def get_task(self, *, external_id: str) -> TaskRecord | None:
        with self._lock:
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT external_id, title, description, deadline_date, author_name, assignee, status
                    FROM tasks
                    WHERE external_id = %s
                    """,
                    (external_id,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return TaskRecord(
            external_id=row["external_id"],
            title=row["title"],
            description=row["description"],
            deadline_date=row["deadline_date"],
            author_name=row["author_name"],
            assignee=row["assignee"],
            status=row["status"],
        )

    def set_task_post_message_id(
        self,
        *,
        chat_id: int,
        thread_id: int,
        external_id: str,
        bot_message_id: int,
    ) -> None:
        with self._lock:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO task_posts (chat_id, thread_id, external_id, bot_message_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (chat_id, thread_id, external_id) DO UPDATE SET
                        bot_message_id = EXCLUDED.bot_message_id,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (chat_id, thread_id, external_id, bot_message_id),
                )

    def get_task_post_message_id(
        self,
        *,
        chat_id: int,
        thread_id: int,
        external_id: str,
    ) -> int | None:
        with self._lock:
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT bot_message_id
                    FROM task_posts
                    WHERE chat_id = %s AND thread_id = %s AND external_id = %s
                    """,
                    (chat_id, thread_id, external_id),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return int(row["bot_message_id"])

    def find_task_external_id_by_post_message(
        self,
        *,
        chat_id: int,
        thread_id: int,
        bot_message_id: int,
    ) -> str | None:
        with self._lock:
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT external_id
                    FROM task_posts
                    WHERE chat_id = %s AND thread_id = %s AND bot_message_id = %s
                    """,
                    (chat_id, thread_id, bot_message_id),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return str(row["external_id"])

    def clear_scope(self, *, chat_id: int, thread_id: int) -> tuple[int, int]:
        with self._lock:
            with self.conn.transaction():
                with self.conn.cursor() as cur:
                    cur.execute(
                        """
                        DELETE FROM messages
                        WHERE chat_id = %s AND thread_id = %s
                        """,
                        (chat_id, thread_id),
                    )
                    deleted_messages = max(0, cur.rowcount)
                    cur.execute("DELETE FROM tasks")
                    deleted_tasks = max(0, cur.rowcount)
                    cur.execute(
                        """
                        DELETE FROM task_posts
                        WHERE chat_id = %s AND thread_id = %s
                        """,
                        (chat_id, thread_id),
                    )
        return deleted_messages, deleted_tasks

    def clear_all(self) -> tuple[int, int, int]:
        with self._lock:
            with self.conn.transaction():
                with self.conn.cursor() as cur:
                    cur.execute("DELETE FROM messages")
                    deleted_messages = max(0, cur.rowcount)
                    cur.execute("DELETE FROM tasks")
                    deleted_tasks = max(0, cur.rowcount)
                    cur.execute("DELETE FROM scope_aliases")
                    deleted_aliases = max(0, cur.rowcount)
                    cur.execute("DELETE FROM task_posts")
        return deleted_messages, deleted_tasks, deleted_aliases

    def learn_scope_alias(self, *, alias: str, chat_id: int, thread_id: int) -> None:
        clean_alias = alias.strip().lower()
        if not clean_alias:
            return

        with self._lock:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scope_aliases (alias, chat_id, thread_id, is_manual)
                    VALUES (%s, %s, %s, FALSE)
                    ON CONFLICT (alias) DO UPDATE SET
                        chat_id = CASE
                            WHEN scope_aliases.is_manual THEN scope_aliases.chat_id
                            ELSE EXCLUDED.chat_id
                        END,
                        thread_id = CASE
                            WHEN scope_aliases.is_manual THEN scope_aliases.thread_id
                            ELSE EXCLUDED.thread_id
                        END,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (clean_alias, chat_id, thread_id),
                )

    def set_manual_scope_alias(self, *, alias: str, chat_id: int, thread_id: int) -> None:
        clean_alias = alias.strip().lower()
        if not clean_alias:
            return

        with self._lock:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scope_aliases (alias, chat_id, thread_id, is_manual)
                    VALUES (%s, %s, %s, TRUE)
                    ON CONFLICT (alias) DO UPDATE SET
                        chat_id = EXCLUDED.chat_id,
                        thread_id = EXCLUDED.thread_id,
                        is_manual = TRUE,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (clean_alias, chat_id, thread_id),
                )

    def resolve_scope_alias(self, *, alias: str) -> tuple[int, int] | None:
        clean_alias = alias.strip().lower()
        if not clean_alias:
            return None

        with self._lock:
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT chat_id, thread_id
                    FROM scope_aliases
                    WHERE alias = %s
                    """,
                    (clean_alias,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return int(row["chat_id"]), int(row["thread_id"])

    def close(self) -> None:
        with self._lock:
            self.conn.close()


class Database:
    def __init__(
        self,
        sqlite_path: str,
        *,
        db_backend: str = "auto",
        postgres_dsn: str | None = None,
    ) -> None:
        backend_name = (db_backend or "auto").strip().lower()
        if backend_name not in {"auto", "sqlite", "postgres"}:
            raise RuntimeError("DB_BACKEND must be one of: auto, sqlite, postgres")

        if backend_name == "auto":
            backend_name = "postgres" if postgres_dsn else "sqlite"

        if backend_name == "postgres":
            if not postgres_dsn:
                raise RuntimeError(
                    "POSTGRES_DSN is required when DB_BACKEND=postgres"
                )
            self._backend: _DatabaseBackend = _PostgresBackend(postgres_dsn)
        else:
            self._backend = _SQLiteBackend(sqlite_path)

    def init_schema(self) -> None:
        self._backend.init_schema()

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
        self._backend.save_message(
            chat_id=chat_id,
            thread_id=thread_id,
            message_id=message_id,
            user_name=user_name,
            text=text,
            created_at=created_at,
        )

    def get_recent_thread_messages(
        self,
        *,
        chat_id: int,
        thread_id: int,
        limit: int,
    ) -> list[StoredMessage]:
        return self._backend.get_recent_thread_messages(
            chat_id=chat_id,
            thread_id=thread_id,
            limit=limit,
        )

    def replace_tasks(self, tasks: Sequence[TaskRecord]) -> None:
        self._backend.replace_tasks(tasks)

    def upsert_task(self, task: TaskRecord, *, source: str = "manual") -> None:
        self._backend.upsert_task(task, source=source)

    def list_tasks(self) -> list[TaskRecord]:
        return self._backend.list_tasks()

    def get_task(self, *, external_id: str) -> TaskRecord | None:
        return self._backend.get_task(external_id=external_id)

    def update_task(
        self,
        *,
        external_id: str,
        title: str | None = None,
        description: str | None = None,
        deadline_date: str | None = None,
        author_name: str | None = None,
        assignee: str | None = None,
        status: str | None = None,
    ) -> TaskRecord | None:
        existing = self._backend.get_task(external_id=external_id)
        if existing is None:
            return None
        updated = TaskRecord(
            external_id=existing.external_id,
            title=title if title is not None else existing.title,
            description=description if description is not None else existing.description,
            deadline_date=deadline_date if deadline_date is not None else existing.deadline_date,
            author_name=author_name if author_name is not None else existing.author_name,
            assignee=assignee if assignee is not None else existing.assignee,
            status=status if status is not None else existing.status,
        )
        self._backend.upsert_task(updated, source="manual")
        return updated

    def set_task_post_message_id(
        self,
        *,
        chat_id: int,
        thread_id: int,
        external_id: str,
        bot_message_id: int,
    ) -> None:
        self._backend.set_task_post_message_id(
            chat_id=chat_id,
            thread_id=thread_id,
            external_id=external_id,
            bot_message_id=bot_message_id,
        )

    def get_task_post_message_id(
        self,
        *,
        chat_id: int,
        thread_id: int,
        external_id: str,
    ) -> int | None:
        return self._backend.get_task_post_message_id(
            chat_id=chat_id,
            thread_id=thread_id,
            external_id=external_id,
        )

    def find_task_external_id_by_post_message(
        self,
        *,
        chat_id: int,
        thread_id: int,
        bot_message_id: int,
    ) -> str | None:
        return self._backend.find_task_external_id_by_post_message(
            chat_id=chat_id,
            thread_id=thread_id,
            bot_message_id=bot_message_id,
        )

    def clear_scope(self, *, chat_id: int, thread_id: int) -> tuple[int, int]:
        return self._backend.clear_scope(chat_id=chat_id, thread_id=thread_id)

    def clear_all(self) -> tuple[int, int, int]:
        return self._backend.clear_all()

    def learn_scope_alias(self, *, alias: str, chat_id: int, thread_id: int) -> None:
        self._backend.learn_scope_alias(alias=alias, chat_id=chat_id, thread_id=thread_id)

    def set_manual_scope_alias(self, *, alias: str, chat_id: int, thread_id: int) -> None:
        self._backend.set_manual_scope_alias(
            alias=alias,
            chat_id=chat_id,
            thread_id=thread_id,
        )

    def resolve_scope_alias(self, *, alias: str) -> tuple[int, int] | None:
        return self._backend.resolve_scope_alias(alias=alias)

    def close(self) -> None:
        self._backend.close()
