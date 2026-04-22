from __future__ import annotations

import logging
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol, Sequence, TypeVar

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

logger = logging.getLogger(__name__)
T = TypeVar("T")


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
    def replace_tasks_for_scope(
        self,
        *,
        chat_id: int,
        thread_id: int,
        tasks: Sequence[TaskRecord],
    ) -> None: ...
    def get_tasks_for_scope(self, *, chat_id: int, thread_id: int) -> list[TaskRecord]: ...
    def update_task_for_scope(
        self,
        *,
        chat_id: int,
        thread_id: int,
        external_id: str,
        title: str | None = None,
        description: str | None = None,
        deadline_date: str | None = None,
        author_name: str | None = None,
        assignee: str | None = None,
        status: str | None = None,
    ) -> bool: ...
    def list_message_scopes(self) -> list[tuple[int, int]]: ...
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
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                self._ensure_column("tasks", "description", "TEXT NOT NULL DEFAULT ''")
                self._ensure_column("tasks", "deadline_date", "TEXT NOT NULL DEFAULT ''")
                self._ensure_column("tasks", "author_name", "TEXT NOT NULL DEFAULT ''")
                self._ensure_column("tasks", "assignee", "TEXT NOT NULL DEFAULT ''")
                self._ensure_column("tasks", "scope_chat_id", "INTEGER NOT NULL DEFAULT 0")
                self._ensure_column("tasks", "scope_thread_id", "INTEGER NOT NULL DEFAULT 0")
                self._ensure_column("tasks", "public_id", "TEXT NOT NULL DEFAULT ''")
                self.conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_tasks_scope
                    ON tasks(scope_chat_id, scope_thread_id, updated_at)
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
                self.conn.execute("DELETE FROM tasks")
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
                            status
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
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

    def replace_tasks_for_scope(
        self,
        *,
        chat_id: int,
        thread_id: int,
        tasks: Sequence[TaskRecord],
    ) -> None:
        with self._lock:
            with self.conn:
                self.conn.execute(
                    """
                    DELETE FROM tasks
                    WHERE scope_chat_id = ? AND scope_thread_id = ?
                    """,
                    (chat_id, thread_id),
                )
                for task in tasks:
                    if task.status not in VALID_STATUSES:
                        continue
                    public_id = (task.external_id or "").strip() or "T"
                    internal_id = f"{chat_id}:{thread_id}:{public_id}"
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
                            scope_chat_id,
                            scope_thread_id,
                            public_id
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(external_id) DO UPDATE SET
                            title = excluded.title,
                            description = excluded.description,
                            deadline_date = excluded.deadline_date,
                            author_name = excluded.author_name,
                            assignee = excluded.assignee,
                            status = excluded.status,
                            scope_chat_id = excluded.scope_chat_id,
                            scope_thread_id = excluded.scope_thread_id,
                            public_id = excluded.public_id,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (
                            internal_id,
                            task.title,
                            task.description,
                            task.deadline_date,
                            task.author_name,
                            task.assignee,
                            task.status,
                            chat_id,
                            thread_id,
                            public_id,
                        ),
                    )

    def get_tasks_for_scope(self, *, chat_id: int, thread_id: int) -> list[TaskRecord]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT
                    COALESCE(NULLIF(public_id, ''), external_id) AS external_id,
                    title,
                    description,
                    deadline_date,
                    author_name,
                    assignee,
                    status
                FROM tasks
                WHERE scope_chat_id = ? AND scope_thread_id = ?
                ORDER BY updated_at DESC, id DESC
                """,
                (chat_id, thread_id),
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

    def update_task_for_scope(
        self,
        *,
        chat_id: int,
        thread_id: int,
        external_id: str,
        title: str | None = None,
        description: str | None = None,
        deadline_date: str | None = None,
        author_name: str | None = None,
        assignee: str | None = None,
        status: str | None = None,
    ) -> bool:
        assignments: list[str] = []
        values: list[object] = []
        if title is not None:
            assignments.append("title = ?")
            values.append(title)
        if description is not None:
            assignments.append("description = ?")
            values.append(description)
        if deadline_date is not None:
            assignments.append("deadline_date = ?")
            values.append(deadline_date)
        if author_name is not None:
            assignments.append("author_name = ?")
            values.append(author_name)
        if assignee is not None:
            assignments.append("assignee = ?")
            values.append(assignee)
        if status is not None:
            if status not in VALID_STATUSES:
                return False
            assignments.append("status = ?")
            values.append(status)
        if not assignments:
            return False

        assignments.append("updated_at = CURRENT_TIMESTAMP")
        values.extend([chat_id, thread_id, external_id])
        sql = (
            f"UPDATE tasks SET {', '.join(assignments)} "
            "WHERE scope_chat_id = ? AND scope_thread_id = ? AND public_id = ?"
        )
        with self._lock:
            with self.conn:
                cur = self.conn.execute(sql, tuple(values))
                updated = cur.rowcount
        return updated > 0

    def list_message_scopes(self) -> list[tuple[int, int]]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT DISTINCT chat_id, thread_id
                FROM messages
                ORDER BY chat_id, thread_id
                """
            ).fetchall()
        return [(int(row["chat_id"]), int(row["thread_id"])) for row in rows]

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
                cur_tasks = self.conn.execute(
                    """
                    DELETE FROM tasks
                    WHERE scope_chat_id = ? AND scope_thread_id = ?
                    """,
                    (chat_id, thread_id),
                )
                deleted_tasks = max(0, cur_tasks.rowcount)
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
        self._dsn = dsn
        self._lock = threading.RLock()
        self.conn = self._connect()

    def _connect(self):
        return psycopg.connect(self._dsn, autocommit=True)

    def _is_connection_error(self, exc: Exception) -> bool:
        return isinstance(exc, (psycopg.OperationalError, psycopg.InterfaceError))

    def _reconnect_locked(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
        self.conn = self._connect()

    def _run_with_reconnect(self, op_name: str, fn: Callable[[], T]) -> T:
        with self._lock:
            max_attempts = 2
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn()
                except Exception as exc:
                    if attempt >= max_attempts or not self._is_connection_error(exc):
                        raise
                    logger.warning(
                        "Postgres connection error during %s: %s. Reconnecting and retrying.",
                        op_name,
                        exc,
                    )
                    self._reconnect_locked()
        raise RuntimeError("unreachable")

    def init_schema(self) -> None:
        def _op() -> None:
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
                    ADD COLUMN IF NOT EXISTS scope_chat_id BIGINT NOT NULL DEFAULT 0
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE tasks
                    ADD COLUMN IF NOT EXISTS scope_thread_id BIGINT NOT NULL DEFAULT 0
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE tasks
                    ADD COLUMN IF NOT EXISTS public_id TEXT NOT NULL DEFAULT ''
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_tasks_scope
                    ON tasks(scope_chat_id, scope_thread_id, updated_at)
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
        self._run_with_reconnect("init_schema", _op)

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

        def _op() -> None:
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
        self._run_with_reconnect("save_message", _op)

    def get_recent_thread_messages(
        self,
        *,
        chat_id: int,
        thread_id: int,
        limit: int,
    ) -> list[StoredMessage]:
        def _op():
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
                return cur.fetchall()

        rows = self._run_with_reconnect("get_recent_thread_messages", _op)

        return [
            StoredMessage(
                created_at=row["created_at"],
                user_name=row["user_name"],
                text=row["text"],
            )
            for row in reversed(rows)
        ]

    def replace_tasks(self, tasks: Sequence[TaskRecord]) -> None:
        def _op() -> None:
            with self.conn.transaction():
                with self.conn.cursor() as cur:
                    cur.execute("DELETE FROM tasks")
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
                                status
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
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
        self._run_with_reconnect("replace_tasks", _op)

    def replace_tasks_for_scope(
        self,
        *,
        chat_id: int,
        thread_id: int,
        tasks: Sequence[TaskRecord],
    ) -> None:
        def _op() -> None:
            with self.conn.transaction():
                with self.conn.cursor() as cur:
                    cur.execute(
                        """
                        DELETE FROM tasks
                        WHERE scope_chat_id = %s AND scope_thread_id = %s
                        """,
                        (chat_id, thread_id),
                    )
                    for task in tasks:
                        if task.status not in VALID_STATUSES:
                            continue
                        public_id = (task.external_id or "").strip() or "T"
                        internal_id = f"{chat_id}:{thread_id}:{public_id}"
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
                                scope_chat_id,
                                scope_thread_id,
                                public_id
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (external_id) DO UPDATE SET
                                title = EXCLUDED.title,
                                description = EXCLUDED.description,
                                deadline_date = EXCLUDED.deadline_date,
                                author_name = EXCLUDED.author_name,
                                assignee = EXCLUDED.assignee,
                                status = EXCLUDED.status,
                                scope_chat_id = EXCLUDED.scope_chat_id,
                                scope_thread_id = EXCLUDED.scope_thread_id,
                                public_id = EXCLUDED.public_id,
                                updated_at = CURRENT_TIMESTAMP
                            """,
                            (
                                internal_id,
                                task.title,
                                task.description,
                                task.deadline_date,
                                task.author_name,
                                task.assignee,
                                task.status,
                                chat_id,
                                thread_id,
                                public_id,
                            ),
                        )

        self._run_with_reconnect("replace_tasks_for_scope", _op)

    def get_tasks_for_scope(self, *, chat_id: int, thread_id: int) -> list[TaskRecord]:
        def _op():
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT
                        COALESCE(NULLIF(public_id, ''), external_id) AS external_id,
                        title,
                        description,
                        deadline_date,
                        author_name,
                        assignee,
                        status
                    FROM tasks
                    WHERE scope_chat_id = %s AND scope_thread_id = %s
                    ORDER BY updated_at DESC, id DESC
                    """,
                    (chat_id, thread_id),
                )
                return cur.fetchall()

        rows = self._run_with_reconnect("get_tasks_for_scope", _op)
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

    def update_task_for_scope(
        self,
        *,
        chat_id: int,
        thread_id: int,
        external_id: str,
        title: str | None = None,
        description: str | None = None,
        deadline_date: str | None = None,
        author_name: str | None = None,
        assignee: str | None = None,
        status: str | None = None,
    ) -> bool:
        assignments: list[str] = []
        values: list[object] = []
        if title is not None:
            assignments.append("title = %s")
            values.append(title)
        if description is not None:
            assignments.append("description = %s")
            values.append(description)
        if deadline_date is not None:
            assignments.append("deadline_date = %s")
            values.append(deadline_date)
        if author_name is not None:
            assignments.append("author_name = %s")
            values.append(author_name)
        if assignee is not None:
            assignments.append("assignee = %s")
            values.append(assignee)
        if status is not None:
            if status not in VALID_STATUSES:
                return False
            assignments.append("status = %s")
            values.append(status)
        if not assignments:
            return False

        assignments.append("updated_at = CURRENT_TIMESTAMP")
        values.extend([chat_id, thread_id, external_id])
        sql = (
            f"UPDATE tasks SET {', '.join(assignments)} "
            "WHERE scope_chat_id = %s AND scope_thread_id = %s AND public_id = %s"
        )

        def _op() -> bool:
            with self.conn.cursor() as cur:
                cur.execute(sql, tuple(values))
                return cur.rowcount > 0

        return self._run_with_reconnect("update_task_for_scope", _op)

    def list_message_scopes(self) -> list[tuple[int, int]]:
        def _op():
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT DISTINCT chat_id, thread_id
                    FROM messages
                    ORDER BY chat_id, thread_id
                    """
                )
                return cur.fetchall()

        rows = self._run_with_reconnect("list_message_scopes", _op)
        return [(int(row["chat_id"]), int(row["thread_id"])) for row in rows]

    def clear_scope(self, *, chat_id: int, thread_id: int) -> tuple[int, int]:
        def _op() -> tuple[int, int]:
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
                    cur.execute(
                        """
                        DELETE FROM tasks
                        WHERE scope_chat_id = %s AND scope_thread_id = %s
                        """,
                        (chat_id, thread_id),
                    )
                    deleted_tasks = max(0, cur.rowcount)
            return deleted_messages, deleted_tasks
        return self._run_with_reconnect("clear_scope", _op)

    def clear_all(self) -> tuple[int, int, int]:
        def _op() -> tuple[int, int, int]:
            with self.conn.transaction():
                with self.conn.cursor() as cur:
                    cur.execute("DELETE FROM messages")
                    deleted_messages = max(0, cur.rowcount)
                    cur.execute("DELETE FROM tasks")
                    deleted_tasks = max(0, cur.rowcount)
                    cur.execute("DELETE FROM scope_aliases")
                    deleted_aliases = max(0, cur.rowcount)
            return deleted_messages, deleted_tasks, deleted_aliases
        return self._run_with_reconnect("clear_all", _op)

    def learn_scope_alias(self, *, alias: str, chat_id: int, thread_id: int) -> None:
        clean_alias = alias.strip().lower()
        if not clean_alias:
            return

        def _op() -> None:
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
        self._run_with_reconnect("learn_scope_alias", _op)

    def set_manual_scope_alias(self, *, alias: str, chat_id: int, thread_id: int) -> None:
        clean_alias = alias.strip().lower()
        if not clean_alias:
            return

        def _op() -> None:
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
        self._run_with_reconnect("set_manual_scope_alias", _op)

    def resolve_scope_alias(self, *, alias: str) -> tuple[int, int] | None:
        clean_alias = alias.strip().lower()
        if not clean_alias:
            return None

        def _op():
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT chat_id, thread_id
                    FROM scope_aliases
                    WHERE alias = %s
                    """,
                    (clean_alias,),
                )
                return cur.fetchone()

        row = self._run_with_reconnect("resolve_scope_alias", _op)
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

    def replace_tasks_for_scope(
        self,
        *,
        chat_id: int,
        thread_id: int,
        tasks: Sequence[TaskRecord],
    ) -> None:
        self._backend.replace_tasks_for_scope(
            chat_id=chat_id,
            thread_id=thread_id,
            tasks=tasks,
        )

    def get_tasks_for_scope(self, *, chat_id: int, thread_id: int) -> list[TaskRecord]:
        return self._backend.get_tasks_for_scope(chat_id=chat_id, thread_id=thread_id)

    def update_task_for_scope(
        self,
        *,
        chat_id: int,
        thread_id: int,
        external_id: str,
        title: str | None = None,
        description: str | None = None,
        deadline_date: str | None = None,
        author_name: str | None = None,
        assignee: str | None = None,
        status: str | None = None,
    ) -> bool:
        return self._backend.update_task_for_scope(
            chat_id=chat_id,
            thread_id=thread_id,
            external_id=external_id,
            title=title,
            description=description,
            deadline_date=deadline_date,
            author_name=author_name,
            assignee=assignee,
            status=status,
        )

    def list_message_scopes(self) -> list[tuple[int, int]]:
        return self._backend.list_message_scopes()

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
