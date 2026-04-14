from __future__ import annotations

import asyncio
import logging
from datetime import timezone

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from ai_extractor import AIExtractor, StatusReport
from database import Database

logger = logging.getLogger(__name__)
STATUS_ORDER = ["В ожидании", "В работе", "Завершена", "Отклонена", "Отозвана"]


def build_router(
    *,
    target_chat_id: int | None,
    target_topic_id: int | None,
    context_messages_limit: int,
    db: Database,
    extractor: AIExtractor,
) -> Router:
    router = Router()

    @router.message(Command("bind"))
    async def cmd_bind(message: Message) -> None:
        alias_raw = _command_argument(message)
        if not alias_raw:
            await message.answer("Использование: /bind <название>, например: /bind Задания")
            return

        chat_id, thread_id = _scope_from_message(message)
        alias = _normalize_alias(alias_raw)
        await asyncio.to_thread(
            db.set_manual_scope_alias,
            alias=alias,
            chat_id=chat_id,
            thread_id=thread_id,
        )
        suffix = f", topic_id={thread_id}" if thread_id else ""
        await message.answer(f"Сохранил цель «{alias_raw}»: chat_id={chat_id}{suffix}")

    @router.message(Command("where"))
    async def cmd_where(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        if thread_id:
            await message.answer(
                f"Текущий контекст: chat_id={chat_id}, topic_id={thread_id}\n"
                "Можно сохранить имя: /bind Задания"
            )
        else:
            await message.answer(
                f"Текущий контекст: chat_id={chat_id} (обычный чат)\n"
                "Можно сохранить имя: /bind Задания"
            )

    @router.message(Command("status"))
    async def cmd_status(message: Message) -> None:
        current_chat_id, current_thread_id = _scope_from_message(message)
        await _learn_auto_aliases(
            db=db,
            message=message,
            chat_id=current_chat_id,
            thread_id=current_thread_id,
        )

        alias_raw = _command_argument(message)
        if alias_raw:
            alias = _normalize_alias(alias_raw)
            resolved = await asyncio.to_thread(db.resolve_scope_alias, alias=alias)
            if not resolved:
                await message.answer(
                    f"Не нашел цель «{alias_raw}». "
                    f"Откройте нужный чат/ветку и выполните: /bind {alias_raw}"
                )
                return
            scope_chat_id, scope_thread_id = resolved
        elif target_chat_id is not None:
            scope_chat_id = target_chat_id
            scope_thread_id = target_topic_id or 0
        else:
            scope_chat_id = current_chat_id
            scope_thread_id = current_thread_id

        rows = await asyncio.to_thread(
            db.get_recent_thread_messages,
            chat_id=scope_chat_id,
            thread_id=scope_thread_id,
            limit=context_messages_limit,
        )
        if not rows:
            if scope_thread_id:
                await message.answer("Пока нет сообщений для анализа в этой ветке.")
            else:
                await message.answer("Пока нет сообщений для анализа в этом чате.")
            return

        try:
            report = await asyncio.to_thread(extractor.extract_status, rows)
            await asyncio.to_thread(db.replace_tasks, report.tasks)
        except Exception as exc:
            logger.exception("Failed to build status report")
            await message.answer(_humanize_llm_error(exc))
            return

        await message.answer(_render_status(report))

    @router.message()
    async def collect_messages(message: Message) -> None:
        text = (message.text or message.caption or "").strip()
        if not text:
            return
        if text.startswith("/"):
            return

        chat_id, thread_id = _scope_from_message(message)
        if not _is_scope_allowed(
            chat_id=chat_id,
            thread_id=thread_id,
            target_chat_id=target_chat_id,
            target_topic_id=target_topic_id,
        ):
            return

        author = _telegram_author(message)
        created_at = message.date.replace(tzinfo=timezone.utc).isoformat()

        await asyncio.to_thread(
            db.save_message,
            chat_id=chat_id,
            thread_id=thread_id,
            message_id=message.message_id,
            user_name=author,
            text=text,
            created_at=created_at,
        )
        await _learn_auto_aliases(
            db=db,
            message=message,
            chat_id=chat_id,
            thread_id=thread_id,
        )

    return router


def _render_status(report: StatusReport) -> str:
    lines = ["Сводка по ветке"]
    lines.extend(_render_section("Что сделано", report.done, "Новых завершенных задач нет."))
    lines.extend(_render_section("Что в работе", report.in_progress, "Активных задач не найдено."))
    lines.extend(_render_section("Что зависло", report.blocked, "Блокеров не найдено."))
    lines.extend(_render_task_registry(report))
    return "\n".join(lines)


def _render_section(title: str, items: list[str], empty_text: str) -> list[str]:
    lines = [f"\n{title}:"]
    if not items:
        lines.append(f"• {empty_text}")
        return lines

    for item in items:
        lines.append(f"• {item}")
    return lines


def _render_task_registry(report: StatusReport) -> list[str]:
    lines = ["\nРеестр задач:"]
    if not report.tasks:
        lines.append("• Задачи не найдены.")
        return lines

    for status in STATUS_ORDER:
        status_tasks = [task for task in report.tasks if task.status == status]
        if not status_tasks:
            continue
        lines.append(f"\n{status}:")
        for task in status_tasks:
            lines.append(f"• [{task.external_id}] {task.title}")
            if task.description:
                lines.append(f"  Описание: {task.description}")
            lines.append(f"  Дедлайн: {task.deadline_date or '—'}")
            lines.append(f"  Автор: {task.author_name}")
            lines.append(f"  Исполнитель: {task.assignee}")
    return lines


def _telegram_author(message: Message) -> str:
    user = message.from_user
    if user is None:
        return "Unknown"
    if user.username:
        return f"@{user.username}"
    if user.full_name:
        return user.full_name
    return f"user_{user.id}"


def _scope_from_message(message: Message) -> tuple[int, int]:
    return message.chat.id, message.message_thread_id or 0


def _is_scope_allowed(
    *,
    chat_id: int,
    thread_id: int,
    target_chat_id: int | None,
    target_topic_id: int | None,
) -> bool:
    if target_chat_id is None:
        return True
    if chat_id != target_chat_id:
        return False
    if target_topic_id is None:
        return True
    return thread_id == target_topic_id


def _command_argument(message: Message) -> str:
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return ""
    return parts[1].strip()


def _normalize_alias(raw: str) -> str:
    return " ".join(raw.strip().lower().split())


async def _learn_auto_aliases(
    *,
    db: Database,
    message: Message,
    chat_id: int,
    thread_id: int,
) -> None:
    if thread_id == 0:
        chat_title = (message.chat.title or "").strip()
        if chat_title:
            await asyncio.to_thread(
                db.learn_scope_alias,
                alias=_normalize_alias(chat_title),
                chat_id=chat_id,
                thread_id=0,
            )
        return

    topic_name = _extract_topic_name(message)
    if topic_name:
        await asyncio.to_thread(
            db.learn_scope_alias,
            alias=_normalize_alias(topic_name),
            chat_id=chat_id,
            thread_id=thread_id,
        )


def _extract_topic_name(message: Message) -> str:
    if message.forum_topic_created:
        return message.forum_topic_created.name
    if message.forum_topic_edited:
        return message.forum_topic_edited.name
    if message.reply_to_message and message.reply_to_message.forum_topic_created:
        return message.reply_to_message.forum_topic_created.name
    return ""


def _humanize_llm_error(exc: Exception) -> str:
    text = str(exc).lower()
    if (
        "insufficient_quota" in text
        or "resource_exhausted" in text
        or "quota exceeded" in text
    ):
        return (
            "Лимит LLM API исчерпан (quota). "
            "Пополните/включите billing и попробуйте снова."
        )
    if "429" in text:
        return "Слишком много запросов к LLM (429). Подождите немного и повторите."
    if "payment required" in text or "status=402" in text:
        return (
            "Amvera вернул 402 Payment Required: неактивны токены/тариф для этой модели. "
            "Проверьте, что AMVERA_LLM_MODEL соответствует модели с доступной квотой в разделе LLM."
        )
    if "502" in text or "bad gateway" in text:
        return (
            "LLM-шлюз временно недоступен (502). "
            "Попробуйте еще раз через 20-30 секунд."
        )
    if "read timeout" in text or "timed out" in text:
        return (
            "LLM отвечает слишком долго (таймаут). "
            "Попробуйте снова или уменьшите CONTEXT_MESSAGES_LIMIT."
        )
    if "amvera request failed" in text and "400" in text:
        return (
            "Amvera вернул 400 Bad Request. "
            "Проверьте AMVERA_LLM_MODEL и API-ключ (теперь детали есть в логах)."
        )
    return "Не удалось получить сводку от LLM. Попробуйте чуть позже."
