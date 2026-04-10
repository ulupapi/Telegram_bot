from __future__ import annotations

import asyncio
import logging
from datetime import timezone

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import Message

from ai_extractor import AIExtractor, StatusReport
from database import Database

logger = logging.getLogger(__name__)


def build_router(
    *,
    target_chat_id: int,
    target_topic_id: int,
    context_messages_limit: int,
    db: Database,
    extractor: AIExtractor,
) -> Router:
    router = Router()

    @router.message(Command("status"))
    async def cmd_status(message: Message) -> None:
        if message.chat.id != target_chat_id or message.message_thread_id != target_topic_id:
            await message.answer("Команда /status доступна только в целевой ветке.")
            return

        rows = await asyncio.to_thread(
            db.get_recent_thread_messages,
            chat_id=target_chat_id,
            thread_id=target_topic_id,
            limit=context_messages_limit,
        )
        if not rows:
            await message.answer("Пока нет сообщений для анализа в этой ветке.")
            return

        try:
            report = await asyncio.to_thread(extractor.extract_status, rows)
            await asyncio.to_thread(db.replace_tasks, report.tasks)
        except Exception:
            logger.exception("Failed to build status report")
            await message.answer("Не удалось получить сводку от LLM. Попробуйте чуть позже.")
            return

        await message.answer(_render_status(report))

    @router.message(F.chat.id == target_chat_id, F.message_thread_id == target_topic_id)
    async def collect_topic_messages(message: Message) -> None:
        text = (message.text or message.caption or "").strip()
        if not text:
            return

        author = message.from_user.full_name if message.from_user else "Unknown"
        created_at = message.date.replace(tzinfo=timezone.utc).isoformat()

        await asyncio.to_thread(
            db.save_message,
            chat_id=message.chat.id,
            thread_id=message.message_thread_id or 0,
            message_id=message.message_id,
            user_name=author,
            text=text,
            created_at=created_at,
        )

    return router


def _render_status(report: StatusReport) -> str:
    lines = ["Сводка по ветке"]
    lines.extend(_render_section("Что сделано", report.done, "Новых завершенных задач нет."))
    lines.extend(_render_section("Что в работе", report.in_progress, "Активных задач не найдено."))
    lines.extend(_render_section("Что зависло", report.blocked, "Блокеров не найдено."))
    return "\n".join(lines)


def _render_section(title: str, items: list[str], empty_text: str) -> list[str]:
    lines = [f"\n{title}:"]
    if not items:
        lines.append(f"• {empty_text}")
        return lines

    for item in items:
        lines.append(f"• {item}")
    return lines
