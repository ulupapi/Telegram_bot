from __future__ import annotations

import asyncio
import logging
from datetime import timezone

from aiogram import Router
try:
    from aiogram import F
except Exception:  # pragma: no cover - compatibility fallback
    from magic_filter import F  # type: ignore
from aiogram.types import Message

from ai_extractor import AIExtractor, StatusReport
from database import Database

logger = logging.getLogger(__name__)
STATUS_ORDER = ["В ожидании", "В работе", "Завершена", "Отклонена", "Отозвана"]


def build_router(
    *,
    target_chat_id: int | None,
    target_topic_id: int | None,
    strict_target_scope: bool,
    context_messages_limit: int,
    db: Database,
    extractor: AIExtractor,
) -> Router:
    try:
        from aiogram.filters import Command as CommandFilter
    except Exception:
        from aiogram.dispatcher.filters import Command as CommandFilter  # type: ignore[import-not-found]

    router = Router()

    @router.message(CommandFilter("bind"))
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

    @router.message(CommandFilter("where"))
    async def cmd_where(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        mode = (
            "Режим области: STRICT_TARGET_SCOPE=on (сбор только из фиксированной цели)."
            if strict_target_scope and target_chat_id is not None
            else "Режим области: multi-chat (по умолчанию, текущий чат/ветка)."
        )
        if thread_id:
            await message.answer(
                f"Текущий контекст: chat_id={chat_id}, topic_id={thread_id}\n"
                "Можно сохранить имя: /bind Задания\n"
                f"{mode}"
            )
        else:
            await message.answer(
                f"Текущий контекст: chat_id={chat_id} (обычный чат)\n"
                "Можно сохранить имя: /bind Задания\n"
                f"{mode}"
            )

    @router.message(CommandFilter("health"))
    async def cmd_health(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        me = await message.bot.get_me()
        can_read_all = getattr(me, "can_read_all_group_messages", None)
        bot_label = f"@{me.username}" if me.username else str(me.id)
        lines = [
            "🩺 Диагностика бота",
            f"• Бот: {bot_label}",
            f"• Chat ID: {chat_id}",
            f"• Topic ID: {thread_id if thread_id else '—'}",
            f"• Тип чата: {message.chat.type}",
        ]
        if strict_target_scope and target_chat_id is not None:
            suffix = f", topic_id={target_topic_id}" if target_topic_id else ""
            lines.append(
                f"• Scope режим: strict (фиксированная цель chat_id={target_chat_id}{suffix})"
            )
        else:
            lines.append("• Scope режим: multi-chat (текущий чат/ветка)")

        if can_read_all is True:
            lines.append("• Privacy Mode: выключен (бот видит обычные сообщения групп).")
        elif can_read_all is False:
            lines.append(
                "• Privacy Mode: включен (бот в группах видит в основном команды/упоминания)."
            )
        else:
            lines.append("• Privacy Mode: не удалось определить через Bot API.")

        if message.chat.type in {"group", "supergroup"}:
            try:
                member = await message.bot.get_chat_member(message.chat.id, me.id)
                status = getattr(member, "status", "unknown")
                lines.append(f"• Статус в группе: {status}")
            except Exception:
                lines.append("• Статус в группе: не удалось получить.")
            lines.extend(
                [
                    "",
                    "Чтобы бот стабильно собирал контекст в группе:",
                    "1. В BotFather отключите Privacy Mode: /setprivacy -> Disable.",
                    "2. Выдайте боту права администратора в группе (рекомендуется).",
                    "3. Напишите 2-3 обычных сообщения и вызовите /status.",
                ]
            )

        await message.answer("\n".join(lines))

    @router.message(CommandFilter("help"))
    async def cmd_help(message: Message) -> None:
        await message.answer(
            "Команды бота:\n"
            "• /status [название] — сводка и задачи (каждая задача отдельным сообщением)\n"
            "• /bind <название> — привязать алиас к текущему чату/ветке\n"
            "• /where — показать текущий chat_id/topic_id\n"
            "• /health — диагностика доступа и режима сбора в текущем чате\n"
            "• /clear_db [название] — очистить данные текущего/указанного контекста\n"
            "• /clear_db all — очистить всю БД\n"
            "• /clear [название] — короткий алиас для /clear_db"
        )

    @router.message(CommandFilter("status"))
    async def cmd_status(message: Message) -> None:
        scope = await _resolve_scope_from_message_or_alias(
            message=message,
            db=db,
            target_chat_id=target_chat_id,
            target_topic_id=target_topic_id,
            strict_target_scope=strict_target_scope,
            alias_raw=_command_argument(message),
        )
        if scope is None:
            return
        scope_chat_id, scope_thread_id = scope

        rows = await asyncio.to_thread(
            db.get_recent_thread_messages,
            chat_id=scope_chat_id,
            thread_id=scope_thread_id,
            limit=context_messages_limit,
        )
        if not rows:
            await message.answer(
                _empty_context_hint(message=message, scope_thread_id=scope_thread_id)
            )
            return

        try:
            report = await asyncio.to_thread(extractor.extract_status, rows)
            await asyncio.to_thread(db.replace_tasks, report.tasks)
        except Exception as exc:
            logger.exception("Failed to build status report")
            await message.answer(_humanize_llm_error(exc))
            return

        for chunk in _render_status_messages(report):
            await message.answer(chunk)

    @router.message(CommandFilter(commands=["clear_db", "clear"]))
    async def cmd_clear_db(message: Message) -> None:
        arg_raw = _command_argument(message)
        arg_clean = _normalize_alias(arg_raw) if arg_raw else ""
        if arg_clean in {"all", "все"}:
            deleted_messages, deleted_tasks, deleted_aliases = await asyncio.to_thread(
                db.clear_all
            )
            await message.answer(
                "🧹 База очищена полностью.\n"
                f"• Сообщений удалено: {deleted_messages}\n"
                f"• Задач удалено: {deleted_tasks}\n"
                f"• Привязок удалено: {deleted_aliases}"
            )
            return

        scope = await _resolve_scope_from_message_or_alias(
            message=message,
            db=db,
            target_chat_id=target_chat_id,
            target_topic_id=target_topic_id,
            strict_target_scope=strict_target_scope,
            alias_raw=arg_raw,
        )
        if scope is None:
            return
        scope_chat_id, scope_thread_id = scope

        deleted_messages, deleted_tasks = await asyncio.to_thread(
            db.clear_scope,
            chat_id=scope_chat_id,
            thread_id=scope_thread_id,
        )
        scope_suffix = f", topic_id={scope_thread_id}" if scope_thread_id else ""
        await message.answer(
            "🧹 Контекст очищен.\n"
            f"• Сообщений удалено: {deleted_messages}\n"
            f"• Задач удалено: {deleted_tasks}\n"
            f"• Контекст: chat_id={scope_chat_id}{scope_suffix}\n"
            "Чтобы очистить все данные сразу, используйте: /clear_db all"
        )

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
            strict_target_scope=strict_target_scope,
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
    lines = ["📌 Сводка по ветке"]
    lines.extend(
        _render_section(
            "✅ Что сделано",
            report.done,
            "Новых завершенных задач нет.",
        )
    )
    lines.extend(
        _render_section(
            "🛠 Что в работе",
            report.in_progress,
            "Активных задач не найдено.",
        )
    )
    lines.extend(
        _render_section(
            "⛔ Что зависло",
            report.blocked,
            "Блокеров не найдено.",
        )
    )
    lines.extend(_render_task_registry(report))
    return "\n".join(lines)


def _render_status_messages(report: StatusReport) -> list[str]:
    messages = [_render_summary_message(report)]
    if not report.tasks:
        messages.append("📋 Реестр задач\n• Задачи не найдены.")
        return messages

    ordered_tasks = _ordered_tasks(report)
    for idx, task in enumerate(ordered_tasks, start=1):
        messages.append(_render_task_message(task_index=idx, total=len(ordered_tasks), task=task))
    return messages


def _render_summary_message(report: StatusReport) -> str:
    lines = ["📌 Сводка по ветке"]
    lines.extend(
        _render_section(
            "✅ Что сделано",
            report.done,
            "Новых завершенных задач нет.",
        )
    )
    lines.extend(
        _render_section(
            "🛠 Что в работе",
            report.in_progress,
            "Активных задач не найдено.",
        )
    )
    lines.extend(
        _render_section(
            "⛔ Что зависло",
            report.blocked,
            "Блокеров не найдено.",
        )
    )
    lines.append("\n📋 Статусы задач:")
    if not report.tasks:
        lines.append("• Задач нет.")
    else:
        for status in STATUS_ORDER:
            count = sum(1 for task in report.tasks if task.status == status)
            if count:
                lines.append(f"• {_status_icon(status)} {status}: {count}")
    return "\n".join(lines)


def _ordered_tasks(report: StatusReport):
    ordered = []
    for status in STATUS_ORDER:
        status_tasks = [task for task in report.tasks if task.status == status]
        ordered.extend(status_tasks)
    return ordered


def _render_task_message(*, task_index: int, total: int, task) -> str:
    description = _trim_text(task.description, limit=900) or "Не указано"
    lines = [
        f"🧩 Задача {task_index} из {total}",
        f"📅 Дедлайн: {task.deadline_date or '—'}",
        "🌐 Основная информация:",
        f"1. Название: {task.title}",
        f"2. Автор: {task.author_name}",
        f"3. Исполнитель: {task.assignee}",
        f"4. Статус: {_status_icon(task.status)} {task.status}",
        f"5. ID: {task.external_id}",
        "6. Описание:",
        f"«{description}»",
    ]
    return "\n".join(lines)


def _trim_text(text: str, *, limit: int) -> str:
    value = (text or "").strip()
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"


def _render_section(title: str, items: list[str], empty_text: str) -> list[str]:
    lines = [f"\n{title}"]
    if not items:
        lines.append(f"• {empty_text}")
        return lines

    for idx, item in enumerate(items, start=1):
        lines.append(f"{idx}. {item}")
    return lines


def _render_task_registry(report: StatusReport) -> list[str]:
    lines = ["\n📋 Реестр задач"]
    if not report.tasks:
        lines.append("• Задачи не найдены.")
        return lines

    for status in STATUS_ORDER:
        status_tasks = [task for task in report.tasks if task.status == status]
        if not status_tasks:
            continue
        lines.append(f"\n{_status_icon(status)} {status} ({len(status_tasks)})")
        for idx, task in enumerate(status_tasks, start=1):
            lines.append(f"{idx}. {task.title}")
            lines.append(f"   🆔 {task.external_id}")
            lines.append(f"   📅 Дедлайн: {task.deadline_date or '—'}")
            lines.append(f"   👤 Автор: {task.author_name}")
            lines.append(f"   👷 Исполнитель: {task.assignee}")
            lines.append(f"   🏷 Статус: {task.status}")
            if task.description:
                lines.append("   📝 Описание:")
                lines.append(f"   └ {task.description}")
    return lines


def _status_icon(status: str) -> str:
    if status == "В ожидании":
        return "🟡"
    if status == "В работе":
        return "🔵"
    if status == "Завершена":
        return "🟢"
    if status == "Отклонена":
        return "🔴"
    if status == "Отозвана":
        return "⚪"
    return "▫️"


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
    strict_target_scope: bool,
) -> bool:
    if not strict_target_scope:
        return True
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


async def _resolve_scope_from_message_or_alias(
    *,
    message: Message,
    db: Database,
    target_chat_id: int | None,
    target_topic_id: int | None,
    strict_target_scope: bool,
    alias_raw: str,
) -> tuple[int, int] | None:
    current_chat_id, current_thread_id = _scope_from_message(message)
    await _learn_auto_aliases(
        db=db,
        message=message,
        chat_id=current_chat_id,
        thread_id=current_thread_id,
    )

    if alias_raw:
        alias = _normalize_alias(alias_raw)
        resolved = await asyncio.to_thread(db.resolve_scope_alias, alias=alias)
        if not resolved:
            await message.answer(
                f"Не нашел цель «{alias_raw}». "
                f"Откройте нужный чат/ветку и выполните: /bind {alias_raw}"
            )
            return None
        return resolved

    if strict_target_scope and target_chat_id is not None:
        return target_chat_id, target_topic_id or 0
    return current_chat_id, current_thread_id


def _empty_context_hint(*, message: Message, scope_thread_id: int) -> str:
    if scope_thread_id:
        base = "Пока нет сообщений для анализа в этой ветке."
    else:
        base = "Пока нет сообщений для анализа в этом чате."

    if message.chat.type not in {"group", "supergroup"}:
        return base

    return (
        f"{base}\n\n"
        "Проверьте, что бот получает обычные сообщения в группе:\n"
        "1. Отключите Privacy Mode у бота в BotFather (/setprivacy -> Disable).\n"
        "2. Добавьте бота в группу и выдайте права администратора (рекомендуется).\n"
        "3. Выполните /health в этой группе для быстрой диагностики."
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
            "Провайдер LLM вернул ограничение по квоте/лимиту (quota или resource_exhausted). "
            "Это не всегда означает, что токены закончились: проверьте лимиты по модели, тариф, "
            "billing и текущую доступность модели у провайдера."
        )
    if "429" in text:
        return "Слишком много запросов к LLM (429). Подождите немного и повторите."
    if "payment required" in text or "status=402" in text:
        return (
            "Amvera вернул 402 Payment Required: неактивны токены/тариф для этой модели. "
            "Проверьте, что AMVERA_LLM_MODEL соответствует модели с доступной квотой в разделе LLM."
        )
    if "502" in text or "504" in text or "bad gateway" in text or "gateway time-out" in text:
        return (
            "LLM-шлюз временно недоступен (502/504). "
            "Попробуйте еще раз через 20-30 секунд или временно переключите модель на gpt-4.1."
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
