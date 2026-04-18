from __future__ import annotations

import asyncio
import logging
import os
from datetime import timezone
from html import escape

from aiogram import F, Bot, Router
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

from ai_extractor import AIExtractor, StatusReport
from database import Database, TaskRecord

logger = logging.getLogger(__name__)
STATUS_ORDER = ["В ожидании", "В работе", "Завершена", "Отклонена", "Отозвана"]

BTN_SUMMARY = "📊 Получить общую сводку"
BTN_HELP = "❓ Помощь"
BTN_DEV_CLEAR_SCOPE = "🧹 Очистить текущий контекст"
BTN_DEV_CLEAR_ALL = "🧨 Очистить всю БД"
BTN_DEV_SCHEDULE = "🕒 Параметры расписания"
BTN_DEV_WHERE = "📍 Текущий контекст"
BTN_DEV_BACK = "🔙 В обычный режим"
BTN_DEV_ENTER = "🛠 Режим программиста"
BTN_DEV_EXIT = "👤 Обычный режим"

DEV_MODE_SCOPES: set[tuple[int, int]] = set()


def build_router(
    *,
    target_chat_id: int | None,
    target_topic_id: int | None,
    context_messages_limit: int,
    db: Database,
    extractor: AIExtractor,
) -> Router:
    _ = (target_chat_id, target_topic_id)
    router = Router()

    @router.message(F.new_chat_members)
    async def on_bot_added(message: Message) -> None:
        members = message.new_chat_members or []
        me = await message.bot.me()
        if not any(member.id == me.id for member in members):
            return
        chat_id, thread_id = _scope_from_message(message)
        await message.answer(
            "Бот подключен. Управление через кнопки внизу.",
            reply_markup=_keyboard_for_scope(chat_id=chat_id, thread_id=thread_id),
        )

    @router.message(F.text.startswith("/start"))
    async def on_start(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        await message.answer(
            "Готов к работе. Отправляйте сообщения в чат и нажимайте кнопку сводки.",
            reply_markup=_keyboard_for_scope(chat_id=chat_id, thread_id=thread_id),
        )

    @router.message(F.text == BTN_HELP)
    async def help_action(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        dev_mode = _is_dev_mode_enabled(chat_id=chat_id, thread_id=thread_id)
        await message.answer(
            "Пользовательские действия:\n"
            f"• Просто отправляйте рабочие сообщения в чат\n"
            f"• Нажмите {BTN_SUMMARY} — бот отправит контекст в LLM и покажет задачи",
            reply_markup=_keyboard_for_scope(chat_id=chat_id, thread_id=thread_id),
        )
        await message.answer(
            "Режимы:\n"
            "• Пользовательский — сводка и задачи\n"
            "• Режим программиста — очистка БД, диагностика расписания",
            reply_markup=_help_mode_switch_keyboard(dev_mode=dev_mode),
        )

    @router.message(F.text == BTN_SUMMARY)
    async def status_action(message: Message) -> None:
        await _send_status_for_message(
            message=message,
            db=db,
            extractor=extractor,
            context_messages_limit=context_messages_limit,
        )

    @router.callback_query(F.data == "mode|dev")
    async def enter_dev_mode(callback: CallbackQuery) -> None:
        if callback.message is None:
            await callback.answer()
            return
        chat_id = callback.message.chat.id
        thread_id = callback.message.message_thread_id or 0
        _set_dev_mode(chat_id=chat_id, thread_id=thread_id, enabled=True)
        await callback.message.answer(
            "🛠 Режим программиста включен.\n"
            "Теперь доступны сервисные действия в клавиатуре.",
            reply_markup=_keyboard_for_scope(chat_id=chat_id, thread_id=thread_id),
        )
        await callback.answer()

    @router.callback_query(F.data == "mode|user")
    async def exit_dev_mode(callback: CallbackQuery) -> None:
        if callback.message is None:
            await callback.answer()
            return
        chat_id = callback.message.chat.id
        thread_id = callback.message.message_thread_id or 0
        _set_dev_mode(chat_id=chat_id, thread_id=thread_id, enabled=False)
        await callback.message.answer(
            "Обычный режим включен.",
            reply_markup=_keyboard_for_scope(chat_id=chat_id, thread_id=thread_id),
        )
        await callback.answer()

    @router.message(F.text == BTN_DEV_WHERE)
    async def dev_where(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        if not _is_dev_mode_enabled(chat_id=chat_id, thread_id=thread_id):
            return
        await message.answer(
            f"Dev context: chat_id={chat_id}, topic_id={thread_id}",
            reply_markup=_keyboard_for_scope(chat_id=chat_id, thread_id=thread_id),
        )

    @router.message(F.text == BTN_DEV_CLEAR_SCOPE)
    async def dev_clear(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        if not _is_dev_mode_enabled(chat_id=chat_id, thread_id=thread_id):
            return
        deleted_messages, deleted_tasks = await asyncio.to_thread(
            db.clear_scope,
            chat_id=chat_id,
            thread_id=thread_id,
        )
        suffix = f", topic_id={thread_id}" if thread_id else ""
        await message.answer(
            "🧹 Dev clear выполнен.\n"
            f"• Сообщений удалено: {deleted_messages}\n"
            f"• Задач удалено: {deleted_tasks}\n"
            f"• Контекст: chat_id={chat_id}{suffix}",
            reply_markup=_keyboard_for_scope(chat_id=chat_id, thread_id=thread_id),
        )

    @router.message(F.text == BTN_DEV_CLEAR_ALL)
    async def dev_clear_all(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        if not _is_dev_mode_enabled(chat_id=chat_id, thread_id=thread_id):
            return
        deleted_messages, deleted_tasks, deleted_aliases = await asyncio.to_thread(
            db.clear_all
        )
        await message.answer(
            "🧹 Dev clear all выполнен.\n"
            f"• Сообщений удалено: {deleted_messages}\n"
            f"• Задач удалено: {deleted_tasks}\n"
            f"• Привязок удалено: {deleted_aliases}",
            reply_markup=_keyboard_for_scope(chat_id=chat_id, thread_id=thread_id),
        )

    @router.message(F.text == BTN_DEV_SCHEDULE)
    async def dev_schedule(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        if not _is_dev_mode_enabled(chat_id=chat_id, thread_id=thread_id):
            return
        schedule_enabled = os.getenv("SCHEDULE_ENABLED", "1")
        morning = os.getenv("SUMMARY_MORNING_TIME", "09:00")
        evening = os.getenv("SUMMARY_EVENING_TIME", "18:00")
        schedule_tz = os.getenv("SCHEDULE_TIMEZONE", "Europe/Moscow")
        target_chat = os.getenv("TARGET_CHAT_ID", "")
        target_topic = os.getenv("TARGET_TOPIC_ID", "")
        await message.answer(
            "Параметры расписания:\n"
            f"• SCHEDULE_ENABLED={schedule_enabled}\n"
            f"• SUMMARY_MORNING_TIME={morning}\n"
            f"• SUMMARY_EVENING_TIME={evening}\n"
            f"• SCHEDULE_TIMEZONE={schedule_tz}\n"
            f"• TARGET_CHAT_ID={target_chat or '(пусто)'}\n"
            f"• TARGET_TOPIC_ID={target_topic or '(пусто)'}",
            reply_markup=_keyboard_for_scope(chat_id=chat_id, thread_id=thread_id),
        )

    @router.message(F.text == BTN_DEV_BACK)
    async def dev_back(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        if not _is_dev_mode_enabled(chat_id=chat_id, thread_id=thread_id):
            return
        _set_dev_mode(chat_id=chat_id, thread_id=thread_id, enabled=False)
        await message.answer(
            "Обычный режим включен.",
            reply_markup=_keyboard_for_scope(chat_id=chat_id, thread_id=thread_id),
        )

    @router.message()
    async def collect_messages(message: Message) -> None:
        text = (message.text or message.caption or "").strip()
        if not text:
            return
        if text.startswith("/"):
            return
        if text in _all_control_button_texts():
            return
        if message.reply_to_message and message.reply_to_message.from_user and message.reply_to_message.from_user.is_bot:
            return

        chat_id, thread_id = _scope_from_message(message)
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

    return router


async def send_status_for_scope(
    *,
    bot: Bot,
    db: Database,
    extractor: AIExtractor,
    chat_id: int,
    thread_id: int,
    context_messages_limit: int,
) -> bool:
    rows = await asyncio.to_thread(
        db.get_recent_thread_messages,
        chat_id=chat_id,
        thread_id=thread_id,
        limit=context_messages_limit,
    )
    if not rows:
        return False

    try:
        report = await asyncio.to_thread(extractor.extract_status, rows)
        await asyncio.to_thread(db.replace_tasks, report.tasks)
        tasks = await asyncio.to_thread(db.list_tasks)
    except Exception as exc:
        if _is_expected_llm_error(exc):
            logger.warning("Scheduled summary skipped due to LLM issue: %s", exc)
        else:
            logger.exception("Scheduled summary failed")
        return False
    merged = StatusReport(
        done=report.done,
        in_progress=report.in_progress,
        blocked=report.blocked,
        tasks=tasks,
    )
    await _send_report(
        bot=bot,
        db=db,
        chat_id=chat_id,
        thread_id=thread_id,
        report=merged,
        include_keyboard=False,
    )
    return True


async def _send_status_for_message(
    *,
    message: Message,
    db: Database,
    extractor: AIExtractor,
    context_messages_limit: int,
) -> None:
    scope_chat_id, scope_thread_id = _scope_from_message(message)

    rows = await asyncio.to_thread(
        db.get_recent_thread_messages,
        chat_id=scope_chat_id,
        thread_id=scope_thread_id,
        limit=context_messages_limit,
    )
    if not rows:
        if scope_thread_id:
            await message.answer(
                "Пока нет сообщений для анализа в этой ветке.",
                reply_markup=_keyboard_for_scope(
                    chat_id=scope_chat_id,
                    thread_id=scope_thread_id,
                ),
            )
        else:
            await message.answer(
                "Пока нет сообщений для анализа в этом чате.",
                reply_markup=_keyboard_for_scope(
                    chat_id=scope_chat_id,
                    thread_id=scope_thread_id,
                ),
            )
        return

    try:
        report = await asyncio.to_thread(extractor.extract_status, rows)
        await asyncio.to_thread(db.replace_tasks, report.tasks)
        tasks = await asyncio.to_thread(db.list_tasks)
    except Exception as exc:
        if _is_expected_llm_error(exc):
            logger.warning("Failed to build status report: %s", exc)
        else:
            logger.exception("Failed to build status report")
        await message.answer(
            _humanize_llm_error(exc),
            reply_markup=_keyboard_for_scope(
                chat_id=scope_chat_id,
                thread_id=scope_thread_id,
            ),
        )
        return

    merged = StatusReport(
        done=report.done,
        in_progress=report.in_progress,
        blocked=report.blocked,
        tasks=tasks,
    )
    await _send_report(
        bot=message.bot,
        db=db,
        chat_id=scope_chat_id,
        thread_id=scope_thread_id,
        report=merged,
        include_keyboard=True,
    )


async def _send_report(
    *,
    bot: Bot,
    db: Database,
    chat_id: int,
    thread_id: int,
    report: StatusReport,
    include_keyboard: bool,
) -> None:
    old_message_ids = await asyncio.to_thread(
        db.list_task_post_message_ids,
        chat_id=chat_id,
        thread_id=thread_id,
    )
    for old_message_id in old_message_ids:
        await _safe_delete_message(
            bot=bot,
            chat_id=chat_id,
            message_id=old_message_id,
        )
    await asyncio.to_thread(
        db.clear_task_posts,
        chat_id=chat_id,
        thread_id=thread_id,
    )

    summary_message = await _send_text(
        bot=bot,
        chat_id=chat_id,
        thread_id=thread_id,
        text=_render_summary_message_html(report),
        include_keyboard=include_keyboard,
    )
    await asyncio.to_thread(
        db.set_task_post_message_id,
        chat_id=chat_id,
        thread_id=thread_id,
        external_id="__meta_summary__",
        bot_message_id=summary_message.message_id,
    )

    ordered_tasks = _ordered_tasks(report.tasks)
    if not ordered_tasks:
        empty_message = await _send_text(
            bot=bot,
            chat_id=chat_id,
            thread_id=thread_id,
            text="📋 <b>Реестр задач</b>\n• Задачи не найдены.",
            include_keyboard=False,
        )
        await asyncio.to_thread(
            db.set_task_post_message_id,
            chat_id=chat_id,
            thread_id=thread_id,
            external_id="__meta_empty__",
            bot_message_id=empty_message.message_id,
        )
        return

    total = len(ordered_tasks)
    for idx, task in enumerate(ordered_tasks, start=1):
        await _send_task_card(
            bot=bot,
            db=db,
            chat_id=chat_id,
            thread_id=thread_id,
            task=task,
            task_index=idx,
            total=total,
            include_keyboard=False,
        )


async def _send_task_card(
    *,
    bot: Bot,
    db: Database,
    chat_id: int,
    thread_id: int,
    task: TaskRecord,
    task_index: int,
    total: int,
    old_message_id: int | None = None,
    include_keyboard: bool,
) -> None:
    previous_id = await asyncio.to_thread(
        db.get_task_post_message_id,
        chat_id=chat_id,
        thread_id=thread_id,
        external_id=task.external_id,
    )
    if previous_id and previous_id != old_message_id:
        await _safe_delete_message(
            bot=bot,
            chat_id=chat_id,
            message_id=previous_id,
        )
    if old_message_id:
        await _safe_delete_message(
            bot=bot,
            chat_id=chat_id,
            message_id=old_message_id,
        )

    sent = await _send_text(
        bot=bot,
        chat_id=chat_id,
        thread_id=thread_id,
        text=_render_task_message_html(task_index=task_index, total=total, task=task),
        inline_keyboard=None,
        include_keyboard=include_keyboard,
    )
    await asyncio.to_thread(
        db.set_task_post_message_id,
        chat_id=chat_id,
        thread_id=thread_id,
        external_id=task.external_id,
        bot_message_id=sent.message_id,
    )


async def _send_text(
    *,
    bot: Bot,
    chat_id: int,
    thread_id: int,
    text: str,
    inline_keyboard: InlineKeyboardMarkup | None = None,
    include_keyboard: bool,
) -> Message:
    reply_markup = inline_keyboard
    if include_keyboard and reply_markup is None:
        reply_markup = _keyboard_for_scope(chat_id=chat_id, thread_id=thread_id)
    return await bot.send_message(
        chat_id=chat_id,
        text=text,
        message_thread_id=thread_id or None,
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=reply_markup,
    )


async def _safe_delete_message(*, bot: Bot, chat_id: int, message_id: int) -> None:
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        logger.debug("Failed to delete old task message chat_id=%s message_id=%s", chat_id, message_id)


def _keyboard_for_scope(*, chat_id: int, thread_id: int) -> ReplyKeyboardMarkup:
    if _is_dev_mode_enabled(chat_id=chat_id, thread_id=thread_id):
        rows = [
            [KeyboardButton(text=BTN_SUMMARY), KeyboardButton(text=BTN_HELP)],
            [KeyboardButton(text=BTN_DEV_CLEAR_SCOPE), KeyboardButton(text=BTN_DEV_CLEAR_ALL)],
            [KeyboardButton(text=BTN_DEV_WHERE), KeyboardButton(text=BTN_DEV_SCHEDULE)],
            [KeyboardButton(text=BTN_DEV_BACK)],
        ]
        return ReplyKeyboardMarkup(
            keyboard=rows,
            resize_keyboard=True,
            is_persistent=True,
            input_field_placeholder="Режим программиста...",
        )

    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_SUMMARY)],
            [KeyboardButton(text=BTN_HELP)],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Выберите действие...",
    )


def _help_mode_switch_keyboard(*, dev_mode: bool) -> InlineKeyboardMarkup:
    if dev_mode:
        button = InlineKeyboardButton(text=BTN_DEV_EXIT, callback_data="mode|user")
    else:
        button = InlineKeyboardButton(text=BTN_DEV_ENTER, callback_data="mode|dev")
    return InlineKeyboardMarkup(inline_keyboard=[[button]])


def _scope_key(*, chat_id: int, thread_id: int) -> tuple[int, int]:
    return chat_id, thread_id


def _is_dev_mode_enabled(*, chat_id: int, thread_id: int) -> bool:
    return _scope_key(chat_id=chat_id, thread_id=thread_id) in DEV_MODE_SCOPES


def _set_dev_mode(*, chat_id: int, thread_id: int, enabled: bool) -> None:
    key = _scope_key(chat_id=chat_id, thread_id=thread_id)
    if enabled:
        DEV_MODE_SCOPES.add(key)
        return
    DEV_MODE_SCOPES.discard(key)


def _all_control_button_texts() -> set[str]:
    # Keep legacy button labels to avoid saving old keyboard clicks into context.
    return {
        BTN_SUMMARY,
        BTN_HELP,
        BTN_DEV_CLEAR_SCOPE,
        BTN_DEV_CLEAR_ALL,
        BTN_DEV_SCHEDULE,
        BTN_DEV_WHERE,
        BTN_DEV_BACK,
        "📊 Получить сводку",
        "✏️ Включить редактирование",
        "✅ Выключить редактирование",
        "🧹 Очистить текущий чат",
        "🕒 Расписание",
        "📍 Где я",
        "🔙 Выйти из режима программиста",
    }


def _render_summary_message_html(report: StatusReport) -> str:
    lines = ["📌 <b>Сводка по ветке</b>"]
    lines.extend(_render_section_html("✅ Что сделано", report.done, "Новых завершенных задач нет."))
    lines.extend(_render_section_html("🛠 Что в работе", report.in_progress, "Активных задач не найдено."))
    lines.extend(_render_section_html("⛔ Что зависло", report.blocked, "Блокеров не найдено."))
    lines.append("\n📋 <b>Статусы задач:</b>")
    if not report.tasks:
        lines.append("• Задач нет.")
    else:
        for status in STATUS_ORDER:
            count = sum(1 for task in report.tasks if task.status == status)
            if count:
                lines.append(f"• {_status_icon(status)} {escape(status)}: {count}")
    return "\n".join(lines)


def _render_task_message_html(*, task_index: int, total: int, task: TaskRecord) -> str:
    title = escape(task.title)
    author = escape(task.author_name)
    assignee = escape(task.assignee)
    status = escape(task.status)
    external_id = escape(task.external_id)
    deadline = escape(task.deadline_date or "—")
    description = escape(_trim_text(task.description, limit=900) or "Не указано")
    return (
        f"🧩 <b>Задача {task_index} из {total}</b>\n"
        f"📅 <b>Дедлайн:</b> {deadline}\n"
        f"🌐 <b>Основная информация:</b>\n\n"
        f"1. <b>Название:</b> {title}\n"
        f"2. <b>Автор:</b> {author}\n"
        f"3. <b>Исполнитель:</b> {assignee}\n"
        f"4. <b>Статус:</b> {_status_icon(task.status)} {status}\n"
        f"5. <b>ID:</b> {external_id}\n"
        "6. <b>Описание:</b>\n"
        f"<blockquote>{description}</blockquote>"
    )


def _render_section_html(title: str, items: list[str], empty_text: str) -> list[str]:
    lines = [f"\n<b>{escape(title)}</b>"]
    if not items:
        lines.append(f"• {escape(empty_text)}")
        return lines
    for idx, item in enumerate(items, start=1):
        lines.append(f"{idx}. {escape(item)}")
    return lines


def _ordered_tasks(tasks: list[TaskRecord]) -> list[TaskRecord]:
    ordered: list[TaskRecord] = []
    for status in STATUS_ORDER:
        ordered.extend([task for task in tasks if task.status == status])
    return ordered


def _trim_text(text: str, *, limit: int) -> str:
    value = (text or "").strip()
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"


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


def _is_expected_llm_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(
        token in text
        for token in (
            "insufficient_quota",
            "resource_exhausted",
            "quota exceeded",
            "429",
            "payment required",
            "status=402",
            "502",
            "504",
            "bad gateway",
            "gateway time-out",
            "read timeout",
            "timed out",
            "amvera request failed",
        )
    )
