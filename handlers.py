from __future__ import annotations

import asyncio
import logging
import os
from datetime import timezone
from html import escape

from aiogram import F, Bot, Router
from aiogram.filters import Command
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

BTN_SUMMARY = "📊 Показать сводку"
BTN_ADD_TASK = "➕ Добавить задачу"
BTN_EDIT_TASK = "✏️ Редактировать задачу"
BTN_HELP = "❓ Помощь"

STATUS_TO_CODE = {
    "В ожидании": "wait",
    "В работе": "work",
    "Завершена": "done",
    "Отклонена": "reject",
    "Отозвана": "revoke",
}
CODE_TO_STATUS = {value: key for key, value in STATUS_TO_CODE.items()}


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

    @router.message(Command("start", "menu"))
    async def cmd_start(message: Message) -> None:
        await message.answer(
            "Управление ботом через кнопки внизу.\n"
            "Выберите действие на клавиатуре.",
            reply_markup=_main_keyboard(),
        )

    @router.message(Command("help"))
    @router.message(F.text == BTN_HELP)
    async def help_action(message: Message) -> None:
        await message.answer(
            "Пользовательские действия:\n"
            f"• {BTN_SUMMARY} — получить свежую сводку\n"
            f"• {BTN_ADD_TASK} — ответьте на сообщение и нажмите кнопку\n"
            f"• {BTN_EDIT_TASK} — инструкция по ручному редактированию\n\n"
            "Как редактировать задачу:\n"
            "1. Ответьте на карточку задачи.\n"
            "2. Напишите изменения построчно, например:\n"
            "статус: В работе\nисполнитель: @username\nдедлайн: 2026-04-25",
            reply_markup=_main_keyboard(),
        )

    @router.message(Command("summary", "status"))
    @router.message(F.text == BTN_SUMMARY)
    async def status_action(message: Message) -> None:
        await _send_status_for_message(
            message=message,
            db=db,
            extractor=extractor,
            context_messages_limit=context_messages_limit,
        )

    @router.message(Command("add_task", "add"))
    @router.message(F.text == BTN_ADD_TASK)
    async def create_task_from_reply(message: Message) -> None:
        quoted = message.reply_to_message
        quote_text = (quoted.text or quoted.caption or "").strip() if quoted else ""
        if not quote_text:
            await message.answer(
                "Ответьте на сообщение и снова нажмите «➕ Добавить задачу».",
                reply_markup=_main_keyboard(),
            )
            return

        task = _manual_task_from_quote(message=message, quoted_text=quote_text)
        await asyncio.to_thread(db.upsert_task, task, source="manual")

        chat_id, thread_id = _scope_from_message(message)
        await _send_task_card(
            bot=message.bot,
            db=db,
            chat_id=chat_id,
            thread_id=thread_id,
            task=task,
            task_index=1,
            total=1,
            include_keyboard=True,
        )
        await message.answer(
            "Задача создана из цитаты. При необходимости ответьте на карточку задачи и исправьте поля.",
            reply_markup=_main_keyboard(),
        )

    @router.message(Command("edit_task", "edit"))
    @router.message(F.text == BTN_EDIT_TASK)
    async def edit_task_help(message: Message) -> None:
        await message.answer(
            "Чтобы отредактировать задачу:\n"
            "1. Ответьте на карточку задачи.\n"
            "2. Напишите поля, которые хотите изменить.\n\n"
            "Пример:\n"
            "статус: В работе\n"
            "исполнитель: @username\n"
            "дедлайн: 2026-04-25\n"
            "описание: Обновленное описание",
            reply_markup=_main_keyboard(),
        )

    @router.message(Command("dev_help"))
    async def dev_help(message: Message) -> None:
        await message.answer(
            "Dev-команды:\n"
            "• /dev_where — текущий chat_id/topic_id\n"
            "• /dev_clear — очистить текущий контекст\n"
            "• /dev_clear_all — очистить всю БД\n"
            "• /dev_schedule — показать параметры расписания",
            reply_markup=_main_keyboard(),
        )

    @router.message(Command("dev_where"))
    async def dev_where(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        if thread_id:
            await message.answer(
                f"Dev context: chat_id={chat_id}, topic_id={thread_id}",
                reply_markup=_main_keyboard(),
            )
        else:
            await message.answer(
                f"Dev context: chat_id={chat_id}, topic_id=0",
                reply_markup=_main_keyboard(),
            )

    @router.message(Command("dev_clear"))
    async def dev_clear(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
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
            reply_markup=_main_keyboard(),
        )

    @router.message(Command("dev_clear_all"))
    async def dev_clear_all(message: Message) -> None:
        deleted_messages, deleted_tasks, deleted_aliases = await asyncio.to_thread(
            db.clear_all
        )
        await message.answer(
            "🧹 Dev clear all выполнен.\n"
            f"• Сообщений удалено: {deleted_messages}\n"
            f"• Задач удалено: {deleted_tasks}\n"
            f"• Привязок удалено: {deleted_aliases}",
            reply_markup=_main_keyboard(),
        )

    @router.message(Command("dev_schedule"))
    async def dev_schedule(message: Message) -> None:
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
            reply_markup=_main_keyboard(),
        )

    @router.callback_query(F.data.startswith("task_set|"))
    async def callback_set_status(callback: CallbackQuery) -> None:
        if callback.message is None:
            await callback.answer()
            return
        parts = (callback.data or "").split("|")
        if len(parts) != 3:
            await callback.answer("Некорректные данные кнопки", show_alert=True)
            return
        _, external_id, status_code = parts
        status = CODE_TO_STATUS.get(status_code)
        if status is None:
            await callback.answer("Неизвестный статус", show_alert=True)
            return

        updated = await asyncio.to_thread(
            db.update_task,
            external_id=external_id,
            status=status,
        )
        if updated is None:
            await callback.answer("Задача не найдена", show_alert=True)
            return

        chat_id = callback.message.chat.id
        thread_id = callback.message.message_thread_id or 0
        await _send_task_card(
            bot=callback.message.bot,
            db=db,
            chat_id=chat_id,
            thread_id=thread_id,
            task=updated,
            task_index=1,
            total=1,
            old_message_id=callback.message.message_id,
            include_keyboard=False,
        )
        await callback.answer("Статус обновлен")

    @router.callback_query(F.data.startswith("task_fix|"))
    async def callback_fix_hint(callback: CallbackQuery) -> None:
        if callback.message is None:
            await callback.answer()
            return
        parts = (callback.data or "").split("|")
        if len(parts) != 2:
            await callback.answer("Некорректные данные кнопки", show_alert=True)
            return
        _, external_id = parts
        await callback.message.answer(
            f"Ответьте на карточку задачи `{external_id}` текстом:\n"
            "название: ...\n"
            "описание: ...\n"
            "дедлайн: 2026-04-25\n"
            "автор: ...\n"
            "исполнитель: ...\n"
            "статус: В работе",
            reply_markup=_main_keyboard(),
        )
        await callback.answer()

    @router.message(F.reply_to_message)
    async def manual_fix_on_reply(message: Message) -> None:
        text = (message.text or message.caption or "").strip()
        if not text:
            return
        if text.startswith("/") or text in {
            BTN_SUMMARY,
            BTN_ADD_TASK,
            BTN_EDIT_TASK,
            BTN_HELP,
        }:
            return

        chat_id, thread_id = _scope_from_message(message)
        reply_id = message.reply_to_message.message_id
        external_id = await asyncio.to_thread(
            db.find_task_external_id_by_post_message,
            chat_id=chat_id,
            thread_id=thread_id,
            bot_message_id=reply_id,
        )
        if not external_id:
            return

        updates = _parse_manual_updates(text)
        if not updates:
            await message.answer(
                "Не понял поля для исправления. Пример:\n"
                "статус: В работе\nисполнитель: @username\nдедлайн: 2026-04-25",
                reply_markup=_main_keyboard(),
            )
            return

        updated = await asyncio.to_thread(db.update_task, external_id=external_id, **updates)
        if updated is None:
            await message.answer("Не удалось обновить задачу.", reply_markup=_main_keyboard())
            return

        await _send_task_card(
            bot=message.bot,
            db=db,
            chat_id=chat_id,
            thread_id=thread_id,
            task=updated,
            task_index=1,
            total=1,
            old_message_id=reply_id,
            include_keyboard=True,
        )
        await message.answer("Исправления применены.", reply_markup=_main_keyboard())

    @router.message()
    async def collect_messages(message: Message) -> None:
        text = (message.text or message.caption or "").strip()
        if not text:
            return
        if text.startswith("/"):
            return
        if text in {BTN_SUMMARY, BTN_ADD_TASK, BTN_EDIT_TASK, BTN_HELP}:
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

    report = await asyncio.to_thread(extractor.extract_status, rows)
    await asyncio.to_thread(db.replace_tasks, report.tasks)
    tasks = await asyncio.to_thread(db.list_tasks)
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
                reply_markup=_main_keyboard(),
            )
        else:
            await message.answer(
                "Пока нет сообщений для анализа в этом чате.",
                reply_markup=_main_keyboard(),
            )
        return

    try:
        report = await asyncio.to_thread(extractor.extract_status, rows)
        await asyncio.to_thread(db.replace_tasks, report.tasks)
        tasks = await asyncio.to_thread(db.list_tasks)
    except Exception as exc:
        logger.exception("Failed to build status report")
        await message.answer(_humanize_llm_error(exc), reply_markup=_main_keyboard())
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
    await _send_text(
        bot=bot,
        chat_id=chat_id,
        thread_id=thread_id,
        text=_render_summary_message_html(report),
        include_keyboard=include_keyboard,
    )

    ordered_tasks = _ordered_tasks(report.tasks)
    if not ordered_tasks:
        await _send_text(
            bot=bot,
            chat_id=chat_id,
            thread_id=thread_id,
            text="📋 <b>Реестр задач</b>\n• Задачи не найдены.",
            include_keyboard=False,
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
        inline_keyboard=_task_inline_keyboard(task.external_id),
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
        reply_markup = _main_keyboard()
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


def _main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_SUMMARY), KeyboardButton(text=BTN_ADD_TASK)],
            [KeyboardButton(text=BTN_EDIT_TASK), KeyboardButton(text=BTN_HELP)],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие...",
    )


def _task_inline_keyboard(external_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🟡 В ожидании",
                    callback_data=f"task_set|{external_id}|wait",
                ),
                InlineKeyboardButton(
                    text="🔵 В работе",
                    callback_data=f"task_set|{external_id}|work",
                ),
                InlineKeyboardButton(
                    text="🟢 Завершена",
                    callback_data=f"task_set|{external_id}|done",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🔴 Отклонена",
                    callback_data=f"task_set|{external_id}|reject",
                ),
                InlineKeyboardButton(
                    text="⚪ Отозвана",
                    callback_data=f"task_set|{external_id}|revoke",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Как исправить",
                    callback_data=f"task_fix|{external_id}",
                )
            ],
        ]
    )


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


def _manual_task_from_quote(*, message: Message, quoted_text: str) -> TaskRecord:
    first_line = next((line.strip() for line in quoted_text.splitlines() if line.strip()), "")
    title = first_line[:100] if first_line else "Новая задача"
    external_id = f"M{int(message.date.timestamp())}{message.message_id}"
    return TaskRecord(
        external_id=external_id,
        title=title,
        description=quoted_text,
        deadline_date="",
        author_name=_telegram_author(message),
        assignee="Не назначен",
        status="В ожидании",
    )


def _parse_manual_updates(raw: str) -> dict[str, str]:
    updates: dict[str, str] = {}
    for line in raw.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        clean_key = _normalize_alias(key)
        clean_value = value.strip()
        if not clean_value:
            continue
        if clean_key in {"название", "title"}:
            updates["title"] = clean_value
        elif clean_key in {"описание", "description"}:
            updates["description"] = clean_value
        elif clean_key in {"дедлайн", "deadline"}:
            updates["deadline_date"] = _normalize_deadline_or_keep(clean_value)
        elif clean_key in {"автор", "author"}:
            updates["author_name"] = clean_value
        elif clean_key in {"исполнитель", "assignee"}:
            updates["assignee"] = clean_value
        elif clean_key in {"статус", "status"}:
            updates["status"] = _normalize_status(clean_value)
    return updates


def _normalize_status(raw_status: str) -> str:
    value = " ".join(raw_status.strip().lower().replace("-", " ").split())
    if value in {"в ожидании", "ожидание", "pending", "todo", "to do"}:
        return "В ожидании"
    if value in {"в работе", "работа", "in progress", "in_progress", "doing"}:
        return "В работе"
    if value in {"завершена", "завершено", "done", "completed", "готово"}:
        return "Завершена"
    if value in {"отклонена", "rejected", "declined", "cancelled by manager"}:
        return "Отклонена"
    if value in {"отозвана", "withdrawn", "canceled", "cancelled"}:
        return "Отозвана"
    return "В ожидании"


def _normalize_deadline_or_keep(raw: str) -> str:
    text = raw.strip()
    if not text:
        return ""
    from datetime import date

    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return text


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


def _normalize_alias(raw: str) -> str:
    return " ".join(raw.strip().lower().split())


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
