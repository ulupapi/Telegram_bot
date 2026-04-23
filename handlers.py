from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from datetime import timezone
from html import escape

from aiogram import Bot, Router
from aiogram.types import (
    CallbackQuery,
    ChatMemberUpdated,
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

BTN_SUMMARY = "📊 Получить сводку"
BTN_SAVED_TASKS = "📋 Показать сохраненные задачи"
BTN_EDIT_TASK = "✏️ Редактировать задачи"
BTN_HELP = "❓ Помощь"
BTN_DEV_ENTER = "🛠 Режим программиста"
BTN_DEV_BACK = "👤 Обычный режим"
BTN_DEV_CLEAR_SCOPE = "🧹 Очистить текущий контекст"
BTN_DEV_CLEAR_ALL = "🧨 Очистить всю БД"
BTN_DEV_SCHEDULE = "🕒 Параметры расписания"
BTN_DEV_WHERE = "📍 Текущий контекст"
BTN_EDIT_CANCEL = "Отмена"

DEV_CLEAR_ALL_CONFIRM_YES = "dev_clear_all|yes"
DEV_CLEAR_ALL_CONFIRM_NO = "dev_clear_all|no"
EDIT_TASK_PICK_PREFIX = "edit_task|"
EDIT_FIELD_PREFIX = "edit_field|"
EDIT_STATUS_PREFIX = "edit_status|"
EDIT_CANCEL_CALLBACK = "edit|cancel"

FIELD_TITLE = "title"
FIELD_DESCRIPTION = "description"
FIELD_DEADLINE = "deadline_date"
FIELD_AUTHOR = "author_name"
FIELD_ASSIGNEE = "assignee"
FIELD_STATUS = "status"

STATUS_TO_CODE = {
    "В ожидании": "w",
    "В работе": "p",
    "Завершена": "d",
    "Отклонена": "r",
    "Отозвана": "x",
}
CODE_TO_STATUS = {value: key for key, value in STATUS_TO_CODE.items()}

CONTROL_BUTTON_TEXTS = {
    BTN_SUMMARY,
    BTN_SAVED_TASKS,
    BTN_EDIT_TASK,
    BTN_HELP,
    BTN_DEV_ENTER,
    BTN_DEV_BACK,
    BTN_DEV_CLEAR_SCOPE,
    BTN_DEV_CLEAR_ALL,
    BTN_DEV_SCHEDULE,
    BTN_DEV_WHERE,
    BTN_EDIT_CANCEL,
}


def _all_control_button_texts() -> set[str]:
    return set(CONTROL_BUTTON_TEXTS)


# key: (chat_id, thread_id, user_id)
DEV_MODE_SCOPES: set[tuple[int, int, int]] = set()
EDIT_SELECTION_OPTIONS: dict[tuple[int, int, int], dict[str, str]] = {}
EDIT_TARGET_TASK: dict[tuple[int, int, int], str] = {}
EDIT_TARGET_SCOPES: dict[tuple[int, int, int], tuple[int, str]] = {}
PENDING_EDIT_TASK: dict[tuple[int, int, int], tuple[str, str, int]] = {}

# key: (chat_id, thread_id)
SUMMARY_MESSAGE_IDS: dict[tuple[int, int], list[int]] = {}


@dataclass(frozen=True)
class RenderedMessage:
    text: str
    parse_mode: str | None = None


async def build_and_publish_scope_summary(
    *,
    bot: Bot,
    db: Database,
    extractor: AIExtractor,
    chat_id: int,
    thread_id: int,
    context_messages_limit: int,
    replace_previous: bool = True,
) -> bool:
    rows = await asyncio.to_thread(
        db.get_recent_thread_messages,
        chat_id=chat_id,
        thread_id=thread_id,
        limit=context_messages_limit,
    )
    if not rows and thread_id:
        logger.info(
            "No messages in chat_id=%s thread_id=%s; trying whole chat fallback",
            chat_id,
            thread_id,
        )
        rows = await asyncio.to_thread(
            db.get_recent_chat_messages,
            chat_id=chat_id,
            limit=context_messages_limit,
        )
    if not rows:
        return False

    report = await asyncio.to_thread(extractor.extract_status, rows)
    await asyncio.to_thread(
        db.replace_tasks_for_scope,
        chat_id=chat_id,
        thread_id=thread_id,
        tasks=report.tasks,
    )
    await _publish_rendered_summary(
        bot=bot,
        chat_id=chat_id,
        thread_id=thread_id,
        messages=render_status_messages_safe(report),
        replace_previous=replace_previous,
    )
    return True


def render_status_messages_safe(report: StatusReport) -> list[RenderedMessage]:
    summary = _render_summary_message(report)
    if not report.tasks:
        return [RenderedMessage(text=summary)]

    tasks = _ordered_tasks(report)
    messages = [RenderedMessage(text=summary)]
    total = len(tasks)
    for idx, task in enumerate(tasks, start=1):
        messages.append(_render_task_message_html(task_index=idx, total=total, task=task))
    return messages


def render_saved_task_messages(tasks: list[TaskRecord]) -> list[RenderedMessage]:
    if not tasks:
        return [RenderedMessage(text="📋 Сохраненные задачи\n• Задачи не найдены.")]

    report = StatusReport(done=[], in_progress=[], blocked=[], tasks=tasks)
    ordered_tasks = _ordered_tasks(report)
    messages = [
        RenderedMessage(
            text=(
                "📋 Сохраненные задачи из БД\n"
                "LLM не вызывался, токены не тратились.\n"
                f"Всего задач: {len(ordered_tasks)}"
            )
        )
    ]
    total = len(ordered_tasks)
    for idx, task in enumerate(ordered_tasks, start=1):
        messages.append(_render_task_message_html(task_index=idx, total=total, task=task))
    return messages


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

    def is_scope_allowed_local(*, chat_id: int, thread_id: int) -> bool:
        if not strict_target_scope:
            return True
        if target_chat_id is None:
            return True
        if chat_id != target_chat_id:
            return False
        if target_topic_id is None:
            return True
        return thread_id == target_topic_id

    def scope_mismatch_text() -> str:
        if target_chat_id is None and target_topic_id is None:
            return ""
        if target_topic_id is None:
            return (
                "Этот бот привязан к другому чату. "
                f"Разрешенный TARGET_CHAT_ID={target_chat_id}."
            )
        return (
            "Этот бот привязан к другому чату/ветке. "
            f"Разрешены TARGET_CHAT_ID={target_chat_id}, TARGET_TOPIC_ID={target_topic_id}."
        )

    async def _send_home(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        user_id = _user_id_from_message(message)
        await message.answer(
            _startup_text(
                chat_id=chat_id,
                thread_id=thread_id,
                target_chat_id=target_chat_id,
                target_topic_id=target_topic_id,
                strict_target_scope=strict_target_scope,
                include_group_tip=message.chat.type in {"group", "supergroup"},
            ),
            reply_markup=_keyboard_for_user_scope(chat_id=chat_id, thread_id=thread_id, user_id=user_id),
        )

    async def _handle_status_request(message: Message, alias_raw: str = "") -> None:
        scope = await _resolve_scope_from_message_or_alias(
            message=message,
            db=db,
            target_chat_id=target_chat_id,
            target_topic_id=target_topic_id,
            strict_target_scope=strict_target_scope,
            alias_raw=alias_raw,
        )
        if scope is None:
            return
        scope_chat_id, scope_thread_id = scope

        try:
            sent = await build_and_publish_scope_summary(
                bot=message.bot,
                db=db,
                extractor=extractor,
                chat_id=scope_chat_id,
                thread_id=scope_thread_id,
                context_messages_limit=context_messages_limit,
                replace_previous=True,
            )
        except Exception as exc:
            logger.exception(
                "Failed to build status report for chat_id=%s thread_id=%s",
                scope_chat_id,
                scope_thread_id,
            )
            await message.answer(_humanize_llm_error(exc))
            return

        if not sent:
            await message.answer(
                _empty_context_hint(message=message, scope_thread_id=scope_thread_id)
            )
            return

    async def _handle_clear_scope(message: Message, arg_raw: str = "") -> None:
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
        await _delete_previous_summary_messages(
            bot=message.bot,
            chat_id=scope_chat_id,
            thread_id=scope_thread_id,
        )
        scope_suffix = f", topic_id={scope_thread_id}" if scope_thread_id else ""
        await message.answer(
            "🧹 Контекст очищен.\n"
            f"• Сообщений удалено: {deleted_messages}\n"
            f"• Задач удалено: {deleted_tasks}\n"
            f"• Контекст: chat_id={scope_chat_id}{scope_suffix}"
        )

    async def _show_saved_tasks(message: Message, alias_raw: str = "") -> None:
        scope = await _resolve_scope_from_message_or_alias(
            message=message,
            db=db,
            target_chat_id=target_chat_id,
            target_topic_id=target_topic_id,
            strict_target_scope=strict_target_scope,
            alias_raw=alias_raw,
        )
        if scope is None:
            return
        scope_chat_id, scope_thread_id = scope

        try:
            tasks = await asyncio.to_thread(
                db.get_tasks_for_scope,
                chat_id=scope_chat_id,
                thread_id=scope_thread_id,
            )
        except Exception:
            logger.exception(
                "Failed to load saved tasks for chat_id=%s thread_id=%s",
                scope_chat_id,
                scope_thread_id,
            )
            await message.answer("Не удалось прочитать сохраненные задачи из БД.")
            return

        if not tasks:
            await message.answer(
                "В БД пока нет сохраненных задач для этого контекста.\n"
                "Сначала нажмите «📊 Получить сводку», чтобы LLM один раз выделил задачи."
            )
            return

        messages = render_saved_task_messages(tasks)
        for item in messages:
            await message.answer(item.text, parse_mode=item.parse_mode)

    async def _resolve_edit_task_from_token(
        *,
        chat_id: int,
        thread_id: int,
        user_id: int,
        token: str,
    ) -> str | None:
        key = (chat_id, thread_id, user_id)
        options = EDIT_SELECTION_OPTIONS.get(key) or {}
        external_id = options.get(token)
        if external_id:
            return external_id

        try:
            task_index = int(token)
        except ValueError:
            return None
        if task_index < 1:
            return None

        try:
            tasks = await asyncio.to_thread(
                db.get_tasks_for_scope,
                chat_id=chat_id,
                thread_id=thread_id,
            )
        except Exception:
            logger.exception(
                "Failed to reload edit task list for chat_id=%s thread_id=%s",
                chat_id,
                thread_id,
            )
            return None

        ordered = sorted(tasks, key=_task_sort_key)
        if task_index > len(ordered):
            return None
        return ordered[task_index - 1].external_id

    async def _show_help(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        user_id = _user_id_from_message(message)
        await message.answer(
            "Команды:\n"
            "• /status [алиас] — получить сводку через LLM\n"
            "• /tasks [алиас] — показать сохраненные задачи без LLM\n"
            "• /bind <алиас> — привязать имя к текущему чату/ветке\n"
            "• /edit — открыть режим редактирования задач\n"
            "• /where — показать текущий chat_id/topic_id\n"
            "• /health — диагностика доступа в текущем чате\n"
            "• /clear_db [алиас] — очистка текущего/указанного контекста\n"
            "• /clear_db all — очистка всей базы\n\n"
            "Кнопки главного меню:\n"
            f"• {BTN_SUMMARY} — запрос сводки у LLM и вывод задач\n"
            f"• {BTN_SAVED_TASKS} — показать уже сохраненные задачи без LLM и без токенов\n"
            f"• {BTN_EDIT_TASK} — выбор задачи и ручное редактирование\n"
            f"• {BTN_HELP} — эта справка\n"
            f"• {BTN_DEV_ENTER} — расширенные операции (очистка/диагностика/расписание)",
            reply_markup=_keyboard_for_user_scope(chat_id=chat_id, thread_id=thread_id, user_id=user_id),
        )

    async def _show_where(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        mode = (
            "Режим области: STRICT_TARGET_SCOPE=on (сбор только из фиксированной цели)."
            if strict_target_scope and target_chat_id is not None
            else "Режим области: multi-chat (текущий чат/ветка)."
        )
        if thread_id:
            await message.answer(
                f"Текущий контекст: chat_id={chat_id}, topic_id={thread_id}\n"
                "Можно сохранить имя: /bind Задания\n"
                f"{mode}"
            )
            return
        await message.answer(
            f"Текущий контекст: chat_id={chat_id} (обычный чат)\n"
            "Можно сохранить имя: /bind Задания\n"
            f"{mode}"
        )

    async def _show_schedule_info(message: Message) -> None:
        enabled = os.getenv("SCHEDULE_ENABLED", "0")
        morning = os.getenv("SUMMARY_MORNING_TIME", "09:00")
        evening = os.getenv("SUMMARY_EVENING_TIME", "18:00")
        timezone_name = (
            os.getenv("SCHEDULE_TIMEZONE")
            or os.getenv("TIMEZONE")
            or os.getenv("TZ")
            or "Europe/Moscow"
        )
        await message.answer(
            "🕒 Параметры расписания\n"
            f"• SCHEDULE_ENABLED={enabled}\n"
            f"• SUMMARY_MORNING_TIME={morning}\n"
            f"• SUMMARY_EVENING_TIME={evening}\n"
            f"• TIMEZONE={timezone_name}\n\n"
            "Если включено, бот отправляет сводку дважды в день для известных контекстов."
        )

    async def _start_edit_mode(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        if not is_scope_allowed_local(chat_id=chat_id, thread_id=thread_id):
            mismatch = scope_mismatch_text()
            if mismatch:
                await message.answer(mismatch)
            return
        user_id = _user_id_from_message(message)
        key = (chat_id, thread_id, user_id)

        try:
            tasks = await asyncio.to_thread(
                db.get_tasks_for_scope,
                chat_id=chat_id,
                thread_id=thread_id,
            )
        except Exception:
            logger.exception(
                "Failed to load tasks for edit mode chat_id=%s thread_id=%s",
                chat_id,
                thread_id,
            )
            await message.answer("Не удалось загрузить задачи из БД. Попробуйте еще раз.")
            return

        if not tasks:
            await message.answer(
                "В этом контексте пока нет задач для редактирования. Сначала нажмите «Получить сводку»."
            )
            return

        ordered = sorted(tasks, key=_task_sort_key)
        selection: dict[str, str] = {}
        buttons: list[list[InlineKeyboardButton]] = []
        max_buttons = 30
        for idx, task in enumerate(ordered[:max_buttons], start=1):
            token = str(idx)
            selection[token] = task.external_id
            title = _trim_text(task.title, limit=40) or "Без названия"
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=f"{idx}. {title}",
                        callback_data=f"{EDIT_TASK_PICK_PREFIX}{token}|{thread_id}",
                    )
                ]
            )
        buttons.append(
            [InlineKeyboardButton(text="Отмена", callback_data=EDIT_CANCEL_CALLBACK)]
        )

        EDIT_SELECTION_OPTIONS[key] = selection
        EDIT_TARGET_TASK.pop(key, None)
        EDIT_TARGET_SCOPES.pop(key, None)
        PENDING_EDIT_TASK.pop(key, None)

        note = ""
        if len(ordered) > max_buttons:
            note = f"\nПоказаны первые {max_buttons} задач из {len(ordered)}."

        await message.answer(
            "Выберите задачу для редактирования:" + note,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )

    @router.my_chat_member()
    async def on_bot_added(event: ChatMemberUpdated) -> None:
        if not _is_bot_connected_to_chat(event):
            return
        if event.chat.type not in {"group", "supergroup"}:
            return
        chat_id = event.chat.id
        thread_id = 0
        if not is_scope_allowed_local(chat_id=chat_id, thread_id=thread_id):
            mismatch = scope_mismatch_text()
            if mismatch:
                await event.bot.send_message(chat_id=chat_id, text=mismatch)
            return

        await event.bot.send_message(
            chat_id=chat_id,
            text=_startup_text(
                chat_id=chat_id,
                thread_id=thread_id,
                target_chat_id=target_chat_id,
                target_topic_id=target_topic_id,
                strict_target_scope=strict_target_scope,
                include_group_tip=True,
            ),
            reply_markup=_keyboard_for_user_scope(chat_id=chat_id, thread_id=thread_id, user_id=0),
        )

    @router.message(CommandFilter("start"))
    async def on_start(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        if not is_scope_allowed_local(chat_id=chat_id, thread_id=thread_id):
            mismatch = scope_mismatch_text()
            if mismatch:
                await message.answer(mismatch)
            return
        await _send_home(message)

    @router.message(CommandFilter("bind"))
    async def cmd_bind(message: Message) -> None:
        alias_raw = _command_argument(message)
        if not alias_raw:
            await message.answer("Использование: /bind <название>, например: /bind Задания")
            return

        chat_id, thread_id = _scope_from_message(message)
        if not is_scope_allowed_local(chat_id=chat_id, thread_id=thread_id):
            mismatch = scope_mismatch_text()
            if mismatch:
                await message.answer(mismatch)
            return

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
        await _show_where(message)

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
            if chat_id != target_chat_id:
                lines.append("• Внимание: этот чат не совпадает с TARGET_CHAT_ID, обычные сообщения будут игнорироваться.")
            elif target_topic_id is not None and thread_id != target_topic_id:
                lines.append("• Внимание: эта ветка не совпадает с TARGET_TOPIC_ID, обычные сообщения будут игнорироваться.")
        else:
            lines.append("• Scope режим: multi-chat (текущий чат/ветка)")

        try:
            thread_message_count = await asyncio.to_thread(
                db.count_thread_messages,
                chat_id=chat_id,
                thread_id=thread_id,
            )
            chat_message_count = await asyncio.to_thread(
                db.count_chat_messages,
                chat_id=chat_id,
            )
            lines.append(f"• Сообщений в текущей ветке в БД: {thread_message_count}")
            lines.append(f"• Сообщений во всем чате в БД: {chat_message_count}")
        except Exception:
            logger.exception(
                "Failed to count stored messages for chat_id=%s thread_id=%s",
                chat_id,
                thread_id,
            )
            lines.append("• Сообщения в БД: не удалось проверить.")

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
                lines.append(f"• Может отправлять сообщения: {_can_send_messages_label(member)}")
                lines.append(
                    "• Может удалять старые сообщения сводки: "
                    f"{_permission_label(getattr(member, 'can_delete_messages', None))}"
                )
                lines.append(
                    "• Может работать с топиками: "
                    f"{_permission_label(getattr(member, 'can_manage_topics', None))}"
                )
            except Exception:
                lines.append("• Статус в группе: не удалось получить.")
            lines.extend(
                [
                    "",
                    "Если эта команда отвечает в группе, значит бот получает message updates.",
                    "Если обычные сообщения не попадают в контекст, почти всегда включен Privacy Mode или бот не администратор.",
                    "Чтобы бот стабильно собирал контекст в группе:",
                    "1. В BotFather отключите Privacy Mode: /setprivacy -> Disable.",
                    "2. Выдайте боту права администратора в группе (рекомендуется).",
                    "3. Напишите 2-3 обычных сообщения и вызовите /status.",
                ]
            )

        await message.answer("\n".join(lines))

    @router.message(CommandFilter("help"))
    async def cmd_help(message: Message) -> None:
        await _show_help(message)

    @router.message(CommandFilter("edit"))
    async def cmd_edit(message: Message) -> None:
        await _start_edit_mode(message)

    @router.message(CommandFilter("tasks"))
    async def cmd_tasks(message: Message) -> None:
        await _show_saved_tasks(message, _command_argument(message))

    @router.message(CommandFilter("status"))
    async def cmd_status(message: Message) -> None:
        await _handle_status_request(message, _command_argument(message))

    @router.message(CommandFilter(commands=["clear_db", "clear"]))
    async def cmd_clear_db(message: Message) -> None:
        arg_raw = _command_argument(message)
        arg_clean = _normalize_alias(arg_raw) if arg_raw else ""
        if arg_clean in {"all", "все"}:
            deleted_messages, deleted_tasks, deleted_aliases = await asyncio.to_thread(db.clear_all)
            SUMMARY_MESSAGE_IDS.clear()
            await message.answer(
                "🧹 База очищена полностью.\n"
                f"• Сообщений удалено: {deleted_messages}\n"
                f"• Задач удалено: {deleted_tasks}\n"
                f"• Привязок удалено: {deleted_aliases}"
            )
            return
        await _handle_clear_scope(message, arg_raw)

    @router.message(lambda m: ((m.text or "").strip() == BTN_SUMMARY))
    async def btn_summary(message: Message) -> None:
        await _handle_status_request(message)

    @router.message(lambda m: ((m.text or "").strip() == BTN_SAVED_TASKS))
    async def btn_saved_tasks(message: Message) -> None:
        await _show_saved_tasks(message)

    @router.message(lambda m: ((m.text or "").strip() == BTN_HELP))
    async def btn_help(message: Message) -> None:
        await _show_help(message)

    @router.message(lambda m: ((m.text or "").strip() == BTN_EDIT_TASK))
    async def btn_edit(message: Message) -> None:
        await _start_edit_mode(message)

    @router.message(lambda m: ((m.text or "").strip() == BTN_DEV_ENTER))
    async def btn_dev_enter(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        user_id = _user_id_from_message(message)
        key = (chat_id, thread_id, user_id)
        DEV_MODE_SCOPES.add(key)
        await message.answer(
            "Режим программиста включен.",
            reply_markup=_keyboard_for_user_scope(chat_id=chat_id, thread_id=thread_id, user_id=user_id),
        )

    @router.message(lambda m: ((m.text or "").strip() == BTN_DEV_BACK))
    async def btn_dev_back(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        user_id = _user_id_from_message(message)
        key = (chat_id, thread_id, user_id)
        DEV_MODE_SCOPES.discard(key)
        EDIT_SELECTION_OPTIONS.pop(key, None)
        EDIT_TARGET_TASK.pop(key, None)
        EDIT_TARGET_SCOPES.pop(key, None)
        PENDING_EDIT_TASK.pop(key, None)
        await message.answer(
            "Режим программиста выключен.",
            reply_markup=_keyboard_for_user_scope(chat_id=chat_id, thread_id=thread_id, user_id=user_id),
        )

    @router.message(lambda m: ((m.text or "").strip() == BTN_DEV_WHERE))
    async def btn_dev_where(message: Message) -> None:
        await _show_where(message)

    @router.message(lambda m: ((m.text or "").strip() == BTN_DEV_SCHEDULE))
    async def btn_dev_schedule(message: Message) -> None:
        await _show_schedule_info(message)

    @router.message(lambda m: ((m.text or "").strip() == BTN_DEV_CLEAR_SCOPE))
    async def btn_dev_clear_scope(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        user_id = _user_id_from_message(message)
        if (chat_id, thread_id, user_id) not in DEV_MODE_SCOPES:
            await message.answer("Сначала войдите в режим программиста.")
            return
        await _handle_clear_scope(message)

    @router.message(lambda m: ((m.text or "").strip() == BTN_DEV_CLEAR_ALL))
    async def btn_dev_clear_all(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        user_id = _user_id_from_message(message)
        if (chat_id, thread_id, user_id) not in DEV_MODE_SCOPES:
            await message.answer("Сначала войдите в режим программиста.")
            return
        await message.answer(
            "Подтвердите полную очистку БД:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="Да, очистить", callback_data=DEV_CLEAR_ALL_CONFIRM_YES),
                        InlineKeyboardButton(text="Отмена", callback_data=DEV_CLEAR_ALL_CONFIRM_NO),
                    ]
                ]
            ),
        )

    @router.callback_query(lambda c: (c.data or "") in {DEV_CLEAR_ALL_CONFIRM_YES, DEV_CLEAR_ALL_CONFIRM_NO})
    async def cb_clear_all_confirm(callback: CallbackQuery) -> None:
        await callback.answer()
        message = callback.message
        if message is None:
            return
        if callback.data == DEV_CLEAR_ALL_CONFIRM_NO:
            await message.answer("Очистка отменена.")
            return

        deleted_messages, deleted_tasks, deleted_aliases = await asyncio.to_thread(db.clear_all)
        SUMMARY_MESSAGE_IDS.clear()
        await message.answer(
            "🧨 База очищена полностью.\n"
            f"• Сообщений удалено: {deleted_messages}\n"
            f"• Задач удалено: {deleted_tasks}\n"
            f"• Привязок удалено: {deleted_aliases}"
        )

    @router.callback_query(lambda c: (c.data or "").startswith(EDIT_TASK_PICK_PREFIX))
    async def cb_edit_pick_task(callback: CallbackQuery) -> None:
        await callback.answer()
        message = callback.message
        if message is None or callback.from_user is None:
            return

        chat_id = message.chat.id
        callback_thread_id = message.message_thread_id or 0
        user_id = callback.from_user.id
        payload = (callback.data or "")[len(EDIT_TASK_PICK_PREFIX) :]
        token, scope_thread_id = _parse_edit_payload_thread(
            payload=payload,
            fallback_thread_id=callback_thread_id,
        )
        key = (chat_id, scope_thread_id, user_id)

        external_id = await _resolve_edit_task_from_token(
            chat_id=chat_id,
            thread_id=scope_thread_id,
            user_id=user_id,
            token=token,
        )
        if not external_id:
            await message.answer("Список задач устарел. Нажмите «Редактировать задачи» еще раз.")
            return

        EDIT_TARGET_TASK[key] = external_id
        EDIT_TARGET_SCOPES[key] = (scope_thread_id, external_id)
        callback_key = (chat_id, callback_thread_id, user_id)
        EDIT_TARGET_TASK[callback_key] = external_id
        EDIT_TARGET_SCOPES[callback_key] = (scope_thread_id, external_id)
        PENDING_EDIT_TASK.pop(key, None)

        await message.answer(
            "Выберите поле для редактирования:",
            reply_markup=_edit_fields_keyboard(scope_thread_id=scope_thread_id),
        )

    @router.callback_query(lambda c: (c.data or "").startswith(EDIT_FIELD_PREFIX))
    async def cb_edit_pick_field(callback: CallbackQuery) -> None:
        await callback.answer()
        message = callback.message
        if message is None or callback.from_user is None:
            return

        chat_id = message.chat.id
        callback_thread_id = message.message_thread_id or 0
        user_id = callback.from_user.id
        payload = (callback.data or "")[len(EDIT_FIELD_PREFIX) :]
        field, scope_thread_id = _parse_edit_payload_thread(
            payload=payload,
            fallback_thread_id=callback_thread_id,
        )
        key = (chat_id, scope_thread_id, user_id)

        target = EDIT_TARGET_SCOPES.get(key) or EDIT_TARGET_SCOPES.get(
            (chat_id, callback_thread_id, user_id)
        )
        external_id = target[1] if target else EDIT_TARGET_TASK.get(key)
        if not external_id:
            await message.answer("Сначала выберите задачу через «Редактировать задачи».")
            return

        if field == FIELD_STATUS:
            await message.answer(
                "Выберите новый статус:",
                reply_markup=_edit_status_keyboard(scope_thread_id=scope_thread_id),
            )
            return

        if field not in {FIELD_TITLE, FIELD_DESCRIPTION, FIELD_DEADLINE, FIELD_AUTHOR, FIELD_ASSIGNEE}:
            await message.answer("Неизвестное поле редактирования.")
            return

        PENDING_EDIT_TASK[key] = (external_id, field, scope_thread_id)
        PENDING_EDIT_TASK[(chat_id, callback_thread_id, user_id)] = (
            external_id,
            field,
            scope_thread_id,
        )
        prompt = {
            FIELD_TITLE: "Введите новое название задачи:",
            FIELD_DESCRIPTION: "Введите новое описание задачи:",
            FIELD_DEADLINE: "Введите дедлайн в формате YYYY-MM-DD (или '-' чтобы очистить):",
            FIELD_AUTHOR: "Введите автора задачи:",
            FIELD_ASSIGNEE: "Введите исполнителя задачи:",
        }[field]
        await message.answer(prompt)

    @router.callback_query(lambda c: (c.data or "").startswith(EDIT_STATUS_PREFIX))
    async def cb_edit_status(callback: CallbackQuery) -> None:
        await callback.answer()
        message = callback.message
        if message is None or callback.from_user is None:
            return

        chat_id = message.chat.id
        callback_thread_id = message.message_thread_id or 0
        user_id = callback.from_user.id
        payload = (callback.data or "")[len(EDIT_STATUS_PREFIX) :]
        code, scope_thread_id = _parse_edit_payload_thread(
            payload=payload,
            fallback_thread_id=callback_thread_id,
        )
        key = (chat_id, scope_thread_id, user_id)

        target = EDIT_TARGET_SCOPES.get(key) or EDIT_TARGET_SCOPES.get(
            (chat_id, callback_thread_id, user_id)
        )
        external_id = target[1] if target else EDIT_TARGET_TASK.get(key)
        if not external_id:
            await message.answer("Сначала выберите задачу через «Редактировать задачи».")
            return

        status = CODE_TO_STATUS.get(code)
        if status is None:
            await message.answer("Неизвестный статус.")
            return

        try:
            updated = await asyncio.to_thread(
                db.update_task_for_scope,
                chat_id=chat_id,
                thread_id=scope_thread_id,
                external_id=external_id,
                status=status,
            )
        except Exception:
            logger.exception(
                "Failed to update status for chat_id=%s thread_id=%s external_id=%s",
                chat_id,
                scope_thread_id,
                external_id,
            )
            await message.answer("Не удалось обновить задачу в БД.")
            return

        if not updated:
            await message.answer("Задача не найдена или не обновилась. Обновите список задач.")
            return

        await message.answer(f"Статус обновлен: {_status_icon(status)} {status}")

    @router.callback_query(lambda c: (c.data or "") == EDIT_CANCEL_CALLBACK)
    async def cb_edit_cancel(callback: CallbackQuery) -> None:
        await callback.answer()
        message = callback.message
        if message is None or callback.from_user is None:
            return
        key = (message.chat.id, message.message_thread_id or 0, callback.from_user.id)
        EDIT_SELECTION_OPTIONS.pop(key, None)
        EDIT_TARGET_TASK.pop(key, None)
        EDIT_TARGET_SCOPES.pop(key, None)
        PENDING_EDIT_TASK.pop(key, None)
        await message.answer("Редактирование отменено.")

    @router.message(lambda m: _has_pending_edit(m))
    async def on_pending_edit_value(message: Message) -> None:
        chat_id, thread_id = _scope_from_message(message)
        user_id = _user_id_from_message(message)
        key = (chat_id, thread_id, user_id)
        pending = PENDING_EDIT_TASK.get(key)
        if pending is None:
            pending = _find_pending_edit_for_user(chat_id=chat_id, user_id=user_id)
        if pending is None:
            return

        text = (message.text or "").strip()
        if not text:
            await message.answer("Нужен текст для обновления поля.")
            return
        if text == BTN_EDIT_CANCEL or text.lower() in {"/cancel", "cancel"}:
            PENDING_EDIT_TASK.pop(key, None)
            await message.answer("Редактирование отменено.")
            return

        external_id, field, scope_thread_id = pending
        update_kwargs: dict[str, str] = {}
        if field == FIELD_DEADLINE:
            normalized = _normalize_deadline_input(text)
            if normalized is None:
                await message.answer("Неверный формат даты. Используйте YYYY-MM-DD или '-'.")
                return
            update_kwargs[field] = normalized
        else:
            update_kwargs[field] = text

        try:
            updated = await asyncio.to_thread(
                db.update_task_for_scope,
                chat_id=chat_id,
                thread_id=scope_thread_id,
                external_id=external_id,
                **update_kwargs,
            )
        except Exception:
            logger.exception(
                "Failed to update task field %s for chat_id=%s thread_id=%s external_id=%s",
                field,
                chat_id,
                scope_thread_id,
                external_id,
            )
            await message.answer("Не удалось обновить задачу в БД.")
            return

        if not updated:
            await message.answer("Задача не найдена или не обновилась. Обновите список задач.")
            return

        PENDING_EDIT_TASK.pop(key, None)
        if scope_thread_id != thread_id:
            PENDING_EDIT_TASK.pop((chat_id, scope_thread_id, user_id), None)
        field_label = {
            FIELD_TITLE: "Название",
            FIELD_DESCRIPTION: "Описание",
            FIELD_DEADLINE: "Дедлайн",
            FIELD_AUTHOR: "Автор",
            FIELD_ASSIGNEE: "Исполнитель",
        }.get(field, "Поле")
        await message.answer(f"{field_label} обновлено.")

    @router.message()
    async def collect_messages(message: Message) -> None:
        text = (message.text or message.caption or "").strip()
        if not text:
            return
        if text.startswith("/"):
            return
        if text in _all_control_button_texts():
            return

        if message.from_user and message.from_user.is_bot:
            return

        chat_id, thread_id = _scope_from_message(message)
        if not is_scope_allowed_local(chat_id=chat_id, thread_id=thread_id):
            return

        author = _telegram_author(message)
        created_at = message.date.replace(tzinfo=timezone.utc).isoformat()

        try:
            await asyncio.to_thread(
                db.save_message,
                chat_id=chat_id,
                thread_id=thread_id,
                message_id=message.message_id,
                user_name=author,
                text=text,
                created_at=created_at,
            )
        except Exception:
            logger.exception(
                "Failed to save incoming message for chat_id=%s thread_id=%s",
                chat_id,
                thread_id,
            )
            return

        await _learn_auto_aliases(
            db=db,
            message=message,
            chat_id=chat_id,
            thread_id=thread_id,
        )

    return router


async def _publish_rendered_summary(
    *,
    bot: Bot,
    chat_id: int,
    thread_id: int,
    messages: list[RenderedMessage],
    replace_previous: bool,
) -> None:
    if replace_previous:
        await _delete_previous_summary_messages(bot=bot, chat_id=chat_id, thread_id=thread_id)

    sent_ids: list[int] = []
    for item in messages:
        sent = await bot.send_message(
            chat_id=chat_id,
            message_thread_id=thread_id or None,
            text=item.text,
            parse_mode=item.parse_mode,
        )
        sent_ids.append(sent.message_id)
    SUMMARY_MESSAGE_IDS[(chat_id, thread_id)] = sent_ids


async def _delete_previous_summary_messages(*, bot: Bot, chat_id: int, thread_id: int) -> None:
    key = (chat_id, thread_id)
    old_ids = SUMMARY_MESSAGE_IDS.pop(key, [])
    for message_id in old_ids:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            # Message may be manually deleted or too old for deletion.
            continue


def _startup_text(
    *,
    chat_id: int,
    thread_id: int,
    target_chat_id: int | None,
    target_topic_id: int | None,
    strict_target_scope: bool,
    include_group_tip: bool,
) -> str:
    scope = f"chat_id={chat_id}, topic_id={thread_id}" if thread_id else f"chat_id={chat_id}"
    lines = [
        "Привет! Я бот задач.",
        f"Текущий контекст: {scope}",
    ]

    if strict_target_scope and target_chat_id is not None:
        suffix = f", topic_id={target_topic_id}" if target_topic_id else ""
        lines.append(f"Scope режим: strict (фиксированная цель chat_id={target_chat_id}{suffix}).")
    else:
        lines.append("Scope режим: multi-chat (текущий чат/ветка).")

    lines.extend(
        [
            "",
            "Основные действия:",
            f"• {BTN_SUMMARY}",
            f"• {BTN_SAVED_TASKS}",
            f"• {BTN_EDIT_TASK}",
            f"• {BTN_HELP}",
            f"• {BTN_DEV_ENTER}",
        ]
    )

    if include_group_tip:
        lines.extend(
            [
                "",
                "Для сбора контекста в группе отключите Privacy Mode в BotFather: /setprivacy -> Disable.",
            ]
        )

    return "\n".join(lines)


def _keyboard_for_user_scope(*, chat_id: int, thread_id: int, user_id: int) -> ReplyKeyboardMarkup:
    key = (chat_id, thread_id, user_id)
    if key in DEV_MODE_SCOPES:
        rows = [
            [KeyboardButton(text=BTN_SUMMARY)],
            [KeyboardButton(text=BTN_SAVED_TASKS), KeyboardButton(text=BTN_EDIT_TASK)],
            [KeyboardButton(text=BTN_DEV_CLEAR_SCOPE), KeyboardButton(text=BTN_DEV_CLEAR_ALL)],
            [KeyboardButton(text=BTN_DEV_WHERE), KeyboardButton(text=BTN_DEV_SCHEDULE)],
            [KeyboardButton(text=BTN_HELP), KeyboardButton(text=BTN_DEV_BACK)],
        ]
    else:
        rows = [
            [KeyboardButton(text=BTN_SUMMARY)],
            [KeyboardButton(text=BTN_SAVED_TASKS), KeyboardButton(text=BTN_EDIT_TASK)],
            [KeyboardButton(text=BTN_HELP), KeyboardButton(text=BTN_DEV_ENTER)],
        ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def _edit_fields_keyboard(*, scope_thread_id: int = 0) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Название", callback_data=f"{EDIT_FIELD_PREFIX}{FIELD_TITLE}|{scope_thread_id}"),
                InlineKeyboardButton(text="Описание", callback_data=f"{EDIT_FIELD_PREFIX}{FIELD_DESCRIPTION}|{scope_thread_id}"),
            ],
            [
                InlineKeyboardButton(text="Дедлайн", callback_data=f"{EDIT_FIELD_PREFIX}{FIELD_DEADLINE}|{scope_thread_id}"),
                InlineKeyboardButton(text="Автор", callback_data=f"{EDIT_FIELD_PREFIX}{FIELD_AUTHOR}|{scope_thread_id}"),
            ],
            [
                InlineKeyboardButton(text="Исполнитель", callback_data=f"{EDIT_FIELD_PREFIX}{FIELD_ASSIGNEE}|{scope_thread_id}"),
                InlineKeyboardButton(text="Статус", callback_data=f"{EDIT_FIELD_PREFIX}{FIELD_STATUS}|{scope_thread_id}"),
            ],
            [InlineKeyboardButton(text="Отмена", callback_data=EDIT_CANCEL_CALLBACK)],
        ]
    )


def _edit_status_keyboard(*, scope_thread_id: int = 0) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for status in STATUS_ORDER:
        code = STATUS_TO_CODE[status]
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{_status_icon(status)} {status}",
                    callback_data=f"{EDIT_STATUS_PREFIX}{code}|{scope_thread_id}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="Отмена", callback_data=EDIT_CANCEL_CALLBACK)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _render_summary_message(report: StatusReport) -> str:
    lines = ["📌 Сводка по ветке"]
    lines.extend(_render_section("✅ Что сделано", report.done, "Новых завершенных задач нет."))
    lines.extend(_render_section("🛠 Что в работе", report.in_progress, "Активных задач не найдено."))
    lines.extend(_render_section("⛔ Что зависло", report.blocked, "Блокеров не найдено."))
    lines.append("\n📋 Статусы задач:")

    if not report.tasks:
        lines.append("• Задач нет.")
    else:
        for status in STATUS_ORDER:
            count = sum(1 for task in report.tasks if task.status == status)
            if count:
                lines.append(f"• {_status_icon(status)} {status}: {count}")
    return "\n".join(lines)


def _render_task_message_html(*, task_index: int, total: int, task: TaskRecord) -> RenderedMessage:
    description = _trim_text(task.description, limit=1300) or "Не указано"
    description_html = escape(description).replace("\n", "<br>")

    lines = [
        f"🧩 <b>Задача {task_index} из {total}</b>",
        f"📅 Дедлайн: <b>{escape(task.deadline_date or '—')}</b>",
        "🌐 <b>Основная информация:</b>",
        f"1. Название: {escape(task.title)}",
        f"2. Автор: {escape(task.author_name)}",
        f"3. Исполнитель: {escape(task.assignee)}",
        f"4. Статус: {_status_icon(task.status)} {escape(task.status)}",
        f"5. ID: {escape(task.external_id)}",
        "6. Описание:",
        f"<blockquote>{description_html}</blockquote>",
    ]
    return RenderedMessage(text="\n".join(lines), parse_mode="HTML")


def _ordered_tasks(report: StatusReport) -> list[TaskRecord]:
    tasks = list(getattr(report, "tasks", []) or [])
    ordered: list[TaskRecord] = []
    for status in STATUS_ORDER:
        status_tasks = [task for task in tasks if task.status == status]
        ordered.extend(status_tasks)
    ordered_ids = {id(task) for task in ordered}
    ordered.extend(task for task in tasks if id(task) not in ordered_ids)
    return ordered


def _task_sort_key(task: TaskRecord) -> tuple[int, str]:
    try:
        order = STATUS_ORDER.index(task.status)
    except ValueError:
        order = len(STATUS_ORDER)
    return order, task.title.lower()


def _parse_edit_payload_thread(*, payload: str, fallback_thread_id: int) -> tuple[str, int]:
    token, separator, raw_thread_id = payload.partition("|")
    if not separator:
        return token, fallback_thread_id
    try:
        return token, int(raw_thread_id)
    except ValueError:
        return token, fallback_thread_id


def _find_pending_edit_for_user(*, chat_id: int, user_id: int) -> tuple[str, str, int] | None:
    matches = [
        pending
        for key, pending in PENDING_EDIT_TASK.items()
        if key[0] == chat_id and key[2] == user_id
    ]
    if len(matches) == 1:
        return matches[0]
    return None


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


def _permission_label(value: object) -> str:
    if value is True:
        return "да"
    if value is False:
        return "нет"
    return "не указано Telegram API"


def _can_send_messages_label(member: object) -> str:
    status = getattr(member, "status", "")
    if status in {"creator", "administrator"}:
        return "да"
    can_send = getattr(member, "can_send_messages", None)
    if can_send is False:
        return "нет"
    return "да"


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


def _scope_key(*, chat_id: int, thread_id: int) -> tuple[int, int]:
    return chat_id, thread_id


def _user_id_from_message(message: Message) -> int:
    if message.from_user is None:
        return 0
    return message.from_user.id


def _command_argument(message: Message) -> str:
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return ""
    return parts[1].strip()


def _normalize_alias(raw: str) -> str:
    return " ".join(raw.strip().lower().split())


def _normalize_deadline_input(raw: str) -> str | None:
    value = raw.strip()
    if value in {"", "-", "—", "none", "null", "без дедлайна"}:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return value
    return None


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
            try:
                await asyncio.to_thread(
                    db.learn_scope_alias,
                    alias=_normalize_alias(chat_title),
                    chat_id=chat_id,
                    thread_id=0,
                )
            except Exception:
                logger.exception(
                    "Failed to learn chat alias for chat_id=%s title=%r",
                    chat_id,
                    chat_title,
                )
        return

    topic_name = _extract_topic_name(message)
    if topic_name:
        try:
            await asyncio.to_thread(
                db.learn_scope_alias,
                alias=_normalize_alias(topic_name),
                chat_id=chat_id,
                thread_id=thread_id,
            )
        except Exception:
            logger.exception(
                "Failed to learn topic alias for chat_id=%s thread_id=%s topic=%r",
                chat_id,
                thread_id,
                topic_name,
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
    try:
        await _learn_auto_aliases(
            db=db,
            message=message,
            chat_id=current_chat_id,
            thread_id=current_thread_id,
        )
    except Exception:
        logger.exception(
            "Unexpected alias learning failure for chat_id=%s thread_id=%s",
            current_chat_id,
            current_thread_id,
        )

    if alias_raw:
        alias = _normalize_alias(alias_raw)
        try:
            resolved = await asyncio.to_thread(db.resolve_scope_alias, alias=alias)
        except Exception:
            logger.exception(
                "Failed to resolve alias %r in DB for chat_id=%s thread_id=%s",
                alias,
                current_chat_id,
                current_thread_id,
            )
            await message.answer(
                "Временная ошибка доступа к базе данных. Попробуйте повторить через 5-10 секунд."
            )
            return None
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


def _has_pending_edit(message: Message) -> bool:
    if message.from_user is None:
        return False
    text = (message.text or "").strip()
    if text.startswith("/") and text.lower() not in {"/cancel"}:
        return False
    key = (message.chat.id, message.message_thread_id or 0, message.from_user.id)
    if key in PENDING_EDIT_TASK:
        return True
    return _find_pending_edit_for_user(chat_id=message.chat.id, user_id=message.from_user.id) is not None


def _is_bot_connected_to_chat(event: ChatMemberUpdated) -> bool:
    old_status = getattr(event.old_chat_member, "status", None)
    new_status = getattr(event.new_chat_member, "status", None)
    if new_status in {"left", "kicked"}:
        return False
    return old_status in {"left", "kicked"} and new_status in {"member", "administrator"}


def _humanize_llm_error(exc: Exception) -> str:
    text = str(exc).lower()
    if (
        "you exceeded your current quota" in text
        or "platform.openai.com/docs/guides/error-codes/api-errors" in text
    ):
        return (
            "Ошибка пришла от OpenAI (quota exceeded). "
            "Это означает, что запрос ушел в OpenAI API. "
            "Если вы хотите использовать Amvera, проверьте переменные окружения: "
            "LLM_PROVIDER=amvera, AMVERA_LLM_API_KEY, AMVERA_LLM_BASE_URL, AMVERA_LLM_MODEL."
        )
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
    if "operationalerror" in text or "ssl error" in text:
        return "Временная ошибка соединения с БД. Повторите запрос через несколько секунд."
    return "Не удалось получить сводку от LLM. Попробуйте чуть позже."
