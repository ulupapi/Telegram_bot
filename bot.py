from __future__ import annotations

import asyncio
import builtins
import contextlib
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher
from dotenv import load_dotenv

from ai_extractor import AIExtractor
from database import Database


def _install_runtime_compat_shims() -> None:
    logger = logging.getLogger(__name__)
    installed: list[str] = []

    # Backward compatibility for drifted handlers that reference F/Command
    # without explicit imports in module scope.
    if not hasattr(builtins, "F"):
        try:
            from aiogram import F as FilterF  # type: ignore
        except Exception:
            try:
                from magic_filter import F as FilterF  # type: ignore
            except Exception:
                FilterF = None  # type: ignore
        if FilterF is not None:
            setattr(builtins, "F", FilterF)
            installed.append("F")

    if not hasattr(builtins, "Command"):
        try:
            from aiogram.filters import Command as CommandFilter  # type: ignore
        except Exception:
            try:
                from aiogram.dispatcher.filters import Command as CommandFilter  # type: ignore
            except Exception:
                CommandFilter = None  # type: ignore
        if CommandFilter is not None:
            setattr(builtins, "Command", CommandFilter)
            installed.append("Command")

    if not hasattr(builtins, "_scope_key"):
        def _scope_key(*, chat_id: int, thread_id: int) -> tuple[int, int]:
            return chat_id, thread_id

        setattr(builtins, "_scope_key", _scope_key)
        installed.append("_scope_key")

    if not hasattr(builtins, "_all_control_button_texts"):
        def _all_control_button_texts() -> set[str]:
            return {
                "📊 Получить сводку",
                "📋 Показать сохраненные задачи",
                "✏️ Редактировать задачи",
                "❓ Помощь",
                "🛠 Режим программиста",
                "👤 Обычный режим",
                "🧹 Очистить текущий контекст",
                "🧨 Очистить всю БД",
                "🕒 Параметры расписания",
                "📍 Текущий контекст",
                "Отмена",
            }

        setattr(builtins, "_all_control_button_texts", _all_control_button_texts)
        installed.append("_all_control_button_texts")

    if not hasattr(builtins, "BotCommand"):
        try:
            from aiogram.types import (
                BotCommand as BotCommandType,
                BotCommandScopeAllGroupChats as BotCommandScopeAllGroupChatsType,
                BotCommandScopeAllPrivateChats as BotCommandScopeAllPrivateChatsType,
                BotCommandScopeDefault as BotCommandScopeDefaultType,
            )
        except Exception:
            BotCommandType = None  # type: ignore
            BotCommandScopeDefaultType = None  # type: ignore
            BotCommandScopeAllPrivateChatsType = None  # type: ignore
            BotCommandScopeAllGroupChatsType = None  # type: ignore

        if BotCommandType is not None:
            setattr(builtins, "BotCommand", BotCommandType)
            setattr(builtins, "BotCommandScopeDefault", BotCommandScopeDefaultType)
            setattr(builtins, "BotCommandScopeAllPrivateChats", BotCommandScopeAllPrivateChatsType)
            setattr(builtins, "BotCommandScopeAllGroupChats", BotCommandScopeAllGroupChatsType)
            installed.extend(
                [
                    "BotCommand",
                    "BotCommandScopeDefault",
                    "BotCommandScopeAllPrivateChats",
                    "BotCommandScopeAllGroupChats",
                ]
            )

    if installed:
        logger.warning("Installed runtime compat shims: %s", ", ".join(installed))


_install_runtime_compat_shims()

from handlers import build_and_publish_scope_summary, build_router


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    target_chat_id: int | None
    target_topic_id: int | None
    strict_target_scope: bool
    llm_provider: str
    llm_model: str
    gemini_api_key: str | None
    openai_api_key: str | None
    openai_base_url: str | None
    amvera_api_key: str | None
    amvera_base_url: str | None
    amvera_fallback_model: str | None
    db_backend: str
    postgres_dsn: str | None
    sqlite_path: str
    context_messages_limit: int
    llm_timeout_seconds: int
    schedule_enabled: bool
    summary_morning_time: str
    summary_evening_time: str
    schedule_timezone: str


def load_settings() -> Settings:
    load_dotenv()

    telegram_bot_token = require_env("TELEGRAM_BOT_TOKEN")
    target_chat_id = parse_optional_int("TARGET_CHAT_ID")
    target_topic_id = parse_optional_int("TARGET_TOPIC_ID")
    strict_target_scope = parse_bool("STRICT_TARGET_SCOPE", default=False)
    llm_provider = os.getenv("LLM_PROVIDER", "gemini").strip().lower()

    gemini_api_key: str | None = None
    openai_api_key: str | None = None
    openai_base_url: str | None = None
    amvera_api_key: str | None = None
    amvera_base_url: str | None = None
    amvera_fallback_model: str | None = None
    db_backend = os.getenv("DB_BACKEND", "auto").strip().lower()
    postgres_dsn = parse_optional_str("POSTGRES_DSN")

    if llm_provider == "openai":
        llm_model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
        openai_api_key = os.getenv("OPENAI_API_KEY")
        openai_base_url = parse_optional_str("OPENAI_BASE_URL")
    elif llm_provider == "amvera":
        llm_model = os.getenv("AMVERA_LLM_MODEL", "gpt-5").strip()
        amvera_api_key = parse_optional_str("AMVERA_LLM_API_KEY")
        amvera_base_url = parse_optional_str("AMVERA_LLM_BASE_URL")
        amvera_fallback_model = parse_optional_str("AMVERA_LLM_FALLBACK_MODEL") or "gpt-4.1"
    else:
        llm_model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash").strip()
        gemini_api_key = parse_optional_str("GEMINI_API_KEY")

    return Settings(
        telegram_bot_token=telegram_bot_token,
        target_chat_id=target_chat_id,
        target_topic_id=target_topic_id,
        strict_target_scope=strict_target_scope,
        llm_provider=llm_provider,
        llm_model=llm_model,
        gemini_api_key=gemini_api_key,
        openai_api_key=openai_api_key,
        openai_base_url=openai_base_url,
        amvera_api_key=amvera_api_key,
        amvera_base_url=amvera_base_url,
        amvera_fallback_model=amvera_fallback_model,
        db_backend=db_backend,
        postgres_dsn=postgres_dsn,
        sqlite_path=os.getenv("SQLITE_PATH", "data/bot.db"),
        context_messages_limit=int(os.getenv("CONTEXT_MESSAGES_LIMIT", "120")),
        llm_timeout_seconds=int(os.getenv("LLM_TIMEOUT_SECONDS", "120")),
        schedule_enabled=parse_bool("SCHEDULE_ENABLED", default=False),
        summary_morning_time=os.getenv("SUMMARY_MORNING_TIME", "09:00").strip(),
        summary_evening_time=os.getenv("SUMMARY_EVENING_TIME", "18:00").strip(),
        schedule_timezone=(
            os.getenv("SCHEDULE_TIMEZONE")
            or os.getenv("TIMEZONE")
            or os.getenv("TZ")
            or "Europe/Moscow"
        ).strip(),
    )


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def parse_optional_int(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    return int(raw.strip())


def parse_optional_str(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def parse_bool(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    settings = load_settings()
    _log_llm_runtime_settings(settings)

    db = Database(
        settings.sqlite_path,
        db_backend=settings.db_backend,
        postgres_dsn=settings.postgres_dsn,
    )
    db.init_schema()

    extractor = AIExtractor(
        provider=settings.llm_provider,
        model=settings.llm_model,
        gemini_api_key=settings.gemini_api_key,
        openai_api_key=settings.openai_api_key,
        openai_base_url=settings.openai_base_url,
        amvera_api_key=settings.amvera_api_key,
        amvera_base_url=settings.amvera_base_url,
        amvera_fallback_model=settings.amvera_fallback_model,
        llm_timeout_seconds=settings.llm_timeout_seconds,
    )

    bot = Bot(token=settings.telegram_bot_token)
    dp = Dispatcher()
    scheduler_task: asyncio.Task | None = None
    dp.include_router(
        build_router(
            target_chat_id=settings.target_chat_id,
            target_topic_id=settings.target_topic_id,
            strict_target_scope=settings.strict_target_scope,
            context_messages_limit=settings.context_messages_limit,
            db=db,
            extractor=extractor,
        )
    )

    await _configure_bot_commands(bot)

    if settings.schedule_enabled:
        scheduler_task = asyncio.create_task(
            _scheduled_summary_loop(
                bot=bot,
                db=db,
                extractor=extractor,
                settings=settings,
            )
        )
        logging.getLogger(__name__).info(
            "Scheduled summaries enabled: morning=%s evening=%s timezone=%s",
            settings.summary_morning_time,
            settings.summary_evening_time,
            settings.schedule_timezone,
        )

    allowed_updates = dp.resolve_used_update_types()
    logging.getLogger(__name__).info("Starting polling with allowed_updates=%s", allowed_updates)
    try:
        await dp.start_polling(bot, allowed_updates=allowed_updates)
    finally:
        if scheduler_task is not None:
            scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await scheduler_task
        db.close()
        await bot.session.close()


def _parse_schedule_time(raw: str, *, fallback: str) -> time:
    value = (raw or "").strip() or fallback
    if not re.fullmatch(r"\d{2}:\d{2}", value):
        value = fallback
    hour, minute = value.split(":", maxsplit=1)
    hh = max(0, min(23, int(hour)))
    mm = max(0, min(59, int(minute)))
    return time(hour=hh, minute=mm)


def _next_schedule_moment(now: datetime, schedule_points: tuple[time, time]) -> datetime:
    candidates: list[datetime] = []
    for point in schedule_points:
        candidate = now.replace(
            hour=point.hour,
            minute=point.minute,
            second=0,
            microsecond=0,
        )
        if candidate <= now:
            candidate = candidate + timedelta(days=1)
        candidates.append(candidate)
    return min(candidates)


async def _run_scheduled_summaries(
    *,
    bot: Bot,
    db: Database,
    extractor: AIExtractor,
    settings: Settings,
) -> None:
    logger = logging.getLogger(__name__)
    if settings.strict_target_scope and settings.target_chat_id is not None:
        scopes = [(settings.target_chat_id, settings.target_topic_id or 0)]
    else:
        try:
            scopes = await asyncio.to_thread(db.list_message_scopes)
        except Exception:
            logger.exception("Failed to list DB scopes for scheduled summary")
            return

    if not scopes:
        logger.info("No known scopes for scheduled summary run")
        return

    logger.info("Scheduled summary run started for %s scope(s)", len(scopes))
    sent_count = 0
    for chat_id, thread_id in scopes:
        try:
            sent = await build_and_publish_scope_summary(
                bot=bot,
                db=db,
                extractor=extractor,
                chat_id=chat_id,
                thread_id=thread_id,
                context_messages_limit=settings.context_messages_limit,
                replace_previous=True,
            )
            if sent:
                sent_count += 1
        except Exception:
            logger.exception(
                "Scheduled summary failed for chat_id=%s thread_id=%s",
                chat_id,
                thread_id,
            )
    logger.info("Scheduled summary run finished: %s/%s scope(s) sent", sent_count, len(scopes))


async def _scheduled_summary_loop(
    *,
    bot: Bot,
    db: Database,
    extractor: AIExtractor,
    settings: Settings,
) -> None:
    logger = logging.getLogger(__name__)
    tz_name = settings.schedule_timezone or "Europe/Moscow"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        logger.warning("Unknown timezone %s; fallback to Europe/Moscow", tz_name)
        tz = ZoneInfo("Europe/Moscow")

    morning = _parse_schedule_time(settings.summary_morning_time, fallback="09:00")
    evening = _parse_schedule_time(settings.summary_evening_time, fallback="18:00")
    schedule_points = (morning, evening)

    while True:
        now = datetime.now(tz)
        next_run = _next_schedule_moment(now, schedule_points)
        wait_seconds = max(1, int((next_run - now).total_seconds()))
        logger.info("Next scheduled summary at %s", next_run.isoformat())
        await asyncio.sleep(wait_seconds)
        await _run_scheduled_summaries(
            bot=bot,
            db=db,
            extractor=extractor,
            settings=settings,
        )


def _load_bot_command_types():
    try:
        from aiogram.types import (
            BotCommand as BotCommandType,
            BotCommandScopeAllGroupChats as BotCommandScopeAllGroupChatsType,
            BotCommandScopeAllPrivateChats as BotCommandScopeAllPrivateChatsType,
            BotCommandScopeDefault as BotCommandScopeDefaultType,
        )
        return (
            BotCommandType,
            BotCommandScopeDefaultType,
            BotCommandScopeAllPrivateChatsType,
            BotCommandScopeAllGroupChatsType,
        )
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "Failed to import BotCommand types, command menu setup is skipped: %s",
            exc,
        )
        return None


async def _configure_bot_commands(bot: Bot) -> None:
    command_types = _load_bot_command_types()
    if command_types is None:
        return

    (
        BotCommandType,
        BotCommandScopeDefaultType,
        BotCommandScopeAllPrivateChatsType,
        BotCommandScopeAllGroupChatsType,
    ) = command_types

    commands = [
        BotCommandType(command="start", description="Показать главное меню с кнопками"),
        BotCommandType(command="status", description="Сводка: сделано / в работе / зависло"),
        BotCommandType(command="tasks", description="Показать сохраненные задачи без LLM"),
        BotCommandType(command="edit", description="Ручное редактирование задач"),
        BotCommandType(command="bind", description="Привязать имя к текущему чату/ветке"),
        BotCommandType(command="where", description="Показать текущий chat_id/topic_id"),
        BotCommandType(command="health", description="Проверка доступа в текущем чате"),
        BotCommandType(command="clear_db", description="Очистить БД: /clear_db или /clear_db all"),
        BotCommandType(command="clear", description="Быстрая очистка: /clear или /clear all"),
        BotCommandType(command="help", description="Показать список команд"),
    ]
    scopes = [
        BotCommandScopeDefaultType(),
        BotCommandScopeAllPrivateChatsType(),
        BotCommandScopeAllGroupChatsType(),
    ]
    for scope in scopes:
        try:
            await bot.set_my_commands(commands, scope=scope)
        except Exception:
            logging.getLogger(__name__).exception(
                "Failed to register bot commands for scope %s",
                type(scope).__name__,
            )


def _log_llm_runtime_settings(settings: Settings) -> None:
    logger = logging.getLogger(__name__)
    logger.info(
        "Schedule enabled=%s morning=%s evening=%s timezone=%s",
        settings.schedule_enabled,
        settings.summary_morning_time,
        settings.summary_evening_time,
        settings.schedule_timezone,
    )
    provider = settings.llm_provider
    if provider == "openai":
        base_url = settings.openai_base_url or "https://api.openai.com/v1 (default)"
        logger.info("LLM provider=openai model=%s base_url=%s", settings.llm_model, base_url)
        if not settings.openai_base_url:
            logger.warning(
                "OPENAI_BASE_URL is empty: requests will be sent to OpenAI directly."
            )
        return

    if provider == "amvera":
        logger.info(
            "LLM provider=amvera model=%s base_url=%s fallback_model=%s",
            settings.llm_model,
            settings.amvera_base_url or "<empty>",
            settings.amvera_fallback_model or "<none>",
        )
        return

    if provider == "gemini":
        logger.info("LLM provider=gemini model=%s", settings.llm_model)
        return

    logger.warning("LLM provider has unexpected value: %s", provider)


if __name__ == "__main__":
    asyncio.run(main())
