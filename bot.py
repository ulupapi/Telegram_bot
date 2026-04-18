from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramConflictError
from aiogram.types import (
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeDefault,
)
from dotenv import load_dotenv

from ai_extractor import AIExtractor
from database import Database
from handlers import build_router, send_status_for_scope


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    target_chat_id: int | None
    target_topic_id: int | None
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
    polling_lock_id: int


def load_settings() -> Settings:
    load_dotenv()

    telegram_bot_token = require_env("TELEGRAM_BOT_TOKEN")
    target_chat_id = parse_optional_int("TARGET_CHAT_ID")
    target_topic_id = parse_optional_int("TARGET_TOPIC_ID")
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
        schedule_enabled=parse_bool(os.getenv("SCHEDULE_ENABLED", "1")),
        summary_morning_time=os.getenv("SUMMARY_MORNING_TIME", "09:00").strip(),
        summary_evening_time=os.getenv("SUMMARY_EVENING_TIME", "18:00").strip(),
        schedule_timezone=os.getenv("SCHEDULE_TIMEZONE", "Europe/Moscow").strip(),
        polling_lock_id=int(os.getenv("POLLING_LOCK_ID", "71482391")),
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


def parse_bool(raw: str | None) -> bool:
    value = (raw or "").strip().lower()
    return value in {"1", "true", "yes", "on", "y"}


def _parse_hhmm(raw: str) -> tuple[int, int]:
    parts = raw.split(":")
    if len(parts) != 2:
        raise ValueError(f"Time must be HH:MM, got {raw!r}")
    hour = int(parts[0])
    minute = int(parts[1])
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"Time must be HH:MM, got {raw!r}")
    return hour, minute


def _next_run_time(
    *,
    now: datetime,
    morning: tuple[int, int],
    evening: tuple[int, int],
) -> datetime:
    candidates = []
    for hour, minute in (morning, evening):
        candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= now:
            candidate += timedelta(days=1)
        candidates.append(candidate)
    return min(candidates)


async def _run_schedule_loop(
    *,
    bot: Bot,
    db: Database,
    extractor: AIExtractor,
    chat_id: int,
    thread_id: int,
    context_messages_limit: int,
    morning_time: str,
    evening_time: str,
    timezone_name: str,
) -> None:
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        logging.warning("Unknown SCHEDULE_TIMEZONE=%s, fallback to UTC", timezone_name)
        tz = ZoneInfo("UTC")

    try:
        morning = _parse_hhmm(morning_time)
    except ValueError:
        logging.warning("Invalid SUMMARY_MORNING_TIME=%s, fallback to 09:00", morning_time)
        morning = (9, 0)
    try:
        evening = _parse_hhmm(evening_time)
    except ValueError:
        logging.warning("Invalid SUMMARY_EVENING_TIME=%s, fallback to 18:00", evening_time)
        evening = (18, 0)
    logging.info(
        "Task summary scheduler enabled: morning=%s evening=%s tz=%s chat_id=%s topic_id=%s",
        morning_time,
        evening_time,
        timezone_name,
        chat_id,
        thread_id,
    )

    while True:
        now = datetime.now(tz)
        run_at = _next_run_time(now=now, morning=morning, evening=evening)
        sleep_seconds = max(1.0, (run_at - now).total_seconds())
        await asyncio.sleep(sleep_seconds)

        try:
            sent = await send_status_for_scope(
                bot=bot,
                db=db,
                extractor=extractor,
                chat_id=chat_id,
                thread_id=thread_id,
                context_messages_limit=context_messages_limit,
            )
            if not sent:
                logging.info(
                    "Scheduler skipped summary: no messages in scope chat_id=%s topic_id=%s",
                    chat_id,
                    thread_id,
                )
        except Exception:
            logging.exception("Scheduled summary failed")


def _try_acquire_polling_lock(
    *,
    db_backend: str,
    postgres_dsn: str | None,
    lock_id: int,
):
    if db_backend != "postgres" or not postgres_dsn:
        return None
    try:
        import psycopg
        from psycopg.rows import tuple_row
    except Exception:
        logging.warning("psycopg not available, polling lock is disabled")
        return None

    conn = psycopg.connect(postgres_dsn, autocommit=True)
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
        row = cur.fetchone()
    got_lock = bool(row and row[0])
    if not got_lock:
        conn.close()
        return False
    return conn


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    settings = load_settings()

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
    dp.include_router(
        build_router(
            target_chat_id=settings.target_chat_id,
            target_topic_id=settings.target_topic_id,
            context_messages_limit=settings.context_messages_limit,
            db=db,
            extractor=extractor,
        )
    )

    # User experience is menu-first: clear slash command lists in all scopes.
    await bot.set_my_commands([], scope=BotCommandScopeDefault())
    await bot.set_my_commands([], scope=BotCommandScopeAllGroupChats())
    await bot.set_my_commands([], scope=BotCommandScopeAllPrivateChats())

    scheduler_task: asyncio.Task | None = None
    polling_lock_conn = _try_acquire_polling_lock(
        db_backend=settings.db_backend,
        postgres_dsn=settings.postgres_dsn,
        lock_id=settings.polling_lock_id,
    )
    if polling_lock_conn is False:
        logging.warning(
            "Another bot instance is already polling (lock_id=%s). Exiting this instance.",
            settings.polling_lock_id,
        )
        db.close()
        await bot.session.close()
        return

    if settings.schedule_enabled:
        if settings.target_chat_id is None:
            logging.warning(
                "SCHEDULE_ENABLED=1 but TARGET_CHAT_ID is not set. Scheduler is disabled."
            )
        else:
            scheduler_task = asyncio.create_task(
                _run_schedule_loop(
                    bot=bot,
                    db=db,
                    extractor=extractor,
                    chat_id=settings.target_chat_id,
                    thread_id=settings.target_topic_id or 0,
                    context_messages_limit=settings.context_messages_limit,
                    morning_time=settings.summary_morning_time,
                    evening_time=settings.summary_evening_time,
                    timezone_name=settings.schedule_timezone,
                )
            )

    try:
        await bot.delete_webhook(drop_pending_updates=False)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except TelegramConflictError:
        logging.warning("Polling conflict detected. Another instance is active; exiting.")
    finally:
        if scheduler_task:
            scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await scheduler_task
        if polling_lock_conn not in (None, False):
            with contextlib.suppress(Exception):
                with polling_lock_conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (settings.polling_lock_id,))
            with contextlib.suppress(Exception):
                polling_lock_conn.close()
        db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
