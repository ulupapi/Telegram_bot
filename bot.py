from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from dataclasses import dataclass

from aiogram import Bot, Dispatcher
from dotenv import load_dotenv

from ai_extractor import AIExtractor
from database import Database
from handlers import build_router


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

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        if scheduler_task is not None:
            scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await scheduler_task
        db.close()
        await bot.session.close()


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
        BotCommandType(command="status", description="Сводка: сделано / в работе / зависло"),
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


if __name__ == "__main__":
    asyncio.run(main())
