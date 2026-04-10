from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand
from dotenv import load_dotenv

from ai_extractor import AIExtractor
from database import Database
from handlers import build_router


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    target_chat_id: int | None
    target_topic_id: int | None
    llm_provider: str
    llm_model: str
    gemini_api_key: str | None
    openai_api_key: str | None
    sqlite_path: str
    context_messages_limit: int


def load_settings() -> Settings:
    load_dotenv()

    telegram_bot_token = require_env("TELEGRAM_BOT_TOKEN")
    target_chat_id = parse_optional_int("TARGET_CHAT_ID")
    target_topic_id = parse_optional_int("TARGET_TOPIC_ID")
    llm_provider = os.getenv("LLM_PROVIDER", "gemini").strip().lower()

    if llm_provider == "openai":
        llm_model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    else:
        llm_model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

    return Settings(
        telegram_bot_token=telegram_bot_token,
        target_chat_id=target_chat_id,
        target_topic_id=target_topic_id,
        llm_provider=llm_provider,
        llm_model=llm_model,
        gemini_api_key=os.getenv("GEMINI_API_KEY"),
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        sqlite_path=os.getenv("SQLITE_PATH", "data/bot.db"),
        context_messages_limit=int(os.getenv("CONTEXT_MESSAGES_LIMIT", "120")),
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


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    settings = load_settings()

    db = Database(settings.sqlite_path)
    db.init_schema()

    extractor = AIExtractor(
        provider=settings.llm_provider,
        model=settings.llm_model,
        gemini_api_key=settings.gemini_api_key,
        openai_api_key=settings.openai_api_key,
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

    await bot.set_my_commands(
        [
            BotCommand(command="status", description="Сводка: сделано / в работе / зависло"),
            BotCommand(command="bind", description="Привязать имя к текущему чату/ветке"),
            BotCommand(command="where", description="Показать текущий chat_id/topic_id"),
        ]
    )

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
