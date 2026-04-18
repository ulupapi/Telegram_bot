# Telegram AI Task Bot

Минималистичный бот для одной ветки (Topic) в Telegram-группе:

- тихо собирает сообщения из выбранной ветки;
- отправляет контекст в LLM (Gemini или OpenAI);
- возвращает сводку и реестр задач:
  - Название
  - Описание
  - Дедлайн (до дня)
  - Автор
  - Исполнитель
  - Статус: `В ожидании`, `В работе`, `Завершена`, `Отклонена`, `Отозвана`
- интерфейс через Reply Keyboard (кнопки внизу чата), включая:
  - обновление сводки;
  - создание задачи из ответа (reply) на сообщение;
  - ручную правку задачи ответом на карточку;
  - очистку контекста;
- автосводка по расписанию утром и вечером.

Также поддерживаются:

- обновление задач в несколько сообщений (одна задача = одно сообщение);
- автоочистка старой карточки задачи при отправке новой версии;
- `/bind Название` и `/where` как совместимость (кнопки — основной сценарий).

### Провайдеры LLM

Поддерживаются три варианта:

- `LLM_PROVIDER=gemini`
- `LLM_PROVIDER=openai`
- `LLM_PROVIDER=amvera` (нативный API Amvera LLM)

Для `amvera` задайте:

- `AMVERA_LLM_API_KEY`
- `AMVERA_LLM_BASE_URL` (endpoint вида `.../v1`)
- `AMVERA_LLM_MODEL` (например `gpt-5` или `gpt-4.1`; важно, чтобы по этой модели была квота)
- `AMVERA_LLM_FALLBACK_MODEL` (опционально, например `gpt-4.1` при таймаутах `gpt-5`)

### Хранилище

По умолчанию используется SQLite.

Для PostgreSQL:

- `DB_BACKEND=postgres`
- `POSTGRES_DSN=postgresql://user:password@host:5432/dbname`

Для автовыбора:

- `DB_BACKEND=auto` (возьмет PostgreSQL, если задан `POSTGRES_DSN`, иначе SQLite)

### Таймаут LLM

Для более медленных моделей (например `gpt-5`) увеличьте:

- `LLM_TIMEOUT_SECONDS=120` (или выше)
- при необходимости уменьшите `CONTEXT_MESSAGES_LIMIT`

### Расписание

Для автоматической отправки сводки 2 раза в день:

- `SCHEDULE_ENABLED=1`
- `SUMMARY_MORNING_TIME=09:00`
- `SUMMARY_EVENING_TIME=18:00`
- `SCHEDULE_TIMEZONE=Europe/Moscow`

### Антиконфликт polling

Чтобы при деплое не было конфликта `getUpdates` между несколькими инстансами, добавлен PostgreSQL advisory lock:

- `POLLING_LOCK_ID=71482391`

Если lock уже занят другим инстансом, текущий процесс завершится без запуска polling.

## Локальный запуск

1. Создайте виртуальное окружение и установите зависимости:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Скопируйте `.env.example` в `.env` и заполните переменные.

3. Запустите бота:

```bash
python bot.py
```

## Deploy (Amvera)

Файл `amvera.yml` уже настроен:

- Python + pip;
- установка зависимостей из `requirements.txt`;
- запуск через `bot.py`.
