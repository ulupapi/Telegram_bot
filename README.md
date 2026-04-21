# Telegram AI Task Bot

Бот для анализа рабочих обсуждений в Telegram (личные чаты, группы, supergroup, topics):

- собирает сообщения в текущем чате/ветке;
- отправляет контекст в LLM (Gemini, OpenAI, Amvera);
- по `/status` возвращает сводку и реестр задач:
  - Название
  - Описание
  - Дедлайн (до дня)
  - Автор
  - Исполнитель
  - Статус: `В ожидании`, `В работе`, `Завершена`, `Отклонена`, `Отозвана`

Также поддерживаются:

- `/status` в обычном чате (без topics);
- `/bind Название` для сохранения имени цели;
- `/status Название` для получения сводки по сохраненному имени;
- `/where` для проверки текущего `chat_id/topic_id`;
- `/health` для диагностики прав и видимости сообщений в текущем чате.

### Режимы области (scope)

По умолчанию бот работает в `multi-chat` режиме: в каждом чате/ветке собирается свой контекст.

Можно включить строгий режим только для одного чата/ветки:

- `TARGET_CHAT_ID=<id>`
- `TARGET_TOPIC_ID=<id>` (опционально)
- `STRICT_TARGET_SCOPE=true`

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

### Важно для групп и supergroup

Чтобы бот видел обычные сообщения (а не только команды), в `@BotFather` отключите privacy mode:

1. `/setprivacy`
2. Выберите вашего бота
3. `Disable`

После добавления в группу выполните `/health` прямо в этой группе.

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
