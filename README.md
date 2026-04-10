# Telegram AI Task Bot

Минималистичный бот для одной ветки (Topic) в Telegram-группе:

- тихо собирает сообщения из выбранной ветки;
- отправляет контекст в LLM (Gemini или OpenAI);
- по `/status` возвращает сводку:
  - Что сделано
  - Что в работе
  - Что зависло

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
