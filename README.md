# Telegram Shift Mini App (Google Sheets)

Простой проект для учёта смен сотрудников в Telegram Mini App.

## Что внутри

- `Telegram Bot` (Telegraf): команда `/start` и кнопка открытия Mini App.
- `HTTP API` (Express): профили, расписание, смены, часы, менеджерский экран.
- `Mini App` (HTML/CSS/JS): 5 экранов на русском языке.
- `Google Sheets` как основное хранилище данных.

## Структура

- `src/index.js` — запуск сервера и бота.
- `src/server.js` — API маршруты.
- `src/bot.js` — логика Telegram Bot.
- `src/sheets.js` — работа с Google Sheets API.
- `src/auth.js` — авторизация Mini App и сессии.
- `miniapp/index.html` — интерфейс Mini App.
- `miniapp/app.js` — фронтенд-логика Mini App.
- `miniapp/styles.css` — стили.

## Google Sheets

При первом старте приложение само создаст (если их нет) листы:

- `users`
- `schedules`
- `shifts`
- `summaries`
- `settings`

В `settings` будут автоматически добавлены ключи:

- `manager_telegram_ids`
- `admin_telegram_ids`

Формат значения: список Telegram ID через запятую, например:

`123456789,987654321`

## Быстрый запуск

1. Установите Node.js 18+.
2. Создайте Telegram-бота через `@BotFather` и получите `BOT_TOKEN`.
3. Создайте Google Spreadsheet и скопируйте `spreadsheetId` из URL.
4. Создайте Service Account в Google Cloud, включите Google Sheets API, скачайте JSON-ключ.
5. Дайте сервисному аккаунту доступ `Редактор` к таблице (через Share в Google Sheets).
6. Скопируйте `.env.example` в `.env` и заполните значения.
7. Установите зависимости и запустите проект:

```bash
npm install
npm start
```

Сервер будет доступен на `http://localhost:3000`.

## Настройка MINI_APP_URL

Telegram Mini App должен открываться по публичному `https` URL.
Для локальной разработки можно использовать `ngrok`:

```bash
ngrok http 3000
```

Возьмите `https://...` URL и вставьте в `.env` как `MINI_APP_URL`.

## Локальная отладка без Telegram

Можно открыть в браузере:

`http://localhost:3000/?dev_id=100000001&dev_name=Тестовый%20Пользователь`

Это работает, если в `.env` установлено:

`DEV_BYPASS_TELEGRAM_AUTH=true`

## Что уже реализовано

- Авторизация через Telegram WebApp `initData`.
- Роли: `employee`, `manager`, `admin`.
- Добавление/удаление расписания (удаляются только будущие интервалы).
- Старт/завершение смены.
- Защита от второй активной смены у одного сотрудника.
- Таймер активной смены в Mini App.
- История завершённых смен.
- Суммарные часы: день/неделя/месяц/всё время.
- Экран менеджера: сотрудники, кто на смене, часы, расписание.

