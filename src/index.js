const { config, validateConfig } = require('./config');
const { ensureSpreadsheetStructure } = require('./sheets');
const { createServer } = require('./server');
const { createBot } = require('./bot');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function startApp() {
  validateConfig();

  const app = createServer();
  const server = app.listen(config.port, '0.0.0.0', () => {
    console.log(`HTTP сервер запущен: http://0.0.0.0:${config.port}`);
  });

  let bot = null;
  let shuttingDown = false;

  // Запускаем интеграции в фоне, чтобы платформа не получила 502 из-за долгого старта.
  (async () => {
    while (!shuttingDown) {
      try {
        await ensureSpreadsheetStructure();
        console.log('Google Sheets готова.');
        break;
      } catch (error) {
        console.error('Google Sheets недоступна, повтор через 5с:', error.message);
        await sleep(5000);
      }
    }

    while (!shuttingDown) {
      try {
        bot = createBot();
        await bot.launch();
        console.log('Telegram Bot запущен.');
        break;
      } catch (error) {
        console.error('Не удалось запустить Telegram Bot, повтор через 5с:', error.message);
        await sleep(5000);
      }
    }
  })();

  const stop = async () => {
    console.log('Останавливаю сервис...');
    shuttingDown = true;

    server.close();

    if (bot) {
      await bot.stop();
    }

    process.exit(0);
  };

  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
}

startApp().catch((error) => {
  console.error('Не удалось запустить приложение:', error.message);
  process.exit(1);
});
