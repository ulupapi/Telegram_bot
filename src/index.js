const { config, validateConfig } = require('./config');
const { ensureSpreadsheetStructure } = require('./sheets');
const { createServer } = require('./server');
const { createBot } = require('./bot');

async function startApp() {
  validateConfig();

  await ensureSpreadsheetStructure();
  console.log('Google Sheets готова.');

  const app = createServer();
  const server = app.listen(config.port, () => {
    console.log(`HTTP сервер запущен: http://localhost:${config.port}`);
  });

  const bot = createBot();
  await bot.launch();
  console.log('Telegram Bot запущен.');

  const stop = async () => {
    console.log('Останавливаю сервис...');
    server.close();
    await bot.stop();
    process.exit(0);
  };

  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
}

startApp().catch((error) => {
  console.error('Не удалось запустить приложение:', error.message);
  process.exit(1);
});
