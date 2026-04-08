const { config, validateConfig } = require('./config');
const { ensureSpreadsheetStructure } = require('./sheets');
const { createServer } = require('./server');
const { createBot } = require('./bot');

async function startApp() {
  validateConfig();

  await ensureSpreadsheetStructure();
  console.log('Google Sheets готова.');

  const app = createServer();
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Server started on port ${PORT}`);
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
