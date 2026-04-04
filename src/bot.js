const { Telegraf, Markup } = require('telegraf');
const { config } = require('./config');

function createBot() {
  const bot = new Telegraf(config.botToken);

  const appButton = Markup.inlineKeyboard([
    [Markup.button.webApp('Открыть приложение', config.miniAppUrl)]
  ]);

  bot.start(async (ctx) => {
    await ctx.reply(
      'Привет! Это мини-приложение для учёта смен. Нажмите кнопку ниже, чтобы открыть приложение.',
      appButton
    );
  });

  bot.command('app', async (ctx) => {
    await ctx.reply('Открыть Mini App:', appButton);
  });

  bot.catch((error) => {
    console.error('Ошибка Telegram Bot:', error);
  });

  return bot;
}

module.exports = { createBot };
