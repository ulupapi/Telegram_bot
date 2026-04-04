const path = require('path');
const dotenv = require('dotenv');

dotenv.config();

const config = {
  port: Number(process.env.PORT || 3000),
  botToken: process.env.BOT_TOKEN || '',
  miniAppUrl: process.env.MINI_APP_URL || '',
  spreadsheetId: process.env.GOOGLE_SPREADSHEET_ID || '',
  timezone: process.env.APP_TIMEZONE || 'Europe/Moscow',
  devBypassTelegramAuth: process.env.DEV_BYPASS_TELEGRAM_AUTH === 'true',
  devTelegramId: process.env.DEV_TELEGRAM_ID || '100000001',
  serviceAccountKeyFile: process.env.GOOGLE_SERVICE_ACCOUNT_KEY_FILE
    ? path.resolve(process.cwd(), process.env.GOOGLE_SERVICE_ACCOUNT_KEY_FILE)
    : '',
  serviceAccountEmail: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || '',
  privateKey: process.env.GOOGLE_PRIVATE_KEY
    ? process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n')
    : ''
};

function validateConfig() {
  const missing = [];

  if (!config.botToken) missing.push('BOT_TOKEN');
  if (!config.miniAppUrl) missing.push('MINI_APP_URL');
  if (!config.spreadsheetId) missing.push('GOOGLE_SPREADSHEET_ID');

  const hasFile = Boolean(config.serviceAccountKeyFile);
  const hasInline = Boolean(config.serviceAccountEmail && config.privateKey);

  if (!hasFile && !hasInline) {
    missing.push('GOOGLE_SERVICE_ACCOUNT_KEY_FILE (или GOOGLE_SERVICE_ACCOUNT_EMAIL + GOOGLE_PRIVATE_KEY)');
  }

  if (missing.length) {
    throw new Error(`Не хватает переменных окружения: ${missing.join(', ')}`);
  }
}

module.exports = { config, validateConfig };
