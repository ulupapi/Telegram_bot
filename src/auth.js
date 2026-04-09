const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');
const { config } = require('./config');
const { getOrCreateUserByTelegram, getUserById } = require('./sheets');

const SESSION_TTL_MS = 24 * 60 * 60 * 1000;
const sessions = new Map();

function sanitizeUser(user) {
  if (!user) return null;
  const { __rowNumber, ...safe } = user;
  return safe;
}

function buildDataCheckString(initData) {
  const params = new URLSearchParams(initData);
  params.delete('hash');

  return Array.from(params.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${key}=${value}`)
    .join('\n');
}

function isValidTelegramInitData(initData) {
  const params = new URLSearchParams(initData);
  const hash = params.get('hash');

  if (!hash) return false;

  const dataCheckString = buildDataCheckString(initData);
  const secretKey = crypto.createHmac('sha256', 'WebAppData').update(config.botToken).digest();
  const calculatedHash = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex');

  const hashBuffer = Buffer.from(hash, 'hex');
  const calculatedBuffer = Buffer.from(calculatedHash, 'hex');

  if (hashBuffer.length !== calculatedBuffer.length) return false;
  return crypto.timingSafeEqual(hashBuffer, calculatedBuffer);
}

function parseTelegramUserFromInitData(initData) {
  const params = new URLSearchParams(initData);
  const rawUser = params.get('user');
  if (!rawUser) return null;

  return JSON.parse(rawUser);
}

function buildDevTelegramUser(payload) {
  const fullName = (payload.devFullName || 'Локальный Пользователь').trim();
  const [firstName = 'Локальный', ...rest] = fullName.split(' ');

  return {
    id: String(payload.devTelegramId || config.devTelegramId),
    username: payload.devUsername || 'local_user',
    first_name: firstName,
    last_name: rest.join(' ')
  };
}

function createSession(user) {
  const token = uuidv4();
  const safeUser = sanitizeUser(user);
  sessions.set(token, {
    userId: user.id,
    user: safeUser,
    expiresAt: Date.now() + SESSION_TTL_MS
  });
  return token;
}

function getSessionTokenFromHeaders(headers) {
  const authHeader = headers.authorization || '';
  if (!authHeader.startsWith('Bearer ')) return '';
  return authHeader.slice('Bearer '.length).trim();
}

async function signInTelegram(payload) {
  const initData = payload.initData || '';
  let telegramUser = null;

  if (initData) {
    if (!isValidTelegramInitData(initData)) {
      throw new Error('Некорректная подпись Telegram initData');
    }

    telegramUser = parseTelegramUserFromInitData(initData);
  } else if (config.devBypassTelegramAuth) {
    telegramUser = buildDevTelegramUser(payload);
  } else {
    throw new Error('initData обязателен при выключенном DEV_BYPASS_TELEGRAM_AUTH');
  }

  if (!telegramUser || !telegramUser.id) {
    throw new Error('Не удалось определить пользователя Telegram');
  }

  const user = await getOrCreateUserByTelegram(telegramUser);
  const token = createSession(user);

  return {
    token,
    user: sanitizeUser(user)
  };
}

async function authMiddleware(req, res, next) {
  try {
    const token = getSessionTokenFromHeaders(req.headers);
    if (!token) {
      return res.status(401).json({ error: 'Необходима авторизация' });
    }

    const session = sessions.get(token);
    if (!session) {
      return res.status(401).json({ error: 'Сессия не найдена' });
    }

    if (Date.now() > session.expiresAt) {
      sessions.delete(token);
      return res.status(401).json({ error: 'Сессия истекла' });
    }

    let user = session.user || null;
    if (!user) {
      user = await getUserById(session.userId);
    }

    if (!user) {
      sessions.delete(token);
      return res.status(401).json({ error: 'Пользователь не найден' });
    }

    req.user = user;
    req.userSafe = sanitizeUser(user);
    req.sessionToken = token;

    return next();
  } catch (error) {
    return next(error);
  }
}

function requireRoles(roles) {
  return (req, res, next) => {
    if (!req.user || !roles.includes(req.user.role)) {
      return res.status(403).json({ error: 'Недостаточно прав' });
    }
    return next();
  };
}

module.exports = {
  signInTelegram,
  authMiddleware,
  requireRoles,
  sanitizeUser
};
