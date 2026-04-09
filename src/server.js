const path = require('path');
const express = require('express');
const cors = require('cors');
const { config } = require('./config');
const { authMiddleware, requireRoles, signInTelegram, sanitizeUser } = require('./auth');
const {
  listSchedulesByUser,
  addSchedule,
  cancelFutureSchedule,
  getActiveShiftByUser,
  startShift,
  endShift,
  listShiftsByUser,
  getUserHours,
  getManagerDashboard
} = require('./sheets');

function asyncHandler(handler) {
  return (req, res, next) => {
    Promise.resolve(handler(req, res, next)).catch(next);
  };
}

function validateScheduleInput(body) {
  const date = String(body.date || '').trim();
  const startTime = String(body.start_time || '').trim();
  const endTime = String(body.end_time || '').trim();

  const dateOk = /^\d{4}-\d{2}-\d{2}$/.test(date);
  const timeOk = /^([01]\d|2[0-3]):[0-5]\d$/.test(startTime) && /^([01]\d|2[0-3]):[0-5]\d$/.test(endTime);

  if (!dateOk || !timeOk) {
    throw new Error('Неверный формат даты или времени');
  }

  if (endTime <= startTime) {
    throw new Error('Время окончания должно быть позже времени начала');
  }

  return { date, startTime, endTime };
}

function createServer() {
  const app = express();
  const miniappDir = path.resolve(__dirname, '../miniapp');

  app.use(cors());
  app.use(express.json());

  app.get('/api/health', (req, res) => {
    res.json({ ok: true, time: new Date().toISOString() });
  });

  app.post(
    '/api/auth/telegram',
    asyncHandler(async (req, res) => {
      const payload = req.body || {};
      const session = await signInTelegram(payload);
      res.json(session);
    })
  );

  app.get(
    '/api/me',
    authMiddleware,
    asyncHandler(async (req, res) => {
      const [activeShift, hours] = await Promise.all([
        getActiveShiftByUser(req.user.id),
        getUserHours(req.user, config.timezone)
      ]);

      res.json({
        user: sanitizeUser(req.user),
        active_shift: activeShift,
        hours
      });
    })
  );

  app.get(
    '/api/my/schedules',
    authMiddleware,
    asyncHandler(async (req, res) => {
      const items = await listSchedulesByUser(req.user.id);
      res.json({ items });
    })
  );

  app.post(
    '/api/my/schedules',
    authMiddleware,
    asyncHandler(async (req, res) => {
      const { date, startTime, endTime } = validateScheduleInput(req.body || {});
      const schedule = await addSchedule(req.user.id, date, startTime, endTime);
      res.status(201).json({ item: schedule });
    })
  );

  app.delete(
    '/api/my/schedules/:id',
    authMiddleware,
    asyncHandler(async (req, res) => {
      await cancelFutureSchedule(req.user.id, req.params.id, config.timezone);
      res.json({ ok: true });
    })
  );

  app.post(
    '/api/my/shifts/start',
    authMiddleware,
    asyncHandler(async (req, res) => {
      const shift = await startShift(req.user.id);
      res.status(201).json({ shift });
    })
  );

  app.post(
    '/api/my/shifts/end',
    authMiddleware,
    asyncHandler(async (req, res) => {
      const activeShift = await getActiveShiftByUser(req.user.id);
      if (!activeShift) {
        return res.status(409).json({ error: 'Активная смена уже завершена или не найдена' });
      }

      const shift = await endShift(req.user.id);
      res.json({ shift });
    })
  );

  app.get(
    '/api/my/shifts',
    authMiddleware,
    asyncHandler(async (req, res) => {
      const items = await listShiftsByUser(req.user.id);
      res.json({ items });
    })
  );

  app.get(
    '/api/my/hours',
    authMiddleware,
    asyncHandler(async (req, res) => {
      const hours = await getUserHours(req.user, config.timezone);
      res.json({ hours });
    })
  );

  app.get(
    '/api/manager/dashboard',
    authMiddleware,
    requireRoles(['manager', 'admin']),
    asyncHandler(async (req, res) => {
      const data = await getManagerDashboard(config.timezone);
      res.json(data);
    })
  );

  app.use('/miniapp', express.static(miniappDir));
  app.use(express.static(miniappDir));

  app.get('*', (req, res, next) => {
    if (req.path.startsWith('/api')) return next();
    return res.sendFile(path.join(miniappDir, 'index.html'));
  });

  app.use((req, res) => {
    res.status(404).json({ error: 'Маршрут не найден' });
  });

  app.use((error, req, res, next) => {
    console.error(error);
    const message = error.message || 'Внутренняя ошибка сервера';
    res.status(400).json({ error: message });
  });

  return app;
}

module.exports = { createServer };
