const fs = require('fs');
const { google } = require('googleapis');
const { v4: uuidv4 } = require('uuid');
const { config } = require('./config');
const { nowIso, diffMinutes, isFutureSchedule, dayjs } = require('./time');

const SHEETS = {
  users: {
    name: 'users',
    headers: ['id', 'telegram_id', 'username', 'full_name', 'role', 'is_active', 'created_at']
  },
  schedules: {
    name: 'schedules',
    headers: ['id', 'user_id', 'date', 'start_time', 'end_time', 'created_at', 'status']
  },
  shifts: {
    name: 'shifts',
    headers: ['id', 'user_id', 'start_datetime', 'end_datetime', 'duration_minutes', 'status', 'created_at', 'updated_at']
  },
  summaries: {
    name: 'summaries',
    headers: [
      'user_id',
      'full_name',
      'total_today_minutes',
      'total_week_minutes',
      'total_month_minutes',
      'total_all_time_minutes',
      'updated_at'
    ]
  },
  settings: {
    name: 'settings',
    headers: ['key', 'value']
  }
};

let sheetsClient = null;
const READ_CACHE_TTL_MS = 5000;
const rowsCache = new Map();

function cloneRows(rows) {
  return rows.map((row) => ({ ...row }));
}

function getCachedRows(sheetKey) {
  const cached = rowsCache.get(sheetKey);
  if (!cached) return null;
  if (Date.now() > cached.expiresAt) {
    rowsCache.delete(sheetKey);
    return null;
  }
  return cloneRows(cached.rows);
}

function setCachedRows(sheetKey, rows) {
  rowsCache.set(sheetKey, {
    rows: cloneRows(rows),
    expiresAt: Date.now() + READ_CACHE_TTL_MS
  });
}

function invalidateSheetCache(sheetKey) {
  rowsCache.delete(sheetKey);
}

function getGoogleAuthOptions() {
  if (config.serviceAccountKeyFile) {
    if (!fs.existsSync(config.serviceAccountKeyFile)) {
      throw new Error(`Файл service account не найден: ${config.serviceAccountKeyFile}`);
    }
    return {
      keyFile: config.serviceAccountKeyFile,
      scopes: ['https://www.googleapis.com/auth/spreadsheets']
    };
  }

  return {
    credentials: {
      client_email: config.serviceAccountEmail,
      private_key: config.privateKey
    },
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  };
}

async function getSheetsClient() {
  if (sheetsClient) return sheetsClient;

  const auth = new google.auth.GoogleAuth(getGoogleAuthOptions());
  sheetsClient = google.sheets({ version: 'v4', auth });
  return sheetsClient;
}

function toRowObject(sheetKey, rowValues, rowNumber) {
  const headers = SHEETS[sheetKey].headers;
  const row = {};

  headers.forEach((header, index) => {
    row[header] = rowValues[index] || '';
  });

  row.__rowNumber = rowNumber;
  return row;
}

function toRowValues(sheetKey, rowObject) {
  const headers = SHEETS[sheetKey].headers;
  return headers.map((header) => (rowObject[header] ?? '').toString());
}

async function ensureSpreadsheetStructure() {
  const sheetsApi = await getSheetsClient();

  const meta = await sheetsApi.spreadsheets.get({
    spreadsheetId: config.spreadsheetId
  });

  const existingSheets = new Set(
    (meta.data.sheets || []).map((sheet) => sheet.properties && sheet.properties.title).filter(Boolean)
  );

  const missingSheetRequests = Object.values(SHEETS)
    .filter((sheet) => !existingSheets.has(sheet.name))
    .map((sheet) => ({ addSheet: { properties: { title: sheet.name } } }));

  if (missingSheetRequests.length > 0) {
    await sheetsApi.spreadsheets.batchUpdate({
      spreadsheetId: config.spreadsheetId,
      requestBody: { requests: missingSheetRequests }
    });
  }

  for (const [sheetKey, sheet] of Object.entries(SHEETS)) {
    const headerRange = `${sheet.name}!1:1`;
    const response = await sheetsApi.spreadsheets.values.get({
      spreadsheetId: config.spreadsheetId,
      range: headerRange
    });

    const currentHeaders = (response.data.values && response.data.values[0]) || [];
    const sameHeaders = JSON.stringify(currentHeaders) === JSON.stringify(sheet.headers);

    if (!sameHeaders) {
      await sheetsApi.spreadsheets.values.update({
        spreadsheetId: config.spreadsheetId,
        range: `${sheet.name}!A1`,
        valueInputOption: 'RAW',
        requestBody: {
          values: [sheet.headers]
        }
      });
    }

    if (sheetKey === 'settings') {
      await ensureDefaultSettings();
    }
  }
}

async function ensureDefaultSettings() {
  const settings = await getSettingsMap();

  const defaults = [
    { key: 'manager_telegram_ids', value: '' },
    { key: 'admin_telegram_ids', value: '' }
  ];

  for (const item of defaults) {
    if (!(item.key in settings)) {
      await appendRow('settings', item);
    }
  }
}

async function getRows(sheetKey) {
  const cached = getCachedRows(sheetKey);
  if (cached) return cached;

  const sheetsApi = await getSheetsClient();
  const sheetName = SHEETS[sheetKey].name;

  const response = await sheetsApi.spreadsheets.values.get({
    spreadsheetId: config.spreadsheetId,
    range: `${sheetName}!A2:Z`
  });

  const rows = response.data.values || [];
  const result = rows.map((rowValues, index) => toRowObject(sheetKey, rowValues, index + 2));
  setCachedRows(sheetKey, result);
  return cloneRows(result);
}

async function appendRow(sheetKey, rowObject) {
  const sheetsApi = await getSheetsClient();
  const sheetName = SHEETS[sheetKey].name;

  await sheetsApi.spreadsheets.values.append({
    spreadsheetId: config.spreadsheetId,
    range: `${sheetName}!A1`,
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: {
      values: [toRowValues(sheetKey, rowObject)]
    }
  });

  invalidateSheetCache(sheetKey);
}

async function updateRow(sheetKey, rowNumber, rowObject) {
  const sheetsApi = await getSheetsClient();
  const sheetName = SHEETS[sheetKey].name;

  await sheetsApi.spreadsheets.values.update({
    spreadsheetId: config.spreadsheetId,
    range: `${sheetName}!A${rowNumber}`,
    valueInputOption: 'RAW',
    requestBody: {
      values: [toRowValues(sheetKey, rowObject)]
    }
  });

  invalidateSheetCache(sheetKey);
}

async function getSettingsMap() {
  const settingsRows = await getRows('settings');
  const map = {};

  for (const row of settingsRows) {
    if (row.key) {
      map[row.key] = row.value || '';
    }
  }

  return map;
}

function parseIdsList(value) {
  return value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

async function resolveRoleByTelegramId(telegramId) {
  const settings = await getSettingsMap();
  const telegramIdStr = String(telegramId);

  const adminIds = parseIdsList(settings.admin_telegram_ids || '');
  if (adminIds.includes(telegramIdStr)) return 'admin';

  const managerIds = parseIdsList(settings.manager_telegram_ids || '');
  if (managerIds.includes(telegramIdStr)) return 'manager';

  return 'employee';
}

async function getUserByTelegramId(telegramId) {
  const users = await getRows('users');
  return users.find((user) => user.telegram_id === String(telegramId) && user.is_active !== 'false') || null;
}

async function getUserById(userId) {
  const users = await getRows('users');
  return users.find((user) => user.id === String(userId) && user.is_active !== 'false') || null;
}

async function listActiveUsers() {
  const users = await getRows('users');
  return users.filter((user) => user.is_active !== 'false');
}

function buildFullName(telegramUser) {
  const firstName = telegramUser.first_name || '';
  const lastName = telegramUser.last_name || '';
  const fullName = `${firstName} ${lastName}`.trim();
  return fullName || telegramUser.username || `user_${telegramUser.id}`;
}

async function getOrCreateUserByTelegram(telegramUser) {
  const telegramId = String(telegramUser.id);
  const detectedRole = await resolveRoleByTelegramId(telegramId);
  const existingUser = await getUserByTelegramId(telegramId);

  if (existingUser) {
    const shouldUpdateRole = existingUser.role !== detectedRole;
    const shouldUpdateName = !existingUser.full_name;

    if (shouldUpdateRole || shouldUpdateName) {
      const updatedUser = {
        ...existingUser,
        role: detectedRole,
        full_name: existingUser.full_name || buildFullName(telegramUser)
      };
      await updateRow('users', existingUser.__rowNumber, updatedUser);
      return updatedUser;
    }

    return existingUser;
  }

  const newUser = {
    id: uuidv4(),
    telegram_id: telegramId,
    username: telegramUser.username || '',
    full_name: buildFullName(telegramUser),
    role: detectedRole,
    is_active: 'true',
    created_at: nowIso()
  };

  await appendRow('users', newUser);
  return newUser;
}

async function addSchedule(userId, date, startTime, endTime) {
  const schedule = {
    id: uuidv4(),
    user_id: userId,
    date,
    start_time: startTime,
    end_time: endTime,
    created_at: nowIso(),
    status: 'planned'
  };

  await appendRow('schedules', schedule);
  return schedule;
}

async function listSchedulesByUser(userId) {
  const schedules = await getRows('schedules');

  return schedules
    .filter((item) => item.user_id === userId && item.status === 'planned')
    .sort((a, b) => `${a.date} ${a.start_time}`.localeCompare(`${b.date} ${b.start_time}`));
}

async function listAllUpcomingSchedules() {
  const schedules = await getRows('schedules');
  return schedules
    .filter((item) => item.status === 'planned')
    .sort((a, b) => `${a.date} ${a.start_time}`.localeCompare(`${b.date} ${b.start_time}`));
}

async function cancelFutureSchedule(userId, scheduleId, timezone) {
  const schedules = await getRows('schedules');
  const item = schedules.find((row) => row.id === scheduleId && row.user_id === userId && row.status === 'planned');

  if (!item) {
    throw new Error('Запись расписания не найдена');
  }

  if (!isFutureSchedule(item.date, item.start_time, timezone)) {
    throw new Error('Можно удалять только будущие записи');
  }

  const updated = {
    ...item,
    status: 'cancelled'
  };

  await updateRow('schedules', item.__rowNumber, updated);
  return updated;
}

async function getActiveShiftByUser(userId) {
  const shifts = await getRows('shifts');
  return shifts.find((shift) => shift.user_id === userId && shift.status === 'active') || null;
}

async function startShift(userId) {
  const activeShift = await getActiveShiftByUser(userId);
  if (activeShift) {
    throw new Error('Сначала завершите текущую смену');
  }

  const now = nowIso();
  const shift = {
    id: uuidv4(),
    user_id: userId,
    start_datetime: now,
    end_datetime: '',
    duration_minutes: '',
    status: 'active',
    created_at: now,
    updated_at: now
  };

  await appendRow('shifts', shift);
  return shift;
}

async function endShift(userId) {
  const activeShift = await getActiveShiftByUser(userId);

  if (!activeShift) {
    throw new Error('Активная смена не найдена');
  }

  const endedAt = nowIso();
  const durationMinutes = diffMinutes(activeShift.start_datetime, endedAt);

  const updatedShift = {
    ...activeShift,
    end_datetime: endedAt,
    duration_minutes: String(durationMinutes),
    status: 'completed',
    updated_at: endedAt
  };

  await updateRow('shifts', activeShift.__rowNumber, updatedShift);
  return updatedShift;
}

async function listShiftsByUser(userId) {
  const shifts = await getRows('shifts');
  return shifts
    .filter((shift) => shift.user_id === userId && shift.status === 'completed')
    .sort((a, b) => (b.start_datetime || '').localeCompare(a.start_datetime || ''));
}

async function listActiveShifts() {
  const shifts = await getRows('shifts');
  return shifts.filter((shift) => shift.status === 'active');
}

function calculateSummaryMinutes(shifts, timezone) {
  const now = dayjs().tz(timezone);
  let totalToday = 0;
  let totalWeek = 0;
  let totalMonth = 0;
  let totalAll = 0;

  for (const shift of shifts) {
    const minutes = Number(shift.duration_minutes || 0);
    if (!minutes || !shift.end_datetime) continue;

    const endAt = dayjs(shift.end_datetime).tz(timezone);
    totalAll += minutes;

    if (endAt.isSame(now, 'day')) {
      totalToday += minutes;
    }
    if (endAt.isSame(now, 'isoWeek')) {
      totalWeek += minutes;
    }
    if (endAt.isSame(now, 'month')) {
      totalMonth += minutes;
    }
  }

  return {
    total_today_minutes: String(totalToday),
    total_week_minutes: String(totalWeek),
    total_month_minutes: String(totalMonth),
    total_all_time_minutes: String(totalAll)
  };
}

async function recalcAndSaveSummary(user, timezone) {
  const shifts = await listShiftsByUser(user.id);
  const totals = calculateSummaryMinutes(shifts, timezone);
  const summaryRows = await getRows('summaries');

  const payload = {
    user_id: user.id,
    full_name: user.full_name,
    ...totals,
    updated_at: nowIso()
  };

  const existing = summaryRows.find((row) => row.user_id === user.id);

  if (existing) {
    await updateRow('summaries', existing.__rowNumber, payload);
  } else {
    await appendRow('summaries', payload);
  }

  return payload;
}

async function getUserHours(user, timezone) {
  const shifts = await listShiftsByUser(user.id);
  const totals = calculateSummaryMinutes(shifts, timezone);

  return {
    user_id: user.id,
    full_name: user.full_name,
    ...totals,
    updated_at: nowIso()
  };
}

async function getManagerDashboard(timezone) {
  const [users, schedules, allShifts] = await Promise.all([
    listActiveUsers(),
    listAllUpcomingSchedules(),
    getRows('shifts')
  ]);

  const userMap = new Map(users.map((user) => [user.id, user]));

  const activeShifts = allShifts.filter((shift) => shift.status === 'active');
  const completedByUser = new Map();

  for (const shift of allShifts) {
    if (shift.status !== 'completed') continue;
    if (!completedByUser.has(shift.user_id)) {
      completedByUser.set(shift.user_id, []);
    }
    completedByUser.get(shift.user_id).push(shift);
  }

  const summaries = users.map((user) => ({
    user_id: user.id,
    full_name: user.full_name,
    ...calculateSummaryMinutes(completedByUser.get(user.id) || [], timezone),
    updated_at: nowIso()
  }));

  const activeNow = activeShifts.map((shift) => ({
    ...shift,
    full_name: (userMap.get(shift.user_id) && userMap.get(shift.user_id).full_name) || 'Неизвестно'
  }));

  const schedulesWithUsers = schedules.map((item) => ({
    ...item,
    full_name: (userMap.get(item.user_id) && userMap.get(item.user_id).full_name) || 'Неизвестно'
  }));

  return {
    users,
    activeNow,
    summaries,
    schedules: schedulesWithUsers
  };
}

module.exports = {
  ensureSpreadsheetStructure,
  getOrCreateUserByTelegram,
  getUserById,
  listSchedulesByUser,
  addSchedule,
  cancelFutureSchedule,
  getActiveShiftByUser,
  startShift,
  endShift,
  listShiftsByUser,
  getUserHours,
  getManagerDashboard
};
