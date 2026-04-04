const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');
const isoWeek = require('dayjs/plugin/isoWeek');

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(isoWeek);

function nowIso() {
  return new Date().toISOString();
}

function toDateTimeLabel(isoString, tz) {
  if (!isoString) return '';
  return dayjs(isoString).tz(tz).format('DD.MM.YYYY HH:mm');
}

function diffMinutes(startIso, endIso) {
  const diff = dayjs(endIso).diff(dayjs(startIso), 'minute');
  return Math.max(diff, 0);
}

function getPeriodBoundaries(tz) {
  const now = dayjs().tz(tz);
  return {
    todayStart: now.startOf('day'),
    weekStart: now.startOf('isoWeek'),
    monthStart: now.startOf('month')
  };
}

function isFutureSchedule(date, startTime, tz) {
  const value = dayjs.tz(`${date} ${startTime}`, 'YYYY-MM-DD HH:mm', tz);
  return value.isAfter(dayjs().tz(tz));
}

module.exports = {
  dayjs,
  nowIso,
  toDateTimeLabel,
  diffMinutes,
  getPeriodBoundaries,
  isFutureSchedule
};
